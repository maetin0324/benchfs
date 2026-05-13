//! UDP-based QP info exchange for locusta peer setup.
//!
//! Replaces the Lustre file-polling protocol that doesn't scale
//! (job 18060: 640 clients × 40 servers → 4213 prewarm timeouts at
//! 120 s each because read_qp_info_when_ready spun on `stat(2)` over
//! many small files). UDP is one round-trip per peer instead.
//!
//! Each rank binds an ephemeral UDP port at startup and publishes its
//! `ip:port` into `{registry_dir}/locusta_udp/{node_id}` once (the
//! sentinel file the existing `wait_and_connect` already watches for).
//! Peers read that file once to learn the address — no polling, no
//! continuous scanning of a directory full of qpinfo files.
//!
//! ## Protocol
//!
//! Single fixed-size packet in each direction:
//!
//! ```text
//! [0..4]    magic = 0xBE16FA01 (big-endian)
//! [4]       version = 1
//! [5]       msg_type: 0 = REQUEST, 1 = RESPONSE
//! [6..8]    flags  (reserved, must be 0)
//! [8..72]   sender's relay-side QpExchangeInfo (64 B, repr(C))
//! [72..136] sender's server-side QpExchangeInfo (64 B)
//! [136]     node_id length (0..64)
//! [137..201] node_id ASCII bytes (zero-padded)
//! ```
//!
//! Total = 201 B (well under any reasonable MTU).
//!
//! ## Handshake ordering (race-free w.r.t. first RDMA send)
//!
//! Client:
//!   1. `prepare_peer` + `prepare_destination` (own QPs in INIT)
//!   2. Send REQUEST over UDP
//!   3. Receive RESPONSE over UDP — by the time this returns, server has
//!      already transitioned its own QPs to RTR/RTS (step 3 of server
//!      flow below), so any subsequent client→server RDMA is safe.
//!   4. `connect_peer` + `connect_destination` (own QPs to RTR/RTS)
//!
//! Server (incoming UDP recv):
//!   1. Parse REQUEST, look up local state for this peer
//!   2. `prepare_peer` + `prepare_destination` (own QPs in INIT)
//!   3. `connect_peer` + `connect_destination` (own QPs to RTR/RTS)
//!   4. Send RESPONSE over UDP
//!
//! Doing server step 3 before step 4 closes the RNR race that bit jobs
//! 18113 / 18114 under the lazy-only `BENCHFS_PREWARM_CONNECTIONS=0`
//! path (CQE syndrome=21 transport-retry-exhausted at first
//! `benchfs_write`).

use std::io;
use std::net::{Ipv4Addr, SocketAddrV4, UdpSocket};
use std::path::{Path, PathBuf};
use std::time::Duration;

use rrrpc::wire::QpExchangeInfo;

pub const MAGIC: u32 = 0xBE16_FA01;
pub const VERSION: u8 = 1;
pub const MSG_REQUEST: u8 = 0;
pub const MSG_RESPONSE: u8 = 1;
pub const NODE_ID_MAX: usize = 64;
pub const PACKET_SIZE: usize = 8 + 64 + 64 + 1 + NODE_ID_MAX;

/// Decoded UDP exchange packet.
#[derive(Debug, Clone)]
pub struct ExchangePacket {
    pub msg_type: u8,
    pub relay_qp: QpExchangeInfo,
    pub server_qp: QpExchangeInfo,
    pub node_id: String,
}

impl ExchangePacket {
    /// Serialise into the fixed-size wire layout described in the module
    /// docs. Returns the filled byte buffer.
    pub fn encode(&self) -> [u8; PACKET_SIZE] {
        let mut buf = [0u8; PACKET_SIZE];
        buf[0..4].copy_from_slice(&MAGIC.to_be_bytes());
        buf[4] = VERSION;
        buf[5] = self.msg_type;
        // bytes 6,7 reserved (zero)
        // SAFETY: QpExchangeInfo is repr(C), 64 bytes, with only POD
        // fields (u32/u16/u64/zeros). We copy it byte-for-byte to a
        // word-aligned offset that matches the assertion in lib code.
        let relay_bytes: &[u8; 64] = unsafe {
            &*(&self.relay_qp as *const QpExchangeInfo as *const [u8; 64])
        };
        buf[8..72].copy_from_slice(relay_bytes);
        let server_bytes: &[u8; 64] = unsafe {
            &*(&self.server_qp as *const QpExchangeInfo as *const [u8; 64])
        };
        buf[72..136].copy_from_slice(server_bytes);

        let id_bytes = self.node_id.as_bytes();
        let id_len = id_bytes.len().min(NODE_ID_MAX);
        buf[136] = id_len as u8;
        buf[137..137 + id_len].copy_from_slice(&id_bytes[..id_len]);
        buf
    }

    /// Decode a wire packet. Returns `Err` on bad magic, version, or
    /// truncated input.
    pub fn decode(buf: &[u8]) -> io::Result<Self> {
        if buf.len() < PACKET_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("packet too short: {} < {}", buf.len(), PACKET_SIZE),
            ));
        }
        let magic = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        if magic != MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("bad magic: 0x{:08x}", magic),
            ));
        }
        let version = buf[4];
        if version != VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported version {version}"),
            ));
        }
        let msg_type = buf[5];
        if msg_type != MSG_REQUEST && msg_type != MSG_RESPONSE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("bad msg_type {msg_type}"),
            ));
        }

        let relay_qp: QpExchangeInfo = unsafe {
            let mut q: QpExchangeInfo = std::mem::zeroed();
            std::ptr::copy_nonoverlapping(
                buf[8..72].as_ptr(),
                &mut q as *mut QpExchangeInfo as *mut u8,
                64,
            );
            q
        };
        let server_qp: QpExchangeInfo = unsafe {
            let mut q: QpExchangeInfo = std::mem::zeroed();
            std::ptr::copy_nonoverlapping(
                buf[72..136].as_ptr(),
                &mut q as *mut QpExchangeInfo as *mut u8,
                64,
            );
            q
        };

        let id_len = buf[136] as usize;
        if id_len > NODE_ID_MAX {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("node_id len {id_len} > {NODE_ID_MAX}"),
            ));
        }
        let node_id = std::str::from_utf8(&buf[137..137 + id_len])
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("node_id utf8: {e}")))?
            .to_owned();

        Ok(ExchangePacket {
            msg_type,
            relay_qp,
            server_qp,
            node_id,
        })
    }
}

/// Filesystem path holding a node's UDP `ip:port` sentinel.
pub fn udp_registry_path(registry_dir: &Path, node_id: &str) -> PathBuf {
    registry_dir.join("locusta_udp").join(node_id)
}

/// Write `addr` into `{registry_dir}/locusta_udp/{node_id}` (creating
/// the subdirectory if needed). Atomic: writes to a `.tmp` file first
/// and renames over the target.
pub fn publish_udp_addr(
    registry_dir: &Path,
    node_id: &str,
    addr: SocketAddrV4,
) -> io::Result<()> {
    let dir = registry_dir.join("locusta_udp");
    std::fs::create_dir_all(&dir)?;
    let final_path = dir.join(node_id);
    let tmp_path = dir.join(format!("{node_id}.tmp"));
    let body = format!("{}:{}\n", addr.ip(), addr.port());
    std::fs::write(&tmp_path, body.as_bytes())?;
    std::fs::rename(&tmp_path, &final_path)?;
    Ok(())
}

/// Read peer's UDP address from the registry, blocking with backoff
/// until the file appears or `deadline` is exceeded.
pub fn read_peer_udp_addr(
    registry_dir: &Path,
    peer_node_id: &str,
    deadline: std::time::Instant,
) -> io::Result<SocketAddrV4> {
    let path = udp_registry_path(registry_dir, peer_node_id);
    loop {
        match std::fs::read_to_string(&path) {
            Ok(s) => {
                let trimmed = s.trim();
                return parse_addr(trimmed).map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("{path}: {e}", path = path.display()),
                    )
                });
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                if std::time::Instant::now() >= deadline {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!("waiting for {}", path.display()),
                    ));
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(e) => return Err(e),
        }
    }
}

fn parse_addr(s: &str) -> io::Result<SocketAddrV4> {
    let (host, port) = s.rsplit_once(':').ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidData, format!("missing colon in {s}"))
    })?;
    let ip: Ipv4Addr = host
        .parse()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("bad ip {host}: {e}")))?;
    let port: u16 = port
        .parse()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("bad port {port}: {e}")))?;
    Ok(SocketAddrV4::new(ip, port))
}

/// Bind a UDP socket on the host's primary outward interface (using
/// `0.0.0.0:0` for ephemeral port), but advertise the IP that other
/// hosts on the cluster can reach us at. The advertised IP comes from
/// `hostname -i` or an explicit `BENCHFS_LOCUSTA_BIND_IP` env var.
pub fn bind_udp_socket() -> io::Result<(UdpSocket, SocketAddrV4)> {
    let socket = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0))?;
    let local_port = match socket.local_addr()? {
        std::net::SocketAddr::V4(v4) => v4.port(),
        _ => unreachable!("we bound v4"),
    };
    let advertised_ip = resolve_advertised_ip()?;
    let advertised = SocketAddrV4::new(advertised_ip, local_port);
    Ok((socket, advertised))
}

fn resolve_advertised_ip() -> io::Result<Ipv4Addr> {
    if let Ok(s) = std::env::var("BENCHFS_LOCUSTA_BIND_IP")
        && let Ok(ip) = s.parse::<Ipv4Addr>()
    {
        return Ok(ip);
    }
    // Sirius compute nodes can't reach the public internet, so the
    // old `connect 8.8.8.8` route-probe trick fails with ENETUNREACH.
    // Walk getifaddrs(3) instead and pick the first non-loopback IPv4.
    // Prefer interfaces named `ib*` (Sirius's RDMA-capable IPoIB
    // interfaces) so the control plane lives on the same fabric as
    // the data plane.
    let mut best: Option<(String, Ipv4Addr)> = None;
    for (name, addr) in list_ipv4_interfaces()? {
        if addr.is_loopback() {
            continue;
        }
        let prefer_this = match &best {
            None => true,
            Some((cur_name, _)) => name.starts_with("ib") && !cur_name.starts_with("ib"),
        };
        if prefer_this {
            best = Some((name, addr));
        }
    }
    best.map(|(_, ip)| ip)
        .ok_or_else(|| io::Error::other("no non-loopback IPv4 interface found"))
}

/// Return `(interface_name, ipv4)` for every IPv4 interface returned
/// by `getifaddrs(3)`. Includes the loopback; caller filters.
fn list_ipv4_interfaces() -> io::Result<Vec<(String, Ipv4Addr)>> {
    let mut head: *mut libc::ifaddrs = std::ptr::null_mut();
    let rc = unsafe { libc::getifaddrs(&mut head) };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    struct Guard(*mut libc::ifaddrs);
    impl Drop for Guard {
        fn drop(&mut self) {
            if !self.0.is_null() {
                unsafe { libc::freeifaddrs(self.0) };
            }
        }
    }
    let _guard = Guard(head);

    let mut out = Vec::new();
    let mut cur = head;
    while !cur.is_null() {
        // SAFETY: getifaddrs gives us a linked list; we deref the head
        // pointer to walk it. Each `ifa_name` is a stable C string and
        // each `ifa_addr` points to a sockaddr the OS keeps alive
        // until we call freeifaddrs.
        unsafe {
            let entry = &*cur;
            cur = entry.ifa_next;
            if entry.ifa_addr.is_null() {
                continue;
            }
            let family = (*entry.ifa_addr).sa_family as i32;
            if family != libc::AF_INET {
                continue;
            }
            let sin = &*(entry.ifa_addr as *const libc::sockaddr_in);
            let raw_be = sin.sin_addr.s_addr;
            let ip = Ipv4Addr::from(u32::from_be(raw_be));
            let name_cstr = std::ffi::CStr::from_ptr(entry.ifa_name);
            let name = name_cstr.to_string_lossy().into_owned();
            out.push((name, ip));
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_encode_decode() {
        let qp = QpExchangeInfo {
            qp_number: 0x1234_5678,
            psn: 0xABCD_EF01,
            lid: 0x4242,
            _pad: 0,
            recv_ring: unsafe { std::mem::zeroed() },
            payload_addr: 0xDEAD_BEEF_F00D_CAFE,
            payload_rkey: 0xCAFEBABE,
            payload_max_size: 0x1000,
        };
        let pkt = ExchangePacket {
            msg_type: MSG_REQUEST,
            relay_qp: qp,
            server_qp: qp,
            node_id: "node_42".to_string(),
        };
        let bytes = pkt.encode();
        let parsed = ExchangePacket::decode(&bytes).expect("decode");
        assert_eq!(parsed.msg_type, MSG_REQUEST);
        assert_eq!(parsed.node_id, "node_42");
        assert_eq!(parsed.relay_qp.qp_number, 0x1234_5678);
        assert_eq!(parsed.server_qp.payload_addr, 0xDEAD_BEEF_F00D_CAFE);
    }

    #[test]
    fn bad_magic_rejected() {
        let mut buf = [0u8; PACKET_SIZE];
        buf[0..4].copy_from_slice(&0u32.to_be_bytes());
        let err = ExchangePacket::decode(&buf).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}
