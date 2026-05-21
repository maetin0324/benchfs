//! File-based handshake (Design C: registry rendezvous).
//!
//! Replaces UDP `REQUEST`/`RESPONSE` with atomic file write + polling read.
//! Each ordered pair `(owner, peer)` has a slot
//!
//!   `{registry_dir}/locusta_qp/{owner}__{peer}.qp`
//!
//! containing 128 bytes of payload (`relay_qp` 64 B + `server_qp` 64 B).
//! Handshake is symmetric: `self` writes `self__peer.qp` and waits for
//! `peer__self.qp`. There is no UDP traffic, so the kernel-side packet
//! drops that limited `add_peer_blocking` at 400+ ranks do not happen.

use rrrpc::wire::QpExchangeInfo;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

pub const QP_PAYLOAD_SIZE: usize = 128;

/// Number of subdirectory shards under `locusta_qp/`. 256 keeps each
/// shard's file count to ~125 at 400-client × 40-server scale.
pub const NUM_SHARDS: u8 = 0xFF;

/// Return the full set of shard subdirectories under `locusta_qp/`,
/// usable by `discover_registry_peers` to walk all peer publications.
pub fn all_shard_dirs(registry_dir: &Path) -> Vec<PathBuf> {
    let base = registry_dir.join("locusta_qp");
    (0u16..=NUM_SHARDS as u16)
        .map(|i| base.join(format!("{:02x}", i)))
        .collect()
}

/// Hash the owner name into a 2-hex-digit subdirectory bucket. At
/// 400-client × 40-server scale a single Lustre directory holds 32 k+
/// .qp + .ack files and rename/lookup latency climbs into seconds,
/// stalling the discover state machine. Sharding into 256 subdirs
/// drops the per-dir count to ~125 and brings Lustre back to ms-class
/// latency. The hash uses owner only (not the pair) so reading the
/// peer's slot for a given pair still hits the *peer's* shard.
pub fn shard_dir(registry_dir: &Path, owner: &str) -> PathBuf {
    let mut h: u32 = 5381;
    for b in owner.as_bytes() {
        h = h.wrapping_mul(33).wrapping_add(*b as u32);
    }
    let bucket = (h & 0xFF) as u8;
    registry_dir
        .join("locusta_qp")
        .join(format!("{:02x}", bucket))
}

pub fn slot_path(registry_dir: &Path, owner: &str, peer: &str) -> PathBuf {
    shard_dir(registry_dir, owner).join(format!("{owner}__{peer}.qp"))
}

fn encode(relay_qp: &QpExchangeInfo, server_qp: &QpExchangeInfo) -> [u8; QP_PAYLOAD_SIZE] {
    let mut buf = [0u8; QP_PAYLOAD_SIZE];
    let relay_bytes: &[u8; 64] =
        unsafe { &*(relay_qp as *const QpExchangeInfo as *const [u8; 64]) };
    buf[..64].copy_from_slice(relay_bytes);
    let server_bytes: &[u8; 64] =
        unsafe { &*(server_qp as *const QpExchangeInfo as *const [u8; 64]) };
    buf[64..].copy_from_slice(server_bytes);
    buf
}

pub fn decode_qp_payload(buf: &[u8]) -> io::Result<(QpExchangeInfo, QpExchangeInfo)> {
    decode(buf)
}

fn decode(buf: &[u8]) -> io::Result<(QpExchangeInfo, QpExchangeInfo)> {
    if buf.len() < QP_PAYLOAD_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("qp payload too short: {} < {}", buf.len(), QP_PAYLOAD_SIZE),
        ));
    }
    let relay_qp: QpExchangeInfo = unsafe {
        let mut q: QpExchangeInfo = std::mem::zeroed();
        std::ptr::copy_nonoverlapping(
            buf[..64].as_ptr(),
            &mut q as *mut QpExchangeInfo as *mut u8,
            64,
        );
        q
    };
    let server_qp: QpExchangeInfo = unsafe {
        let mut q: QpExchangeInfo = std::mem::zeroed();
        std::ptr::copy_nonoverlapping(
            buf[64..128].as_ptr(),
            &mut q as *mut QpExchangeInfo as *mut u8,
            64,
        );
        q
    };
    Ok((relay_qp, server_qp))
}

/// Publish `owner`'s QP info for talking to `peer`. Atomic via
/// tempfile + rename. The subdirectory is created on first call.
pub fn publish_local_qp(
    registry_dir: &Path,
    owner: &str,
    peer: &str,
    relay_qp: &QpExchangeInfo,
    server_qp: &QpExchangeInfo,
) -> io::Result<()> {
    let d = shard_dir(registry_dir, owner);
    std::fs::create_dir_all(&d)?;
    let final_path = d.join(format!("{owner}__{peer}.qp"));
    let tmp_path = d.join(format!(".{owner}__{peer}.qp.tmp.{}", std::process::id()));
    let payload = encode(relay_qp, server_qp);
    std::fs::write(&tmp_path, &payload)?;
    std::fs::rename(&tmp_path, &final_path)?;
    Ok(())
}

/// Block until `owner__peer.qp` exists with `>= QP_PAYLOAD_SIZE` bytes,
/// or `deadline` elapses. Polling uses 50 ms initial backoff, doubled up
/// to 2 s cap.
pub fn read_peer_qp(
    registry_dir: &Path,
    owner: &str,
    peer: &str,
    deadline: Instant,
) -> io::Result<(QpExchangeInfo, QpExchangeInfo)> {
    let path = slot_path(registry_dir, owner, peer);
    let mut backoff = Duration::from_millis(50);
    let max_backoff = Duration::from_secs(2);
    loop {
        match std::fs::read(&path) {
            Ok(buf) if buf.len() >= QP_PAYLOAD_SIZE => return decode(&buf),
            Ok(_) => { /* partial write during rename race — retry */ }
            Err(e) if e.kind() == io::ErrorKind::NotFound => { /* not yet */ }
            Err(e) => return Err(e),
        }
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "registry qp slot {} not visible before deadline",
                    path.display()
                ),
            ));
        }
        std::thread::sleep(backoff);
        backoff = std::cmp::min(backoff * 2, max_backoff);
    }
}

/// Publish a 0-byte ACK at `{registry_dir}/locusta_qp/{owner}__{peer}.ack`
/// to signal that `owner` has completed `connect_peer`/`connect_destination`
/// against `peer`'s QPs. Without this barrier the peer may post an RDMA
/// send before our QP transitions to RTS, producing `IBV_WC_RNR_RETRY_EXC`
/// errors (visible upstream as `peer absent` retries).
pub fn publish_ack(registry_dir: &Path, owner: &str, peer: &str) -> io::Result<()> {
    let d = shard_dir(registry_dir, owner);
    std::fs::create_dir_all(&d)?;
    let final_path = d.join(format!("{owner}__{peer}.ack"));
    let tmp_path = d.join(format!(
        ".{owner}__{peer}.ack.tmp.{}",
        std::process::id()
    ));
    std::fs::write(&tmp_path, b"ok")?;
    std::fs::rename(&tmp_path, &final_path)?;
    Ok(())
}

/// Block until peer publishes its ACK file (`peer__self.ack`), confirming
/// the peer has finished connecting to our QPs and is ready to receive
/// RDMA traffic.
pub fn wait_peer_ack(
    registry_dir: &Path,
    owner: &str,
    peer: &str,
    deadline: Instant,
) -> io::Result<()> {
    let path = shard_dir(registry_dir, owner).join(format!("{owner}__{peer}.ack"));
    let mut backoff = Duration::from_millis(20);
    let max_backoff = Duration::from_millis(500);
    loop {
        match std::fs::metadata(&path) {
            Ok(_) => return Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "registry ack {} not visible before deadline",
                    path.display()
                ),
            ));
        }
        std::thread::sleep(backoff);
        backoff = std::cmp::min(backoff * 2, max_backoff);
    }
}
