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

fn dir(registry_dir: &Path) -> PathBuf {
    registry_dir.join("locusta_qp")
}

pub fn slot_path(registry_dir: &Path, owner: &str, peer: &str) -> PathBuf {
    dir(registry_dir).join(format!("{owner}__{peer}.qp"))
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
    let d = dir(registry_dir);
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
