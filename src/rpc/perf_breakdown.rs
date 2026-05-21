//! Per-stage timing accumulators for WriteChunkById eager path.
//!
//! Pluvio runs single-threaded, so we use plain `Cell<u64>` (no atomics).
//! Each stage stores cumulative microseconds and a count; an emit helper
//! prints the running average every N samples via `tracing::info!()`.
//!
//! Enabled when `BENCHFS_RPC_PROFILE=1` is set in the process env so the
//! hot path stays branch-free for normal benchmark runs.

use std::cell::Cell;
use std::sync::OnceLock;
use std::time::Instant;

/// Read once at first access; subsequent calls hit the cached value.
fn profile_enabled() -> bool {
    static FLAG: OnceLock<bool> = OnceLock::new();
    *FLAG.get_or_init(|| crate::runtime_config::RuntimeConfig::global().rpc.profile)
}

thread_local! {
    // Client-side WriteChunkById eager-path counters.
    static CLI_COUNT: Cell<u64> = const { Cell::new(0) };
    static CLI_ENCODE_US: Cell<u64> = const { Cell::new(0) };
    static CLI_SEND_US: Cell<u64> = const { Cell::new(0) };
    static CLI_WAIT_US: Cell<u64> = const { Cell::new(0) };
    static CLI_DECODE_US: Cell<u64> = const { Cell::new(0) };
    static CLI_TOTAL_US: Cell<u64> = const { Cell::new(0) };
    // Server-side WriteChunkById eager handler counters.
    static SRV_COUNT: Cell<u64> = const { Cell::new(0) };
    static SRV_PARSE_US: Cell<u64> = const { Cell::new(0) };
    static SRV_PATH_US: Cell<u64> = const { Cell::new(0) };
    static SRV_DISK_US: Cell<u64> = const { Cell::new(0) };
    static SRV_REPLY_US: Cell<u64> = const { Cell::new(0) };
    static SRV_TOTAL_US: Cell<u64> = const { Cell::new(0) };
}

const EMIT_EVERY: u64 = 5000;

/// Helper to time a synchronous block. Returns the elapsed µs and the
/// block's return value. Returns (0, value) when profiling is off so the
/// caller still gets the result without a needless `Instant::now()`.
#[inline]
pub fn timed_sync<R>(f: impl FnOnce() -> R) -> (u64, R) {
    if !profile_enabled() {
        return (0, f());
    }
    let t = Instant::now();
    let r = f();
    (t.elapsed().as_micros() as u64, r)
}

/// Record one client-side WriteChunkById eager-path sample. Inputs in µs.
pub fn cli_record(encode: u64, send: u64, wait: u64, decode: u64, total: u64) {
    if !profile_enabled() {
        return;
    }
    CLI_ENCODE_US.with(|c| c.set(c.get() + encode));
    CLI_SEND_US.with(|c| c.set(c.get() + send));
    CLI_WAIT_US.with(|c| c.set(c.get() + wait));
    CLI_DECODE_US.with(|c| c.set(c.get() + decode));
    CLI_TOTAL_US.with(|c| c.set(c.get() + total));
    let n = CLI_COUNT.with(|c| {
        let v = c.get() + 1;
        c.set(v);
        v
    });
    if n.is_multiple_of(EMIT_EVERY) {
        let enc = CLI_ENCODE_US.with(|c| c.get());
        let snd = CLI_SEND_US.with(|c| c.get());
        let wt = CLI_WAIT_US.with(|c| c.get());
        let dc = CLI_DECODE_US.with(|c| c.get());
        let tot = CLI_TOTAL_US.with(|c| c.get());
        tracing::info!(
            target: "rpc_profile",
            kind = "write_chunk_client_eager",
            n = n,
            avg_encode_us = enc / n,
            avg_send_us = snd / n,
            avg_wait_us = wt / n,
            avg_decode_us = dc / n,
            avg_total_us = tot / n,
            "WRITE_CLIENT_EAGER_BREAKDOWN"
        );
    }
}

/// Record one server-side WriteChunkById eager-handler sample.
pub fn srv_record(parse: u64, path: u64, disk: u64, reply: u64, total: u64) {
    if !profile_enabled() {
        return;
    }
    SRV_PARSE_US.with(|c| c.set(c.get() + parse));
    SRV_PATH_US.with(|c| c.set(c.get() + path));
    SRV_DISK_US.with(|c| c.set(c.get() + disk));
    SRV_REPLY_US.with(|c| c.set(c.get() + reply));
    SRV_TOTAL_US.with(|c| c.set(c.get() + total));
    let n = SRV_COUNT.with(|c| {
        let v = c.get() + 1;
        c.set(v);
        v
    });
    if n.is_multiple_of(EMIT_EVERY) {
        let p = SRV_PARSE_US.with(|c| c.get());
        let pth = SRV_PATH_US.with(|c| c.get());
        let d = SRV_DISK_US.with(|c| c.get());
        let r = SRV_REPLY_US.with(|c| c.get());
        let t = SRV_TOTAL_US.with(|c| c.get());
        tracing::info!(
            target: "rpc_profile",
            kind = "write_chunk_server_eager",
            n = n,
            avg_parse_us = p / n,
            avg_path_us = pth / n,
            avg_disk_us = d / n,
            avg_reply_us = r / n,
            avg_total_us = t / n,
            "WRITE_SERVER_EAGER_BREAKDOWN"
        );
    }
}

/// True when profiling is on. Skip Instant::now() in hot paths if false.
#[inline]
pub fn is_enabled() -> bool {
    profile_enabled()
}
