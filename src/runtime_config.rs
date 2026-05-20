//! BenchFS runtime tuning configuration (replaces a tangle of `BENCHFS_*`
//! env vars with a single TOML file).
//!
//! Loaded once at process startup via [`RuntimeConfig::global`]. The
//! loader probes, in order:
//!
//! 1. The path in the `BENCHFS_CONFIG` env var, if set.
//! 2. `./benchfs.toml` relative to the current working directory.
//! 3. Built-in defaults (matches what the old env-var paths gave you
//!    when nothing was set).
//!
//! Example `benchfs.toml`:
//!
//! ```toml
//! [transport]
//! backend = "locusta"
//!
//! [locusta]
//! arena_size       = 268435456  # 256 MiB
//! max_inflight     = 128
//! recv_ring_size   = 32768
//! send_buf_size    = 32768
//! ring_capacity    = 128
//! accept_interval_ms = 100
//!
//! [prewarm]
//! enabled     = true
//! concurrency = 80
//!
//! [iouring]
//! queue_size            = 2048
//! submit_depth          = 512
//! sq_poll_ms            = 200
//! chunk_fd_cache_size   = 0
//!
//! [scheduling]
//! reactor_poll_interval = 2
//! status_cache_iters    = 100
//!
//! [rpc]
//! max_concurrent_chunk_rpcs = 16
//! sequential_chunk_rpcs     = false
//! disable_rdma              = false
//!
//! [stats]
//! enabled          = false
//! ucx_am_breakdown = false
//! ```

use serde::Deserialize;
use std::sync::OnceLock;

/// Top-level runtime tuning config. Every section is `#[serde(default)]`
/// so an empty file still deserializes — missing keys take the field's
/// `Default::default()` value.
///
/// Unknown sections at the top level are tolerated (we share a TOML file
/// with `ServerConfig` for `benchfs.toml`), but unknown keys *inside*
/// each declared section are rejected so typos surface early.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct RuntimeConfig {
    pub transport: TransportConfig,
    pub locusta: LocustaTuning,
    pub prewarm: PrewarmConfig,
    pub iouring: IoUringConfig,
    pub scheduling: SchedulingTuning,
    pub rpc: RpcTuning,
    pub stats: StatsConfig,
    pub metadata: MetadataConfig,
    pub cluster: ClusterConfig,
    pub observability: ObservabilityConfig,
    pub storage: StorageConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TransportConfig {
    /// "locusta" or "ucx". Empty/default means UCX (matches the
    /// historical absence of `BENCHFS_TRANSPORT`).
    pub backend: String,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            backend: String::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LocustaTuning {
    /// DMA arena size in bytes. Field is `u32` so the locusta API matches.
    pub arena_size: u32,
    /// Per-peer in-flight write credit.
    pub max_inflight: u16,
    /// RC RQ size per peer.
    pub recv_ring_size: u32,
    /// RC SQ buffer size per peer.
    pub send_buf_size: u32,
    /// SHM ring slots between client and relay.
    pub ring_capacity: u32,
    /// How often the server's UDP accept loop drains incoming REQUESTs.
    pub accept_interval_ms: u64,
    /// When true, the pluvio Reactor is the sole driver of
    /// `LocustaInner::tick()` (see transport_locusta.rs:reactor_mode_enabled).
    pub reactor_mode: bool,
    /// Server-side dispatch task idle sleep (microseconds) after
    /// `dispatch_idle_threshold` empty polls.
    pub dispatch_idle_sleep_us: u64,
    /// Number of empty `drain_and_spawn` polls before the dispatch task
    /// switches from `YieldOnce` to `sleep(dispatch_idle_sleep_us)`.
    pub dispatch_idle_threshold: u32,
    /// Override the UDP-handshake bind IP (defaults to interface auto-detect).
    pub bind_ip: Option<String>,
    /// Force a specific mlx5 device name (e.g. `"mlx5_2"`). When None and
    /// `mlx5_device_index` is also None, the device is picked by
    /// `mlx5_auto_spread` round-robin if enabled, else index 0.
    pub mlx5_device: Option<String>,
    /// Force a specific mlx5 device index. Mutually exclusive with
    /// `mlx5_device`.
    pub mlx5_device_index: Option<u32>,
    /// Round-robin mlx5 device across local MPI ranks
    /// (`OMPI_COMM_WORLD_LOCAL_RANK % n_devices`).
    pub mlx5_auto_spread: bool,
    /// Allow `LocustaTransport::send_get` to write the response payload
    /// directly into the registered IO buffer (zero-copy). Disable to
    /// fall back to the legacy memcpy path for diagnostics.
    pub skip_recv_copy: bool,
}

impl Default for LocustaTuning {
    fn default() -> Self {
        Self {
            // Match the pre-config-refactor defaults so behavior is
            // unchanged when no file is present.
            arena_size: 8 * 1024 * 1024,
            max_inflight: 256,
            recv_ring_size: 64 * 1024,
            send_buf_size: 64 * 1024,
            ring_capacity: 128,
            accept_interval_ms: 100,
            reactor_mode: false,
            dispatch_idle_sleep_us: 20,
            dispatch_idle_threshold: 16,
            bind_ip: None,
            mlx5_device: None,
            mlx5_device_index: None,
            // Default true so multi-rank-per-host runs spread HCAs
            // round-robin without explicit config (matches the
            // pre-config-refactor BENCHFS_MLX5_AUTO_SPREAD default).
            mlx5_auto_spread: true,
            // Off by default — skipping the recv memcpy produces invalid
            // data, which only makes sense for the network-only upper-
            // bound experiment.
            skip_recv_copy: false,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PrewarmConfig {
    /// Pre-warm endpoints during FFI init.
    pub enabled: bool,
    /// `buffer_unordered` concurrency for the per-rank pre-warm loop.
    /// Set to >= peer count (e.g. 80) to mimic `join_all` — small values
    /// (8 etc.) caused 392-second pre-warms at 10-phys because a single
    /// 120 s timeout serially blocked the next chunk.
    pub concurrency: usize,
    /// Per-rank delay before starting pre-warm, in ms × rank_id.
    /// Stagger spreads the metadata-pinned-server (node_0) fan-in over
    /// time so a 24-rank job doesn't slam 24 simultaneous UDP REQUESTs
    /// on a single server. Set to 0 to disable.
    pub stagger_ms_per_rank: u64,
}

impl Default for PrewarmConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            concurrency: 8,
            stagger_ms_per_rank: 0,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct IoUringConfig {
    pub queue_size: u32,
    pub submit_depth: u32,
    /// SQPOLL kernel-thread keep-alive in ms (0 disables SQPOLL).
    pub sq_poll_ms: u64,
    /// Per-server cache for open chunk-file descriptors (0 disables).
    pub chunk_fd_cache_size: usize,
    /// `io_uring_submit_with_timeout` deadline in microseconds.
    pub submit_timeout_us: u64,
    /// `io_uring_wait_complete_with_timeout` deadline in microseconds.
    pub complete_timeout_us: u64,
}

impl Default for IoUringConfig {
    fn default() -> Self {
        Self {
            queue_size: 256,
            submit_depth: 64,
            sq_poll_ms: 0,
            chunk_fd_cache_size: 1024,
            submit_timeout_us: 100,
            complete_timeout_us: 10,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SchedulingTuning {
    /// `SchedulingConfig::reactor_poll_interval` — every N executor
    /// iterations the reactor is polled.
    pub reactor_poll_interval: u32,
    /// `SchedulingConfig::status_cache_iterations` — for how many
    /// iterations `Reactor::status()` is cached.
    pub status_cache_iters: u64,
}

impl Default for SchedulingTuning {
    fn default() -> Self {
        Self {
            // Mirror pluvio_runtime::SchedulingConfig defaults so a
            // user with no TOML sees identical behavior.
            reactor_poll_interval: 8,
            status_cache_iters: 100,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RpcTuning {
    pub max_concurrent_chunk_rpcs: usize,
    /// Diagnostic: await each chunk-write task in sequence (no
    /// `buffer_unordered`). Used to isolate UCX hangs at t≥8m.
    pub sequential_chunk_rpcs: bool,
    /// Force eager protocol on UCX path (skip the RDMA grant flow).
    pub disable_rdma: bool,
    /// Force RDMA path regardless of `RDMA_THRESHOLD` (debug only).
    pub force_rdma: bool,
    /// RPC call wall-clock timeout in seconds (0 disables).
    pub timeout_secs: u64,
    /// Max retry attempts on transient RPC errors.
    pub max_retries: u32,
    /// Initial retry backoff in milliseconds.
    pub retry_delay_ms: u64,
    /// Multiplier applied to `retry_delay_ms` per attempt.
    pub retry_backoff: f32,
}

impl Default for RpcTuning {
    fn default() -> Self {
        Self {
            max_concurrent_chunk_rpcs: 64,
            sequential_chunk_rpcs: false,
            disable_rdma: false,
            force_rdma: false,
            timeout_secs: 600,
            max_retries: 0,
            retry_delay_ms: 100,
            retry_backoff: 2.0,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct StatsConfig {
    /// Enables `is_stats_enabled()` — sprinkles `Instant::now()` calls
    /// through the RPC hot paths and emits `*_TIMING` log lines.
    pub enabled: bool,
    /// In async-ucx, splits `am_send` timing into `call_us` + `await_us`
    /// and logs at target `ucx_am_breakdown`.
    pub ucx_am_breakdown: bool,
}

/// Metadata persistence policy. See `PLAN_METADATA_PERSISTENCE.md`.
///
/// - `"off"`: in-memory only (original behavior, protects io500 baseline).
/// - `"writethrough"`: every mutating metadata RPC pwrites a 4 KiB inode
///   file before replying.
/// - `"writeback"`: mutations mark the path dirty; a background task
///   flushes the queue every `flush_interval_ms`.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MetadataConfig {
    pub persist: String,
    /// Writeback only — drain cadence in ms.
    pub flush_interval_ms: u64,
    /// Writeback only — high watermark on the dirty set; the next
    /// mutation yields synchronously to drain when this is exceeded.
    pub dirty_high_watermark: usize,
    /// Distribute metadata ownership across all discovered server nodes
    /// via consistent hashing. Default true — mdtest scales across the
    /// cluster. Set to false to pin all metadata to node_0 (old
    /// workaround for an ppn=16 b=16g shared hang that has not
    /// re-appeared at ppn≤8 fpp).
    pub distributed: bool,
    /// CHFS-style central parent index — mirror (parent, child) entries
    /// to the parent's owner so readdir is a single RPC.
    pub central_parent_index: bool,
}

impl Default for MetadataConfig {
    fn default() -> Self {
        Self {
            persist: "off".to_string(),
            flush_interval_ms: 50,
            dirty_high_watermark: 16384,
            distributed: true,
            central_parent_index: false,
        }
    }
}

/// MPI/cluster topology hints — separate from tuning knobs so a TOML
/// reader can scan one section to know how the cluster is configured.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ClusterConfig {
    /// Expected number of MPI ranks (= benchfsd_mpi servers). Used by
    /// the FFI client init to size pre-warm fanout. 0 = "auto-detect
    /// from the registry directory".
    pub expected_nodes: usize,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self { expected_nodes: 0 }
    }
}

/// Per-RPC integrity log, dhat profiling, chrome tracing, retry stats
/// — anything that emits a side-channel artifact at runtime.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ObservabilityConfig {
    /// Enable chrome-flamegraph tracing output.
    pub chrome_tracing: bool,
    /// dhat heap profile output file path. If both `dhat_path` and
    /// `dhat_dir` are set, `dhat_path` wins.
    pub dhat_path: Option<String>,
    /// Directory under which dhat dumps a per-PID heap profile.
    pub dhat_dir: Option<String>,
    /// Per-RPC integrity-log emission (FNV-1a checksum + timestamps).
    pub integrity_log: bool,
    /// Directory to write per-PID integrity log files into.
    pub integrity_dir: String,
    /// RPC client retry stats output file path. Empty disables.
    pub retry_stats_output: Option<String>,
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            chrome_tracing: false,
            dhat_path: None,
            dhat_dir: None,
            integrity_log: false,
            integrity_dir: "/tmp".to_string(),
            retry_stats_output: None,
        }
    }
}

/// Local on-disk storage layout knobs.
// NOTE: no `deny_unknown_fields` here on purpose. The `[storage]` TOML
// table is shared with `ServerConfig::StorageConfig` (chunk_size,
// use_iouring, max_storage_gb) which lives in `src/config.rs`. Rejecting
// unknowns would propagate a parse error all the way up and force the
// entire `RuntimeConfig` to fall back to `Default::default()` — silently
// breaking `[transport] backend="locusta"` selection. Other sections
// keep `deny_unknown_fields` because they have private namespaces.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    /// Chunk file layout. `"per_chunk"` (default) is one file per chunk;
    /// `"unified"` packs chunks into per-(file, shard) bundles.
    pub chunk_layout: String,
    /// Shard count for the `"unified"` layout.
    pub unified_shards: u32,
    /// Use `mmap` for chunk writes (vs pwrite via io_uring).
    pub chunk_mmap_write: bool,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            chunk_layout: "per_chunk".to_string(),
            unified_shards: 1,
            chunk_mmap_write: false,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RuntimeConfigError {
    #[error("read {path}: {source}")]
    Read {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("parse {path}: {source}")]
    Parse {
        path: String,
        #[source]
        source: toml::de::Error,
    },
}

impl RuntimeConfig {
    /// Load from an explicit file path.
    pub fn from_file(path: &str) -> Result<Self, RuntimeConfigError> {
        let contents = std::fs::read_to_string(path).map_err(|e| RuntimeConfigError::Read {
            path: path.to_string(),
            source: e,
        })?;
        toml::from_str(&contents).map_err(|e| RuntimeConfigError::Parse {
            path: path.to_string(),
            source: e,
        })
    }

    /// Load using the discovery rules described in the module docs.
    /// Logs a warning and falls back to `Default` if parsing fails.
    pub fn load_or_default() -> Self {
        if let Ok(path) = std::env::var("BENCHFS_CONFIG") {
            match Self::from_file(&path) {
                Ok(cfg) => {
                    tracing::info!("Loaded benchfs runtime config from {path}");
                    return cfg;
                }
                Err(e) => {
                    tracing::warn!(
                        "BENCHFS_CONFIG={path} could not be loaded ({e}); using defaults"
                    );
                    return Self::default();
                }
            }
        }
        let local = "./benchfs.toml";
        if std::path::Path::new(local).exists() {
            match Self::from_file(local) {
                Ok(cfg) => {
                    tracing::info!("Loaded benchfs runtime config from {local}");
                    return cfg;
                }
                Err(e) => {
                    tracing::warn!("./benchfs.toml could not be loaded ({e}); using defaults");
                }
            }
        }
        Self::default()
    }

    /// Process-wide singleton. First call loads; subsequent calls return
    /// the cached value. Safe to call from any thread.
    pub fn global() -> &'static RuntimeConfig {
        static CFG: OnceLock<RuntimeConfig> = OnceLock::new();
        CFG.get_or_init(Self::load_or_default)
    }

    /// Test-only: install a hand-crafted config. Panics if `global()` has
    /// already been called.
    #[cfg(test)]
    pub fn install_for_test(cfg: RuntimeConfig) {
        // Get-or-init pattern: if already initialised, panic.
        let slot: &OnceLock<RuntimeConfig> = {
            // Re-borrow the same static the public accessor uses by
            // calling `global` once with our cfg as the producer.
            static CFG: OnceLock<RuntimeConfig> = OnceLock::new();
            &CFG
        };
        slot.set(cfg).expect("RuntimeConfig already initialised");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_file_takes_defaults() {
        let cfg: RuntimeConfig = toml::from_str("").unwrap();
        assert_eq!(cfg.locusta.max_inflight, 256);
        assert_eq!(cfg.scheduling.reactor_poll_interval, 8);
        assert_eq!(cfg.prewarm.concurrency, 8);
        assert_eq!(cfg.stats.enabled, false);
    }

    #[test]
    fn parses_full_profile() {
        let toml = r#"
            [transport]
            backend = "locusta"

            [locusta]
            arena_size = 268435456
            max_inflight = 128
            recv_ring_size = 32768
            send_buf_size = 32768
            ring_capacity = 128
            accept_interval_ms = 100

            [prewarm]
            enabled = true
            concurrency = 80

            [iouring]
            queue_size = 2048
            submit_depth = 512
            sq_poll_ms = 200
            chunk_fd_cache_size = 0

            [scheduling]
            reactor_poll_interval = 2
            status_cache_iters = 100

            [rpc]
            max_concurrent_chunk_rpcs = 16
            sequential_chunk_rpcs = false
            disable_rdma = false

            [stats]
            enabled = false
            ucx_am_breakdown = false
        "#;
        let cfg: RuntimeConfig = toml::from_str(toml).unwrap();
        assert_eq!(cfg.transport.backend, "locusta");
        assert_eq!(cfg.locusta.arena_size, 268_435_456);
        assert_eq!(cfg.prewarm.concurrency, 80);
        assert_eq!(cfg.iouring.queue_size, 2048);
        assert_eq!(cfg.scheduling.reactor_poll_interval, 2);
        assert_eq!(cfg.rpc.max_concurrent_chunk_rpcs, 16);
    }

    #[test]
    fn unknown_keys_inside_section_are_rejected() {
        let bad = r#"
            [locusta]
            nonsense_key = 1
        "#;
        let r: Result<RuntimeConfig, _> = toml::from_str(bad);
        assert!(
            r.is_err(),
            "unknown keys inside a declared section should fail deny_unknown_fields"
        );
    }

    #[test]
    fn unknown_top_level_section_is_tolerated() {
        // Shared with ServerConfig sections like [node]/[storage].
        let mixed = r#"
            [node]
            node_id = "node1"

            [locusta]
            max_inflight = 64
        "#;
        let cfg: RuntimeConfig = toml::from_str(mixed).unwrap();
        assert_eq!(cfg.locusta.max_inflight, 64);
    }
}
