//! BenchFS Server Daemon with MPI Support
//!
//! This is the MPI-enabled server binary for BenchFS distributed file system.
//! It uses MPI to distribute servers across multiple nodes in an HPC cluster.
//!
//! Usage:
//!   mpirun -n <num_nodes> benchfsd_mpi <registry_dir> [config_file] [--trace-output <path>]
//!
//! Each MPI rank runs a BenchFS server instance:
//! - Rank 0: Primary metadata server
//! - Other ranks: Storage servers and secondary metadata servers
//!
//! Options:
//!   --trace-output <path>  Enable Perfetto tracing and save to specified path.
//!                          Each rank will create a separate trace file with suffix _rank<N>.json

use benchfs::cache::CachePolicy;
use benchfs::config::ServerConfig;
use benchfs::logging::{TraceGuard, init_with_chrome};
use benchfs::metadata::MetadataManager;
use benchfs::rpc::connection::ConnectionPool;
use benchfs::rpc::handlers::RpcHandlerContext;
use benchfs::rpc::server::{RpcServer, get_server_rpc_stats};
use benchfs::storage::{
    ChunkStore, DummyChunkStore, FileChunkStore, IOUringBackend, IOUringChunkStore, PosixChunkStore,
};

use pluvio_runtime::executor::{Runtime, SchedulingConfig};
use pluvio_timer::TimerReactor;
use pluvio_ucx::{Context as UcxContext, reactor::UCXReactor};
use pluvio_uring::reactor::IoUringReactor;

use mpi::traits::*;

use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// One-shot yield future: returns Pending once (registering its waker as
/// already-ready so the executor re-polls it on the next iteration), then
/// resolves. Lets a hot poll loop give other tasks a slot without going
/// through a timer.
#[cfg(feature = "transport-locusta")]
#[derive(Default)]
struct YieldOnce {
    yielded: bool,
}

#[cfg(feature = "transport-locusta")]
impl std::future::Future for YieldOnce {
    type Output = ();
    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        if self.yielded {
            std::task::Poll::Ready(())
        } else {
            self.yielded = true;
            cx.waker().wake_by_ref();
            std::task::Poll::Pending
        }
    }
}

#[cfg(feature = "transport-locusta")]
struct LocustaRuntimeState {
    transport: Rc<benchfs::rpc::transport_locusta::LocustaTransport>,
    dispatch: Rc<benchfs::rpc::locusta_server::LocustaServerDispatch>,
}

#[cfg(feature = "transport-locusta")]
fn init_locusta_runtime(
    node_id: &str,
    mpi_rank: i32,
    mpi_size: i32,
    registry_dir: &str,
    handler_context: Rc<RpcHandlerContext>,
) -> Result<LocustaRuntimeState, Box<dyn std::error::Error>> {
    use benchfs::rpc::data_ops::{ReadChunkByIdRequest, WriteChunkByIdRequest};
    use benchfs::rpc::locusta_server::LocustaServerDispatch;
    use benchfs::rpc::metadata_ops::{
        MetadataCreateDirRequest, MetadataCreateFileRequest, MetadataDeleteRequest,
        MetadataLookupRequest, MetadataUpdateRequest,
    };
    use benchfs::rpc::transport_locusta::{LocustaConfig, LocustaTransport};

    // Peer list: only other servers (`node_<rank>`). io500 clients
    // (`ior_client_*`) are NOT prewarmed here — that would deadlock the
    // job script's `check_server_ready` barrier (server init blocks
    // waiting for clients that are themselves blocked waiting for
    // `node_*.addr` to appear). Instead, clients call `add_peer(server)`
    // on connect, and the server's `discover_registry_peers` (one peer
    // per tick) handshakes back asynchronously.
    let peer_node_ids: Vec<String> = (0..mpi_size)
        .filter(|&r| r != mpi_rank)
        .map(|r| format!("node_{}", r))
        .collect();

    // Tuning from [locusta] section in benchfs.toml; see runtime_config.rs.
    let rc = benchfs::runtime_config::RuntimeConfig::global();
    let cfg = LocustaConfig {
        registry_dir: PathBuf::from(registry_dir),
        local_node_id: node_id.to_string(),
        peer_node_ids,
        external_server_allocator: Some(Rc::clone(&handler_context.allocator)),
        arena_size: rc.locusta.arena_size,
        ring_capacity: rc.locusta.ring_capacity,
        ..LocustaConfig::default()
    };
    std::fs::create_dir_all(&cfg.registry_dir)?;
    tracing::info!(
        "Initializing LocustaTransport (peers={}, registry={})",
        cfg.peer_node_ids.len(),
        cfg.registry_dir.display()
    );
    let transport = Rc::new(LocustaTransport::init(&cfg)?);
    tracing::info!(
        "LocustaTransport connected to {} peers",
        cfg.peer_node_ids.len()
    );

    // Registry mode: spawn the server-side discover task that accepts
    // dynamic client handshakes (`ior_client_*`) via the async
    // state machine. The task yields the reactor between Lustre
    // scans/polls so the locusta tick keeps polling RDMA CQs.
    if rc.locusta.handshake_mode == "registry" {
        let t = Rc::clone(&transport);
        pluvio_runtime::spawn_with_name(
            async move {
                t.server_discover_task().await;
                Ok::<(), std::io::Error>(())
            },
            "locusta_server_discover".to_string(),
        );
        tracing::info!("Spawned locusta server discover task (registry handshake)");
    }

    // Register every supported RPC's `LocustaServerHandler`.
    let mut dispatch = LocustaServerDispatch::new(handler_context);
    dispatch.register::<MetadataLookupRequest>();
    dispatch.register::<MetadataCreateFileRequest>();
    dispatch.register::<MetadataCreateDirRequest>();
    dispatch.register::<MetadataDeleteRequest>();
    dispatch.register::<MetadataUpdateRequest>();
    dispatch.register::<WriteChunkByIdRequest<'_>>();
    dispatch.register::<ReadChunkByIdRequest<'_>>();
    dispatch.register::<benchfs::rpc::readdir_ops::ReaddirRequest>();
    dispatch.register::<benchfs::rpc::dir_index_ops::DirIndexUpdateRequest>();

    Ok(LocustaRuntimeState {
        transport,
        dispatch: Rc::new(dispatch),
    })
}

/// Server state
struct ServerState {
    config: ServerConfig,
    running: Arc<AtomicBool>,
    mpi_rank: i32,
    mpi_size: i32,
    registry_dir: PathBuf,
    stats_file: Option<PathBuf>,
}

impl ServerState {
    fn new(
        config: ServerConfig,
        mpi_rank: i32,
        mpi_size: i32,
        registry_dir: PathBuf,
        stats_file: Option<PathBuf>,
    ) -> Self {
        Self {
            config,
            running: Arc::new(AtomicBool::new(true)),
            mpi_rank,
            mpi_size,
            registry_dir,
            stats_file,
        }
    }

    fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    fn node_id(&self) -> String {
        format!("node_{}", self.mpi_rank)
    }

    fn is_primary(&self) -> bool {
        self.mpi_rank == 0
    }
}

fn dump_kernel_state() {
    // RLIMIT_MEMLOCK — relevant to io_uring registered buffers (page pin).
    unsafe {
        let mut lim: libc::rlimit = std::mem::zeroed();
        if libc::getrlimit(libc::RLIMIT_MEMLOCK, &mut lim) == 0 {
            tracing::info!(
                target: "kernel_state",
                rlim_cur = lim.rlim_cur,
                rlim_max = lim.rlim_max,
                "RLIMIT_MEMLOCK"
            );
        }
    }

    // /proc/meminfo subset — page allocation pressure indicators.
    if let Ok(s) = std::fs::read_to_string("/proc/meminfo") {
        for line in s.lines().filter(|l| {
            l.starts_with("MemTotal:")
                || l.starts_with("MemAvailable:")
                || l.starts_with("MemFree:")
                || l.starts_with("Mlocked:")
                || l.starts_with("Unevictable:")
                || l.starts_with("Slab:")
                || l.starts_with("SReclaimable:")
                || l.starts_with("SUnreclaim:")
                || l.starts_with("Cached:")
        }) {
            tracing::info!(target: "kernel_state", "meminfo: {}", line.trim());
        }
    }

    // cgroup v2 memory limits + current usage, if we're in one.
    for key in [
        "/sys/fs/cgroup/memory.current",
        "/sys/fs/cgroup/memory.max",
        "/sys/fs/cgroup/memory.high",
        "/sys/fs/cgroup/memory.events",
    ] {
        if let Ok(s) = std::fs::read_to_string(key) {
            tracing::info!(target: "kernel_state", "{}: {}", key, s.trim().replace('\n', " "));
        }
    }

    // /proc/self/limits — full per-process resource limits dump.
    if let Ok(s) = std::fs::read_to_string("/proc/self/limits") {
        for line in s.lines().filter(|l| {
            l.contains("locked memory")
                || l.contains("open files")
                || l.contains("address space")
                || l.contains("pending signals")
        }) {
            tracing::info!(target: "kernel_state", "limits: {}", line.trim());
        }
    }
}

fn main() {
    // Initialize MPI
    let universe = mpi::initialize().unwrap();
    let world = universe.world();
    let mpi_rank = world.rank();
    let mpi_size = world.size();

    // Parse command line arguments
    let args: Vec<String> = std::env::args().collect();

    // Parse --trace-output, --stats-output, and --enable-stats options
    let mut trace_output: Option<PathBuf> = None;
    let mut stats_output: Option<PathBuf> = None;
    let mut enable_stats: bool = false;
    let mut positional_args: Vec<&String> = Vec::new();

    // Set BENCHFS_RPC_TIMEOUT to 3 second for MPI environment
    unsafe {
        std::env::set_var("BENCHFS_RPC_TIMEOUT", "3");
    }

    let mut i = 1;
    while i < args.len() {
        if args[i] == "--trace-output" {
            if i + 1 < args.len() {
                // Create rank-specific trace file path
                let base_path = PathBuf::from(&args[i + 1]);
                let stem = base_path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("trace");
                let ext = base_path
                    .extension()
                    .and_then(|s| s.to_str())
                    .unwrap_or("json");
                let parent = base_path.parent().unwrap_or(std::path::Path::new("."));
                let rank_path = parent.join(format!("{}_rank{}.{}", stem, mpi_rank, ext));
                trace_output = Some(rank_path);
                i += 2;
            } else {
                if mpi_rank == 0 {
                    eprintln!("Error: --trace-output requires a path argument");
                }
                std::process::exit(1);
            }
        } else if args[i] == "--stats-output" {
            if i + 1 < args.len() {
                // Create rank-specific stats file path
                let base_path = PathBuf::from(&args[i + 1]);
                let stem = base_path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("stats");
                let ext = base_path
                    .extension()
                    .and_then(|s| s.to_str())
                    .unwrap_or("csv");
                let parent = base_path.parent().unwrap_or(std::path::Path::new("."));
                let rank_path = parent.join(format!("{}_rank{}.{}", stem, mpi_rank, ext));
                stats_output = Some(rank_path);
                i += 2;
            } else {
                if mpi_rank == 0 {
                    eprintln!("Error: --stats-output requires a path argument");
                }
                std::process::exit(1);
            }
        } else if args[i] == "--enable-stats" {
            enable_stats = true;
            i += 1;
        } else {
            positional_args.push(&args[i]);
            i += 1;
        }
    }

    if positional_args.is_empty() {
        if mpi_rank == 0 {
            eprintln!(
                "Usage: mpirun -n <num_nodes> {} <registry_dir> [config_file] [options]",
                args[0]
            );
            eprintln!(
                "  registry_dir:            Shared directory for service discovery (required)"
            );
            eprintln!(
                "  config_file:             Configuration file (optional, default: benchfs.toml)"
            );
            eprintln!("  --trace-output <path>:   Enable Perfetto tracing (optional)");
            eprintln!("                           Each rank creates <path>_rank<N>.json");
            eprintln!("  --stats-output <path>:   Enable CSV stats output (optional)");
            eprintln!("                           Each rank creates <path>_rank<N>.csv");
            eprintln!(
                "  --enable-stats:          Enable detailed timing statistics collection (optional)"
            );
            eprintln!(
                "                           Adds overhead, use only for performance analysis"
            );
        }
        std::process::exit(1);
    }

    let registry_dir = PathBuf::from(positional_args[0]);
    let config_path = if positional_args.len() > 1 {
        positional_args[1].as_str()
    } else {
        "benchfs.toml"
    };

    // Enable detailed timing statistics if requested (CLI flag or
    // benchfs.toml [stats] enabled = true). CLI flag wins if set.
    if !enable_stats
        && benchfs::runtime_config::RuntimeConfig::global()
            .stats
            .enabled
    {
        enable_stats = true;
    }
    if enable_stats {
        benchfs::stats::set_stats_enabled(true);
        if mpi_rank == 0 {
            eprintln!("Stats collection enabled - timing overhead will be incurred");
        }
    }

    // Verify registry directory exists (only rank 0)
    if mpi_rank == 0 {
        if !registry_dir.exists() {
            eprintln!("Registry directory does not exist: {:?}", registry_dir);
            eprintln!("Creating registry directory...");
            if let Err(e) = std::fs::create_dir_all(&registry_dir) {
                eprintln!("Failed to create registry directory: {}", e);
                std::process::exit(1);
            }
        }
    }

    // Barrier to ensure registry dir is created before other ranks proceed
    world.barrier();

    // Load configuration
    let mut config = match ServerConfig::from_file(config_path) {
        Ok(cfg) => cfg,
        Err(e) => {
            if mpi_rank == 0 {
                eprintln!("Failed to load configuration: {}", e);
                eprintln!("Using default configuration");
            }
            ServerConfig::default()
        }
    };

    // Override node_id with MPI rank
    config.node.node_id = format!("node_{}", mpi_rank);

    // Create rank-specific data directory
    let base_data_dir = config.node.data_dir.clone();
    config.node.data_dir = base_data_dir.join(format!("rank_{}", mpi_rank));

    // Setup logging (only detailed logs from rank 0)
    let log_level = if mpi_rank == 0 {
        &config.node.log_level
    } else {
        "warn" // Less verbose for other ranks
    };
    let _perfetto_guard = setup_logging(log_level, trace_output.as_ref(), mpi_rank);

    tracing::info!("Starting BenchFS MPI server");
    tracing::info!("MPI Rank: {} / {}", mpi_rank, mpi_size);
    tracing::info!("Node ID: {}", config.node.node_id);
    tracing::info!("Data directory: {}", config.node.data_dir.display());
    tracing::info!("Registry directory: {}", registry_dir.display());

    // Dump kernel state relevant to io_uring ENOMEM diagnostics
    // (RLIMIT_MEMLOCK, MemAvailable, Slab, cgroup memory.*). These do not
    // change at runtime, so a single emission at startup is enough.
    dump_kernel_state();

    // Create data directory if it doesn't exist
    if let Err(e) = std::fs::create_dir_all(&config.node.data_dir) {
        eprintln!("Rank {}: Failed to create data directory: {}", mpi_rank, e);
        std::process::exit(1);
    }

    // Create server state
    let state = Rc::new(ServerState::new(
        config.clone(),
        mpi_rank,
        mpi_size,
        registry_dir,
        stats_output.clone(),
    ));

    // Install SIGTERM / SIGINT handler so the graceful-shutdown loop
    // (which runs `flush_all_sync` for the OnFinalize metadata policy)
    // gets a chance to execute before the process dies. Without this the
    // io500 wrapper's `pkill -TERM benchfsd_mpi` killed the process
    // before any `running.store(false)` callback could fire.
    //
    // Using `libc::signal` keeps us in already-pulled dependencies (no
    // signal-hook). The handler is `extern "C"`, performs an async-
    // signal-safe atomic store on a `OnceLock<Arc<AtomicBool>>`, and
    // returns. The runtime loop polls `state.is_running()` and runs the
    // graceful path.
    {
        use std::sync::OnceLock;
        static SHUTDOWN_FLAG: OnceLock<Arc<AtomicBool>> = OnceLock::new();
        extern "C" fn handle_signal(_sig: libc::c_int) {
            if let Some(flag) = SHUTDOWN_FLAG.get() {
                flag.store(false, Ordering::Release);
            }
        }
        let _ = SHUTDOWN_FLAG.set(Arc::clone(&state.running));
        unsafe {
            libc::signal(libc::SIGTERM, handle_signal as *const () as usize);
            libc::signal(libc::SIGINT, handle_signal as *const () as usize);
        }
    }

    // Log stats output configuration
    if let Some(ref stats_path) = stats_output {
        tracing::info!("Stats output enabled: {}", stats_path.display());
    }

    // Synchronize all ranks before starting servers
    world.barrier();

    // Run the server (enable perfetto tracks if tracing is enabled)
    let enable_perfetto_tracks = trace_output.is_some();
    if let Err(e) = run_server(state.clone(), enable_perfetto_tracks) {
        eprintln!("Rank {}: Server error: {}", mpi_rank, e);
        std::process::exit(1);
    }

    // Synchronize before finalization
    world.barrier();

    tracing::info!("Rank {}: BenchFS server stopped", mpi_rank);
}

fn run_server(
    state: Rc<ServerState>,
    enable_perfetto_tracks: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let config = &state.config;
    let node_id = state.node_id();
    let registry_dir = state
        .registry_dir
        .to_str()
        .ok_or("Registry directory path is not valid UTF-8")?;

    // Create pluvio runtime with optional Perfetto task tracking.
    // Scheduling tuning comes from [scheduling] in benchfs.toml.
    // task_batch_size stays hard-coded at 64 (no production reason to
    // change it on the server); reactor_poll_interval and
    // status_cache_iters are user-tunable via the TOML config.
    let rc_sched = benchfs::runtime_config::RuntimeConfig::global();
    let scheduling_config = SchedulingConfig {
        task_batch_size: 64,
        reactor_poll_interval: rc_sched.scheduling.reactor_poll_interval,
        status_cache_iterations: rc_sched.scheduling.status_cache_iters,
        enable_perfetto_tracks,
        ..Default::default()
    };
    let runtime = Runtime::with_config(2048, scheduling_config);

    if enable_perfetto_tracks {
        tracing::info!("Perfetto task tracking enabled - spawned tasks will have separate tracks");
    }

    // Set runtime in TLS for TLS-based APIs
    pluvio_runtime::set_runtime(runtime.clone());

    // Create UCX context and reactor
    let ucx_context = Rc::new(UcxContext::new()?);
    let ucx_reactor = UCXReactor::current();
    runtime.register_reactor("ucx", ucx_reactor.clone());

    // Register timer reactor for async sleep support
    let timer_reactor = TimerReactor::current();
    runtime.register_reactor("timer", timer_reactor);

    // Create UCX worker
    let worker = ucx_context.create_worker()?;
    ucx_reactor.register_worker(worker.clone());

    // Create metadata manager
    let cache_policy = if config.cache.cache_ttl_secs > 0 {
        CachePolicy::lru_with_ttl(
            config.cache.metadata_cache_entries,
            std::time::Duration::from_secs(config.cache.cache_ttl_secs),
        )
    } else {
        CachePolicy::lru(config.cache.metadata_cache_entries)
    };

    let metadata_manager = Rc::new(MetadataManager::with_cache_policy(
        node_id.clone(),
        cache_policy,
    ));

    // CHFS-style central parent index: populate the metadata ring with all
    // peer node_ids so handlers can compute `get_owner_node(parent_path)`
    // and route `DirIndexUpdate` to the right peer. Without this, the ring
    // only contains self_node_id (added in `MetadataManager::with_cache_policy`)
    // and all writes appear to be local. Expected_nodes comes from
    // `[cluster] expected_nodes` in benchfs.toml; 0 means "single node".
    let expected_nodes: usize = benchfs::runtime_config::RuntimeConfig::global()
        .cluster
        .expected_nodes
        .max(1);
    if expected_nodes > 1 {
        for i in 0..expected_nodes {
            let peer = format!("node_{}", i);
            if peer != node_id {
                metadata_manager.add_node(peer);
            }
        }
        tracing::info!(
            "Metadata ring populated with {} peers (incl self)",
            expected_nodes
        );
    }

    // Create chunk store based on configuration
    let chunk_store_dir = config.node.data_dir.join("chunks");
    if let Err(e) = std::fs::create_dir_all(&chunk_store_dir) {
        return Err(format!("Failed to create chunk store directory: {}", e).into());
    }

    let effective_backend = config.storage.effective_backend().to_string();
    let (chunk_store, allocator): (
        Rc<dyn ChunkStore>,
        Rc<pluvio_uring::allocator::FixedBufferAllocator>,
    ) = match effective_backend.as_str() {
        "iouring" => {
            tracing::info!("Using io_uring for storage backend");

            // Create io_uring reactor
            // Buffer size must match the chunk_size from config to support chunk-sized I/O operations
            //
            // At ppn=16 × 10 nodes = 640 clients hitting 40 server vnodes, each server
            // sees up to 16 clients × 64 in-flight per client (MAX_CONCURRENT_CHUNK_RPCS)
            // = 1024 concurrent WriteChunk RPCs. queue_size=512 was the cap that exhausted
            // and caused 30s WriteChunk timeouts during io500 ior-easy.
            // Override via benchfs.toml [iouring].
            let rc_iouring = benchfs::runtime_config::RuntimeConfig::global();
            let chunk_size = config.storage.chunk_size;
            let base_chunk_size: usize = 4 * 1024 * 1024; // 4 MiB baseline
            let chunk_multiplier = (base_chunk_size / chunk_size).max(1) as u32;
            let queue_size = rc_iouring
                .iouring
                .queue_size
                .saturating_mul(chunk_multiplier);
            let submit_depth = rc_iouring
                .iouring
                .submit_depth
                .saturating_mul(chunk_multiplier)
                .min(queue_size);

            // Pluvio's io_uring reactor reports `Stopped` when SQ is below
            // submit_depth and these timeouts haven't elapsed, which means
            // SQEs can sit in the in-memory submission queue for up to
            // `wait_submit_timeout` before being flushed via `io_uring_enter`.
            // At low/medium load this caps server-side write concurrency.
            // Override via `[iouring] submit_timeout_us` /
            // `complete_timeout_us` in benchfs.toml.
            let submit_timeout_us = rc_iouring.iouring.submit_timeout_us;
            let complete_timeout_us = rc_iouring.iouring.complete_timeout_us;

            tracing::info!(
                "Configuring io_uring: buffer_size={} bytes ({} MiB), queue_size={}, submit_depth={}, submit_timeout_us={}, complete_timeout_us={}, memory={}GiB",
                chunk_size,
                chunk_size / (1024 * 1024),
                queue_size,
                submit_depth,
                submit_timeout_us,
                complete_timeout_us,
                (queue_size as usize * chunk_size) / (1024 * 1024 * 1024)
            );
            // sq_poll_ms in TOML: 0 → disabled, >0 → SQPOLL idle ms.
            let sq_poll_idle_ms: Option<u32> = match rc_iouring.iouring.sq_poll_ms {
                0 => None,
                n => Some(n as u32),
            };
            let mut builder = IoUringReactor::builder()
                .queue_size(queue_size)
                .buffer_size(chunk_size)
                .submit_depth(submit_depth)
                .wait_submit_timeout(std::time::Duration::from_micros(submit_timeout_us))
                .wait_complete_timeout(std::time::Duration::from_micros(complete_timeout_us));
            if let Some(ms) = sq_poll_idle_ms {
                tracing::info!("Enabling io_uring SQ_POLL with idle_ms={}", ms);
                builder = builder.sq_poll(ms);
            }
            let uring_reactor = builder.build();

            let allocator = uring_reactor.allocator.clone();
            let reactor_for_backend = uring_reactor.clone();
            runtime.register_reactor("io_uring", uring_reactor);

            // Create IOUringBackend and chunk store
            // Pass reactor explicitly to ensure DmaFile uses the same io_uring instance
            // that has the registered buffers (fixes SEGFAULT with 4+ nodes)
            let io_backend = Rc::new(IOUringBackend::new(allocator.clone(), reactor_for_backend));
            // Increase LRU cache size to 32768 to reduce cache thrashing
            // With 4 ppn × 4096 chunks = 16384 files per node, 8192 was causing ~50% miss rate
            let chunk_store = Rc::new(IOUringChunkStore::with_capacity(
                &chunk_store_dir,
                io_backend.clone(),
                32768, // Increased from default 8192 to handle large-scale benchmarks
            )?);
            (chunk_store, allocator)
        }
        "dummy" => {
            tracing::info!("Using dummy (no-op) storage backend for RPC overhead measurement");

            // io_uring reactor for allocator only (no actual I/O submission).
            // buffer_size must match chunk_size so that RPC data transfers can
            // send/receive a full chunk in a single round trip.
            // queue_size=128: enough for peak RPC concurrency (96 clients) with headroom,
            // while keeping memory usage at 128*4MiB=512MiB (vs 2GiB with 512).
            // Unlike iouring backend, we don't need io_uring SQ depth, only buffer pool.
            let chunk_size = config.storage.chunk_size;
            let uring_reactor = IoUringReactor::builder()
                .queue_size(128)
                .buffer_size(chunk_size)
                .submit_depth(4)
                .wait_submit_timeout(std::time::Duration::from_micros(10))
                .wait_complete_timeout(std::time::Duration::from_micros(10))
                .build();

            let allocator = uring_reactor.allocator.clone();
            let chunk_store = Rc::new(DummyChunkStore::new());
            (chunk_store, allocator)
        }
        "posix" => {
            tracing::info!("Using POSIX synchronous I/O for storage backend");

            // io_uring reactor for allocator only (no actual I/O submission).
            // buffer_size must match chunk_size so that RPC data transfers can
            // send/receive a full chunk in a single round trip.
            // queue_size=128: enough for peak RPC concurrency (96 clients) with headroom,
            // while keeping memory usage at 128*4MiB=512MiB (vs 2GiB with 512).
            // Unlike iouring backend, we don't need io_uring SQ depth, only buffer pool.
            let chunk_size = config.storage.chunk_size;
            let uring_reactor = IoUringReactor::builder()
                .queue_size(128)
                .buffer_size(chunk_size)
                .submit_depth(4)
                .wait_submit_timeout(std::time::Duration::from_micros(10))
                .wait_complete_timeout(std::time::Duration::from_micros(10))
                .build();

            let allocator = uring_reactor.allocator.clone();
            let chunk_store = Rc::new(PosixChunkStore::new(&chunk_store_dir, true)?);
            (chunk_store, allocator)
        }
        _ => {
            tracing::info!(
                "Using file-based storage backend (backend={})",
                effective_backend
            );

            // io_uring reactor for allocator only.
            // buffer_size must match chunk_size for RPC data transfers.
            // queue_size=128: enough for peak RPC concurrency with headroom,
            // while keeping memory usage at 128*4MiB=512MiB (vs 2GiB with 512).
            let chunk_size = config.storage.chunk_size;
            let uring_reactor = IoUringReactor::builder()
                .queue_size(128)
                .buffer_size(chunk_size)
                .submit_depth(4)
                .wait_submit_timeout(std::time::Duration::from_micros(10))
                .wait_complete_timeout(std::time::Duration::from_micros(10))
                .build();

            let allocator = uring_reactor.allocator.clone();
            let chunk_store = Rc::new(FileChunkStore::new(&chunk_store_dir)?);
            (chunk_store, allocator)
        }
    };

    // Build the persistent inode store if `[metadata].persist` is set
    // to anything other than "off". Lives under <data_dir>/inodes/.
    let inode_store: Option<Rc<benchfs::storage::InodeStore>> = {
        let rt_cfg = benchfs::runtime_config::RuntimeConfig::global();
        let persist = rt_cfg.metadata.persist.to_lowercase();
        let policy = match persist.as_str() {
            "off" | "" => benchfs::storage::FlushPolicy::Off,
            "writethrough" | "write-through" | "wt" => benchfs::storage::FlushPolicy::WriteThrough,
            "writeback" | "write-back" | "wb" => benchfs::storage::FlushPolicy::WriteBack,
            "onfinalize" | "on-finalize" | "finalize" => benchfs::storage::FlushPolicy::OnFinalize,
            other => {
                eprintln!(
                    "[benchfsd_mpi] unknown [metadata].persist={:?}, defaulting to off",
                    other
                );
                benchfs::storage::FlushPolicy::Off
            }
        };
        if policy == benchfs::storage::FlushPolicy::Off {
            None
        } else {
            match benchfs::storage::InodeStore::new(
                &config.node.data_dir,
                config.storage.chunk_size as u64,
                policy,
            ) {
                Ok(s) => {
                    eprintln!(
                        "[benchfsd_mpi] inode_store enabled: policy={:?} base={}/inodes",
                        policy,
                        config.node.data_dir.display()
                    );
                    Some(Rc::new(s))
                }
                Err(e) => {
                    eprintln!(
                        "[benchfsd_mpi] failed to init inode_store ({}); persistence disabled",
                        e
                    );
                    None
                }
            }
        }
    };

    // Create RPC handler context
    let handler_context = Rc::new(RpcHandlerContext::with_inode_store(
        metadata_manager.clone(),
        chunk_store,
        allocator,
        Rc::new(benchfs::rpc::file_id::FileIdRegistry::with_capacity(1024)),
        inode_store,
    ));

    // Create RPC server
    let rpc_server = Rc::new(RpcServer::new(worker.clone(), handler_context.clone()));

    // [transport] backend = "locusta" swaps the connection pool for a
    // locusta-backed one *and* fires up the server-side dispatch loop.
    // All other state (chunk store, metadata manager, RpcHandlerContext)
    // is reused as-is — the only difference is the wire mechanism.
    let use_locusta = benchfs::runtime_config::RuntimeConfig::global()
        .transport
        .backend
        .eq_ignore_ascii_case("locusta");

    #[cfg(feature = "transport-locusta")]
    let locusta_state = if use_locusta {
        Some(init_locusta_runtime(
            &node_id,
            state.mpi_rank,
            state.mpi_size,
            registry_dir,
            handler_context.clone(),
        )?)
        // NOTE: no `runtime.register_reactor("locusta", ...)` here on
        // purpose. The dispatch task below (poll_once_spawn) ticks
        // locusta state machines itself. WaitForResponse::poll also
        // ticks inside its busy-poll loop. Registering a separate
        // tick reactor caused intermittent freezes on rank 0 in jobs
        // 17035 / 17038 — likely a try_borrow_mut race with the
        // dispatch task. Pluvio's stuck watchdog is satisfied as
        // long as some task completes; the dispatch task does.
    } else {
        None
    };
    #[cfg(not(feature = "transport-locusta"))]
    let locusta_state: Option<()> = if use_locusta {
        return Err("BENCHFS_TRANSPORT=locusta requires --features transport-locusta".into());
    } else {
        None
    };

    // Create connection pool for inter-node communication
    let connection_pool = {
        #[cfg(feature = "transport-locusta")]
        {
            if let Some(state_locusta) = &locusta_state {
                Rc::new(ConnectionPool::new_locusta(
                    worker.clone(),
                    registry_dir,
                    Rc::clone(&state_locusta.transport),
                )?)
            } else {
                Rc::new(ConnectionPool::new(worker.clone(), registry_dir)?)
            }
        }
        #[cfg(not(feature = "transport-locusta"))]
        {
            let _ = &locusta_state;
            Rc::new(ConnectionPool::new(worker.clone(), registry_dir)?)
        }
    };

    // Registration will be done after runtime starts (moved to async task below)

    // Register all RPC handlers FIRST (before publishing address)
    // This ensures that when clients connect, the server is ready to handle requests
    let server_clone = rpc_server.clone();

    let handler_ready = std::rc::Rc::new(std::cell::RefCell::new(false));
    let handler_ready_clone = handler_ready.clone();

    #[cfg(feature = "transport-locusta")]
    let locusta_ready_flag = locusta_state.is_some();
    #[cfg(not(feature = "transport-locusta"))]
    let locusta_ready_flag = false;

    if locusta_ready_flag {
        // Locusta path: server-side dispatch is wired the moment
        // `LocustaServerDispatch::new` returned. The UCX handler
        // registration would crash (no UCX endpoint setup), so skip it
        // entirely.
        *handler_ready_clone.borrow_mut() = true;
        #[cfg(feature = "transport-locusta")]
        {
            // Dispatch task: drains ready locusta requests and spawns
            // handler futures. Uses futures_timer::Delay(100µs) between
            // iterations — short enough to keep latency low, long
            // enough to avoid starving timer-woken tasks (10x the
            // 1µs that caused issues; futures_timer's thread-based
            // implementation handles this rate fine).
            let dispatch = Rc::clone(&locusta_state.as_ref().unwrap().dispatch);
            let transport = Rc::clone(&locusta_state.as_ref().unwrap().transport);

            // Register the LocustaTransport as a pluvio Reactor when
            // BENCHFS_LOCUSTA_REACTOR=1. This makes the runtime call
            // `transport.poll()` (= `inner.tick()`) every
            // `reactor_poll_interval` runtime iterations. The dispatch
            // task above is conditioned on this flag too: in reactor
            // mode it uses `drain_and_spawn` (no tick), in legacy mode
            // it uses `poll_once_spawn` (tick + drain).
            if benchfs::rpc::transport_locusta::reactor_mode_enabled() {
                use pluvio_runtime::executor::get_runtime;
                if let Some(rt) = get_runtime() {
                    rt.register_reactor("locusta", Rc::clone(&transport));
                    tracing::info!("Registered LocustaTransport as pluvio Reactor (reactor mode)");
                } else {
                    tracing::warn!(
                        "BENCHFS_LOCUSTA_REACTOR=1 but no thread-local runtime; reactor not registered"
                    );
                }
            }

            pluvio_runtime::spawn_with_name(
                async move {
                    tracing::info!(
                        "Starting LocustaServerDispatch drain task ({} handlers registered)",
                        std::any::type_name::<benchfs::rpc::locusta_server::LocustaServerDispatch>(
                        )
                    );
                    // Adaptive backoff so the dispatch loop doesn't impose
                    // a hard 100us latency floor on every RPC. When work was
                    // drained, busy-yield to come back fast (the executor
                    // gives other tasks one round between yields). When
                    // idle for a few iterations, fall back to a short
                    // sleep so this task doesn't starve other work.
                    //
                    // Tuning knobs in benchfs.toml [locusta]:
                    //   dispatch_idle_sleep_us  (default 20)
                    //   dispatch_idle_threshold (default 16)
                    let rc_locusta = &benchfs::runtime_config::RuntimeConfig::global().locusta;
                    let idle_sleep_us: u64 = rc_locusta.dispatch_idle_sleep_us;
                    let idle_threshold: u32 = rc_locusta.dispatch_idle_threshold;
                    // BENCHFS_LOCUSTA_REACTOR=1 — when on, the pluvio
                    // Reactor (registered below) is the sole driver of
                    // `inner.tick()`. The dispatch task only drains and
                    // spawns; this removes the try_borrow_mut race that
                    // killed earlier attempts (jobs 17035/17038).
                    let reactor_mode = benchfs::rpc::transport_locusta::reactor_mode_enabled();
                    if reactor_mode {
                        tracing::info!(
                            "locusta dispatch in reactor mode: drain-only (tick handled by registered Reactor)"
                        );
                    }
                    let mut iter: u64 = 0;
                    let mut empty_polls: u32 = 0;
                    // Hang-detection counters.
                    let mut total_drained: u64 = 0;
                    let mut last_progress_at = std::time::Instant::now();
                    let mut last_seen_total: u64 = 0;
                    let stall_warn = std::time::Duration::from_secs(10);
                    let mut next_stall_warn_at = last_progress_at + stall_warn;
                    // Periodic WriteChunkById Put pipeline counter dump.
                    // Reveals which stage is the choke point during hangs:
                    //   received → granted → ready → replied
                    // If granted == ready ≪ replied, server is stuck on
                    // chunk_store. If received ≫ granted, allocator is
                    // throttling. Etc.
                    let mut next_pipeline_dump_at =
                        last_progress_at + std::time::Duration::from_secs(5);
                    loop {
                        let drained = if reactor_mode {
                            // Drain only — Reactor handles inner.tick().
                            dispatch.drain_and_spawn(&transport)
                        } else {
                            dispatch.poll_once_spawn(&transport)
                        };
                        iter = iter.wrapping_add(1);
                        if drained > 0 {
                            total_drained = total_drained.saturating_add(drained as u64);
                            last_progress_at = std::time::Instant::now();
                            next_stall_warn_at = last_progress_at + stall_warn;
                        }
                        if iter == 1 || iter % 1_000_000 == 0 {
                            tracing::info!(
                                "locusta dispatch drain heartbeat iter={} total_drained={} (idle_threshold={}, idle_sleep_us={})",
                                iter,
                                total_drained,
                                idle_threshold,
                                idle_sleep_us
                            );
                        }
                        let now = std::time::Instant::now();
                        if now >= next_stall_warn_at && total_drained > 0 {
                            let stalled_secs = now.duration_since(last_progress_at).as_secs();
                            let delta = total_drained.saturating_sub(last_seen_total);
                            tracing::warn!(
                                "locusta dispatch STALL: no drained requests for {}s (iter={}, total_drained={}, since_last_warn={})",
                                stalled_secs,
                                iter,
                                total_drained,
                                delta
                            );
                            last_seen_total = total_drained;
                            next_stall_warn_at = now + stall_warn;
                        }
                        if now >= next_pipeline_dump_at {
                            use std::sync::atomic::Ordering;
                            let received = benchfs::rpc::locusta_handlers::WCB_PUT_RECEIVED
                                .load(Ordering::Relaxed);
                            let granted = benchfs::rpc::locusta_handlers::WCB_PUT_GRANTED
                                .load(Ordering::Relaxed);
                            let grant_rej = benchfs::rpc::locusta_handlers::WCB_PUT_GRANT_REJECTED
                                .load(Ordering::Relaxed);
                            let ready = benchfs::rpc::locusta_handlers::WCB_PUT_READY
                                .load(Ordering::Relaxed);
                            let replied = benchfs::rpc::locusta_handlers::WCB_PUT_REPLIED
                                .load(Ordering::Relaxed);
                            let pending_grant = received.saturating_sub(granted + grant_rej);
                            let pending_write = granted.saturating_sub(ready);
                            let pending_reply = ready.saturating_sub(replied);
                            tracing::info!(
                                target: "wcb_put_pipeline",
                                received = received,
                                granted = granted,
                                grant_rejected = grant_rej,
                                ready = ready,
                                replied = replied,
                                pending_grant = pending_grant,
                                pending_write = pending_write,
                                pending_reply = pending_reply,
                                "WCB_PUT_PIPELINE"
                            );
                            next_pipeline_dump_at = now + std::time::Duration::from_secs(5);
                        }
                        if drained > 0 {
                            empty_polls = 0;
                            // Hot path — yield once so other tasks
                            // (handlers, accept loop) get a slot, then
                            // re-poll immediately.
                            YieldOnce::default().await;
                        } else {
                            empty_polls = empty_polls.saturating_add(1);
                            if empty_polls < idle_threshold {
                                YieldOnce::default().await;
                            } else {
                                pluvio_timer::sleep(std::time::Duration::from_micros(
                                    idle_sleep_us,
                                ))
                                .await;
                            }
                        }
                    }
                },
                "locusta_dispatch_drain".to_string(),
            );

            // Accept loop: scan the registry directory for late-joining
            // clients (IOR ranks, etc.) that publish their QP info after
            // server startup. Static peers wired in `init_locusta_runtime`
            // are already connected; this only handles the dynamic ones.
            let accept_transport = Rc::clone(&locusta_state.as_ref().unwrap().transport);
            pluvio_runtime::spawn_with_name(
                async move {
                    tracing::info!("Starting locusta client_accept loop");
                    // Drains incoming UDP REQUEST packets every
                    // [locusta] accept_interval_ms ms. Default 100ms
                    // gates accept throughput at ~256/100ms = 2560 peers/s
                    // per server. At 10 phys × ppn=4 (3200 conn fan-in),
                    // 100ms ticks combined with 32 peer-process-per-host
                    // means bursts dropped (job 20364/20365 udp timeouts).
                    let scan_ms = benchfs::runtime_config::RuntimeConfig::global()
                        .locusta
                        .accept_interval_ms;
                    let scan_interval = std::time::Duration::from_millis(scan_ms);
                    let per_peer_timeout = std::time::Duration::from_secs(10);
                    let mut iter: u64 = 0;
                    loop {
                        iter = iter.wrapping_add(1);
                        match accept_transport.try_accept_pending_peers(per_peer_timeout) {
                            Ok(added) if !added.is_empty() => {
                                tracing::info!(
                                    "locusta accepted {} new client peer(s) on iter {}: {:?}",
                                    added.len(),
                                    iter,
                                    added
                                );
                            }
                            Ok(_) => {
                                if iter == 1 || iter % 50 == 0 {
                                    tracing::info!(
                                        "locusta accept loop heartbeat iter={} (no new peers)",
                                        iter
                                    );
                                }
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "try_accept_pending_peers error on iter {}: {:?}",
                                    iter,
                                    e
                                );
                            }
                        }
                        pluvio_timer::sleep(scan_interval).await;
                    }
                },
                "locusta_client_accept".to_string(),
            );
        }
    } else {
        pluvio_runtime::spawn_with_name(
            async move {
                tracing::info!("Registering RPC handlers...");
                match server_clone.register_all_handlers().await {
                    Ok(_) => {
                        tracing::info!("RPC handlers registered successfully");
                        *handler_ready_clone.borrow_mut() = true;
                    }
                    Err(e) => {
                        tracing::error!("Failed to register RPC handlers: {:?}", e);
                    }
                }
            },
            "rpc_handler_registration".to_string(),
        );
    }

    // Spawn node registration task (must run after RPC handlers are ready)
    let pool_clone = connection_pool.clone();
    let node_id_clone = node_id.clone();
    let registry_dir_clone = PathBuf::from(registry_dir);
    let mpi_rank_clone = state.mpi_rank;
    let mpi_size_clone = state.mpi_size;

    let _registration_handle = pluvio_runtime::spawn_with_name(
        async move {
            // Wait for RPC handlers to be ready before publishing address
            tracing::info!("Waiting for RPC handlers to be ready...");
            let max_wait = std::time::Duration::from_secs(30);
            let start = std::time::Instant::now();

            loop {
                if *handler_ready.borrow() {
                    tracing::info!("RPC handlers confirmed ready");
                    break;
                }

                if start.elapsed() > max_wait {
                    tracing::error!("RPC handler registration timeout");
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "RPC handler registration timeout",
                    ));
                }

                pluvio_timer::sleep(std::time::Duration::from_millis(100)).await;
            }

            tracing::info!("RPC handlers ready, now registering node address");

            // Small delay to ensure AM streams are fully established
            pluvio_timer::sleep(std::time::Duration::from_millis(200)).await;

            // Register this node's worker address
            if let Err(e) = pool_clone.register_self(&node_id_clone) {
                tracing::error!("Failed to register node address: {:?}", e);
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Registration failed: {:?}", e),
                ));
            }
            tracing::info!("Node {} registered to registry", node_id_clone);

            // Wait for all nodes to register
            let max_wait_secs = 120; // Increased timeout for large-scale deployments
            let start_time = std::time::Instant::now();

            tracing::info!("Waiting for {} nodes to register...", mpi_size_clone);

            let mut registered_count;
            loop {
                // Count how many nodes are currently registered (including self)
                registered_count = 1; // This node is already registered

                for rank in 0..mpi_size_clone {
                    if rank != mpi_rank_clone {
                        let other_node_id = format!("node_{}", rank);
                        let registry_file =
                            registry_dir_clone.join(format!("{}.addr", other_node_id));
                        if registry_file.exists() {
                            registered_count += 1;
                        }
                    }
                }

                if registered_count >= mpi_size_clone {
                    tracing::info!("All {} nodes registered successfully", mpi_size_clone);
                    break;
                }

                if start_time.elapsed().as_secs() >= max_wait_secs {
                    let error_msg = format!(
                        "Only {}/{} nodes registered after {} seconds. Missing {} nodes.",
                        registered_count,
                        mpi_size_clone,
                        max_wait_secs,
                        mpi_size_clone - registered_count
                    );
                    tracing::error!("{}", error_msg);

                    // Log which nodes are missing for debugging
                    for rank in 0..mpi_size_clone {
                        let node_id = format!("node_{}", rank);
                        let registry_file = registry_dir_clone.join(format!("{}.addr", node_id));
                        if !registry_file.exists() {
                            tracing::error!("Missing node: {}", node_id);
                        }
                    }

                    return Err(std::io::Error::new(std::io::ErrorKind::TimedOut, error_msg));
                }

                if registered_count % 4 == 0 || start_time.elapsed().as_secs() % 10 == 0 {
                    tracing::info!(
                        "Registration progress: {}/{} nodes ({}s elapsed)",
                        registered_count,
                        mpi_size_clone,
                        start_time.elapsed().as_secs()
                    );
                }

                pluvio_timer::sleep(std::time::Duration::from_millis(500)).await;
            }

            Ok::<(), std::io::Error>(())
        },
        "node_registration".to_string(),
    );

    // Spawn stats logging task (writes final stats on shutdown)
    {
        let state_clone = state.clone();
        let node_id_clone = node_id.clone();
        let stats_file_path = state.stats_file.clone();
        pluvio_runtime::spawn_with_name(
            async move {
                if stats_file_path.is_some() {
                    tracing::info!("Stats output enabled - will write final stats on shutdown");
                }

                // Wait for server to stop
                loop {
                    if !state_clone.is_running() {
                        break;
                    }
                    pluvio_timer::sleep(std::time::Duration::from_millis(100)).await;
                }

                // Write final stats to CSV file
                if let Some(ref path) = stats_file_path {
                    // Create parent directory if needed
                    if let Some(parent) = path.parent() {
                        if !parent.exists() {
                            let _ = std::fs::create_dir_all(parent);
                        }
                    }

                    let stats = get_server_rpc_stats();
                    match File::create(path) {
                        Ok(mut file) => {
                            // Write CSV header and data
                            let _ = writeln!(file, "node_id,received,completed,peak");
                            let _ = writeln!(
                                file,
                                "{},{},{},{}",
                                node_id_clone,
                                stats.total_received,
                                stats.total_completed,
                                stats.peak_concurrent
                            );
                            tracing::info!(
                                "Stats written to {}: received={}, completed={}, peak={}",
                                path.display(),
                                stats.total_received,
                                stats.total_completed,
                                stats.peak_concurrent
                            );
                        }
                        Err(e) => {
                            tracing::error!(
                                "Failed to create stats file {}: {}",
                                path.display(),
                                e
                            );
                        }
                    }
                }
            },
            "server_stats_logger".to_string(),
        );
    }

    // Spawn periodic RPC concurrency logging task for time-series analysis
    {
        use benchfs::rpc::server::log_rpc_concurrency_stats;
        let state_clone = state.clone();
        pluvio_runtime::spawn_with_name(
            async move {
                // Wait for server to start up
                pluvio_timer::sleep(std::time::Duration::from_secs(1)).await;

                // Log RPC concurrency stats every 100ms for detailed time-series analysis
                let interval = std::time::Duration::from_millis(100);
                while state_clone.is_running() {
                    log_rpc_concurrency_stats();
                    pluvio_timer::sleep(interval).await;
                }
            },
            "rpc_concurrency_logger".to_string(),
        );
    }

    // Start server main loop
    let server_handle = {
        let state_clone = state.clone();
        let rpc_server_clone = rpc_server.clone();
        let runtime_clone = runtime.clone();

        pluvio_runtime::spawn_with_name(
            async move {
                tracing::info!("RPC server listening for requests");

                // Keep server alive while running
                loop {
                    if !state_clone.is_running() {
                        break;
                    }
                    // Yield to allow other tasks to run
                    pluvio_timer::sleep(std::time::Duration::from_millis(100)).await;
                }

                // ========== Graceful Shutdown Sequence ==========
                tracing::info!("Initiating graceful shutdown...");

                // Step 0: OnFinalize metadata flush. The run paid no
                // per-RPC disk cost for `persist=onfinalize`; do the
                // single batched write now, before the runtime is torn
                // down. Off/WriteThrough/WriteBack flush_all_sync is a
                // cheap no-op (Off: returns 0 immediately; the others
                // already wrote during the run, so re-writing here is
                // also fine and idempotent for our schema).
                {
                    let ctx = rpc_server_clone.handler_context();
                    if let Some(store) = ctx.inode_store.as_ref() {
                        let policy = store.policy();
                        if policy != benchfs::storage::FlushPolicy::Off {
                            let t0 = std::time::Instant::now();
                            let files = ctx.metadata_manager.snapshot_local_files();
                            let dirs = ctx.metadata_manager.snapshot_local_dirs();
                            let snapshot_us = t0.elapsed().as_micros();
                            let t1 = std::time::Instant::now();
                            let (fw, dw, fe, de) = store.flush_all_sync(&files, &dirs);
                            tracing::info!(
                                "[benchfsd_mpi] flush_all_sync policy={:?} snapshot_us={} flush_us={} files={}/{} dirs={}/{} ferr={} derr={}",
                                policy,
                                snapshot_us,
                                t1.elapsed().as_micros(),
                                fw,
                                files.len(),
                                dw,
                                dirs.len(),
                                fe,
                                de,
                            );
                        }
                    }
                }

                // Step 1: Set shutdown flag on handler context
                rpc_server_clone.handler_context().set_shutdown_flag();

                // Step 2: Close all AM streams to wake up blocked listeners
                rpc_server_clone.shutdown_all_streams();

                // Step 3: Wait briefly for listener tasks to exit gracefully
                let shutdown_timeout = std::time::Duration::from_millis(500);
                tracing::info!(
                    "Waiting {:?} for listener tasks to exit...",
                    shutdown_timeout
                );
                pluvio_timer::sleep(shutdown_timeout).await;

                // Step 4: Check if tasks are still running and force shutdown if needed
                let remaining_tasks = runtime_clone.task_pool.borrow().len();
                if remaining_tasks > 1 {
                    // >1 because this task itself is still running
                    tracing::warn!(
                        "{} tasks still running after shutdown wait, requesting runtime shutdown",
                        remaining_tasks - 1
                    );
                    runtime_clone.request_shutdown();
                }

                tracing::info!("RPC server stopped");
                Ok::<(), std::io::Error>(())
            },
            "rpc_server".to_string(),
        )
    };

    // Run the runtime
    if state.is_primary() {
        tracing::info!("Primary server is running (Press Ctrl+C to stop)");
    } else {
        tracing::info!("Storage server {} is running", node_id);
    }

    // Run the runtime with the server handle
    // Note: We don't await registration_handle here because it would create a deadlock.
    // The registration task is spawned and will run concurrently with the server.
    pluvio_runtime::run_with_name("benchfsd_mpi_server_main", async move {
        // Wait for server to complete
        match server_handle.await {
            Ok(_) => {
                tracing::info!("Server shutdown complete");
                Ok(())
            }
            Err(e) => {
                tracing::error!("Server error: {:?}", e);
                Err(e)
            }
        }
    });

    // Convert runtime result to Box<dyn Error>
    Ok(())
}

/// Setup logging with optional trace output
///
/// Trace format is determined by environment variables:
/// - ENABLE_CHROME=1 + trace_output set: emit Chrome trace JSON.
/// - Otherwise: console logging only.
fn setup_logging(level: &str, trace_output: Option<&PathBuf>, mpi_rank: i32) -> Option<TraceGuard> {
    let enable_chrome = benchfs::runtime_config::RuntimeConfig::global()
        .observability
        .chrome_tracing;

    if let (Some(trace_path), true) = (trace_output, enable_chrome) {
        if let Some(parent) = trace_path.parent() {
            if !parent.exists() {
                if let Err(e) = std::fs::create_dir_all(parent) {
                    eprintln!(
                        "Rank {}: Failed to create trace output directory: {}",
                        mpi_rank, e
                    );
                }
            }
        }
        let guard = init_with_chrome(level, trace_path);
        tracing::info!("Chrome tracing enabled, output: {}", trace_path.display());
        Some(guard)
    } else {
        benchfs::logging::init_with_hostname(level);
        None
    }
}
