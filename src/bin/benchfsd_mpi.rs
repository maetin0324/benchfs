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
use benchfs::logging::{init_with_chrome, init_with_perfetto, TraceGuard};
use benchfs::metadata::MetadataManager;
use benchfs::rpc::connection::ConnectionPool;
use benchfs::rpc::handlers::RpcHandlerContext;
use benchfs::rpc::server::{RpcServer, get_server_rpc_stats};
use benchfs::storage::{ChunkStore, FileChunkStore, IOUringBackend, IOUringChunkStore};

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
    unsafe { std::env::set_var("BENCHFS_RPC_TIMEOUT", "3"); }

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
            eprintln!("  registry_dir:            Shared directory for service discovery (required)");
            eprintln!("  config_file:             Configuration file (optional, default: benchfs.toml)");
            eprintln!("  --trace-output <path>:   Enable Perfetto tracing (optional)");
            eprintln!("                           Each rank creates <path>_rank<N>.json");
            eprintln!("  --stats-output <path>:   Enable CSV stats output (optional)");
            eprintln!("                           Each rank creates <path>_rank<N>.csv");
            eprintln!("  --enable-stats:          Enable detailed timing statistics collection (optional)");
            eprintln!("                           Adds overhead, use only for performance analysis");
        }
        std::process::exit(1);
    }

    let registry_dir = PathBuf::from(positional_args[0]);
    let config_path = if positional_args.len() > 1 {
        positional_args[1].as_str()
    } else {
        "benchfs.toml"
    };

    // Enable detailed timing statistics if requested
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

fn run_server(state: Rc<ServerState>, enable_perfetto_tracks: bool) -> Result<(), Box<dyn std::error::Error>> {
    let config = &state.config;
    let node_id = state.node_id();
    let registry_dir = state
        .registry_dir
        .to_str()
        .ok_or("Registry directory path is not valid UTF-8")?;

    // Create pluvio runtime with optional Perfetto task tracking
    // Tuned for high-throughput I/O: larger batch size and more frequent reactor polling
    let scheduling_config = SchedulingConfig {
        task_batch_size: 64,          // Increased from 16 for better throughput
        reactor_poll_interval: 2,     // Reduced from 8 for lower io_uring latency
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

    // Create chunk store based on configuration
    let chunk_store_dir = config.node.data_dir.join("chunks");
    if let Err(e) = std::fs::create_dir_all(&chunk_store_dir) {
        return Err(format!("Failed to create chunk store directory: {}", e).into());
    }

    let (chunk_store, allocator): (
        Rc<dyn ChunkStore>,
        Rc<pluvio_uring::allocator::FixedBufferAllocator>,
    ) = if config.storage.use_iouring {
        tracing::info!("Using io_uring for storage backend");

        // Create io_uring reactor
        // Buffer size must match the chunk_size from config to support chunk-sized I/O operations
        // Optimized parameters for high-throughput workloads:
        // - queue_size: 512 buffers (reduced from 4096 to cut memory usage)
        //   With 4 MiB chunks: 512 * 4 MiB = 2 GiB (was 16 GiB with 4096)
        //   512 is still far above concurrent RPC limit (64), so no throughput loss
        // - submit_depth: 128 for better batching and throughput
        // - wait_submit_timeout/wait_complete_timeout: 1ms for low-latency reactor polling
        let chunk_size = config.storage.chunk_size;
        let base_chunk_size: usize = 4 * 1024 * 1024; // 4 MiB baseline
        let chunk_multiplier = (base_chunk_size / chunk_size).max(1);
        // Reduced from 4096 to 512: cuts memory from 16 GiB to 2 GiB per process,
        // reducing startup time from ~70s to ~10s while maintaining enough queue depth
        let queue_size = (512 * chunk_multiplier as u32).min(512);
        let submit_depth = (128 * chunk_multiplier as u32).min(512); // Cap at 512

        tracing::info!(
            "Configuring io_uring: buffer_size={} bytes ({} MiB), queue_size={}, submit_depth={}, memory={}GiB",
            chunk_size,
            chunk_size / (1024 * 1024),
            queue_size,
            submit_depth,
            (queue_size as usize * chunk_size) / (1024 * 1024 * 1024)
        );
        let uring_reactor = IoUringReactor::builder()
            .queue_size(queue_size)
            .buffer_size(chunk_size)
            .submit_depth(submit_depth)
            .wait_submit_timeout(std::time::Duration::from_millis(1))
            .wait_complete_timeout(std::time::Duration::from_millis(1))
            .build();

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
    } else {
        tracing::info!("io_uring disabled - using file-based storage backend");

        // Create a minimal io_uring reactor just for allocator
        let uring_reactor = IoUringReactor::builder()
            .queue_size(32)
            .buffer_size(1 << 16) // 64 KiB - minimal size
            .submit_depth(4)
            .wait_submit_timeout(std::time::Duration::from_micros(10))
            .wait_complete_timeout(std::time::Duration::from_micros(10))
            .build();

        let allocator = uring_reactor.allocator.clone();
        let chunk_store = Rc::new(FileChunkStore::new(&chunk_store_dir)?);
        (chunk_store, allocator)
    };

    // Create RPC handler context
    let handler_context = Rc::new(RpcHandlerContext::new(
        metadata_manager.clone(),
        chunk_store,
        allocator,
    ));

    // Create RPC server
    let rpc_server = Rc::new(RpcServer::new(worker.clone(), handler_context));

    // Create connection pool for inter-node communication
    let connection_pool = Rc::new(ConnectionPool::new(worker.clone(), registry_dir)?);

    // Registration will be done after runtime starts (moved to async task below)

    // Register all RPC handlers FIRST (before publishing address)
    // This ensures that when clients connect, the server is ready to handle requests
    let server_clone = rpc_server.clone();

    let handler_ready = std::rc::Rc::new(std::cell::RefCell::new(false));
    let handler_ready_clone = handler_ready.clone();

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

                futures_timer::Delay::new(std::time::Duration::from_millis(100)).await;
            }

            tracing::info!("RPC handlers ready, now registering node address");

            // Small delay to ensure AM streams are fully established
            futures_timer::Delay::new(std::time::Duration::from_millis(200)).await;

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

                futures_timer::Delay::new(std::time::Duration::from_millis(500)).await;
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
                            tracing::error!("Failed to create stats file {}: {}", path.display(), e);
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
                    futures_timer::Delay::new(std::time::Duration::from_millis(100)).await;
                }

                // ========== Graceful Shutdown Sequence ==========
                tracing::info!("Initiating graceful shutdown...");

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
/// - ENABLE_PERFETTO=1: Use Perfetto format (.pftrace) with task-level tracks
/// - ENABLE_CHROME=1: Use Chrome trace format (.json)
/// - Neither set: No tracing (console logging only)
///
/// If `trace_output` is Some and a trace format is enabled, the trace file
/// will be flushed when the returned guard is dropped.
fn setup_logging(level: &str, trace_output: Option<&PathBuf>, mpi_rank: i32) -> Option<TraceGuard> {
    // Check which trace format to use
    let enable_perfetto = std::env::var("ENABLE_PERFETTO")
        .map(|v| v == "1" || v.to_lowercase() == "true")
        .unwrap_or(false);
    let enable_chrome = std::env::var("ENABLE_CHROME")
        .map(|v| v == "1" || v.to_lowercase() == "true")
        .unwrap_or(false);

    if let Some(trace_path) = trace_output {
        // Create parent directory if needed
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

        if enable_perfetto {
            let guard = init_with_perfetto(level, trace_path);
            tracing::info!(
                "Perfetto tracing enabled (task-level tracks), output: {}",
                trace_path.display()
            );
            Some(guard)
        } else if enable_chrome {
            let guard = init_with_chrome(level, trace_path);
            tracing::info!(
                "Chrome tracing enabled, output: {}",
                trace_path.display()
            );
            Some(guard)
        } else {
            // Trace output path given but no format enabled - default to Perfetto
            let guard = init_with_perfetto(level, trace_path);
            tracing::info!(
                "Perfetto tracing enabled (default), output: {}",
                trace_path.display()
            );
            Some(guard)
        }
    } else {
        benchfs::logging::init_with_hostname(level);
        None
    }
}
