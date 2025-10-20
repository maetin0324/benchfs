//! BenchFS Server Daemon with MPI Support
//!
//! This is the MPI-enabled server binary for BenchFS distributed file system.
//! It uses MPI to distribute servers across multiple nodes in an HPC cluster.
//!
//! Usage:
//!   mpirun -n <num_nodes> benchfsd_mpi <config_file> <registry_dir>
//!
//! Each MPI rank runs a BenchFS server instance:
//! - Rank 0: Primary metadata server
//! - Other ranks: Storage servers and secondary metadata servers

use benchfs::config::ServerConfig;
use benchfs::rpc::server::RpcServer;
use benchfs::rpc::handlers::RpcHandlerContext;
use benchfs::rpc::connection::ConnectionPool;
use benchfs::metadata::MetadataManager;
use benchfs::storage::{IOUringChunkStore, IOUringBackend};
use benchfs::cache::CachePolicy;

use pluvio_runtime::executor::Runtime;
use pluvio_uring::reactor::IoUringReactor;
use pluvio_ucx::{Context as UcxContext, reactor::UCXReactor};

use mpi::traits::*;

use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::path::PathBuf;

/// Server state
struct ServerState {
    config: ServerConfig,
    running: Arc<AtomicBool>,
    mpi_rank: i32,
    mpi_size: i32,
    registry_dir: PathBuf,
}

impl ServerState {
    fn new(config: ServerConfig, mpi_rank: i32, mpi_size: i32, registry_dir: PathBuf) -> Self {
        Self {
            config,
            running: Arc::new(AtomicBool::new(true)),
            mpi_rank,
            mpi_size,
            registry_dir,
        }
    }

    fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
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

    if args.len() < 2 {
        if mpi_rank == 0 {
            eprintln!("Usage: mpirun -n <num_nodes> {} <registry_dir> [config_file]", args[0]);
            eprintln!("  registry_dir: Shared directory for service discovery (required)");
            eprintln!("  config_file:  Configuration file (optional, default: benchfs.toml)");
        }
        std::process::exit(1);
    }

    let registry_dir = PathBuf::from(&args[1]);
    let config_path = if args.len() > 2 {
        &args[2]
    } else {
        "benchfs.toml"
    };

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
    setup_logging(log_level);

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
    let state = Rc::new(ServerState::new(config.clone(), mpi_rank, mpi_size, registry_dir));

    // Setup signal handlers
    setup_signal_handlers(state.running.clone());

    // Synchronize all ranks before starting servers
    world.barrier();

    // Run the server
    if let Err(e) = run_server(state.clone()) {
        eprintln!("Rank {}: Server error: {}", mpi_rank, e);
        std::process::exit(1);
    }

    // Synchronize before finalization
    world.barrier();

    tracing::info!("Rank {}: BenchFS server stopped", mpi_rank);
}

fn run_server(state: Rc<ServerState>) -> Result<(), Box<dyn std::error::Error>> {
    let config = &state.config;
    let node_id = state.node_id();
    let registry_dir = state.registry_dir.to_str()
        .ok_or("Registry directory path is not valid UTF-8")?;

    // Create pluvio runtime
    let runtime = Runtime::new(256);

    // Create io_uring reactor
    let uring_reactor = IoUringReactor::builder()
        .queue_size(2048)
        .buffer_size(1 << 20) // 1 MiB
        .submit_depth(64)
        .wait_submit_timeout(std::time::Duration::from_micros(10))
        .wait_complete_timeout(std::time::Duration::from_micros(10))
        .build();

    let allocator = uring_reactor.allocator.clone();
    runtime.register_reactor("io_uring", uring_reactor);

    // Create UCX context and reactor
    let ucx_context = Rc::new(UcxContext::new()?);
    let ucx_reactor = UCXReactor::current();
    runtime.register_reactor("ucx", ucx_reactor.clone());

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

    // Create IOUringBackend and chunk store
    let io_backend = Rc::new(IOUringBackend::new(allocator));
    let chunk_store_dir = config.node.data_dir.join("chunks");
    if let Err(e) = std::fs::create_dir_all(&chunk_store_dir) {
        return Err(format!("Failed to create chunk store directory: {}", e).into());
    }

    let chunk_store = Rc::new(
        IOUringChunkStore::new(&chunk_store_dir, io_backend.clone())?
    );

    // Create RPC handler context
    let handler_context = Rc::new(RpcHandlerContext::new(
        metadata_manager.clone(),
        chunk_store.clone(),
    ));

    // Create RPC server
    let rpc_server = Rc::new(RpcServer::new(worker.clone(), handler_context));

    // Create connection pool for inter-node communication
    let connection_pool = Rc::new(
        ConnectionPool::new(worker.clone(), registry_dir)?
    );

    // Register this node's worker address
    if let Err(e) = connection_pool.register_self(&node_id) {
        return Err(format!("Failed to register node address: {:?}", e).into());
    }
    tracing::info!("Node {} registered to registry", node_id);

    // Wait for all nodes to register (blocking with timeout)
    let mut registered_count = 1; // This node is already registered
    let max_wait_secs = 60;
    let start_time = std::time::Instant::now();

    tracing::info!("Waiting for {} nodes to register...", state.mpi_size);

    while registered_count < state.mpi_size && start_time.elapsed().as_secs() < max_wait_secs {
        // Check how many nodes are registered
        for rank in 0..state.mpi_size {
            if rank != state.mpi_rank {
                let other_node_id = format!("node_{}", rank);
                // Try to check if node is registered by attempting to read its address
                let registry_file = PathBuf::from(registry_dir)
                    .join(format!("{}.addr", other_node_id));
                if registry_file.exists() {
                    if rank + 1 > registered_count {
                        registered_count = rank + 1;
                        tracing::debug!("Detected {} registered", other_node_id);
                    }
                }
            }
        }

        if registered_count >= state.mpi_size {
            break;
        }

        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    if registered_count < state.mpi_size {
        tracing::warn!("Only {}/{} nodes registered after {} seconds",
                      registered_count, state.mpi_size, max_wait_secs);
    } else {
        tracing::info!("All {} nodes registered successfully", state.mpi_size);
    }

    // Register all RPC handlers (spawn in background)
    let server_clone = rpc_server.clone();
    let runtime_clone = runtime.clone();
    runtime.spawn(async move {
        match server_clone.register_all_handlers(runtime_clone).await {
            Ok(_) => tracing::info!("RPC handlers registered successfully"),
            Err(e) => tracing::error!("Failed to register RPC handlers: {:?}", e),
        }
    });
    tracing::info!("RPC handler registration initiated");

    // Start server main loop
    let server_handle = {
        let state_clone = state.clone();

        runtime.spawn_with_name(
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

    runtime.clone().run(async move {
        match server_handle.await {
            Ok(_) => {
                tracing::info!("Server shutdown complete");
            }
            Err(e) => {
                tracing::error!("Server error: {:?}", e);
            }
        }
    });

    Ok(())
}

fn setup_logging(level: &str) {
    use tracing_subscriber::fmt;
    use tracing_subscriber::EnvFilter;

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(level));

    fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false)
        .compact()
        .init();
}

fn setup_signal_handlers(running: Arc<AtomicBool>) {
    use std::sync::Mutex;

    // Store the running flag in a static for signal handler access
    static RUNNING_FLAG: Mutex<Option<Arc<AtomicBool>>> = Mutex::new(None);
    *RUNNING_FLAG.lock().unwrap() = Some(running);

    // Setup SIGINT handler (Ctrl+C)
    #[cfg(unix)]
    {
        use libc::{SIGINT, SIGTERM};

        unsafe {
            libc::signal(SIGINT, signal_handler as libc::sighandler_t);
            libc::signal(SIGTERM, signal_handler as libc::sighandler_t);
        }
    }

    #[cfg(unix)]
    extern "C" fn signal_handler(_: libc::c_int) {
        use std::sync::Mutex;

        static RUNNING_FLAG: Mutex<Option<Arc<AtomicBool>>> = Mutex::new(None);

        if let Some(flag) = RUNNING_FLAG.lock().unwrap().as_ref() {
            eprintln!("\nReceived shutdown signal, stopping server...");
            flag.store(false, Ordering::Relaxed);
        }
    }
}
