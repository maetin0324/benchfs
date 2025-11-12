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

use benchfs::cache::CachePolicy;
use benchfs::config::ServerConfig;
use benchfs::metadata::MetadataManager;
use benchfs::rpc::RpcError;
use benchfs::rpc::connection::ConnectionPool;
use benchfs::rpc::handlers::RpcHandlerContext;
use benchfs::rpc::server::RpcServer;
use benchfs::rpc::stream_server::StreamRpcServer;
use benchfs::storage::{ChunkStore, FileChunkStore, IOUringBackend, IOUringChunkStore};

use pluvio_runtime::executor::Runtime;
use pluvio_ucx::{Context as UcxContext, reactor::UCXReactor};
use pluvio_uring::reactor::IoUringReactor;

use mpi::traits::*;

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

    fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);
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
            eprintln!(
                "Usage: mpirun -n <num_nodes> {} <registry_dir> [config_file]",
                args[0]
            );
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
    let state = Rc::new(ServerState::new(
        config.clone(),
        mpi_rank,
        mpi_size,
        registry_dir,
    ));

    // Setup signal handlers
    benchfs::server::signals::setup_signal_handlers(state.running.clone());

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
    let registry_dir = state
        .registry_dir
        .to_str()
        .ok_or("Registry directory path is not valid UTF-8")?;

    // Create pluvio runtime
    let runtime = Runtime::new(256);

    // Set runtime in TLS for TLS-based APIs
    pluvio_runtime::set_runtime(runtime.clone());

    // Create UCX context and reactor
    let ucx_context = Arc::new(UcxContext::new()?);
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
        // Buffer size must be at least as large as the maximum transfer size used by IOR
        // IOR typically uses 2MB-4MB transfer sizes, so we use 4MB to be safe
        // Optimized parameters for high-throughput workloads:
        // - queue_size: 2048 (moderate to avoid excessive memory usage)
        // - submit_depth: 128 (up from 64) for better batching and throughput
        // - Aggressive timeouts (1μs) to minimize latency in polling mode
        let uring_reactor = IoUringReactor::builder()
            .queue_size(2048)
            .buffer_size(4 << 20) // 4 MiB (increased from 1 MiB to support larger IOR transfer sizes)
            .submit_depth(128)
            .wait_submit_timeout(std::time::Duration::from_micros(1))
            .wait_complete_timeout(std::time::Duration::from_micros(1))
            .build();

        let allocator = uring_reactor.allocator.clone();
        runtime.register_reactor("io_uring", uring_reactor);

        // Create IOUringBackend and chunk store
        let io_backend = Rc::new(IOUringBackend::new(allocator.clone()));
        let chunk_store = Rc::new(IOUringChunkStore::new(
            &chunk_store_dir,
            io_backend.clone(),
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

    // Create RPC server (Active Message based - legacy support)
    let rpc_server = Rc::new(RpcServer::new(worker.clone(), handler_context.clone()));

    // Create Stream RPC server (Stream + RMA based)
    let stream_rpc_server = Rc::new(StreamRpcServer::new(
        worker.clone(),
        handler_context.clone(),
    ));

    // Create connection pool for inter-node communication
    // Note: bind_and_register() will be called later to create the socket listener
    let connection_pool = Rc::new(ConnectionPool::new(
        worker.clone(),
        ucx_context.clone(),
        registry_dir,
    )?);

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
    let stream_rpc_server_clone = stream_rpc_server.clone();
    let state_clone_for_registration = state.clone();
    let worker_clone_for_registration = worker.clone();

    // Registration will be done synchronously before starting the main loop
    let registration_future = async move {
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

        // Small delay to ensure runtime is fully initialized
        futures_timer::Delay::new(std::time::Duration::from_millis(200)).await;

        // Bind to a socket and register this node's address
        let base_port = 50051u16;
        let listen_port = base_port + mpi_rank_clone as u16;
        let listen_addr = std::net::SocketAddr::from(([0, 0, 0, 0], listen_port));

        tracing::info!("Attempting to bind AM RPC socket at {}", listen_addr);

        const MAX_PORT_RETRIES: u16 = 5;
        let mut last_error: Option<RpcError> = None;
        let mut bound_addr: Option<std::net::SocketAddr> = None;

        for attempt in 0..MAX_PORT_RETRIES {
            let port_offset = attempt * (mpi_size_clone as u16); // 同一ランクが別ポートを試す際に重複しないよう mpi サイズでシフト
            let candidate_port = listen_port.saturating_add(port_offset);
            let candidate_addr = std::net::SocketAddr::from(([0, 0, 0, 0], candidate_port));

            tracing::info!(
                "Attempting to bind AM RPC socket at {} (attempt {}/{})",
                candidate_addr,
                attempt + 1,
                MAX_PORT_RETRIES
            );

            match pool_clone.bind_and_register(&node_id_clone, candidate_addr) {
                Ok(addr) => {
                    tracing::info!(
                        "Node {} bound and registered at {} (attempt {}/{})",
                        node_id_clone,
                        addr,
                        attempt + 1,
                        MAX_PORT_RETRIES
                    );
                    bound_addr = Some(addr);
                    break;
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to bind node {} at {}: {:?}",
                        node_id_clone,
                        candidate_addr,
                        e
                    );
                    last_error = Some(e);
                    futures_timer::Delay::new(std::time::Duration::from_millis(250)).await;
                }
            }
        }

        let bound_addr = match bound_addr {
            Some(addr) => addr,
            None => {
                let err = last_error.unwrap_or_else(|| {
                    RpcError::ConnectionError("Unknown error during port binding".to_string())
                });
                tracing::error!(
                    "Failed to bind and register node address after {} attempts: {:?}",
                    MAX_PORT_RETRIES,
                    err
                );
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Bind and registration failed: {:?}", err),
                ));
            }
        };

        tracing::info!(
            "Node {} registered with address {}",
            node_id_clone,
            bound_addr
        );

        // Start Stream RPC acceptor loop now that listener is bound
        // Take the listener from the connection pool
        if let Some(stream_listener) = pool_clone.take_listener() {
            tracing::info!("Starting Stream RPC acceptor on {}", bound_addr);

            let stream_server_clone_inner = stream_rpc_server_clone.clone();
            let state_clone_inner = state_clone_for_registration.clone();
            let worker_clone_inner = worker_clone_for_registration.clone();
            let mut stream_listener_mut = stream_listener;

            pluvio_runtime::spawn_with_name(
                async move {
                    tracing::info!("Stream RPC server accepting connections on {}", bound_addr);

                    loop {
                        if !state_clone_inner.is_running() {
                            break;
                        }

                        // Accept incoming connection
                        let connection = stream_listener_mut.next().await;

                        match worker_clone_inner.accept(connection).await {
                            Ok(endpoint) => {
                                tracing::info!("Stream RPC: accepted new client connection");

                                // Spawn a task to serve this connection
                                let server_clone = stream_server_clone_inner.clone();

                                pluvio_runtime::spawn_with_name(
                                    async move {
                                        if let Err(e) = server_clone.serve(endpoint).await {
                                            tracing::error!("Stream RPC connection error: {:?}", e);
                                        }
                                        Ok::<(), std::io::Error>(())
                                    },
                                    format!("stream_rpc_connection"),
                                );
                            }
                            Err(e) => {
                                tracing::error!("Stream RPC accept error: {:?}", e);
                                // Brief delay before retrying
                                futures_timer::Delay::new(std::time::Duration::from_millis(100))
                                    .await;
                            }
                        }
                    }

                    tracing::info!("Stream RPC server stopped accepting connections");
                    Ok::<(), std::io::Error>(())
                },
                "stream_rpc_acceptor".to_string(),
            );
        } else {
            tracing::warn!("No listener available for Stream RPC acceptor");
        }

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
                    let registry_file = registry_dir_clone.join(format!("{}.addr", other_node_id));
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
    };

    // Start server main loop (AM-based legacy server)
    let server_handle = {
        let state_clone = state.clone();

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

    // Run the runtime with proper initialization sequence
    // First complete registration, then start the server
    pluvio_runtime::run_with_name("benchfsd_mpi_server_main", async move {
        // Execute registration synchronously before starting the server
        tracing::info!("Starting node registration...");
        if let Err(e) = registration_future.await {
            tracing::error!("Registration failed: {:?}", e);
            return Err(e);
        }
        tracing::info!("Node registration completed successfully");

        // Now wait for server to complete
        match server_handle.await {
            Ok(_) => {
                tracing::info!("Server shutdown complete");
                Ok(())
            }
            Err(e) => {
                let error_msg = format!("Server error: {}", e);
                tracing::error!("{}", error_msg);
                Err(std::io::Error::new(std::io::ErrorKind::Other, error_msg))
            }
        }
    });

    // Convert runtime result to Box<dyn Error>
    Ok(())
}

fn setup_logging(level: &str) {
    use tracing_subscriber::EnvFilter;
    use tracing_subscriber::fmt;

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level));

    fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false)
        .compact()
        .init();
}

// Signal handlers moved to benchfs::server::signals module
