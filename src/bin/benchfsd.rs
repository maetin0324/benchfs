//! BenchFS Server Daemon
//!
//! This is the main server binary for BenchFS distributed file system.
//! It initializes the RPC server, storage backend, and handles incoming requests.

use benchfs::cache::CachePolicy;
use benchfs::config::ServerConfig;
use benchfs::metadata::MetadataManager;
use benchfs::rpc::handlers::RpcHandlerContext;
use benchfs::rpc::server::RpcServer;
use benchfs::storage::{IOUringBackend, IOUringChunkStore};

use pluvio_runtime::executor::Runtime;
use pluvio_ucx::{Context as UcxContext, reactor::UCXReactor};
use pluvio_uring::reactor::IoUringReactor;

use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Server state
struct ServerState {
    config: ServerConfig,
    running: Arc<AtomicBool>,
}

impl ServerState {
    fn new(config: ServerConfig) -> Self {
        Self {
            config,
            running: Arc::new(AtomicBool::new(true)),
        }
    }

    fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    #[allow(dead_code)]
    fn shutdown(&self) {
        self.running.store(false, Ordering::Relaxed);
    }
}

fn main() {
    // Parse command line arguments
    let args: Vec<String> = std::env::args().collect();

    let config_path = if args.len() > 1 {
        &args[1]
    } else {
        "benchfs.toml"
    };

    // Load configuration
    let config = match ServerConfig::from_file(config_path) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("Failed to load configuration: {}", e);
            eprintln!("Using default configuration");
            ServerConfig::default()
        }
    };

    // Setup logging
    setup_logging(&config.node.log_level);

    tracing::info!("Starting BenchFS server");
    tracing::info!("Node ID: {}", config.node.node_id);
    tracing::info!("Data directory: {}", config.node.data_dir.display());
    tracing::info!("Bind address: {}", config.network.bind_addr);

    // Create data directory if it doesn't exist
    if let Err(e) = std::fs::create_dir_all(&config.node.data_dir) {
        eprintln!("Failed to create data directory: {}", e);
        std::process::exit(1);
    }

    // Create server state
    let state = Rc::new(ServerState::new(config.clone()));

    // Setup signal handlers
    benchfs::server::signals::setup_signal_handlers(state.running.clone());

    // Run the server
    if let Err(e) = run_server(state.clone()) {
        eprintln!("Server error: {}", e);
        std::process::exit(1);
    }

    tracing::info!("BenchFS server stopped");
}

fn run_server(state: Rc<ServerState>) -> Result<(), Box<dyn std::error::Error>> {
    let config = &state.config;

    // Create pluvio runtime
    let runtime = Runtime::new(256);

    // Set runtime in TLS for TLS-based APIs
    pluvio_runtime::set_runtime(runtime.clone());

    // Create io_uring reactor with minimal timeouts for low latency
    let uring_reactor = IoUringReactor::builder()
        .queue_size(2048)
        .buffer_size(1 << 20) // 1 MiB
        .submit_depth(64)
        .wait_submit_timeout(std::time::Duration::from_micros(10))
        .wait_complete_timeout(std::time::Duration::from_micros(10))
        .build();

    // Get buffer allocator from reactor
    let allocator = uring_reactor.allocator.clone();

    runtime.register_reactor("io_uring", uring_reactor.clone());

    // Create UCX context and reactor
    let ucx_context = Rc::new(UcxContext::new()?);
    let ucx_reactor = UCXReactor::new();
    runtime.register_reactor("ucx", Rc::new(ucx_reactor));

    // Create UCX worker
    let worker = ucx_context.create_worker()?;
    // Note: Worker is already an Rc<Worker>

    // Create metadata manager with cache policy
    let cache_policy = if config.cache.cache_ttl_secs > 0 {
        CachePolicy::lru_with_ttl(
            config.cache.metadata_cache_entries,
            std::time::Duration::from_secs(config.cache.cache_ttl_secs),
        )
    } else {
        CachePolicy::lru(config.cache.metadata_cache_entries)
    };

    let metadata_manager = Rc::new(MetadataManager::with_cache_policy(
        config.node.node_id.clone(),
        cache_policy,
    ));

    // Create IOUringBackend for chunk storage
    // Pass reactor explicitly to ensure DmaFile uses the same io_uring instance
    let io_backend = Rc::new(IOUringBackend::new(allocator.clone(), uring_reactor.clone()));

    // Create chunk store with io_uring backend
    let chunk_store_dir = config.node.data_dir.join("chunks");
    let chunk_store = Rc::new(
        IOUringChunkStore::new(&chunk_store_dir, io_backend.clone())
            .expect("Failed to create chunk store"),
    );

    // Create RPC handler context
    let handler_context = Rc::new(RpcHandlerContext::new(
        metadata_manager.clone(),
        chunk_store.clone(),
        allocator.clone(),
    ));

    // Create RPC server
    let rpc_server = Rc::new(RpcServer::new(worker, handler_context));

    // Note: This server uses WorkerAddress-based connections (not socket-based),
    // so no listener is needed. Clients connect via worker.connect_addr() using
    // WorkerAddress exchange through the address registry.

    // Start server main loop
    let server_handle = {
        let rpc_server_clone = rpc_server.clone();
        let state_clone = state.clone();

        pluvio_runtime::spawn_with_name(
            async move {
                tracing::info!("RPC server listening for requests");

                // Register all RPC handlers
                if let Err(e) = rpc_server_clone.register_all_handlers().await {
                    tracing::error!("Failed to register RPC handlers: {:?}", e);
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Handler registration failed: {:?}", e),
                    ));
                }

                // Keep server alive while running
                // Note: The RPC handlers are now running in spawned tasks.
                // This main task just needs to keep the runtime alive until shutdown.
                loop {
                    if !state_clone.is_running() {
                        break;
                    }
                    // Yield to allow other tasks to run
                    // We use a short sleep via futures_timer to avoid busy-waiting
                    futures_timer::Delay::new(std::time::Duration::from_millis(100)).await;
                }

                tracing::info!("RPC server stopped");
                Ok::<(), std::io::Error>(())
            },
            "rpc_server".to_string(),
        )
    };

    // Run the runtime
    tracing::info!("Server is running (Press Ctrl+C to stop)");

    pluvio_runtime::run_with_name("benchfsd_server_main", async move {
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
    use tracing_subscriber::EnvFilter;
    use tracing_subscriber::fmt;

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level));

    fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_thread_ids(false)
        .with_file(true)
        .with_line_number(true)
        .init();
}

// Signal handlers moved to benchfs::server::signals module
