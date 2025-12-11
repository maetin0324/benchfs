//! BenchFS Client Daemon Binary
//!
//! This daemon runs on each client node and aggregates connections from
//! multiple IOR processes to reduce the total number of UCX connections
//! to BenchFS servers.
//!
//! # Usage
//!
//! ```bash
//! benchfs_daemon --shm-name /benchfs_daemon_hostname \
//!                --registry-dir /path/to/registry \
//!                --data-dir /path/to/data
//! ```

use std::path::PathBuf;
use std::rc::Rc;

use clap::Parser;
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;

use benchfs::api::file_ops::BenchFS;
use benchfs::daemon::shm::ShmConfig;
use benchfs::daemon::{default_shm_name, ClientDaemon};
use benchfs::rpc::connection::ConnectionPool;
use benchfs::storage::{IOUringBackend, IOUringChunkStore};

use pluvio_runtime::executor::Runtime;
use pluvio_timer::TimerReactor;
use pluvio_ucx::{reactor::UCXReactor, Context as UcxContext};
use pluvio_uring::reactor::IoUringReactor;

/// BenchFS Client Daemon
#[derive(Parser, Debug)]
#[command(name = "benchfs_daemon")]
#[command(about = "BenchFS client daemon for connection aggregation")]
struct Args {
    /// Shared memory name
    #[arg(long, default_value_t = default_shm_name())]
    shm_name: String,

    /// Registry directory for BenchFS server discovery
    #[arg(long)]
    registry_dir: PathBuf,

    /// Data directory for local chunk storage
    #[arg(long)]
    data_dir: PathBuf,

    /// Number of client slots
    #[arg(long, default_value_t = 256)]
    num_slots: u32,

    /// Data buffer size per slot (bytes)
    #[arg(long, default_value_t = 4 * 1024 * 1024)]
    data_buffer_size: usize,

    /// Request ring size (number of entries, must be power of 2)
    #[arg(long, default_value_t = 64)]
    request_ring_size: u32,

    /// Response ring size (number of entries, must be power of 2)
    #[arg(long, default_value_t = 64)]
    response_ring_size: u32,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long, default_value = "info")]
    log_level: String,
}

fn main() {
    let args = Args::parse();

    // Initialize logging
    let log_level = match args.log_level.to_lowercase().as_str() {
        "trace" => Level::TRACE,
        "debug" => Level::DEBUG,
        "info" => Level::INFO,
        "warn" => Level::WARN,
        "error" => Level::ERROR,
        _ => Level::INFO,
    };

    let subscriber = FmtSubscriber::builder()
        .with_max_level(log_level)
        .with_target(false)
        .with_thread_ids(true)
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("Failed to set tracing subscriber");

    info!("BenchFS Client Daemon starting");
    info!("  Shared memory: {}", args.shm_name);
    info!("  Registry dir: {:?}", args.registry_dir);
    info!("  Data dir: {:?}", args.data_dir);
    info!("  Slots: {}", args.num_slots);
    info!("  Buffer size: {} bytes", args.data_buffer_size);

    // Run the daemon
    if let Err(e) = run_daemon(&args) {
        error!("Daemon failed: {}", e);
        std::process::exit(1);
    }
}

fn run_daemon(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    // Create runtime
    let runtime = Runtime::new(256);

    // Set runtime in TLS
    pluvio_runtime::set_runtime(runtime.clone());

    // Initialize io_uring reactor
    let uring_reactor = IoUringReactor::builder()
        .queue_size(512)
        .buffer_size(1024 * 1024)
        .submit_depth(64)
        .wait_submit_timeout(std::time::Duration::from_micros(10))
        .wait_complete_timeout(std::time::Duration::from_micros(10))
        .build();

    let allocator = uring_reactor.allocator.clone();
    runtime.register_reactor("io_uring", uring_reactor.clone());

    // Initialize UCX
    let ucx_context = Rc::new(UcxContext::new()?);
    let ucx_reactor = UCXReactor::current();
    runtime.register_reactor("ucx", ucx_reactor.clone());

    // Register timer reactor
    let timer_reactor = TimerReactor::current();
    runtime.register_reactor("timer", timer_reactor);

    // Create UCX worker
    let worker = ucx_context.create_worker()?;

    // Register worker with UCX reactor
    ucx_reactor.register_worker(worker.clone());

    // Create connection pool
    let registry_dir = args.registry_dir.to_string_lossy().to_string();
    let connection_pool = Rc::new(ConnectionPool::new(worker.clone(), &registry_dir)?);

    // Discover data nodes
    let data_nodes = discover_data_nodes(&args.registry_dir)?;
    if data_nodes.is_empty() {
        return Err("No data nodes found in registry".into());
    }
    info!("Discovered {} data nodes", data_nodes.len());

    // Connect to data nodes
    let pool_clone = connection_pool.clone();
    let nodes_clone = data_nodes.clone();
    pluvio_runtime::run_with_name("connect_to_nodes", async move {
        for node in &nodes_clone {
            if let Err(e) = pool_clone.wait_and_connect(node, 30).await {
                tracing::error!("Failed to connect to node {}: {:?}", node, e);
            }
        }
    });
    info!("Connected to all data nodes");

    // Create chunk store
    std::fs::create_dir_all(&args.data_dir)?;
    let chunk_dir = args.data_dir.join("chunks");
    std::fs::create_dir_all(&chunk_dir)?;

    // Create IOUringBackend for chunk storage
    let io_backend = Rc::new(IOUringBackend::new(allocator, uring_reactor));
    let chunk_store = Rc::new(IOUringChunkStore::new(&chunk_dir, io_backend)?);

    // Create BenchFS client with distributed metadata
    // node_0 is the metadata server, all nodes are data servers
    let metadata_nodes = vec!["node_0".to_string()];
    let benchfs = Rc::new(BenchFS::with_distributed_metadata(
        format!("daemon_{}", std::process::id()),
        chunk_store,
        Rc::clone(&connection_pool),
        data_nodes,
        metadata_nodes,
    ));

    // Create shared memory config
    let shm_config = ShmConfig {
        num_slots: args.num_slots,
        data_buffer_size: args.data_buffer_size,
        request_ring_size: args.request_ring_size,
        response_ring_size: args.response_ring_size,
    };

    // Create daemon
    let daemon = ClientDaemon::new(&args.shm_name, shm_config, benchfs, connection_pool)?;

    // Mark as ready
    daemon.set_ready();

    // Setup signal handlers
    setup_signal_handlers();

    // Run daemon main loop
    pluvio_runtime::run_with_name("daemon_main", async move {
        if let Err(e) = daemon.run().await {
            tracing::error!("Daemon error: {:?}", e);
        }
    });

    info!("Daemon shutdown complete");
    Ok(())
}

/// Discover data nodes from registry directory.
fn discover_data_nodes(registry_dir: &PathBuf) -> Result<Vec<String>, std::io::Error> {
    let mut nodes = Vec::new();

    if registry_dir.exists() {
        for entry in std::fs::read_dir(registry_dir)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().to_string();

            if name.starts_with("node_") && name.ends_with(".addr") {
                // Extract node ID: node_xxx.addr -> node_xxx
                let node_id = name.trim_end_matches(".addr").to_string();
                nodes.push(node_id);
            }
        }
    }

    nodes.sort();
    Ok(nodes)
}

/// Setup signal handlers for graceful shutdown.
fn setup_signal_handlers() {
    // Register SIGTERM/SIGINT handlers using unsafe global state
    // This is a simplified version - production code should be more careful
    unsafe {
        libc::signal(libc::SIGTERM, handle_signal as usize);
        libc::signal(libc::SIGINT, handle_signal as usize);
    }
}

extern "C" fn handle_signal(_sig: libc::c_int) {
    // In a real implementation, we'd signal the daemon to shutdown
    // For now, just exit
    info!("Received shutdown signal");
    std::process::exit(0);
}
