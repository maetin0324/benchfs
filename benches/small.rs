//! Small-scale 2-node benchmark
//!
//! This benchmark runs a server and client as separate processes,
//! communicating over UCX with worker addresses exchanged via a shared filesystem.
//!
//! Usage:
//!   # Terminal 1 (Server):
//!   cargo bench --bench small -- --mode server --registry-dir /shared/registry --data-dir /tmp/server_data
//!
//!   # Terminal 2 (Client):
//!   cargo bench --bench small -- --mode client --registry-dir /shared/registry
//!
//! Benchmarks:
//! - Metadata operations (create, lookup, delete)
//! - Small file I/O (1KB, 4KB, 16KB)
//! - RPC latency and throughput

use std::rc::Rc;
use std::time::{Duration, Instant};

use benchfs::rpc::server::RpcServer;
use benchfs::rpc::handlers::RpcHandlerContext;
use benchfs::rpc::connection::ConnectionPool;
use benchfs::metadata::MetadataManager;
use benchfs::storage::{IOUringBackend, IOUringChunkStore};
use benchfs::api::file_ops::BenchFS;
use benchfs::api::types::OpenFlags;

use pluvio_runtime::executor::Runtime;
use pluvio_uring::reactor::IoUringReactor;
use pluvio_ucx::{Context as UcxContext, reactor::UCXReactor};

/// Benchmark configuration
struct BenchConfig {
    /// Number of iterations for each test
    iterations: usize,
    /// File sizes to test (in bytes)
    file_sizes: Vec<usize>,
    /// Warmup iterations
    warmup: usize,
}

impl Default for BenchConfig {
    fn default() -> Self {
        Self {
            iterations: 100,
            file_sizes: vec![1024, 4096, 16384], // 1KB, 4KB, 16KB
            warmup: 10,
        }
    }
}

/// Benchmark results for a single operation
struct BenchResult {
    name: String,
    iterations: usize,
    total_duration: Duration,
    min: Duration,
    max: Duration,
    avg: Duration,
}

impl BenchResult {
    fn new(name: String, durations: Vec<Duration>) -> Self {
        let iterations = durations.len();
        let total_duration: Duration = durations.iter().sum();
        let min = *durations.iter().min().unwrap();
        let max = *durations.iter().max().unwrap();
        let avg = total_duration / iterations as u32;

        Self {
            name,
            iterations,
            total_duration,
            min,
            max,
            avg,
        }
    }

    fn print(&self) {
        println!("  {}", self.name);
        println!("    Iterations: {}", self.iterations);
        println!("    Total:      {:?}", self.total_duration);
        println!("    Average:    {:?}", self.avg);
        println!("    Min:        {:?}", self.min);
        println!("    Max:        {:?}", self.max);
        println!("    Throughput: {:.2} ops/sec",
                 self.iterations as f64 / self.total_duration.as_secs_f64());
    }
}

/// Command line arguments
#[derive(Debug)]
struct Args {
    mode: Mode,
    registry_dir: String,
    data_dir: Option<String>,
}

#[derive(Debug, PartialEq)]
enum Mode {
    Server,
    Client,
}

impl Args {
    fn parse() -> Result<Self, String> {
        let args: Vec<String> = std::env::args().collect();

        let mut mode = None;
        let mut registry_dir = None;
        let mut data_dir = None;

        let mut i = 1;
        while i < args.len() {
            match args[i].as_str() {
                "--mode" => {
                    if i + 1 >= args.len() {
                        return Err("--mode requires an argument (server or client)".to_string());
                    }
                    mode = Some(match args[i + 1].as_str() {
                        "server" => Mode::Server,
                        "client" => Mode::Client,
                        _ => return Err(format!("Invalid mode: {}. Use 'server' or 'client'", args[i + 1])),
                    });
                    i += 2;
                }
                "--registry-dir" => {
                    if i + 1 >= args.len() {
                        return Err("--registry-dir requires a path argument".to_string());
                    }
                    registry_dir = Some(args[i + 1].clone());
                    i += 2;
                }
                "--data-dir" => {
                    if i + 1 >= args.len() {
                        return Err("--data-dir requires a path argument".to_string());
                    }
                    data_dir = Some(args[i + 1].clone());
                    i += 2;
                }
                _ => {
                    // Skip unknown arguments (benchmark harness arguments)
                    i += 1;
                }
            }
        }

        let mode = mode.ok_or("--mode is required (server or client)".to_string())?;
        let registry_dir = registry_dir.ok_or("--registry-dir is required".to_string())?;

        Ok(Args {
            mode,
            registry_dir,
            data_dir,
        })
    }

    fn print_usage() {
        eprintln!("Usage:");
        eprintln!("  Server mode:");
        eprintln!("    cargo bench --bench small -- --mode server --registry-dir <path> --data-dir <path>");
        eprintln!();
        eprintln!("  Client mode:");
        eprintln!("    cargo bench --bench small -- --mode client --registry-dir <path>");
        eprintln!();
        eprintln!("Arguments:");
        eprintln!("  --mode <server|client>    Run as server or client");
        eprintln!("  --registry-dir <path>     Shared directory for worker address exchange");
        eprintln!("  --data-dir <path>         Data directory (server mode only)");
    }
}

/// Setup server node
fn setup_server(runtime: &Runtime, data_dir: &str, registry_dir: &str) -> Rc<RpcServer> {
    println!("Setting up server node...");

    // Create io_uring reactor with minimal timeouts for low latency
    let uring_reactor = IoUringReactor::builder()
        .queue_size(256)
        .buffer_size(1 << 20) // 1 MiB
        .submit_depth(32)
        .wait_submit_timeout(std::time::Duration::from_micros(10))
        .wait_complete_timeout(std::time::Duration::from_micros(10))
        .build();

    let allocator = uring_reactor.allocator.clone();
    runtime.register_reactor("io_uring", uring_reactor.clone());

    // Create UCX context and reactor
    let ucx_context = Rc::new(UcxContext::new().expect("Failed to create UCX context"));
    let ucx_reactor = UCXReactor::current();
    runtime.register_reactor("ucx", ucx_reactor.clone());

    // Create UCX worker
    let worker = ucx_context.create_worker().expect("Failed to create worker");

    ucx_reactor.register_worker(worker.clone());

    // Create metadata manager
    let metadata_manager = Rc::new(MetadataManager::new("server".to_string()));

    // Create IOUringBackend
    let io_backend = Rc::new(IOUringBackend::new(allocator));

    // Create chunk store
    let chunk_store_dir = format!("{}/chunks", data_dir);
    std::fs::create_dir_all(&chunk_store_dir).expect("Failed to create chunk store dir");
    let chunk_store = Rc::new(
        IOUringChunkStore::new(&chunk_store_dir, io_backend.clone())
            .expect("Failed to create chunk store")
    );

    // Create RPC handler context
    let handler_context = Rc::new(RpcHandlerContext::new(
        metadata_manager.clone(),
        chunk_store.clone(),
    ));

    // Create connection pool for server to register its address
    let server_pool = Rc::new(
        ConnectionPool::new(worker.clone(), registry_dir)
            .expect("Failed to create server connection pool")
    );

    // Register server's worker address
    server_pool.register_self("server").expect("Failed to register server address");
    println!("Server worker address registered to {}", registry_dir);

    // Create RPC server
    Rc::new(RpcServer::new(worker, handler_context))
}

/// Setup client node
async fn setup_client(runtime: &Runtime, registry_dir: &str) -> Rc<BenchFS> {
    println!("Setting up client node...");

    // Create io_uring reactor for client-side chunk store
    let uring_reactor = IoUringReactor::builder()
        .queue_size(256)
        .buffer_size(1 << 20) // 1 MiB
        .submit_depth(32)
        .wait_submit_timeout(std::time::Duration::from_micros(10))
        .wait_complete_timeout(std::time::Duration::from_micros(10))
        .build();

    let allocator = uring_reactor.allocator.clone();
    runtime.register_reactor("io_uring", uring_reactor.clone());

    // Create UCX context and reactor
    let ucx_context = Rc::new(UcxContext::new().expect("Failed to create UCX context"));
    let ucx_reactor = UCXReactor::current();
    runtime.register_reactor("ucx", ucx_reactor.clone());

    // Create UCX worker
    let worker = ucx_context.create_worker().expect("Failed to create worker");

    ucx_reactor.register_worker(worker.clone());

    // Create IOUringBackend and chunk store for client
    let io_backend = Rc::new(IOUringBackend::new(allocator));
    let client_data_dir = "/tmp/benchfs_client";
    let chunk_store_dir = format!("{}/chunks", client_data_dir);
    std::fs::create_dir_all(&chunk_store_dir).expect("Failed to create client chunk store dir");
    let chunk_store = Rc::new(
        IOUringChunkStore::new(&chunk_store_dir, io_backend.clone())
            .expect("Failed to create client chunk store")
    );

    // Create connection pool
    let connection_pool = Rc::new(
        ConnectionPool::new(worker, registry_dir)
            .expect("Failed to create connection pool")
    );

    // Wait for server to register and establish connection
    println!("Waiting for server to register (timeout: 30 seconds)...");
    match connection_pool.wait_and_connect("server", 30).await {
        Ok(_) => println!("Successfully connected to server"),
        Err(e) => {
            eprintln!("Failed to connect to server: {:?}", e);
            std::process::exit(1);
        }
    }

    // Create BenchFS client with target node set to "server"
    // This ensures all chunks are placed on the server node
    Rc::new(BenchFS::with_connection_pool_and_targets(
        "client".to_string(),
        chunk_store,
        connection_pool,
        vec!["server".to_string()],
    ))
}

/// Run metadata create benchmark
async fn bench_metadata_create(fs: &BenchFS, config: &BenchConfig) -> BenchResult {
    let mut durations = Vec::new();

    // Warmup
    for i in 0..config.warmup {
        let path = format!("/warmup_{}.txt", i);
        let handle = fs.benchfs_open(&path, OpenFlags::create()).await.unwrap();
        fs.benchfs_close(&handle).unwrap();
        fs.benchfs_unlink(&path).await.unwrap();
    }

    // Benchmark
    for i in 0..config.iterations {
        let path = format!("/bench_{}.txt", i);

        let start = Instant::now();
        let handle = fs.benchfs_open(&path, OpenFlags::create()).await.unwrap();
        fs.benchfs_close(&handle).unwrap();
        let duration = start.elapsed();

        durations.push(duration);

        // Cleanup
        fs.benchfs_unlink(&path).await.unwrap();
    }

    BenchResult::new("Metadata Create (file creation)".to_string(), durations)
}

/// Run small file write benchmark
async fn bench_small_write(fs: &BenchFS, config: &BenchConfig, size: usize) -> BenchResult {
    let mut durations = Vec::new();
    let data = vec![0xAA; size];

    // Warmup
    for i in 0..config.warmup {
        let path = format!("/warmup_write_{}.txt", i);
        let handle = fs.benchfs_open(&path, OpenFlags::create()).await.unwrap();
        fs.benchfs_write(&handle, &data).await.unwrap();
        fs.benchfs_close(&handle).unwrap();
        fs.benchfs_unlink(&path).await.unwrap();
    }

    // Benchmark
    for i in 0..config.iterations {
        let path = format!("/bench_write_{}.txt", i);
        let handle = fs.benchfs_open(&path, OpenFlags::create()).await.unwrap();

        let start = Instant::now();
        fs.benchfs_write(&handle, &data).await.unwrap();
        let duration = start.elapsed();

        durations.push(duration);

        fs.benchfs_close(&handle).unwrap();
        fs.benchfs_unlink(&path).await.unwrap();
    }

    BenchResult::new(
        format!("Small Write ({} bytes)", size),
        durations
    )
}

/// Run small file read benchmark
async fn bench_small_read(fs: &BenchFS, config: &BenchConfig, size: usize) -> BenchResult {
    let mut durations = Vec::new();
    let data = vec![0xBB; size];

    // Create test files
    let mut test_files = Vec::new();
    for i in 0..config.iterations {
        let path = format!("/bench_read_{}.txt", i);
        let handle = fs.benchfs_open(&path, OpenFlags::create()).await.unwrap();
        fs.benchfs_write(&handle, &data).await.unwrap();
        fs.benchfs_close(&handle).unwrap();
        test_files.push(path);
    }

    // Warmup
    for path in test_files.iter().take(config.warmup) {
        let handle = fs.benchfs_open(path, OpenFlags::read_only()).await.unwrap();
        let mut buf = vec![0u8; size];
        fs.benchfs_read(&handle, &mut buf).await.unwrap();
        fs.benchfs_close(&handle).unwrap();
    }

    // Benchmark
    for path in &test_files {
        let handle = fs.benchfs_open(path, OpenFlags::read_only()).await.unwrap();
        let mut buf = vec![0u8; size];

        let start = Instant::now();
        fs.benchfs_read(&handle, &mut buf).await.unwrap();
        let duration = start.elapsed();

        durations.push(duration);

        fs.benchfs_close(&handle).unwrap();
    }

    // Cleanup
    for path in test_files {
        fs.benchfs_unlink(&path).await.unwrap();
    }

    BenchResult::new(
        format!("Small Read ({} bytes)", size),
        durations
    )
}

/// Run all benchmarks
async fn run_benchmarks(fs: Rc<BenchFS>) {
    let config = BenchConfig::default();
    let mut results = Vec::new();

    println!("\n=== BenchFS 2-Node Benchmark (Distributed Mode) ===\n");
    println!("Configuration:");
    println!("  Iterations: {}", config.iterations);
    println!("  Warmup:     {}", config.warmup);
    println!("  File sizes: {:?} bytes\n", config.file_sizes);

    // Metadata create benchmark
    println!("Running metadata create benchmark...");
    let result = bench_metadata_create(&fs, &config).await;
    results.push(result);

    // Small write benchmarks
    for &size in &config.file_sizes {
        println!("Running small write benchmark ({} bytes)...", size);
        let result = bench_small_write(&fs, &config, size).await;
        results.push(result);
    }

    // Small read benchmarks
    for &size in &config.file_sizes {
        println!("Running small read benchmark ({} bytes)...", size);
        let result = bench_small_read(&fs, &config, size).await;
        results.push(result);
    }

    // Print results
    println!("\n=== Benchmark Results ===\n");
    for result in results {
        result.print();
        println!();
    }
}

/// Run server mode
fn run_server(args: &Args) {
    let data_dir = args.data_dir.as_ref()
        .expect("--data-dir is required for server mode");

    // Create data and registry directories
    std::fs::create_dir_all(data_dir).expect("Failed to create data directory");
    std::fs::create_dir_all(&args.registry_dir).expect("Failed to create registry directory");

    println!("Starting BenchFS server...\n");
    println!("  Data directory:     {}", data_dir);
    println!("  Registry directory: {}\n", args.registry_dir);

    // Create runtime
    let runtime = Runtime::new(256);

    // Setup server
    let server = setup_server(&runtime, data_dir, &args.registry_dir);

    // Get server address
    let server_addr = server.get_address().expect("Failed to get server address");
    println!("Server address: {:?}\n", server_addr);

    // Register all RPC handlers and keep runtime running
    println!("Registering RPC handlers...");
    let server_clone = server.clone();
    let runtime_clone = runtime.clone();

    runtime.run(async move {
        match server_clone.register_all_handlers(runtime_clone).await {
            Ok(_) => {
                tracing::info!("All RPC handlers registered successfully");
                println!("Server is ready and listening for requests.");
                println!("Press Ctrl+C to stop the server.\n");
            }
            Err(e) => {
                eprintln!("Failed to register handlers: {:?}", e);
                std::process::exit(1);
            }
        }

        // Keep server running by waiting forever
        // Use a sleep loop instead of pending() to allow other tasks to run
        loop {
            futures_timer::Delay::new(std::time::Duration::from_secs(1)).await;
        }
    });
}

/// Run client mode
fn run_client(args: &Args) {
    // Ensure registry directory exists
    if !std::path::Path::new(&args.registry_dir).exists() {
        eprintln!("Error: Registry directory does not exist: {}", args.registry_dir);
        eprintln!("Make sure the server is running and has created the registry.");
        std::process::exit(1);
    }

    println!("Starting BenchFS client...\n");
    println!("  Registry directory: {}\n", args.registry_dir);

    // Create runtime
    let runtime = Runtime::new(256);

    // Setup client and run benchmarks
    let rt = runtime.clone();
    let registry_dir = args.registry_dir.clone();
    runtime.run(async move {
        let client = setup_client(&rt, &registry_dir).await;
        run_benchmarks(client).await;
    });

    println!("\nBenchmark complete!");
}

fn main() {
    // Setup logging with configurable level
    let log_level = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());
    tracing_subscriber::fmt()
        .with_env_filter(log_level)
        .with_target(false)
        .init();

    // Parse command line arguments
    let args = match Args::parse() {
        Ok(args) => args,
        Err(e) => {
            eprintln!("Error: {}\n", e);
            Args::print_usage();
            std::process::exit(1);
        }
    };

    // Validate server mode has data-dir
    if args.mode == Mode::Server && args.data_dir.is_none() {
        eprintln!("Error: --data-dir is required for server mode\n");
        Args::print_usage();
        std::process::exit(1);
    }

    // Run in appropriate mode
    match args.mode {
        Mode::Server => run_server(&args),
        Mode::Client => run_client(&args),
    }
}
