//! BenchFS Performance Analysis Tool
//!
//! A standalone server/client for performance analysis with Perfetto tracing.
//! Does not require MPI - uses WorkerAddress exchange via shared filesystem.
//!
//! Usage:
//!   Server: benchfs_perf server --registry-dir /shared/path --data-dir /local/data
//!   Client: benchfs_perf client --registry-dir /shared/path --server-node server

use benchfs::logging::{init_with_perfetto, PerfettoGuard};
use benchfs::metadata::MetadataManager;
use benchfs::rpc::connection::ConnectionPool;
use benchfs::rpc::data_ops::{ReadChunkRequest, WriteChunkRequest};
use benchfs::rpc::handlers::RpcHandlerContext;
use benchfs::rpc::server::RpcServer;
use benchfs::rpc::AmRpc;
use benchfs::storage::{IOUringBackend, IOUringChunkStore};

use clap::{Parser, Subcommand};
use pluvio_runtime::executor::Runtime;
use pluvio_timer::TimerReactor;
use pluvio_ucx::reactor::UCXReactor;
use pluvio_ucx::Context as UcxContext;
use pluvio_uring::reactor::IoUringReactor;
use tracing::instrument;

use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// BenchFS Performance Analysis Tool
#[derive(Parser)]
#[command(name = "benchfs_perf")]
#[command(about = "Performance analysis tool for BenchFS with Perfetto tracing")]
struct Args {
    #[command(subcommand)]
    mode: Mode,
}

#[derive(Subcommand)]
enum Mode {
    /// Run as server
    Server {
        /// Directory for WorkerAddress exchange (shared filesystem)
        #[arg(long, default_value = "/tmp/benchfs_perf/registry")]
        registry_dir: PathBuf,

        /// Directory for chunk data storage
        #[arg(long, default_value = "/tmp/benchfs_perf/data")]
        data_dir: PathBuf,

        /// Output path for trace file (Chrome trace format, compatible with Perfetto UI)
        #[arg(long, default_value = "benchfs_server.json")]
        output: PathBuf,

        /// Log level
        #[arg(long, default_value = "info")]
        log_level: String,

        /// Node ID (default: hostname)
        #[arg(long)]
        node_id: Option<String>,
    },

    /// Run as client
    Client {
        /// Directory for WorkerAddress exchange (shared filesystem)
        #[arg(long, default_value = "/tmp/benchfs_perf/registry")]
        registry_dir: PathBuf,

        /// Server node ID to connect to
        #[arg(long, default_value = "server")]
        server_node: String,

        /// Output path for trace file (Chrome trace format, compatible with Perfetto UI)
        #[arg(long, default_value = "benchfs_client.json")]
        output: PathBuf,

        /// Log level
        #[arg(long, default_value = "info")]
        log_level: String,

        /// Node ID (default: hostname)
        #[arg(long)]
        node_id: Option<String>,

        /// Number of iterations for each benchmark (default: 128 for 4GB with 32MB chunks)
        #[arg(long, default_value = "128")]
        iterations: u32,

        /// Block size in bytes (default: 32MB)
        #[arg(long, default_value = "33554432")]
        block_size: usize,

        /// Connection timeout in seconds
        #[arg(long, default_value = "30")]
        timeout: u64,
    },
}

/// Benchmark statistics
#[derive(Debug, Default)]
struct BenchmarkStats {
    latencies: Vec<Duration>,
    total_bytes: u64,
    total_time: Duration,
}

impl BenchmarkStats {
    fn new() -> Self {
        Self::default()
    }

    fn record(&mut self, latency: Duration, bytes: u64) {
        self.latencies.push(latency);
        self.total_bytes += bytes;
    }

    fn finalize(&mut self, total_time: Duration) {
        self.total_time = total_time;
        self.latencies.sort();
    }

    fn min_latency(&self) -> Duration {
        *self.latencies.first().unwrap_or(&Duration::ZERO)
    }

    fn max_latency(&self) -> Duration {
        *self.latencies.last().unwrap_or(&Duration::ZERO)
    }

    fn avg_latency(&self) -> Duration {
        if self.latencies.is_empty() {
            Duration::ZERO
        } else {
            let total: Duration = self.latencies.iter().sum();
            total / self.latencies.len() as u32
        }
    }

    fn p50_latency(&self) -> Duration {
        self.percentile(50)
    }

    fn p99_latency(&self) -> Duration {
        self.percentile(99)
    }

    fn percentile(&self, p: u32) -> Duration {
        if self.latencies.is_empty() {
            return Duration::ZERO;
        }
        let idx = (self.latencies.len() * p as usize / 100).min(self.latencies.len() - 1);
        self.latencies[idx]
    }

    fn throughput_mbps(&self) -> f64 {
        if self.total_time.as_secs_f64() == 0.0 {
            return 0.0;
        }
        (self.total_bytes as f64 / 1024.0 / 1024.0) / self.total_time.as_secs_f64()
    }

    fn iops(&self) -> f64 {
        if self.total_time.as_secs_f64() == 0.0 {
            return 0.0;
        }
        self.latencies.len() as f64 / self.total_time.as_secs_f64()
    }

    fn print_report(&self, name: &str) {
        println!("\n===== {} Results =====", name);
        println!("Operations:   {}", self.latencies.len());
        println!("Total bytes:  {} bytes ({:.2} MB)", self.total_bytes, self.total_bytes as f64 / 1024.0 / 1024.0);
        println!("Total time:   {:.3} ms", self.total_time.as_secs_f64() * 1000.0);
        println!("Throughput:   {:.2} MB/s", self.throughput_mbps());
        println!("IOPS:         {:.2}", self.iops());
        println!("Latency (min):  {:.3} us", self.min_latency().as_secs_f64() * 1_000_000.0);
        println!("Latency (avg):  {:.3} us", self.avg_latency().as_secs_f64() * 1_000_000.0);
        println!("Latency (p50):  {:.3} us", self.p50_latency().as_secs_f64() * 1_000_000.0);
        println!("Latency (p99):  {:.3} us", self.p99_latency().as_secs_f64() * 1_000_000.0);
        println!("Latency (max):  {:.3} us", self.max_latency().as_secs_f64() * 1_000_000.0);
    }
}

fn main() {
    let args = Args::parse();

    match args.mode {
        Mode::Server {
            registry_dir,
            data_dir,
            output,
            log_level,
            node_id,
        } => {
            let node_id = node_id.unwrap_or_else(|| "server".to_string());
            let _guard = init_with_perfetto(&log_level, &output);

            tracing::info!("Starting BenchFS Performance Server");
            tracing::info!("Node ID: {}", node_id);
            tracing::info!("Registry directory: {}", registry_dir.display());
            tracing::info!("Data directory: {}", data_dir.display());
            tracing::info!("Trace output: {}", output.display());

            if let Err(e) = run_server(&registry_dir, &data_dir, &node_id, _guard) {
                tracing::error!("Server error: {}", e);
                std::process::exit(1);
            }
        }

        Mode::Client {
            registry_dir,
            server_node,
            output,
            log_level,
            node_id,
            iterations,
            block_size,
            timeout,
        } => {
            let node_id = node_id.unwrap_or_else(|| "client".to_string());
            let _guard = init_with_perfetto(&log_level, &output);

            tracing::info!("Starting BenchFS Performance Client");
            tracing::info!("Node ID: {}", node_id);
            tracing::info!("Registry directory: {}", registry_dir.display());
            tracing::info!("Server node: {}", server_node);
            tracing::info!("Trace output: {}", output.display());
            tracing::info!("Iterations: {}", iterations);
            tracing::info!("Block size: {} bytes", block_size);

            if let Err(e) = run_client(
                &registry_dir,
                &server_node,
                &node_id,
                iterations,
                block_size,
                timeout,
                _guard,
            ) {
                tracing::error!("Client error: {}", e);
                std::process::exit(1);
            }
        }
    }
}

fn run_server(
    registry_dir: &PathBuf,
    data_dir: &PathBuf,
    node_id: &str,
    _perfetto_guard: PerfettoGuard,
) -> Result<(), Box<dyn std::error::Error>> {
    // Create directories
    std::fs::create_dir_all(registry_dir)?;
    std::fs::create_dir_all(data_dir)?;

    let running = Arc::new(AtomicBool::new(true));
    let running_clone = running.clone();

    // Setup signal handler
    ctrlc::set_handler(move || {
        tracing::info!("Received shutdown signal");
        running_clone.store(false, Ordering::Release);
    })?;

    // Create runtime
    let runtime = Runtime::new(256);
    pluvio_runtime::set_runtime(runtime.clone());

    // Create io_uring reactor
    // buffer_size = 1MB, need enough buffers for 32MB chunks (32 buffers per chunk)
    let uring_reactor = IoUringReactor::builder()
        .queue_size(4096)
        .buffer_size(1 << 20) // 1MB per buffer
        .submit_depth(128)
        .wait_submit_timeout(Duration::from_micros(10))
        .wait_complete_timeout(Duration::from_micros(10))
        .build();

    let allocator = uring_reactor.allocator.clone();
    runtime.register_reactor("io_uring", uring_reactor.clone());

    // Create UCX context and reactor
    // Use UCXReactor::current() to ensure worker is registered to the same reactor
    let ucx_context = Rc::new(UcxContext::new()?);
    let ucx_reactor = UCXReactor::current();
    runtime.register_reactor("ucx", ucx_reactor);

    // Register timer reactor
    let timer_reactor = TimerReactor::current();
    runtime.register_reactor("timer", timer_reactor);

    // Create UCX worker
    let worker = ucx_context.create_worker()?;
    tracing::info!("UCX worker created successfully");

    // Create connection pool and register self
    let pool = ConnectionPool::new(worker.clone(), registry_dir)?;
    pool.register_self(node_id)?;

    // Log the registry file path for debugging
    let addr_file = registry_dir.join(format!("{}.addr", node_id));
    tracing::info!(
        "Server registered at {} (address file: {}, size: {} bytes)",
        node_id,
        addr_file.display(),
        std::fs::metadata(&addr_file).map(|m| m.len()).unwrap_or(0)
    );

    // Create metadata manager
    let metadata_manager = Rc::new(MetadataManager::new(node_id.to_string()));

    // Create IOUringBackend for chunk storage
    let io_backend = Rc::new(IOUringBackend::new(allocator.clone(), uring_reactor.clone()));

    // Create chunk store
    let chunk_store_dir = data_dir.join("chunks");
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

    // Start server main loop
    let server_handle = {
        let rpc_server_clone = rpc_server.clone();
        let running_clone = running.clone();
        let runtime_clone = runtime.clone();

        pluvio_runtime::spawn_with_name(
            async move {
                tracing::info!("RPC server starting, registering handlers...");

                // Register all RPC handlers
                if let Err(e) = rpc_server_clone.register_all_handlers().await {
                    tracing::error!("Failed to register RPC handlers: {:?}", e);
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Handler registration failed: {:?}", e),
                    ));
                }

                tracing::info!("RPC handlers registered successfully, server is ready");

                // Keep server alive while running
                loop {
                    if !running_clone.load(Ordering::Acquire) {
                        break;
                    }
                    pluvio_timer::sleep(Duration::from_millis(100)).await;
                }

                // Graceful shutdown
                tracing::info!("Initiating graceful shutdown...");
                rpc_server_clone.handler_context().set_shutdown_flag();
                rpc_server_clone.shutdown_all_streams();

                let shutdown_timeout = Duration::from_millis(500);
                tracing::info!("Waiting {:?} for listener tasks to exit...", shutdown_timeout);
                pluvio_timer::sleep(shutdown_timeout).await;

                let remaining_tasks = runtime_clone.task_pool.borrow().len();
                if remaining_tasks > 1 {
                    tracing::warn!(
                        "{} tasks still running after shutdown wait",
                        remaining_tasks - 1
                    );
                    runtime_clone.request_shutdown();
                }

                tracing::info!("RPC server stopped");
                Ok::<(), std::io::Error>(())
            },
            "perf_server".to_string(),
        )
    };

    tracing::info!("Server is running (Press Ctrl+C to stop)");

    pluvio_runtime::run_with_name("benchfs_perf_server", async move {
        match server_handle.await {
            Ok(_) => {
                tracing::info!("Server shutdown complete");
            }
            Err(e) => {
                tracing::error!("Server error: {:?}", e);
            }
        }
    });

    // Chrome trace guard will flush on drop
    tracing::info!("Flushing trace file...");

    Ok(())
}

fn run_client(
    registry_dir: &PathBuf,
    server_node: &str,
    node_id: &str,
    iterations: u32,
    block_size: usize,
    timeout: u64,
    _perfetto_guard: PerfettoGuard,
) -> Result<(), Box<dyn std::error::Error>> {
    // Create registry directory if needed
    std::fs::create_dir_all(registry_dir)?;

    // Create runtime
    let runtime = Runtime::new(256);
    pluvio_runtime::set_runtime(runtime.clone());

    // Create io_uring reactor for client
    // buffer_size = 1MB to handle 32MB chunk transfers
    let uring_reactor = IoUringReactor::builder()
        .queue_size(4096)
        .buffer_size(1 << 20) // 1MB per buffer
        .submit_depth(128)
        .wait_submit_timeout(Duration::from_micros(10))
        .wait_complete_timeout(Duration::from_micros(10))
        .build();

    runtime.register_reactor("io_uring", uring_reactor);

    // Create UCX context and reactor
    // Use UCXReactor::current() to ensure worker is registered to the same reactor
    let ucx_context = Rc::new(UcxContext::new()?);
    let ucx_reactor = UCXReactor::current();
    runtime.register_reactor("ucx", ucx_reactor);

    // Register timer reactor
    let timer_reactor = TimerReactor::current();
    runtime.register_reactor("timer", timer_reactor);

    // Create UCX worker
    let worker = ucx_context.create_worker()?;

    // Create connection pool and register self
    let pool = Rc::new(ConnectionPool::new(worker.clone(), registry_dir)?);
    pool.register_self(node_id)?;

    tracing::info!("Client registered as {}", node_id);

    // Run benchmark
    let server_node = server_node.to_string();
    let iterations = iterations;
    let block_size = block_size;

    pluvio_runtime::run_with_name("benchfs_perf_client", async move {
        match run_benchmark(&pool, &server_node, iterations, block_size, timeout).await {
            Ok(()) => {
                tracing::info!("Benchmark completed successfully");
            }
            Err(e) => {
                tracing::error!("Benchmark error: {:?}", e);
            }
        }
    });

    // Chrome trace guard will flush on drop
    tracing::info!("Flushing trace file...");

    Ok(())
}

#[instrument(level = "info", skip(pool))]
async fn run_benchmark(
    pool: &Rc<ConnectionPool>,
    server_node: &str,
    iterations: u32,
    block_size: usize,
    timeout: u64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing::info!("Waiting for server {} to be available...", server_node);

    let client = pool
        .wait_and_connect(server_node, timeout)
        .await
        .map_err(|e| format!("Failed to connect to server: {:?}", e))?;

    tracing::info!("Connected to server {}", server_node);

    // Prepare test data
    let test_path = "/benchmark/test_file".to_string();
    let test_data: Vec<u8> = (0..block_size).map(|i| (i % 256) as u8).collect();
    let mut read_buffer = vec![0u8; block_size];

    // Warmup
    tracing::info!("Running warmup...");
    for i in 0..10 {
        let write_req = WriteChunkRequest::new(i as u64, 0, &test_data, test_path.clone());
        let _ = write_req.call(&client).await;
    }

    // Write benchmark
    tracing::info!("Running write benchmark ({} iterations, {} bytes each)...", iterations, block_size);
    let mut write_stats = BenchmarkStats::new();
    let write_start = Instant::now();

    for i in 0..iterations {
        let chunk_index = i as u64;
        let start = Instant::now();

        let write_req = WriteChunkRequest::new(chunk_index, 0, &test_data, test_path.clone());
        let response = write_req
            .call(&client)
            .await
            .map_err(|e| format!("Write RPC failed: {:?}", e))?;

        let latency = start.elapsed();

        if response.is_success() {
            write_stats.record(latency, response.bytes_written);
        } else {
            tracing::warn!("Write failed for chunk {}: status={}", chunk_index, response.status);
        }

        if (i + 1) % 100 == 0 {
            tracing::debug!("Write progress: {}/{}", i + 1, iterations);
        }
    }

    write_stats.finalize(write_start.elapsed());
    write_stats.print_report("Write Benchmark");

    // Read benchmark
    tracing::info!("Running read benchmark ({} iterations, {} bytes each)...", iterations, block_size);
    let mut read_stats = BenchmarkStats::new();
    let read_start = Instant::now();

    for i in 0..iterations {
        let chunk_index = i as u64;
        let start = Instant::now();

        let read_req = ReadChunkRequest::new(
            chunk_index,
            0,
            block_size as u64,
            test_path.clone(),
            &mut read_buffer,
        );
        let response = read_req
            .call(&client)
            .await
            .map_err(|e| format!("Read RPC failed: {:?}", e))?;

        let latency = start.elapsed();

        if response.is_success() {
            read_stats.record(latency, response.bytes_read);
        } else {
            tracing::warn!("Read failed for chunk {}: status={}", chunk_index, response.status);
        }

        if (i + 1) % 100 == 0 {
            tracing::debug!("Read progress: {}/{}", i + 1, iterations);
        }
    }

    read_stats.finalize(read_start.elapsed());
    read_stats.print_report("Read Benchmark");

    // Summary
    println!("\n===== Overall Summary =====");
    println!("Server:      {}", server_node);
    println!("Iterations:  {}", iterations);
    println!("Block size:  {} bytes", block_size);
    println!("Write IOPS:  {:.2}", write_stats.iops());
    println!("Read IOPS:   {:.2}", read_stats.iops());
    println!("Write BW:    {:.2} MB/s", write_stats.throughput_mbps());
    println!("Read BW:     {:.2} MB/s", read_stats.throughput_mbps());

    Ok(())
}
