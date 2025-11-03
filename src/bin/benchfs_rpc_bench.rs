//! BenchFS RPC Benchmark
//!
//! Simple benchmark program to test RPC communication without IOR or io_uring.
//! This helps isolate RPC performance issues from storage I/O.
//!
//! Usage:
//!   mpirun -n <num_nodes> benchfs_rpc_bench <registry_dir> [options]
//!
//! Roles:
//! - Rank 0: Client that sends benchmark RPCs and measures metrics
//! - Other ranks: Servers that respond to RPCs

use benchfs::rpc::bench_ops::BenchPingRequest;
use benchfs::rpc::connection::ConnectionPool;
use benchfs::rpc::handlers::RpcHandlerContext;
use benchfs::rpc::server::RpcServer;
use benchfs::rpc::AmRpc;

use pluvio_runtime::executor::Runtime;
use pluvio_ucx::{Context as UcxContext, reactor::UCXReactor};

use mpi::traits::*;

use std::path::PathBuf;
use std::rc::Rc;
use std::time::{Duration, Instant};

struct BenchmarkConfig {
    ping_iterations: usize,
    output_file: Option<String>,
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        Self {
            ping_iterations: 10000,
            output_file: None,
        }
    }
}

fn parse_args(args: &[String]) -> BenchmarkConfig {
    let mut config = BenchmarkConfig::default();

    let mut i = 2; // Skip program name and registry_dir
    while i < args.len() {
        match args[i].as_str() {
            "--ping-iterations" if i + 1 < args.len() => {
                config.ping_iterations = args[i + 1].parse().unwrap_or(10000);
                i += 2;
            }
            "--output" if i + 1 < args.len() => {
                config.output_file = Some(args[i + 1].clone());
                i += 2;
            }
            _ => {
                i += 1;
            }
        }
    }

    config
}

fn main() {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    // Initialize MPI
    let universe = mpi::initialize().unwrap();
    let world = universe.world();
    let mpi_rank = world.rank();
    let mpi_size = world.size();

    // Parse command line arguments
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        if mpi_rank == 0 {
            eprintln!("Usage: mpirun -n <num_nodes> {} <registry_dir> [options]", args[0]);
            eprintln!("  registry_dir: Shared directory for service discovery (required)");
            eprintln!("\nOptions:");
            eprintln!("  --ping-iterations N    Number of ping-pong iterations (default: 10000)");
            eprintln!("  --output FILE          Output results to JSON file");
        }
        std::process::exit(1);
    }

    let registry_dir = PathBuf::from(&args[1]);
    let config = parse_args(&args);

    // Create registry directory (rank 0 only)
    if mpi_rank == 0 {
        if !registry_dir.exists() {
            tracing::info!("Creating registry directory: {:?}", registry_dir);
            std::fs::create_dir_all(&registry_dir).unwrap();
        }
    }

    // Barrier to ensure registry dir is created
    world.barrier();

    let node_id = format!("node_{}", mpi_rank);

    if mpi_rank == 0 {
        tracing::info!("==============================================");
        tracing::info!("BenchFS RPC Benchmark");
        tracing::info!("==============================================");
        tracing::info!("Nodes: {}", mpi_size);
        tracing::info!("Registry: {:?}", registry_dir);
        tracing::info!("Ping iterations: {}", config.ping_iterations);
        tracing::info!("==============================================");
    }

    // Create pluvio runtime
    let runtime = Runtime::new(256);

    // Create UCX context and reactor
    let ucx_context = Rc::new(UcxContext::new().expect("Failed to create UCX context"));
    let ucx_reactor = UCXReactor::current();
    runtime.register_reactor("ucx", ucx_reactor.clone());

    // Create and register Timer reactor for pluvio_timer::sleep()
    let timer_reactor = pluvio_timer::TimerReactor::current();
    runtime.register_reactor("timer", timer_reactor);

    // Create UCX worker
    let worker = ucx_context
        .create_worker()
        .expect("Failed to create worker");

    // Create RPC handler context (minimal for benchmark)
    let handler_context = Rc::new(RpcHandlerContext::new_bench(worker.clone()));

    // Create RPC server
    let rpc_server = Rc::new(RpcServer::new(worker.clone(), handler_context.clone()));

    // Create connection pool
    let connection_pool = Rc::new(
        ConnectionPool::new(worker.clone(), &registry_dir)
            .expect("Failed to create connection pool")
    );

    if mpi_rank == 0 {
        // Client mode
        run_client(
            runtime,
            rpc_server,
            connection_pool,
            mpi_size,
            config,
            node_id,
        );
    } else {
        // Server mode
        run_server(
            runtime,
            rpc_server,
            connection_pool,
            node_id,
        );
    }

    // Barrier before exit
    world.barrier();

    if mpi_rank == 0 {
        tracing::info!("Benchmark completed successfully");
    }
}

fn run_client(
    runtime: Rc<Runtime>,
    rpc_server: Rc<RpcServer>,
    connection_pool: Rc<ConnectionPool>,
    mpi_size: i32,
    config: BenchmarkConfig,
    node_id: String,
) {
    let runtime_clone = runtime.clone();
    runtime_clone.run(async move {
        tracing::info!("Client starting...");

        // Note: Client does not need to register handlers as it only sends requests,
        // not receives them. This avoids having idle handler tasks that need cleanup.

        // Register self in connection pool
        if let Err(e) = connection_pool.register_self(&node_id) {
            tracing::error!("Failed to register self: {:?}", e);
            return;
        }

        // Wait for all servers to register
        tracing::info!("Waiting for all servers to register...");
        let mut registered_count = 0;
        while registered_count < mpi_size {
            registered_count = 0;
            for rank in 0..mpi_size {
                let other_node_id = format!("node_{}", rank);
                let registry_file = connection_pool.registry_dir().join(format!("{}.addr", other_node_id));
                if registry_file.exists() {
                    registered_count += 1;
                }
            }
            if registered_count < mpi_size {
                pluvio_timer::sleep(Duration::from_millis(100)).await;
            }
        }

        tracing::info!("All servers registered. Starting benchmark...");

        // Run ping-pong benchmark with server nodes (rank 1+)
        let server_nodes: Vec<String> = (1..mpi_size)
            .map(|rank| format!("node_{}", rank))
            .collect();

        run_ping_benchmark(
            &connection_pool,
            &server_nodes,
            config.ping_iterations,
        ).await;

        // Send shutdown RPC to all servers
        tracing::info!("Sending shutdown requests to all servers...");
        send_shutdown_to_servers(&connection_pool, &server_nodes).await;

        tracing::info!("Client finished");
    });
}

fn run_server(
    runtime: Rc<Runtime>,
    rpc_server: Rc<RpcServer>,
    connection_pool: Rc<ConnectionPool>,
    node_id: String,
) {
    let runtime_clone = runtime.clone();
    runtime_clone.run(async move {
        tracing::info!("Server starting...");

        // Register handlers
        if let Err(e) = rpc_server.register_bench_handlers(runtime.clone()).await {
            tracing::error!("Failed to register handlers: {:?}", e);
            return;
        }

        // Register self in connection pool
        if let Err(e) = connection_pool.register_self(&node_id) {
            tracing::error!("Failed to register self: {:?}", e);
            return;
        }

        tracing::info!("Server ready and waiting for requests...");

        // Server loop - keep running until shutdown is requested
        loop {
            // Check shutdown flag
            if rpc_server.handler_context().should_shutdown() {
                tracing::info!("Shutdown requested, exiting server loop...");
                break;
            }

            pluvio_timer::sleep(Duration::from_millis(100)).await;
        }

        tracing::info!("Server shutting down gracefully");
    });
}

async fn run_ping_benchmark(
    pool: &ConnectionPool,
    server_nodes: &[String],
    iterations: usize,
) {
    tracing::info!("========================================== ");
    tracing::info!("Ping-Pong Benchmark");
    tracing::info!("==========================================");
    tracing::info!("Iterations: {}", iterations);
    tracing::info!("Servers: {}", server_nodes.len());

    for server_node in server_nodes {
        tracing::info!("------------------------------------------");
        tracing::info!("Testing server: {}", server_node);

        // Connect to server
        let client = match pool.get_or_connect(server_node).await {
            Ok(client) => client,
            Err(e) => {
                tracing::error!("Failed to connect to {}: {:?}", server_node, e);
                continue;
            }
        };

        let mut latencies = Vec::with_capacity(iterations);

        // Warm-up
        for seq in 0..100 {
            let timestamp_ns = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;

            let request = BenchPingRequest::new(
                seq,
                timestamp_ns,
                client.worker_address().to_vec(),
            );

            if let Err(e) = request.call(&*client).await {
                tracing::warn!("Warm-up ping failed: {:?}", e);
            }
        }

        // Actual benchmark
        for seq in 0..iterations {
            let timestamp_ns = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;

            let request = BenchPingRequest::new(
                seq as u64,
                timestamp_ns,
                client.worker_address().to_vec(),
            );

            let start = Instant::now();
            match request.call(&*client).await {
                Ok(_response) => {
                    let latency = start.elapsed();
                    latencies.push(latency);
                }
                Err(e) => {
                    tracing::error!("Ping {} failed: {:?}", seq, e);
                }
            }

            // Progress indicator every 1000 iterations
            if (seq + 1) % 1000 == 0 {
                tracing::debug!("Completed {} / {} pings", seq + 1, iterations);
            }
        }

        // Calculate statistics
        if !latencies.is_empty() {
            latencies.sort();
            let min = latencies[0];
            let max = latencies[latencies.len() - 1];
            let avg: Duration = latencies.iter().sum::<Duration>() / latencies.len() as u32;
            let p50 = latencies[latencies.len() / 2];
            let p99 = latencies[latencies.len() * 99 / 100];

            tracing::info!("Results for {}:", server_node);
            tracing::info!("  Min latency:  {:?}", min);
            tracing::info!("  Avg latency:  {:?}", avg);
            tracing::info!("  P50 latency:  {:?}", p50);
            tracing::info!("  P99 latency:  {:?}", p99);
            tracing::info!("  Max latency:  {:?}", max);
        }
    }

    tracing::info!("==========================================");
    tracing::info!("Ping-Pong Benchmark Complete");
    tracing::info!("==========================================");
}

async fn send_shutdown_to_servers(
    pool: &ConnectionPool,
    server_nodes: &[String],
) {
    use benchfs::rpc::bench_ops::{BenchShutdownRequest, BenchPingRequest};
    use benchfs::rpc::AmRpc;

    tracing::info!("Sending shutdown to {} servers", server_nodes.len());

    for server_node in server_nodes {
        tracing::info!("Sending shutdown to {}", server_node);

        // Connect to server
        let client = match pool.get_or_connect(server_node).await {
            Ok(client) => client,
            Err(e) => {
                tracing::error!("Failed to connect to {} for shutdown: {:?}", server_node, e);
                continue;
            }
        };

        // Send shutdown request
        let request = BenchShutdownRequest::new(client.worker_address().to_vec());

        match request.call(&*client).await {
            Ok(response) => {
                if response.success == 1 {
                    tracing::info!("Server {} acknowledged shutdown", server_node);
                } else {
                    tracing::warn!("Server {} rejected shutdown", server_node);
                }
            }
            Err(e) => {
                tracing::error!("Failed to send shutdown to {}: {:?}", server_node, e);
            }
        }
    }

    // Give servers time to process shutdown
    pluvio_timer::sleep(Duration::from_millis(100)).await;

    // Send wake-up pings to unblock BenchPing handlers waiting on wait_msg()
    // This allows them to check the shutdown flag and exit gracefully
    tracing::debug!("Sending wake-up pings to unblock handlers...");
    for server_node in server_nodes {
        if let Ok(client) = pool.get_or_connect(server_node).await {
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;

            let wake_up_ping = BenchPingRequest::new(0, timestamp, client.worker_address().to_vec());

            // Send ping but ignore response (server is shutting down)
            let _ = wake_up_ping.call(&*client).await;
        }
    }

    // Give handlers time to wake up and check shutdown flag
    pluvio_timer::sleep(Duration::from_millis(200)).await;

    tracing::info!("Shutdown requests sent to all servers");
}
