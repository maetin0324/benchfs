//! BenchFS RPC Benchmark
//!
//! Simple benchmark program to test RPC communication without IOR or io_uring.
//! This helps isolate RPC performance issues from storage I/O.
//!
//! Usage:
//!   mpirun -n <num_nodes> benchfs_rpc_bench <registry_dir> [options]
//!
//! Roles (assuming N nodes with N/2 servers and N/2 clients):
//! - Ranks 0 to N/2-1: Servers that respond to RPCs
//! - Ranks N/2 to N-1: Clients that send benchmark RPCs
//! - Each client connects to a different server (round-robin mapping)
//! - All client results are aggregated to compute total IOPS

use benchfs::rpc::AmRpc;
use benchfs::rpc::bench_ops::BenchPingRequest;
use benchfs::rpc::connection::ConnectionPool;
use benchfs::rpc::handlers::RpcHandlerContext;
use benchfs::rpc::server::RpcServer;

use pluvio_runtime::executor::Runtime;
use pluvio_ucx::{Context as UcxContext, reactor::UCXReactor};

use mpi::traits::*;

use std::path::PathBuf;
use std::rc::Rc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct LatencyStats {
    min_us: f64,
    avg_us: f64,
    p50_us: f64,
    p99_us: f64,
    max_us: f64,
}

#[derive(Debug, Serialize, Deserialize)]
struct ClientResult {
    client_rank: i32,
    server_node: String,
    operations: usize,
    duration_secs: f64,
    iops: f64,
    miops: f64,
    latency: LatencyStats,
}

#[derive(Debug, Serialize, Deserialize)]
struct BenchmarkResults {
    timestamp: String,
    total_servers: usize,
    total_clients: usize,
    ping_iterations: usize,
    client_results: Vec<ClientResult>,
    aggregate_iops: f64,
    aggregate_miops: f64,
}

struct BenchmarkConfig {
    ping_iterations: usize,
    json_output: Option<String>,
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        Self {
            ping_iterations: 10000,
            json_output: None,
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
            "--output" | "--json-output" if i + 1 < args.len() => {
                config.json_output = Some(args[i + 1].clone());
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

    // Validate that we have even number of ranks
    if mpi_size % 2 != 0 {
        if mpi_rank == 0 {
            eprintln!(
                "ERROR: This benchmark requires an even number of MPI ranks (got {})",
                mpi_size
            );
            eprintln!("Half will be servers, half will be clients");
        }
        std::process::exit(1);
    }

    // Parse command line arguments
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        if mpi_rank == 0 {
            eprintln!(
                "Usage: mpirun -n <num_nodes> {} <registry_dir> [options]",
                args[0]
            );
            eprintln!("  registry_dir: Shared directory for service discovery (required)");
            eprintln!("\nOptions:");
            eprintln!("  --ping-iterations N    Number of ping-pong iterations (default: 10000)");
            eprintln!("  --output FILE          Output results to JSON file");
            eprintln!(
                "\nNote: Requires even number of MPI ranks. First half are servers, second half are clients."
            );
        }
        std::process::exit(1);
    }

    let registry_dir = PathBuf::from(&args[1]);
    let config = parse_args(&args);

    // Log parsed configuration (rank 0 only)
    if mpi_rank == 0 {
        tracing::info!("Command line arguments:");
        for (i, arg) in args.iter().enumerate() {
            tracing::info!("  args[{}] = {}", i, arg);
        }
        tracing::info!("Parsed configuration:");
        tracing::info!("  ping_iterations: {}", config.ping_iterations);
        tracing::info!("  json_output: {:?}", config.json_output);
    }

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

    // Determine server and client groups
    let num_servers = mpi_size / 2;
    let num_clients = mpi_size / 2;
    let is_server = mpi_rank < num_servers;
    let first_client_rank = num_servers;

    if mpi_rank == 0 {
        tracing::info!("==============================================");
        tracing::info!("BenchFS RPC Benchmark");
        tracing::info!("==============================================");
        tracing::info!("Total ranks: {}", mpi_size);
        tracing::info!(
            "Server ranks: 0-{} ({} servers)",
            num_servers - 1,
            num_servers
        );
        tracing::info!(
            "Client ranks: {}-{} ({} clients)",
            num_servers,
            mpi_size - 1,
            num_clients
        );
        tracing::info!("Registry: {:?}", registry_dir);
        tracing::info!("Ping iterations: {}", config.ping_iterations);
        if let Some(ref json_path) = config.json_output {
            tracing::info!("JSON output: {}", json_path);
        } else {
            tracing::info!("JSON output: <not specified>");
        }
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
    let handler_context = Rc::new(RpcHandlerContext::new_bench());

    // Create RPC server
    let rpc_server = Rc::new(RpcServer::new(worker.clone(), handler_context.clone()));

    // Create connection pool
    let connection_pool = Rc::new(
        ConnectionPool::new(worker.clone(), &registry_dir)
            .expect("Failed to create connection pool"),
    );

    if is_server {
        // Server mode
        run_server(
            runtime,
            rpc_server,
            connection_pool,
            node_id,
            &world,
            mpi_rank,
        );
    } else {
        // Client mode - each client connects to a specific server
        let client_idx = mpi_rank - num_servers;
        let target_server_rank = client_idx % num_servers;
        let target_server_id = format!("node_{}", target_server_rank);

        tracing::info!(
            "Client {} (rank {}) will connect to server {} (rank {})",
            client_idx,
            mpi_rank,
            target_server_id,
            target_server_rank
        );

        run_client(
            runtime,
            rpc_server,
            connection_pool,
            mpi_rank,
            num_servers,
            num_clients,
            first_client_rank,
            target_server_id,
            config,
            node_id,
            &world,
        );
    }

    if mpi_rank == 0 {
        tracing::info!("Benchmark completed successfully");
    }
}

fn run_client(
    runtime: Rc<Runtime>,
    _rpc_server: Rc<RpcServer>,
    connection_pool: Rc<ConnectionPool>,
    mpi_rank: i32,
    num_servers: i32,
    num_clients: i32,
    first_client_rank: i32,
    target_server_id: String,
    config: BenchmarkConfig,
    node_id: String,
    world: &mpi::topology::SimpleCommunicator,
) {
    let runtime_clone = runtime.clone();

    // Clone world for async use
    let is_root_client = mpi_rank == first_client_rank;

    // Use std::cell::RefCell to store result across async boundary
    use std::cell::RefCell;
    let result_cell: Rc<RefCell<Option<ClientResult>>> = Rc::new(RefCell::new(None));
    let result_cell_clone = result_cell.clone();

    // Clone connection_pool for later use in shutdown
    let connection_pool_for_shutdown = connection_pool.clone();

    runtime_clone.run(async move {
        tracing::info!("Client starting...");

        // Note: Client does not need to register handlers as it only sends requests,
        // not receives them. This avoids having idle handler tasks that need cleanup.

        // Register self in connection pool
        if let Err(e) = connection_pool.register_self(&node_id) {
            tracing::error!("Failed to register self: {:?}", e);
            return;
        }

        // Wait for all nodes (servers + clients) to register
        tracing::info!("Waiting for all nodes to register...");
        let total_ranks = num_servers + num_clients;
        let mut registered_count = 0;
        while registered_count < total_ranks {
            registered_count = 0;
            for rank in 0..total_ranks {
                let other_node_id = format!("node_{}", rank);
                let registry_file = connection_pool
                    .registry_dir()
                    .join(format!("{}.addr", other_node_id));
                if registry_file.exists() {
                    registered_count += 1;
                }
            }
            if registered_count < total_ranks {
                pluvio_timer::sleep(Duration::from_millis(100)).await;
            }
        }

        tracing::info!("All nodes registered. Starting benchmark...");

        // Run ping-pong benchmark with assigned server
        let my_result = run_ping_benchmark_single(
            &connection_pool,
            &target_server_id,
            config.ping_iterations,
            mpi_rank,
        )
        .await;

        tracing::info!(
            "Local benchmark completed. IOPS: {:.2}, MIOPS: {:.6}",
            my_result.iops,
            my_result.miops
        );

        // Store result for MPI communication
        *result_cell_clone.borrow_mut() = Some(my_result);

        tracing::info!("Client finished");
    });

    // Extract result from cell
    let my_result = result_cell.borrow_mut().take().expect("Result not set");

    // IMPORTANT: Send shutdown to servers BEFORE MPI operations
    // This ensures servers can exit their async loop and participate in final barrier
    if is_root_client {
        tracing::info!("Sending shutdown to all servers...");
        let runtime_shutdown = Runtime::new(256);
        runtime_shutdown.run(async move {
            let server_nodes: Vec<String> = (0..num_servers)
                .map(|rank| format!("node_{}", rank))
                .collect();
            send_shutdown_to_servers(&connection_pool_for_shutdown, &server_nodes).await;
            tracing::info!("Shutdown requests sent to all servers");
        });
    }

    // Wait for all clients to finish and servers to receive shutdown
    world.barrier();

    tracing::info!("Starting result aggregation...");

    // Serialize local result for MPI transfer
    let my_result_json = serde_json::to_string(&my_result).expect("Failed to serialize result");
    let my_result_bytes = my_result_json.as_bytes();

    // First, gather the sizes of each result
    let my_size = my_result_bytes.len() as i32;
    let mut sizes = if is_root_client {
        vec![0i32; num_clients as usize]
    } else {
        vec![]
    };

    if is_root_client {
        tracing::info!(
            "Root client (rank {}) gathering result sizes from {} clients...",
            mpi_rank,
            num_clients
        );
    }

    // Gather sizes at first client rank
    if is_root_client {
        world
            .process_at_rank(mpi_rank)
            .gather_into_root(&my_size, &mut sizes[..]);
    } else {
        world
            .process_at_rank(first_client_rank)
            .gather_into(&my_size);
    }

    // Prepare receive buffer
    let mut all_results: Vec<ClientResult> = Vec::new();

    if is_root_client {
        tracing::info!("Gathering results from all clients...");

        // Gather results one by one
        for client_idx in 0..num_clients {
            let client_rank = first_client_rank + client_idx;
            let size = sizes[client_idx as usize] as usize;

            let result_bytes = if client_rank == mpi_rank {
                // Own result
                my_result_bytes.to_vec()
            } else {
                // Receive from other client
                let mut buf = vec![0u8; size];
                world
                    .process_at_rank(client_rank)
                    .receive_into(&mut buf[..]);
                buf
            };

            let result_json = String::from_utf8(result_bytes).expect("Invalid UTF-8");
            let result: ClientResult =
                serde_json::from_str(&result_json).expect("Failed to deserialize");
            all_results.push(result);
        }

        // Calculate aggregate metrics
        let total_iops: f64 = all_results.iter().map(|r| r.iops).sum();
        let total_miops = total_iops / 1_000_000.0;

        tracing::info!("==========================================");
        tracing::info!("Aggregate Results");
        tracing::info!("==========================================");
        tracing::info!("Total clients: {}", num_clients);
        tracing::info!("Total servers: {}", num_servers);
        tracing::info!("Aggregate IOPS: {:.2}", total_iops);
        tracing::info!("Aggregate MIOPS: {:.6}", total_miops);
        tracing::info!("==========================================");

        // Write JSON output if requested
        if let Some(json_path) = &config.json_output {
            tracing::info!("Writing aggregate results to JSON: {}", json_path);

            let timestamp = chrono::Utc::now().to_rfc3339();
            let results = BenchmarkResults {
                timestamp,
                total_servers: num_servers as usize,
                total_clients: num_clients as usize,
                ping_iterations: config.ping_iterations,
                client_results: all_results,
                aggregate_iops: total_iops,
                aggregate_miops: total_miops,
            };

            match serde_json::to_string_pretty(&results) {
                Ok(json_str) => {
                    if let Err(e) = std::fs::write(json_path, json_str) {
                        tracing::error!("Failed to write JSON: {:?}", e);
                        eprintln!("ERROR: Failed to write JSON: {:?}", e);
                    } else {
                        tracing::info!("JSON results written successfully");
                        eprintln!("SUCCESS: JSON results written to {}", json_path);
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to serialize: {:?}", e);
                    eprintln!("ERROR: Failed to serialize: {:?}", e);
                }
            }
        }
    } else {
        // Non-root clients send their results
        world
            .process_at_rank(first_client_rank)
            .send(my_result_bytes);
    }

    // Final barrier to ensure all clients have completed MPI operations
    world.barrier();

    tracing::info!("Client rank {} exiting", mpi_rank);
}

fn run_server(
    runtime: Rc<Runtime>,
    rpc_server: Rc<RpcServer>,
    connection_pool: Rc<ConnectionPool>,
    node_id: String,
    world: &mpi::topology::SimpleCommunicator,
    mpi_rank: i32,
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

    // After async runtime exits, participate in synchronization barriers
    // First barrier: Wait for all clients to finish and send shutdown
    tracing::info!("Server rank {} waiting at first barrier...", mpi_rank);
    world.barrier();

    // Second barrier: Wait for all clients to complete result aggregation
    tracing::info!("Server rank {} waiting at second barrier...", mpi_rank);
    world.barrier();

    tracing::info!("Server rank {} exiting", mpi_rank);
}

// Run ping benchmark for a single client against one server
async fn run_ping_benchmark_single(
    pool: &ConnectionPool,
    server_node: &str,
    iterations: usize,
    client_rank: i32,
) -> ClientResult {
    tracing::info!("------------------------------------------");
    tracing::info!("Client {} testing server: {}", client_rank, server_node);

    // Connect to server
    let client = match pool.get_or_connect(server_node).await {
        Ok(client) => client,
        Err(e) => {
            tracing::error!("Failed to connect to {}: {:?}", server_node, e);
            // Return dummy result on connection failure
            return ClientResult {
                client_rank,
                server_node: server_node.to_string(),
                operations: 0,
                duration_secs: 0.0,
                iops: 0.0,
                miops: 0.0,
                latency: LatencyStats {
                    min_us: 0.0,
                    avg_us: 0.0,
                    p50_us: 0.0,
                    p99_us: 0.0,
                    max_us: 0.0,
                },
            };
        }
    };

    let mut latencies = Vec::with_capacity(iterations);

    // Warm-up
    for seq in 0..100 {
        let timestamp_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;

        let request = BenchPingRequest::new(seq, timestamp_ns);

        if let Err(e) = request.call(&*client).await {
            tracing::warn!("Warm-up ping failed: {:?}", e);
        }
    }

    // Actual benchmark
    let benchmark_start = Instant::now();
    for seq in 0..iterations {
        let timestamp_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;

        let request = BenchPingRequest::new(seq as u64, timestamp_ns);

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
    let benchmark_duration = benchmark_start.elapsed();

    // Calculate statistics
    if !latencies.is_empty() {
        latencies.sort();
        let min = latencies[0];
        let max = latencies[latencies.len() - 1];
        let avg: Duration = latencies.iter().sum::<Duration>() / latencies.len() as u32;
        let p50 = latencies[latencies.len() / 2];
        let p99 = latencies[latencies.len() * 99 / 100];

        // Calculate IOPS (operations per second)
        let total_ops = latencies.len() as f64;
        let duration_secs = benchmark_duration.as_secs_f64();
        let iops = total_ops / duration_secs;
        let miops = iops / 1_000_000.0;

        tracing::info!("Results for client {} -> {}:", client_rank, server_node);
        tracing::info!("  Operations:   {}", latencies.len());
        tracing::info!("  Duration:     {:.3}s", duration_secs);
        tracing::info!("  IOPS:         {:.0}", iops);
        tracing::info!("  MIOPS:        {:.3}", miops);
        tracing::info!("  Min latency:  {:?}", min);
        tracing::info!("  Avg latency:  {:?}", avg);
        tracing::info!("  P50 latency:  {:?}", p50);
        tracing::info!("  P99 latency:  {:?}", p99);
        tracing::info!("  Max latency:  {:?}", max);

        ClientResult {
            client_rank,
            server_node: server_node.to_string(),
            operations: latencies.len(),
            duration_secs,
            iops,
            miops,
            latency: LatencyStats {
                min_us: min.as_micros() as f64,
                avg_us: avg.as_micros() as f64,
                p50_us: p50.as_micros() as f64,
                p99_us: p99.as_micros() as f64,
                max_us: max.as_micros() as f64,
            },
        }
    } else {
        // No latencies collected
        ClientResult {
            client_rank,
            server_node: server_node.to_string(),
            operations: 0,
            duration_secs: 0.0,
            iops: 0.0,
            miops: 0.0,
            latency: LatencyStats {
                min_us: 0.0,
                avg_us: 0.0,
                p50_us: 0.0,
                p99_us: 0.0,
                max_us: 0.0,
            },
        }
    }
}

async fn send_shutdown_to_servers(pool: &ConnectionPool, server_nodes: &[String]) {
    use benchfs::rpc::AmRpc;
    use benchfs::rpc::bench_ops::{BenchPingRequest, BenchShutdownRequest};

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
        let request = BenchShutdownRequest::new();

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

            let wake_up_ping = BenchPingRequest::new(0, timestamp);

            // Send ping but ignore response (server is shutting down)
            let _ = wake_up_ping.call(&*client).await;
        }
    }

    // Give handlers time to wake up and check shutdown flag
    pluvio_timer::sleep(Duration::from_millis(200)).await;

    tracing::info!("Shutdown requests sent to all servers");
}
