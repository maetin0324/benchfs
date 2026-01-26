//! Initialization and finalization for C FFI
//!
//! This module provides the lifecycle management functions for BenchFS:
//! - [`benchfs_init`]: Initialize a BenchFS instance (client or server)
//! - [`benchfs_finalize`]: Clean up and destroy a BenchFS instance
//!
//! These functions are the entry and exit points for C applications (like IOR)
//! using BenchFS.

use std::ffi::CStr;
use std::os::raw::c_char;
use std::rc::Rc;

use super::error::*;
use super::runtime::{
    block_on_with_name, get_node_id, set_benchfs_ctx, set_connection_pool, set_node_id,
    set_rpc_server, set_runtime,
};
use crate::api::file_ops::BenchFS;
use crate::metadata::MetadataManager;
use crate::rpc::connection::ConnectionPool;
use crate::rpc::handlers::RpcHandlerContext;
use crate::rpc::server::RpcServer;
use crate::storage::{IOUringBackend, IOUringChunkStore};

use pluvio_runtime::executor::Runtime;
use pluvio_timer::TimerReactor;
use pluvio_ucx::{Context as UcxContext, reactor::UCXReactor};
use pluvio_uring::reactor::IoUringReactor;

/// Discover available nodes from registry directory
///
/// This function scans the registry directory for `node_*.addr` files
/// and extracts the node IDs from the filenames.
///
/// # Arguments
///
/// * `registry_dir` - Path to the registry directory
///
/// # Returns
///
/// * `Ok(Vec<String>)` - List of discovered node IDs (e.g., ["node_0", "node_1", ...])
/// * `Err(String)` - Error message if discovery fails
///
/// # Environment Variables
///
/// * `BENCHFS_EXPECTED_NODES` - Expected number of data nodes. If set, the function will
///   wait until this many nodes are registered before returning. This prevents load imbalance
///   issues when servers take different amounts of time to start (e.g., due to large buffer
///   allocations with small chunk sizes).
fn discover_data_nodes(registry_dir: &str) -> Result<Vec<String>, String> {
    use std::fs;
    use std::path::Path;
    use std::thread;
    use std::time::{Duration, Instant};

    let registry_path = Path::new(registry_dir);
    if !registry_path.exists() {
        return Err(format!(
            "Registry directory does not exist: {}",
            registry_dir
        ));
    }

    // Check if expected node count is specified via environment variable
    let expected_nodes: Option<usize> = std::env::var("BENCHFS_EXPECTED_NODES")
        .ok()
        .and_then(|s| s.parse().ok());

    if let Some(expected) = expected_nodes {
        tracing::info!(
            "BENCHFS_EXPECTED_NODES={}: Will wait for all {} nodes to register",
            expected,
            expected
        );
    }

    // Extended retry logic when waiting for expected nodes
    // With expected_nodes: wait up to 60 seconds with 500ms intervals
    // Without expected_nodes: use original 5 retries with 100ms intervals
    let max_wait_secs = if expected_nodes.is_some() { 60 } else { 1 };
    let retry_delay = Duration::from_millis(if expected_nodes.is_some() { 500 } else { 100 });
    let start_time = Instant::now();

    loop {
        let entries = match fs::read_dir(registry_path) {
            Ok(e) => e,
            Err(err) => {
                if start_time.elapsed().as_secs() < max_wait_secs {
                    tracing::debug!(
                        "Failed to read registry: {}. Retrying in {:?}...",
                        err,
                        retry_delay
                    );
                    thread::sleep(retry_delay);
                    continue;
                }
                return Err(format!(
                    "Failed to read registry directory {} after {} seconds: {}",
                    registry_dir, max_wait_secs, err
                ));
            }
        };

        let mut node_ids = Vec::new();

        for entry in entries {
            let entry = match entry {
                Ok(e) => e,
                Err(_) => continue, // Skip unreadable entries
            };

            let filename = entry.file_name();
            let filename_str = match filename.to_str() {
                Some(s) => s,
                None => continue, // Skip non-UTF8 filenames
            };

            // Match pattern: node_*.addr
            if filename_str.starts_with("node_") && filename_str.ends_with(".addr") {
                // Extract node ID: "node_0.addr" -> "node_0"
                let node_id = &filename_str[..filename_str.len() - 5]; // Remove ".addr" suffix
                node_ids.push(node_id.to_string());
            }
        }

        if !node_ids.is_empty() {
            // Sort node IDs numerically (e.g., node_0, node_1, ..., node_10, node_11)
            // Alphabetical sort would produce: node_0, node_1, node_10, node_11, ..., node_2
            // which causes uneven chunk distribution with koyama_hash
            node_ids.sort_by(|a, b| {
                // Extract numeric suffix from node_N format
                let extract_num = |s: &str| -> Option<u32> {
                    s.strip_prefix("node_").and_then(|n| n.parse().ok())
                };
                match (extract_num(a), extract_num(b)) {
                    (Some(na), Some(nb)) => na.cmp(&nb),
                    _ => a.cmp(b), // Fallback to alphabetical for non-standard names
                }
            });

            // If expected_nodes is set, wait until we have that many nodes
            if let Some(expected) = expected_nodes {
                if node_ids.len() >= expected {
                    tracing::info!(
                        "Discovered all {} expected data nodes: {:?} (waited {:.1}s)",
                        node_ids.len(),
                        node_ids,
                        start_time.elapsed().as_secs_f64()
                    );
                    return Ok(node_ids);
                } else if start_time.elapsed().as_secs() < max_wait_secs {
                    tracing::debug!(
                        "Found {}/{} nodes so far, waiting for more... (elapsed: {:.1}s)",
                        node_ids.len(),
                        expected,
                        start_time.elapsed().as_secs_f64()
                    );
                    thread::sleep(retry_delay);
                    continue;
                } else {
                    // Timeout reached but not all nodes found - proceed with what we have
                    tracing::warn!(
                        "Timeout waiting for {} nodes. Proceeding with {} nodes: {:?}",
                        expected,
                        node_ids.len(),
                        node_ids
                    );
                    return Ok(node_ids);
                }
            } else {
                // No expected_nodes set - return as soon as we find any nodes
                tracing::info!(
                    "Discovered {} data nodes: {:?}",
                    node_ids.len(),
                    node_ids
                );
                return Ok(node_ids);
            }
        }

        // No nodes found yet
        if start_time.elapsed().as_secs() < max_wait_secs {
            tracing::debug!(
                "No nodes found yet, retrying... (elapsed: {:.1}s)",
                start_time.elapsed().as_secs_f64()
            );
            thread::sleep(retry_delay);
        } else {
            break;
        }
    }

    Err(format!(
        "No data nodes found in registry directory: {} after {} seconds",
        registry_dir, max_wait_secs
    ))
}

/// Opaque BenchFS context type for C code
///
/// This type prevents C code from accessing the internal Rust structure.
/// It is a zero-sized type that acts as an opaque handle.
#[repr(C)]
pub struct benchfs_context_t {
    _private: [u8; 0],
}

/// Initialize a BenchFS instance (client or server mode)
///
/// This is the primary entry point for C applications using BenchFS. It creates
/// and initializes all necessary components including:
///
/// - Async runtime with io_uring and UCX reactors
/// - Metadata manager for file/directory tracking
/// - Chunk store for local data storage
/// - RPC server (server mode) or connection pool (distributed mode)
/// - BenchFS filesystem instance
///
/// The function operates in two modes:
///
/// ## Server Mode (`is_server = 1`)
///
/// Creates a BenchFS server that:
/// - Listens for incoming RPC requests from clients
/// - Stores metadata and data chunks locally
/// - Registers its worker address in the registry directory
/// - Requires `data_dir` to be specified
///
/// ## Client Mode (`is_server = 0`)
///
/// Creates a BenchFS client that:
/// - Connects to remote servers for metadata and data operations
/// - Uses the registry directory for service discovery
/// - Waits for server to be available (30 second timeout)
/// - Uses temporary directory if `data_dir` is NULL
///
/// # Arguments
///
/// * `node_id` - Unique identifier for this node (e.g., "client_1", "node_0")
///   - Must be valid UTF-8
///   - Used for consistent hashing and service discovery
/// * `registry_dir` - Path to shared directory for worker address registry
///   - Must be accessible by all nodes (e.g., on a shared filesystem)
///   - Required for distributed mode service discovery
/// * `data_dir` - Path to local data storage directory
///   - Required for server mode
///   - Optional for client mode (defaults to `/tmp/benchfs_client_{node_id}`)
///   - Used for storing chunk data and metadata
/// * `is_server` - Mode flag: 1 for server, 0 for client
///
/// # Returns
///
/// * Pointer to opaque `benchfs_context_t` on success
/// * `NULL` on error (call [`benchfs_get_error`] for details)
///
/// # Safety
///
/// - `node_id` and `registry_dir` must be valid, null-terminated C strings
/// - `data_dir` may be `NULL` (only for clients)
/// - All string pointers must point to valid UTF-8 data
/// - The returned pointer must be freed with [`benchfs_finalize`]
/// - Do not use the returned pointer after calling [`benchfs_finalize`]
///
/// # Example (C)
///
/// ```c
/// // Initialize server
/// benchfs_context_t* server = benchfs_init(
///     "node_0",
///     "/tmp/benchfs_registry",
///     "/mnt/storage/benchfs",
///     1  // is_server = 1
/// );
///
/// if (!server) {
///     fprintf(stderr, "Server init failed: %s\n", benchfs_get_error());
///     return -1;
/// }
///
/// // Initialize client
/// benchfs_context_t* client = benchfs_init(
///     "client_1",
///     "/tmp/benchfs_registry",
///     NULL,  // Use default temp directory
///     0      // is_server = 0
/// );
///
/// if (!client) {
///     fprintf(stderr, "Client init failed: %s\n", benchfs_get_error());
///     return -1;
/// }
/// ```
///
/// # Errors
///
/// Returns `NULL` and sets error message if:
/// - `node_id` or `registry_dir` is `NULL`
/// - String parameters contain invalid UTF-8
/// - `data_dir` is `NULL` in server mode
/// - UCX context or worker creation fails
/// - Chunk store directory creation fails
/// - Server registration fails
/// - Client cannot connect to server (timeout after 30 seconds)
/// Initialize a BenchFS context
///
/// # Arguments
///
/// * `node_id` - Node identifier (unique per process)
/// * `registry_dir` - Directory for service discovery
/// * `data_dir` - Data storage directory (required for server, optional for client)
/// * `is_server` - Non-zero for server mode, zero for client mode
/// * `chunk_size` - Chunk size in bytes (0 to use default 4MiB)
///
/// # Returns
///
/// Pointer to initialized context, or NULL on error
#[unsafe(no_mangle)]
pub extern "C" fn benchfs_init(
    node_id: *const c_char,
    registry_dir: *const c_char,
    data_dir: *const c_char,
    is_server: i32,
    chunk_size: usize,
) -> *mut benchfs_context_t {
    // Use default chunk size if 0 is passed
    let chunk_size = if chunk_size == 0 {
        crate::metadata::CHUNK_SIZE
    } else {
        chunk_size
    };
    crate::logging::init_with_hostname("info");

    // Validate pointers
    if node_id.is_null() || registry_dir.is_null() {
        set_error_message("node_id and registry_dir must not be null");
        return std::ptr::null_mut();
    }

    // Convert C strings to Rust strings
    let node_id_str = unsafe {
        match CStr::from_ptr(node_id).to_str() {
            Ok(s) => s,
            Err(_) => {
                set_error_message("Invalid UTF-8 in node_id");
                return std::ptr::null_mut();
            }
        }
    };

    let registry_dir_str = unsafe {
        match CStr::from_ptr(registry_dir).to_str() {
            Ok(s) => s,
            Err(_) => {
                set_error_message("Invalid UTF-8 in registry_dir");
                return std::ptr::null_mut();
            }
        }
    };

    let data_dir_str = if data_dir.is_null() {
        None
    } else {
        unsafe {
            match CStr::from_ptr(data_dir).to_str() {
                Ok(s) => Some(s),
                Err(_) => {
                    set_error_message("Invalid UTF-8 in data_dir");
                    return std::ptr::null_mut();
                }
            }
        }
    };

    tracing::info!(
        "benchfs_init: node_id={}, is_server={}, registry_dir={}, data_dir={:?}",
        node_id_str,
        is_server != 0,
        registry_dir_str,
        data_dir_str
    );

    // Create BenchFS instance based on mode
    let benchfs = if is_server != 0 {
        // ===== SERVER MODE =====
        tracing::info!(
            "Initializing BenchFS server: node_id={}, registry_dir={}, data_dir={:?}",
            node_id_str,
            registry_dir_str,
            data_dir_str
        );

        // Require data_dir for server
        let data_dir = match data_dir_str {
            Some(d) => d,
            None => {
                set_error_message("data_dir is required for server mode");
                return std::ptr::null_mut();
            }
        };

        // Create runtime (Runtime::new() returns Rc<Runtime>)
        let runtime = Runtime::new(256);
        set_runtime(runtime.clone());

        // Register timer reactor (required for Delay/timeout futures)
        let timer_reactor = TimerReactor::current();
        runtime.register_reactor("timer", timer_reactor.clone());

        // Create io_uring reactor
        tracing::info!("Starting IoUringReactor initialization...");
        let start = std::time::Instant::now();

        tracing::info!("Configuring io_uring with buffer_size={} bytes ({} MiB)",
            chunk_size, chunk_size / (1024 * 1024));
        let uring_reactor = IoUringReactor::builder()
            .queue_size(32) // Reduced to prevent kernel resource contention
            .buffer_size(chunk_size) // Match chunk_size from config
            .submit_depth(16) // Reduced from 128
            .wait_submit_timeout(std::time::Duration::from_micros(1))
            .wait_complete_timeout(std::time::Duration::from_micros(1))
            .build();

        tracing::info!(
            "IoUringReactor initialization completed in {:?}",
            start.elapsed()
        );

        let allocator = uring_reactor.allocator.clone();
        let reactor_for_backend = uring_reactor.clone();
        runtime.register_reactor("io_uring", uring_reactor);

        // Create UCX context and reactor
        let ucx_context = match UcxContext::new() {
            Ok(ctx) => Rc::new(ctx),
            Err(e) => {
                set_error_message(&format!("Failed to create UCX context: {:?}", e));
                return std::ptr::null_mut();
            }
        };

        let ucx_reactor = UCXReactor::current();
        runtime.register_reactor("ucx", ucx_reactor.clone());

        // Create UCX worker
        let worker = match ucx_context.create_worker() {
            Ok(w) => w,
            Err(e) => {
                set_error_message(&format!("Failed to create UCX worker: {:?}", e));
                return std::ptr::null_mut();
            }
        };

        ucx_reactor.register_worker(worker.clone());

        // Create metadata manager
        let metadata_manager = Rc::new(MetadataManager::new(node_id_str.to_string()));

        // Create IOUringBackend and ChunkStore
        // Pass reactor explicitly to ensure DmaFile uses the same io_uring instance
        let io_backend = Rc::new(IOUringBackend::new(allocator.clone(), reactor_for_backend));
        let chunk_store_dir = format!("{}/chunks", data_dir);
        if let Err(e) = std::fs::create_dir_all(&chunk_store_dir) {
            set_error_message(&format!("Failed to create chunk store directory: {}", e));
            return std::ptr::null_mut();
        }

        let chunk_store = match IOUringChunkStore::new(&chunk_store_dir, io_backend.clone()) {
            Ok(store) => Rc::new(store),
            Err(e) => {
                set_error_message(&format!("Failed to create chunk store: {:?}", e));
                return std::ptr::null_mut();
            }
        };

        // Create RPC handler context
        let handler_context = Rc::new(RpcHandlerContext::new(
            metadata_manager.clone(),
            chunk_store.clone(),
            allocator.clone(),
        ));

        // Create RPC server
        let rpc_server = Rc::new(RpcServer::new(worker.clone(), handler_context));

        // Create connection pool
        let connection_pool = match ConnectionPool::new(worker.clone(), registry_dir_str) {
            Ok(pool) => Rc::new(pool),
            Err(e) => {
                set_error_message(&format!("Failed to create connection pool: {:?}", e));
                return std::ptr::null_mut();
            }
        };

        // Register server's worker address
        if let Err(e) = connection_pool.register_self(node_id_str) {
            set_error_message(&format!("Failed to register server address: {:?}", e));
            return std::ptr::null_mut();
        }
        tracing::info!("Server worker address registered to {}", registry_dir_str);

        // Register all RPC handlers (spawn in background, don't block)
        // These handlers run perpetual listening loops, so we can't block_on() them
        let server_clone = rpc_server.clone();
        pluvio_runtime::spawn_with_name(
            async move {
                match server_clone.register_all_handlers().await {
                    Ok(_) => tracing::info!("RPC handlers registered successfully"),
                    Err(e) => tracing::error!("Failed to register RPC handlers: {:?}", e),
                }
            },
            "ffi_rpc_handler_registration".to_string(),
        );
        tracing::info!("RPC handler registration initiated");

        // Create BenchFS instance with distributed metadata
        // For single-server setup: metadata_nodes and data_nodes both point to this server
        let metadata_nodes = vec![node_id_str.to_string()];
        let data_nodes = vec![node_id_str.to_string()];
        let benchfs = Rc::new(BenchFS::with_distributed_metadata(
            node_id_str.to_string(),
            chunk_store,
            connection_pool.clone(),
            data_nodes,
            metadata_nodes,
            chunk_size,
        ));

        // Store in thread-local storage
        set_rpc_server(rpc_server);
        set_connection_pool(connection_pool);

        benchfs
    } else {
        // ===== CLIENT MODE =====
        tracing::info!(
            "Initializing BenchFS client: node_id={}, registry_dir={}",
            node_id_str,
            registry_dir_str
        );

        // Create runtime (Runtime::new() returns Rc<Runtime>)
        let runtime = Runtime::new(256);

        // Set runtime in thread-local storage BEFORE any async operations
        set_runtime(runtime.clone());

        // Register timer reactor for client as well
        let timer_reactor = TimerReactor::current();
        runtime.register_reactor("timer", timer_reactor.clone());

        // Create UCX context and reactor
        let ucx_context = match UcxContext::new() {
            Ok(ctx) => Rc::new(ctx),
            Err(e) => {
                set_error_message(&format!("Failed to create UCX context: {:?}", e));
                return std::ptr::null_mut();
            }
        };

        let ucx_reactor = UCXReactor::current();
        runtime.register_reactor("ucx", ucx_reactor.clone());

        // Create UCX worker
        let worker = match ucx_context.create_worker() {
            Ok(w) => w,
            Err(e) => {
                set_error_message(&format!("Failed to create UCX worker: {:?}", e));
                return std::ptr::null_mut();
            }
        };

        ucx_reactor.register_worker(worker.clone());

        // Create connection pool
        let connection_pool = match ConnectionPool::new(worker, registry_dir_str) {
            Ok(pool) => Rc::new(pool),
            Err(e) => {
                set_error_message(&format!("Failed to create connection pool: {:?}", e));
                return std::ptr::null_mut();
            }
        };

        // Discover data nodes BEFORE connecting (critical for load distribution)
        tracing::info!("Discovering data nodes from registry...");
        let discovered_nodes = match discover_data_nodes(registry_dir_str) {
            Ok(nodes) => {
                tracing::info!(
                    "Discovered {} data nodes for distributed connections",
                    nodes.len()
                );
                nodes
            }
            Err(e) => {
                tracing::warn!(
                    "Failed to discover data nodes: {}. Falling back to node_0 only",
                    e
                );
                vec!["node_0".to_string()]
            }
        };

        // Distribute initial connections across nodes using client node_id hash
        // This prevents all 256 clients from connecting to node_0 simultaneously
        let target_node = if discovered_nodes.len() > 1 {
            // Hash the node_id to select a target node
            let hash = node_id_str
                .bytes()
                .fold(0u64, |acc, b| acc.wrapping_add(b as u64));
            let index = (hash as usize) % discovered_nodes.len();
            &discovered_nodes[index]
        } else {
            &discovered_nodes[0]
        };

        tracing::info!(
            "Connecting to distributed node: {} (selected from {} nodes)",
            target_node,
            discovered_nodes.len()
        );

        let pool_clone = connection_pool.clone();
        let target_node_owned = target_node.to_string();
        let connect_result = block_on_with_name("connect_to_server", async move {
            pool_clone.wait_and_connect(&target_node_owned, 30).await
        });

        match connect_result {
            Ok(_) => tracing::info!("Successfully connected to server: {}", target_node),
            Err(e) => {
                set_error_message(&format!(
                    "Failed to connect to server {}: {:?}",
                    target_node, e
                ));
                return std::ptr::null_mut();
            }
        }

        // Create BenchFS client instance with distributed metadata
        // Client mode does NOT need io_uring or local chunk store - all I/O goes via RPC
        // In MPI mode: node_0 is the metadata server, all nodes are data servers
        // Use the nodes discovered earlier during connection setup
        let metadata_nodes = vec!["node_0".to_string()];
        let data_nodes = discovered_nodes; // Reuse nodes discovered earlier

        tracing::info!(
            "Creating BenchFS client with {} data nodes for distributed storage (no local io_uring)",
            data_nodes.len()
        );

        let benchfs = Rc::new(BenchFS::new_distributed_client(
            node_id_str.to_string(),
            connection_pool.clone(),
            data_nodes,
            metadata_nodes,
            chunk_size,
        ));

        // Runtime already stored in thread-local storage above
        set_connection_pool(connection_pool);

        benchfs
    };

    // Store in thread-local context
    set_benchfs_ctx(benchfs.clone());

    // Store node_id for use in statistics output during finalization
    set_node_id(node_id_str.to_string());

    // Return opaque pointer
    // We box the Rc to pass ownership to C
    let boxed = Box::new(benchfs);
    Box::into_raw(boxed) as *mut benchfs_context_t
}

/// Finalize and destroy a BenchFS instance
///
/// This function performs cleanup and destroys the BenchFS instance created
/// by [`benchfs_init`]. It:
///
/// - Closes all open file handles
/// - Flushes pending I/O operations
/// - Releases RPC server resources (server mode)
/// - Disconnects from remote servers (client mode)
/// - Frees all allocated memory
///
/// After calling this function, the `ctx` pointer becomes invalid and must
/// not be used.
///
/// # Arguments
///
/// * `ctx` - BenchFS context pointer returned by [`benchfs_init`]
///
/// # Safety
///
/// - `ctx` must be a valid pointer from [`benchfs_init`]
/// - `ctx` must not be `NULL` (but `NULL` is safely handled as a no-op)
/// - `ctx` must not be used after this call
/// - Do not call this function multiple times with the same `ctx`
///
/// # Example (C)
///
/// ```c
/// benchfs_context_t* ctx = benchfs_init("client", "/tmp/registry", NULL, 0);
///
/// // ... use BenchFS ...
///
/// benchfs_finalize(ctx);
/// // ctx is now invalid - do not use it again
/// ```
///
/// # Note
///
/// This function logs "BenchFS finalized" at INFO level before returning.
#[unsafe(no_mangle)]
pub extern "C" fn benchfs_finalize(ctx: *mut benchfs_context_t) {
    if ctx.is_null() {
        return;
    }

    tracing::info!("benchfs_finalize: cleaning up resources");

    // NOTE: We do NOT send shutdown RPCs from benchfs_finalize() because:
    //
    // 1. IOR Benchmark:
    //    - Servers and clients are separate processes
    //    - IOR clients call benchfs_finalize() when they finish
    //    - Servers are terminated by the job script with 'kill' command
    //    - Sending shutdown RPCs from IOR clients would interfere with servers
    //
    // 2. RPC Benchmark:
    //    - Client explicitly sends shutdown RPCs in run_client() (benchfs_rpc_bench.rs:280)
    //    - benchfs_finalize() is not called (not using C FFI)
    //
    // Therefore, benchfs_finalize() should only clean up local resources,
    // not send shutdown RPCs to other nodes.

    // Write retry statistics to CSV if BENCHFS_RETRY_STATS_OUTPUT is set
    // The path can be either:
    // - A directory: stats will be written to <dir>/<node_id>.csv
    // - A file path: stats will be written to the specified file
    if let Ok(stats_path) = std::env::var("BENCHFS_RETRY_STATS_OUTPUT") {
        let node_id = get_node_id().unwrap_or_else(|| "unknown".to_string());

        // Determine the actual file path
        let actual_path = if std::path::Path::new(&stats_path).is_dir() {
            format!("{}/{}.csv", stats_path, node_id)
        } else if stats_path.ends_with('/') {
            // Path ends with / but directory doesn't exist yet - treat as directory
            format!("{}{}.csv", stats_path, node_id)
        } else {
            stats_path
        };

        if let Err(e) = crate::rpc::write_retry_stats_to_csv(&actual_path, &node_id) {
            tracing::error!("Failed to write retry stats to {}: {}", actual_path, e);
        }
    }

    // Convert back to Rc<BenchFS> and drop it
    // This will automatically trigger Drop implementations for all components
    unsafe {
        let _ = Box::from_raw(ctx as *mut Rc<BenchFS>);
    }

    // Clean up thread-local storage
    super::runtime::cleanup_runtime();

    tracing::info!("BenchFS finalized");
}
