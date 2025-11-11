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
use std::sync::Arc;

use super::error::*;
use super::runtime::{
    set_benchfs_ctx, set_connection_pool, set_rpc_server, set_runtime,
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
fn discover_data_nodes(registry_dir: &str) -> Result<Vec<String>, String> {
    use std::fs;
    use std::path::Path;
    use std::thread;
    use std::time::Duration;

    let registry_path = Path::new(registry_dir);
    if !registry_path.exists() {
        return Err(format!(
            "Registry directory does not exist: {}",
            registry_dir
        ));
    }

    // Retry logic to handle race conditions when 256 clients simultaneously scan directory
    const MAX_RETRIES: u32 = 5;
    const RETRY_DELAY_MS: u64 = 100;

    for attempt in 0..MAX_RETRIES {
        let entries = match fs::read_dir(registry_path) {
            Ok(e) => e,
            Err(err) => {
                if attempt < MAX_RETRIES - 1 {
                    tracing::debug!(
                        "Failed to read registry (attempt {}): {}. Retrying...",
                        attempt + 1,
                        err
                    );
                    thread::sleep(Duration::from_millis(RETRY_DELAY_MS));
                    continue;
                }
                return Err(format!(
                    "Failed to read registry directory {} after {} retries: {}",
                    registry_dir, MAX_RETRIES, err
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

            // Match pattern: node_*.stream_hostname (new socket-based registration)
            // or node_*.addr (legacy WorkerAddress-based registration)
            if filename_str.starts_with("node_") && filename_str.ends_with(".stream_hostname") {
                // Extract node ID: "node_0.stream_hostname" -> "node_0"
                let node_id = &filename_str[..filename_str.len() - 16]; // Remove ".stream_hostname" suffix
                node_ids.push(node_id.to_string());
                tracing::debug!("Discovered data node: {}", node_id);
            } else if filename_str.starts_with("node_") && filename_str.ends_with(".addr") {
                // Extract node ID: "node_0.addr" -> "node_0"
                let node_id = &filename_str[..filename_str.len() - 5]; // Remove ".addr" suffix
                node_ids.push(node_id.to_string());
                tracing::debug!("Discovered data node (legacy): {}", node_id);
            }
        }

        if !node_ids.is_empty() {
            // Sort node IDs for deterministic ordering
            node_ids.sort();

            tracing::info!(
                "Discovered {} data nodes: {:?} (attempt {})",
                node_ids.len(),
                node_ids,
                attempt + 1
            );

            return Ok(node_ids);
        }

        // No nodes found, retry
        if attempt < MAX_RETRIES - 1 {
            tracing::debug!("No nodes found in attempt {}, retrying...", attempt + 1);
            thread::sleep(Duration::from_millis(RETRY_DELAY_MS));
        }
    }

    Err(format!(
        "No data nodes found in registry directory: {} after {} retries",
        registry_dir, MAX_RETRIES
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
#[unsafe(no_mangle)]
pub extern "C" fn benchfs_init(
    node_id: *const c_char,
    registry_dir: *const c_char,
    data_dir: *const c_char,
    is_server: i32,
) -> *mut benchfs_context_t {
    use tracing_subscriber::EnvFilter;
    use tracing_subscriber::fmt;

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_thread_ids(false)
        .with_file(true)
        .with_line_number(true)
        .init();

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

        let uring_reactor = IoUringReactor::builder()
            .queue_size(512) // Reduced to prevent kernel resource contention
            .buffer_size(1 << 20) // 1 MiB per buffer (512 × 1MiB = 512MiB total)
            .submit_depth(64) // Reduced from 128
            .wait_submit_timeout(std::time::Duration::from_micros(1))
            .wait_complete_timeout(std::time::Duration::from_micros(1))
            .build();

        tracing::info!(
            "IoUringReactor initialization completed in {:?}",
            start.elapsed()
        );

        let allocator = uring_reactor.allocator.clone();
        runtime.register_reactor("io_uring", uring_reactor);

        // Create UCX context and reactor
        let ucx_context = match UcxContext::new() {
            Ok(ctx) => Arc::new(ctx),
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
        let io_backend = Rc::new(IOUringBackend::new(allocator.clone()));
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
        let connection_pool = match ConnectionPool::new(
            worker.clone(),
            ucx_context.clone(),
            registry_dir_str,
        ) {
            Ok(pool) => Rc::new(pool),
            Err(e) => {
                set_error_message(&format!("Failed to create connection pool: {:?}", e));
                return std::ptr::null_mut();
            }
        };

        // Bind to socket and register this server's address
        let base_port = 50051u16;
        // For FFI init, we use a fixed port since there's no MPI rank
        let listen_addr = std::net::SocketAddr::from(([0, 0, 0, 0], base_port));

        tracing::info!("Attempting to bind AM RPC socket at {}", listen_addr);

        match connection_pool.bind_and_register(node_id_str, listen_addr) {
            Ok(addr) => {
                tracing::info!("Server {} bound and registered at {}", node_id_str, addr);
            }
            Err(e) => {
                set_error_message(&format!("Failed to bind and register server address: {:?}", e));
                return std::ptr::null_mut();
            }
        }

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
            Ok(ctx) => Arc::new(ctx),
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
        let connection_pool = match ConnectionPool::new(
            worker,
            ucx_context.clone(),
            registry_dir_str,
        ) {
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

        // 旧 AM RPC ではここで先行接続してサーバ起動を検証していたが、
        // Stream RPC では初回 RPC 時に遅延接続する設計に切り替える。
        // （大量クライアントによる同時接続集中やプロトコル不一致を避けるため）
        if !discovered_nodes.is_empty() {
            tracing::info!(
                "Skipping eager wait_and_connect; {} nodes will be connected lazily",
                discovered_nodes.len()
            );
        } else {
            tracing::warn!(
                "No data nodes discovered; Stream connections will be attempted lazily"
            );
        }

        // Get data_dir for client (use temp dir if not specified)
        let client_data_dir = match data_dir_str {
            Some(d) => d.to_string(),
            None => format!("/tmp/benchfs_client_{}", node_id_str),
        };

        // Create io_uring reactor (same as server)
        tracing::info!("Starting IoUringReactor initialization...");
        let start = std::time::Instant::now();

        let uring_reactor = IoUringReactor::builder()
            .queue_size(512) // Reduced from 2048 to prevent kernel resource contention
            .buffer_size(1 << 20) // 1 MiB (512 × 1MiB = 512MiB total, reduced from 8GiB)
            .submit_depth(64) // Reduced from 128
            .wait_submit_timeout(std::time::Duration::from_micros(1))
            .wait_complete_timeout(std::time::Duration::from_micros(1))
            .build();

        tracing::info!(
            "IoUringReactor initialization completed in {:?}",
            start.elapsed()
        );

        let allocator = uring_reactor.allocator.clone();
        runtime.register_reactor("io_uring", uring_reactor);

        // Create IOUringBackend and ChunkStore for client
        let io_backend = Rc::new(IOUringBackend::new(allocator));
        let chunk_store_dir = format!("{}/chunks", client_data_dir);
        if let Err(e) = std::fs::create_dir_all(&chunk_store_dir) {
            set_error_message(&format!(
                "Failed to create client chunk store directory: {}",
                e
            ));
            return std::ptr::null_mut();
        }

        let chunk_store = match IOUringChunkStore::new(&chunk_store_dir, io_backend.clone()) {
            Ok(store) => Rc::new(store),
            Err(e) => {
                set_error_message(&format!("Failed to create client chunk store: {:?}", e));
                return std::ptr::null_mut();
            }
        };

        // Create BenchFS instance with distributed metadata
        // In MPI mode: node_0 is the metadata server, all nodes are data servers
        // Use the nodes discovered earlier during connection setup
        let metadata_nodes = vec!["node_0".to_string()];
        let data_nodes = discovered_nodes; // Reuse nodes discovered earlier

        tracing::info!(
            "Creating BenchFS client with {} data nodes for distributed storage",
            data_nodes.len()
        );

        let benchfs = Rc::new(BenchFS::with_distributed_metadata(
            node_id_str.to_string(),
            chunk_store,
            connection_pool.clone(),
            data_nodes,
            metadata_nodes,
        ));

        // Runtime already stored in thread-local storage above
        set_connection_pool(connection_pool);

        benchfs
    };

    // Store in thread-local context
    set_benchfs_ctx(benchfs.clone());

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

    // Convert back to Rc<BenchFS> and drop it
    // This will automatically trigger Drop implementations for all components
    unsafe {
        let _ = Box::from_raw(ctx as *mut Rc<BenchFS>);
    }

    // Clean up thread-local storage
    super::runtime::cleanup_runtime();

    tracing::info!("BenchFS finalized");
}
