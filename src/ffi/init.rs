// Initialization and finalization for C FFI
//
// This module provides benchfs_init() and benchfs_finalize() functions
// that are called from IOR to setup and teardown BenchFS instances.

use std::ffi::CStr;
use std::os::raw::c_char;
use std::rc::Rc;

use super::runtime::{set_benchfs_ctx, set_runtime, set_rpc_server, set_connection_pool, block_on};
use super::error::*;
use crate::api::file_ops::BenchFS;
use crate::rpc::server::RpcServer;
use crate::rpc::handlers::RpcHandlerContext;
use crate::rpc::connection::ConnectionPool;
use crate::metadata::MetadataManager;
use crate::storage::{IOUringBackend, IOUringChunkStore};

use pluvio_runtime::executor::Runtime;
use pluvio_uring::reactor::IoUringReactor;
use pluvio_ucx::{Context as UcxContext, reactor::UCXReactor};

// Opaque type for BenchFS context
// This prevents C code from accessing internal structure
#[repr(C)]
pub struct benchfs_context_t {
    _private: [u8; 0],
}

/// Initialize BenchFS instance
///
/// # Arguments
/// * `node_id` - Node identifier (e.g., "client_1", "server")
/// * `registry_dir` - Shared directory for service discovery (required)
/// * `data_dir` - Data directory for server nodes (optional for clients)
/// * `is_server` - 1 if this is a server node, 0 for client
///
/// # Returns
/// Opaque pointer to BenchFS context, or NULL on error
///
/// # Safety
/// - `node_id` and `registry_dir` must be valid C strings
/// - The returned pointer must be freed with benchfs_finalize()
#[unsafe(no_mangle)]
pub extern "C" fn benchfs_init(
    node_id: *const c_char,
    registry_dir: *const c_char,
    data_dir: *const c_char,
    is_server: i32,
) -> *mut benchfs_context_t {
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

    // Create BenchFS instance based on mode
    let benchfs = if is_server != 0 {
        // ===== SERVER MODE =====
        tracing::info!("Initializing BenchFS server: node_id={}, registry_dir={}, data_dir={:?}",
                      node_id_str, registry_dir_str, data_dir_str);

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

        // Create io_uring reactor
        let uring_reactor = IoUringReactor::builder()
            .queue_size(256)
            .buffer_size(1 << 20) // 1 MiB
            .submit_depth(32)
            .wait_submit_timeout(std::time::Duration::from_micros(10))
            .wait_complete_timeout(std::time::Duration::from_micros(10))
            .build();

        let allocator = uring_reactor.allocator.clone();
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
        let io_backend = Rc::new(IOUringBackend::new(allocator));
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
        let runtime_clone = runtime.clone();
        runtime.spawn(async move {
            match server_clone.register_all_handlers(runtime_clone).await {
                Ok(_) => tracing::info!("RPC handlers registered successfully"),
                Err(e) => tracing::error!("Failed to register RPC handlers: {:?}", e),
            }
        });
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
        set_runtime(runtime);
        set_rpc_server(rpc_server);
        set_connection_pool(connection_pool);

        benchfs
    } else {
        // ===== CLIENT MODE =====
        tracing::info!("Initializing BenchFS client: node_id={}, registry_dir={}",
                      node_id_str, registry_dir_str);

        // Create runtime (Runtime::new() returns Rc<Runtime>)
        let runtime = Runtime::new(256);

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

        // Wait for server to register and connect
        tracing::info!("Waiting for server to register (timeout: 30 seconds)...");
        let pool_clone = connection_pool.clone();
        let connect_result = block_on(async move {
            pool_clone.wait_and_connect("node_0", 30).await
        });

        match connect_result {
            Ok(_) => tracing::info!("Successfully connected to server"),
            Err(e) => {
                set_error_message(&format!("Failed to connect to server: {:?}", e));
                return std::ptr::null_mut();
            }
        }

        // Get data_dir for client (use temp dir if not specified)
        let client_data_dir = match data_dir_str {
            Some(d) => d.to_string(),
            None => format!("/tmp/benchfs_client_{}", node_id_str),
        };

        // Create io_uring reactor (same as server)
        let uring_reactor = IoUringReactor::builder()
            .queue_size(256)
            .buffer_size(1 << 20) // 1 MiB
            .submit_depth(32)
            .wait_submit_timeout(std::time::Duration::from_micros(10))
            .wait_complete_timeout(std::time::Duration::from_micros(10))
            .build();

        let allocator = uring_reactor.allocator.clone();
        runtime.register_reactor("io_uring", uring_reactor);

        // Create IOUringBackend and ChunkStore for client
        let io_backend = Rc::new(IOUringBackend::new(allocator));
        let chunk_store_dir = format!("{}/chunks", client_data_dir);
        if let Err(e) = std::fs::create_dir_all(&chunk_store_dir) {
            set_error_message(&format!("Failed to create client chunk store directory: {}", e));
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
        // Discover available nodes from registry (for now, use node_0 as metadata server)
        let metadata_nodes = vec!["node_0".to_string()];
        // Use multiple data nodes for distributed storage
        let data_nodes = vec![
            "node_0".to_string(),
            "node_1".to_string(),
            "node_2".to_string(),
            "node_3".to_string(),
        ];
        let benchfs = Rc::new(BenchFS::with_distributed_metadata(
            node_id_str.to_string(),
            chunk_store,
            connection_pool.clone(),
            data_nodes,
            metadata_nodes,
        ));

        // Store in thread-local storage
        set_runtime(runtime);
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

/// Finalize BenchFS instance
///
/// # Arguments
/// * `ctx` - BenchFS context from benchfs_init()
///
/// # Safety
/// - `ctx` must be a valid pointer from benchfs_init()
/// - `ctx` must not be used after this call
#[unsafe(no_mangle)]
pub extern "C" fn benchfs_finalize(ctx: *mut benchfs_context_t) {
    if ctx.is_null() {
        return;
    }

    // Convert back to Rc<BenchFS> and drop it
    unsafe {
        let _ = Box::from_raw(ctx as *mut Rc<BenchFS>);
    }

    tracing::info!("BenchFS finalized");
}
