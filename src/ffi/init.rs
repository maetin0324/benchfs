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

#[cfg(feature = "dhat-heap")]
mod dhat_profile {
    //! Process-global dhat profiler lifecycle.
    //!
    //! `benchfs_init` starts the profiler on the first call from this
    //! process. `benchfs_finalize` flushes the report when the last
    //! `benchfs_context_t` is freed by dropping the profiler. Multi-phase
    //! io500 (init/finalize per phase) is handled by reference-counting
    //! the live context pointers.
    use std::sync::Mutex;
    use std::sync::OnceLock;

    static PROFILER: OnceLock<Mutex<Option<dhat::Profiler>>> = OnceLock::new();
    static CTX_COUNT: Mutex<usize> = Mutex::new(0);

    fn slot() -> &'static Mutex<Option<dhat::Profiler>> {
        PROFILER.get_or_init(|| Mutex::new(None))
    }

    pub fn start_if_first() {
        let mut count = CTX_COUNT.lock().expect("CTX_COUNT poisoned");
        if *count == 0 {
            // `[observability] dhat_path` — explicit full path; wins if set.
            // `[observability] dhat_dir`  — directory; we write `dhat-<pid>.json`.
            // Fallback: CWD/dhat-<pid>.json.
            let pid = std::process::id();
            let obs = &crate::runtime_config::RuntimeConfig::global().observability;
            let out = if let Some(p) = obs.dhat_path.as_deref() {
                p.to_string()
            } else if let Some(dir) = obs.dhat_dir.as_deref() {
                let _ = std::fs::create_dir_all(dir);
                format!("{dir}/dhat-{pid}.json")
            } else {
                format!("dhat-benchfs-{pid}.json")
            };
            let p = dhat::Profiler::builder().file_name(&out).build();
            *slot().lock().expect("PROFILER poisoned") = Some(p);
            eprintln!("[dhat] profiler started (pid={pid}), output={out}");
        }
        *count += 1;
    }

    pub fn stop_if_last() {
        let mut count = CTX_COUNT.lock().expect("CTX_COUNT poisoned");
        if *count == 0 {
            return;
        }
        *count -= 1;
        if *count == 0 {
            // Drop the profiler to flush the JSON report.
            let mut guard = slot().lock().expect("PROFILER poisoned");
            if guard.take().is_some() {
                eprintln!("[dhat] profiler stopped, report written");
            }
        }
    }
}

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

    // Check if expected node count is specified via [cluster] expected_nodes.
    let expected_nodes: Option<usize> = match crate::runtime_config::RuntimeConfig::global()
        .cluster
        .expected_nodes
    {
        0 => None,
        n => Some(n),
    };

    if let Some(expected) = expected_nodes {
        tracing::info!(
            "cluster.expected_nodes={}: Will wait for all {} nodes to register",
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
                tracing::info!("Discovered {} data nodes: {:?}", node_ids.len(), node_ids);
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
    #[cfg(feature = "dhat-heap")]
    dhat_profile::start_if_first();
    // Use default chunk size if 0 is passed
    let chunk_size = if chunk_size == 0 {
        crate::metadata::CHUNK_SIZE
    } else {
        chunk_size
    };
    crate::logging::init_with_hostname("info");

    // Honor [stats] enabled in benchfs.toml on the client (FFI) side so
    // that RPC_CLIENT_TIMING and other stats-gated paths emit data.
    // Same flag is read by benchfsd_mpi.
    if crate::runtime_config::RuntimeConfig::global().stats.enabled {
        crate::stats::set_stats_enabled(true);
    }

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

    // io500 calls benchfs_init/finalize once per phase (ior-easy, mdtest-easy,
    // ...). Under BENCHFS_TRANSPORT=locusta, tearing down the locusta
    // transport between phases leaves the server side with stale dest_id→QP
    // mappings; the next phase's RPCs hit dead QPs and trigger CQE
    // syndrome=21 (TRANSPORT_RETRY_EXC_ERR) after the ~30s retry budget
    // exhausts (job 17042). Make the FFI idempotent for locusta clients:
    // if TLS already holds a BenchFS instance, hand out a fresh Box
    // wrapping a clone of the existing Rc and skip the full re-init.
    let use_locusta_client = is_server == 0
        && crate::runtime_config::RuntimeConfig::global()
            .transport
            .backend
            .eq_ignore_ascii_case("locusta");
    if use_locusta_client {
        if let Some(existing) = super::runtime::get_benchfs_ctx_rc() {
            tracing::info!(
                "benchfs_init: reusing existing locusta client context (idempotent re-init)"
            );
            let boxed = Box::new(existing);
            return Box::into_raw(boxed) as *mut benchfs_context_t;
        }
    }

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

        // Create runtime. Larger initial capacity reduces Slab Vec growths,
        // which previously realloc'd the backing buffer during mdtest-hard's
        // 100k+ small-task churn — suspected interaction with the heap UB.
        let runtime = Runtime::new(65536);
        set_runtime(runtime.clone());

        // Register timer reactor (required for Delay/timeout futures)
        let timer_reactor = TimerReactor::current();
        runtime.register_reactor("timer", timer_reactor.clone());

        // Create io_uring reactor
        tracing::info!("Starting IoUringReactor initialization...");
        let start = std::time::Instant::now();

        tracing::info!(
            "Configuring io_uring with buffer_size={} bytes ({} MiB)",
            chunk_size,
            chunk_size / (1024 * 1024)
        );
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

        // Create runtime. 65536 initial slots = enough headroom that the
        // Slab backing Vec never grows during a 1M-iter mdtest-hard run.
        // Scheduling tuning is now in benchfs.toml ([scheduling] section)
        // instead of BENCHFS_STATUS_CACHE_ITERS / BENCHFS_REACTOR_POLL_INTERVAL
        // env vars. See runtime_config.rs.
        let rc = crate::runtime_config::RuntimeConfig::global();
        let runtime = Runtime::with_config(
            65536,
            pluvio_runtime::executor::SchedulingConfig {
                status_cache_iterations: rc.scheduling.status_cache_iters,
                reactor_poll_interval: rc.scheduling.reactor_poll_interval,
                ..Default::default()
            },
        );

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

        // BENCHFS_TRANSPORT=locusta: build a locusta-backed connection pool
        // with no static peers. Every `get_or_connect(node)` will lazy-run
        // `LocustaTransport::add_peer(node, 30s)` before returning a
        // locusta-backed RpcClient. Falls through to UCX otherwise.
        // Transport selection now comes from benchfs.toml [transport]
        // backend = "locusta" instead of BENCHFS_TRANSPORT env var.
        let use_locusta = rc.transport.backend.eq_ignore_ascii_case("locusta");
        #[cfg(not(feature = "transport-locusta"))]
        if use_locusta {
            set_error_message(
                "transport.backend=\"locusta\" requires the transport-locusta feature",
            );
            return std::ptr::null_mut();
        }
        #[cfg(feature = "transport-locusta")]
        let locusta_transport: Option<Rc<crate::rpc::transport_locusta::LocustaTransport>> =
            if use_locusta {
                use crate::rpc::transport_locusta::{LocustaConfig, LocustaTransport};
                use std::path::PathBuf;
                let cfg = LocustaConfig {
                    registry_dir: PathBuf::from(registry_dir_str).join("locusta_qp"),
                    local_node_id: node_id_str.to_string(),
                    peer_node_ids: Vec::new(),
                    arena_size: rc.locusta.arena_size,
                    ring_capacity: rc.locusta.ring_capacity,
                    ..LocustaConfig::default()
                };
                if let Err(e) = std::fs::create_dir_all(&cfg.registry_dir) {
                    set_error_message(&format!(
                        "Failed to create locusta registry dir {}: {}",
                        cfg.registry_dir.display(),
                        e
                    ));
                    return std::ptr::null_mut();
                }
                match LocustaTransport::init(&cfg) {
                    Ok(t) => {
                        tracing::info!(
                            "FFI client: LocustaTransport ready, registry={}",
                            cfg.registry_dir.display()
                        );
                        let transport_rc = Rc::new(t);
                        if crate::rpc::transport_locusta::reactor_mode_enabled() {
                            // Reactor mode: register the LocustaTransport
                            // itself so the runtime ticks it directly.
                            // `WaitForResponse::poll` skips its own tick.
                            runtime.register_reactor("locusta", Rc::clone(&transport_rc));
                            tracing::info!(
                                "FFI client: registered LocustaTransport as pluvio Reactor (reactor mode)"
                            );
                        } else {
                            // Legacy mode: register a no-op "keepalive"
                            // reactor so the pluvio executor's stuck-
                            // watchdog (1M iter without made_progress)
                            // doesn't fire while a long async RPC is in
                            // flight. The reactor doesn't touch locusta
                            // state — tick happens inside
                            // `WaitForResponse::poll`.
                            struct KeepAliveReactor;
                            impl pluvio_runtime::reactor::Reactor for KeepAliveReactor {
                                fn poll(&self) {}
                                fn status(&self) -> pluvio_runtime::reactor::ReactorStatus {
                                    pluvio_runtime::reactor::ReactorStatus::Running
                                }
                            }
                            runtime.register_reactor(
                                "locusta_keepalive",
                                Rc::new(KeepAliveReactor),
                            );
                        }
                        Some(transport_rc)
                    }
                    Err(e) => {
                        set_error_message(&format!("Failed to init LocustaTransport: {:?}", e));
                        return std::ptr::null_mut();
                    }
                }
            } else {
                None
            };

        // Create connection pool. Locusta path skips UCX endpoint setup
        // entirely and short-circuits inside ConnectionPool::get_or_connect.
        let connection_pool = {
            #[cfg(feature = "transport-locusta")]
            {
                if let Some(transport) = locusta_transport {
                    match ConnectionPool::new_locusta(worker.clone(), registry_dir_str, transport) {
                        Ok(pool) => Rc::new(pool),
                        Err(e) => {
                            set_error_message(&format!(
                                "Failed to create locusta connection pool: {:?}",
                                e
                            ));
                            return std::ptr::null_mut();
                        }
                    }
                } else {
                    match ConnectionPool::new(worker, registry_dir_str) {
                        Ok(pool) => Rc::new(pool),
                        Err(e) => {
                            set_error_message(&format!(
                                "Failed to create connection pool: {:?}",
                                e
                            ));
                            return std::ptr::null_mut();
                        }
                    }
                }
            }
            #[cfg(not(feature = "transport-locusta"))]
            {
                match ConnectionPool::new(worker, registry_dir_str) {
                    Ok(pool) => Rc::new(pool),
                    Err(e) => {
                        set_error_message(&format!("Failed to create connection pool: {:?}", e));
                        return std::ptr::null_mut();
                    }
                }
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

        // Pre-warm UCX endpoints to every data node so the first write to each
        // server pays only the steady-state RPC cost (~10 ms RTT) instead of
        // first-connect cold-start (~1 s observed in job 16578). At 576 clients
        // × 36 servers, the lazy-connect burst eats the first 30-50 sec of any
        // shared-file write phase. Toggle with [prewarm] enabled = false
        // in benchfs.toml to measure the unprewarmed baseline.
        let prewarm = rc.prewarm.enabled;
        if prewarm && discovered_nodes.len() > 1 {
            tracing::info!(
                "Pre-warming UCX endpoints to all {} data nodes",
                discovered_nodes.len()
            );
            let pool_for_prewarm = connection_pool.clone();
            // Per-rank shuffled order: every client visits the server
            // list in a different rotation so all 320 clients don't
            // thundering-herd on `node_0` at t=0. Job 20212 saw 80-server
            // 2× density fail this way — half the peers UDP-timed out
            // during the prewarm burst. The rotation offset is derived
            // from the client's own node_id_str so it's deterministic
            // per process and zero overhead.
            let rank_hash: u64 = {
                use std::hash::{Hash, Hasher};
                let mut h = std::collections::hash_map::DefaultHasher::new();
                node_id_str.hash(&mut h);
                h.finish()
            };
            let mut nodes_for_prewarm = discovered_nodes.clone();
            let n = nodes_for_prewarm.len();
            if n > 1 {
                let offset = (rank_hash as usize) % n;
                nodes_for_prewarm.rotate_left(offset);
            }
            // Optional per-rank stagger: each rank waits
            // `stagger_ms_per_rank × rank_id` ms before starting pre-warm.
            // Spreads node_0 (metadata-pinned) fan-in so a 24-rank job
            // doesn't slam 24 simultaneous UDP REQUESTs on one server
            // (job 20489 hit this at 4 phys × ppn=6 = 24 ranks).
            if rc.prewarm.stagger_ms_per_rank > 0 {
                let rank_id_for_stagger: u64 = node_id_str
                    .strip_prefix("node_")
                    .and_then(|n| n.parse().ok())
                    .unwrap_or(0);
                let wait_ms = rc.prewarm.stagger_ms_per_rank * rank_id_for_stagger;
                if wait_ms > 0 {
                    tracing::info!("Pre-warm stagger: sleeping {} ms before connect", wait_ms);
                    std::thread::sleep(std::time::Duration::from_millis(wait_ms));
                }
            }
            let prewarm_start = std::time::Instant::now();
            // Parallel pre-warm via join_all so the locusta transport can
            // process multiple UDP exchanges + RDMA QP setups concurrently.
            // Sequential await with 80 peers was taking >>900s at 10 phys
            // 2×S (job 20223/20236 hit walltime before writes started).
            // Chunked pre-warm: bounded concurrency avoids the thundering
            // herd at high ppn (80 ranks × 80 peers = 6400 simultaneous UDP
            // exchanges overwhelmed locusta's single UDP recv loop at
            // 10 phys × ppn≥4 — job 20364/20365 saw "udp exchange timeout"
            // for ~30 peers each). [prewarm] concurrency in benchfs.toml
            // controls this; set to >= peer_count to mimic join_all (lower
            // values can serially block on a single 120 s timeout).
            let chunk: usize = rc.prewarm.concurrency;
            let _ = block_on_with_name("prewarm_connections", async move {
                use futures::stream::{self, StreamExt};
                let nodes = nodes_for_prewarm.clone();
                stream::iter(nodes.into_iter().map(|node| {
                    let pool = pool_for_prewarm.clone();
                    async move {
                        if let Err(e) = pool.get_or_connect(&node).await {
                            tracing::warn!("Pre-warm: failed to connect to {}: {:?}", node, e);
                        }
                    }
                }))
                .buffer_unordered(chunk)
                .collect::<Vec<()>>()
                .await;
                Ok::<(), crate::rpc::RpcError>(())
            });
            tracing::info!(
                "Pre-warm complete in {} ms",
                prewarm_start.elapsed().as_millis()
            );
        }

        // Metadata distribution. Default = all discovered nodes (consistent
        // hash sharding) so mdtest scales across the cluster. Set
        // `[metadata] distributed = false` in benchfs.toml to pin to
        // node_0 — a known workaround for an old ppn=16 b=16g shared
        // hang (5/11) that has not re-appeared at ppn≤8 fpp. Pinning
        // starves mdtest (320 clients × 1 server).
        let metadata_distributed = rc.metadata.distributed;
        let metadata_nodes = if metadata_distributed {
            discovered_nodes.clone()
        } else {
            vec!["node_0".to_string()]
        };
        let data_nodes = discovered_nodes;

        tracing::info!(
            "Creating BenchFS client with {} metadata + {} data nodes (metadata pinned to node_0)",
            metadata_nodes.len(),
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

    // Write retry statistics to CSV if `[observability] retry_stats_output`
    // is set. The path can be either:
    // - A directory: stats will be written to <dir>/<node_id>.csv
    // - A file path: stats will be written to the specified file
    if let Some(stats_path) = crate::runtime_config::RuntimeConfig::global()
        .observability
        .retry_stats_output
        .clone()
    {
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

    // Locusta clients: io500 calls benchfs_init/finalize per phase. We
    // keep TLS state alive across phases so the locusta server-side
    // dest_id→QP mapping stays valid (see benchfs_init for context).
    // Only drop the Box wrapper that C owns; the underlying Rc is
    // still held by the TLS BENCHFS_CTX and will be dropped on
    // process exit.
    let use_locusta_client = crate::runtime_config::RuntimeConfig::global()
        .transport
        .backend
        .eq_ignore_ascii_case("locusta")
        && super::runtime::get_benchfs_ctx_rc().is_some();

    // Convert back to Rc<BenchFS> and drop one Rc clone. Other Rc
    // clones (TLS, ConnectionPool, etc.) keep the instance alive on
    // the locusta path.
    unsafe {
        let _ = Box::from_raw(ctx as *mut Rc<BenchFS>);
    }

    if use_locusta_client {
        tracing::info!("benchfs_finalize: locusta client — preserving TLS state for next phase");
        #[cfg(feature = "dhat-heap")]
        dhat_profile::stop_if_last();
        return;
    }

    // Clean up thread-local storage
    super::runtime::cleanup_runtime();

    #[cfg(feature = "dhat-heap")]
    dhat_profile::stop_if_last();

    tracing::info!("BenchFS finalized");
}
