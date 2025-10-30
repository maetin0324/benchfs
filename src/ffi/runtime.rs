//! Global runtime management for FFI
//!
//! This module manages the async runtime and global state required for executing
//! BenchFS async operations from C code. It provides:
//!
//! - **Thread-local Runtime**: Each thread maintains its own async runtime instance
//! - **BenchFS Context Storage**: Thread-local storage for BenchFS instances
//! - **RPC Components**: Storage for RPC server and connection pool in distributed mode
//! - **Async-to-Sync Conversion**: [`block_on`] helper for executing async functions synchronously
//!
//! # Thread-Local Design
//!
//! All state is stored in thread-local storage to support multi-threaded environments:
//!
//! - **MPI Applications**: Each MPI rank can have its own BenchFS instance
//! - **No Shared State**: Eliminates the need for synchronization between threads
//! - **Safe Concurrent Access**: Multiple threads can use BenchFS simultaneously
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────┐
//! │  C Code (IOR, MPI Application)     │
//! └────────────┬────────────────────────┘
//!              │ FFI calls
//!              ▼
//! ┌─────────────────────────────────────┐
//! │  Thread-Local Runtime & Context     │
//! │  - LOCAL_RUNTIME                    │
//! │  - BENCHFS_CTX                      │
//! │  - RPC_SERVER (server mode)         │
//! │  - CONNECTION_POOL (distributed)    │
//! └────────────┬────────────────────────┘
//!              │ block_on()
//!              ▼
//! ┌─────────────────────────────────────┐
//! │  Async BenchFS Operations           │
//! │  (File I/O, RPC, Metadata)          │
//! └─────────────────────────────────────┘
//! ```

use crate::api::file_ops::BenchFS;
use crate::rpc::connection::ConnectionPool;
use crate::rpc::server::RpcServer;
use pluvio_runtime::executor::Runtime;
use std::cell::RefCell;
use std::rc::Rc;
use futures::{select, FutureExt};
use futures_timer::Delay;
use std::sync::atomic::{AtomicBool, Ordering};

thread_local! {
    /// Thread-local async runtime
    ///
    /// Each thread maintains its own runtime instance because `Runtime` contains
    /// `RefCell` which is not `Sync`. For distributed mode, this runtime is
    /// initialized in `benchfs_init()` with UCX and io_uring reactors registered.
    ///
    /// In local mode (non-distributed), a basic runtime is created on-demand.
    static LOCAL_RUNTIME: RefCell<Option<Rc<Runtime>>> = RefCell::new(None);
}

thread_local! {
    /// Thread-local BenchFS context
    ///
    /// Each thread (MPI rank) has its own BenchFS instance stored here.
    /// This allows concurrent access from multiple MPI processes without conflicts.
    ///
    /// The context is set by `benchfs_init()` and cleared by `benchfs_finalize()`.
    pub static BENCHFS_CTX: RefCell<Option<Rc<BenchFS>>> = RefCell::new(None);
}

thread_local! {
    /// Thread-local RPC server (for server mode only)
    ///
    /// When BenchFS is initialized in server mode, this stores the RPC server
    /// instance that handles incoming requests from clients.
    pub static RPC_SERVER: RefCell<Option<Rc<RpcServer>>> = RefCell::new(None);
}

thread_local! {
    /// Thread-local connection pool (for distributed mode)
    ///
    /// When BenchFS is initialized in distributed mode (client or server),
    /// this stores the connection pool used for RPC communication with other nodes.
    pub static CONNECTION_POOL: RefCell<Option<Rc<ConnectionPool>>> = RefCell::new(None);
}

/// Set the async runtime for the current thread
///
/// This function stores the runtime instance in thread-local storage.
/// The runtime is initialized in `benchfs_init()` with appropriate reactors
/// (io_uring, UCX) registered for the mode (client or server).
///
/// # Arguments
///
/// * `runtime` - Shared reference to the runtime instance
pub fn set_runtime(runtime: Rc<Runtime>) {
    LOCAL_RUNTIME.with(|rt| {
        *rt.borrow_mut() = Some(runtime);
    });
}

/// Set the BenchFS context for the current thread
///
/// This function stores the BenchFS instance in thread-local storage, making it
/// accessible to all FFI functions called from this thread.
///
/// # Arguments
///
/// * `benchfs` - Shared reference to the BenchFS instance
pub fn set_benchfs_ctx(benchfs: Rc<BenchFS>) {
    BENCHFS_CTX.with(|ctx| {
        *ctx.borrow_mut() = Some(benchfs);
    });
}

/// Set the RPC server for the current thread (server mode only)
///
/// This function stores the RPC server instance in thread-local storage.
/// The server handles incoming RPC requests from clients.
///
/// # Arguments
///
/// * `server` - Shared reference to the RPC server instance
pub fn set_rpc_server(server: Rc<RpcServer>) {
    RPC_SERVER.with(|srv| {
        *srv.borrow_mut() = Some(server);
    });
}

/// Set the connection pool for the current thread (distributed mode)
///
/// This function stores the connection pool in thread-local storage.
/// The connection pool is used for establishing and managing connections
/// to remote nodes for RPC communication.
///
/// # Arguments
///
/// * `pool` - Shared reference to the connection pool instance
pub fn set_connection_pool(pool: Rc<ConnectionPool>) {
    CONNECTION_POOL.with(|p| {
        *p.borrow_mut() = Some(pool);
    });
}

/// Execute a closure with access to the BenchFS context
///
/// This helper function provides safe access to the thread-local BenchFS instance.
/// It returns an error if BenchFS has not been initialized for this thread.
///
/// # Arguments
///
/// * `f` - Closure that receives a reference to the BenchFS instance
///
/// # Returns
///
/// * `Ok(R)` - Result from the closure if BenchFS is initialized
/// * `Err(String)` - Error message if BenchFS is not initialized
///
/// # Example
///
/// ```ignore
/// let result = with_benchfs_ctx(|fs| {
///     // Use fs to perform operations
///     fs.is_distributed()
/// });
/// ```
pub fn with_benchfs_ctx<F, R>(f: F) -> Result<R, String>
where
    F: FnOnce(&BenchFS) -> R,
{
    BENCHFS_CTX.with(|ctx| {
        ctx.borrow()
            .as_ref()
            .map(|fs| f(fs.as_ref()))
            .ok_or_else(|| "BenchFS not initialized".to_string())
    })
}

/// Execute an async function synchronously
///
/// This is the core of async-to-sync conversion for the FFI layer. It takes
/// an async future and blocks the current thread until the future completes,
/// returning its result.
///
/// The function uses the thread-local runtime to execute the future. If no
/// runtime has been set, it creates a basic fallback runtime (for local mode).
///
/// # Arguments
///
/// * `future` - Async function to execute
///
/// # Returns
///
/// The output of the future once it completes
///
/// # Panics
///
/// Panics if the future does not complete (should not happen in normal operation)
///
/// # Example
///
/// ```ignore
/// let result = block_on(async move {
///     fs.benchfs_open("/test.txt", OpenFlags::read_only()).await
/// });
/// ```
pub fn block_on<F>(future: F) -> F::Output
where
    F: std::future::Future + 'static,
    F::Output: 'static,
{
    block_on_with_name("unnamed", future)
}

/// Execute an async function synchronously with operation name for debugging
///
/// This is similar to [`block_on`] but includes an operation name for better
/// debugging and timeout tracking. When a timeout occurs, the operation name
/// is included in the error message to help identify which operation failed.
///
/// # Arguments
///
/// * `operation_name` - Name of the operation (e.g., "open", "write", "read")
/// * `future` - Async function to execute
///
/// # Returns
///
/// The output of the future once it completes
///
/// # Panics
///
/// Exits the process if the operation times out
///
/// # Example
///
/// ```ignore
/// let result = block_on_with_name("open", async move {
///     fs.benchfs_open("/test.txt", OpenFlags::read_only()).await
/// });
/// ```
pub fn block_on_with_name<F>(operation_name: &str, future: F) -> F::Output
where
    F: std::future::Future + 'static,
    F::Output: 'static,
{
    LOCAL_RUNTIME.with(|runtime_cell| {
        let runtime = runtime_cell.borrow();

        // Get or create runtime
        let rt: Rc<Runtime> = if let Some(ref rt) = *runtime {
            rt.clone()
        } else {
            // Fallback: create a basic runtime if not initialized
            // This should only happen in local mode
            drop(runtime);
            let new_runtime_rc: Rc<Runtime> = Runtime::new(256); // Runtime::new() returns Rc<Runtime>
            *runtime_cell.borrow_mut() = Some(new_runtime_rc.clone());
            new_runtime_rc
        };

        // Get timeout from environment or use default of 120 seconds
        let timeout_secs = std::env::var("BENCHFS_OPERATION_TIMEOUT")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(120);

        // Debug logging if enabled
        let debug_enabled = std::env::var("BENCHFS_DEBUG").unwrap_or_default() == "1";
        if debug_enabled {
            eprintln!("[BENCHFS] Starting operation '{}' with {}s timeout",
                     operation_name, timeout_secs);
        }

        // Create a holder for the result and timeout flag
        let result_holder = Rc::new(RefCell::new(None));
        let result_holder_clone = result_holder.clone();
        let timed_out = std::sync::Arc::new(AtomicBool::new(false));
        let timed_out_clone = timed_out.clone();

        // Create timeout future
        let timeout_duration = std::time::Duration::from_secs(timeout_secs);
        let op_name = operation_name.to_string();

        // Combine the original future with timeout using select!
        let combined_future = async move {
            // Pin the future to make it work with select!
            futures::pin_mut!(future);
            let mut user_future = future.fuse();
            let mut timeout_future = Delay::new(timeout_duration).fuse();

            select! {
                result = user_future => {
                    if debug_enabled {
                        eprintln!("[BENCHFS] Operation '{}' completed successfully", op_name);
                    }
                    *result_holder_clone.borrow_mut() = Some(result);
                },
                _ = timeout_future => {
                    eprintln!(
                        "ERROR: Operation '{}' timed out after {} seconds. Aborting to prevent hang.",
                        op_name, timeout_secs
                    );
                    timed_out_clone.store(true, Ordering::Relaxed);
                    // Don't abort immediately, let runtime finish cleanly
                }
            }
        };

        rt.run(combined_future);

        // Check if timeout occurred
        if timed_out.load(Ordering::Relaxed) {
            eprintln!("ERROR: Exiting due to timeout in operation '{}'", operation_name);
            std::process::exit(1);
        }

        // Extract the result
        result_holder
            .borrow_mut()
            .take()
            .unwrap_or_else(|| {
                panic!("Future for operation '{}' did not complete", operation_name)
            })
    })
}
