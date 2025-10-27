// Global runtime management for FFI
//
// This module provides:
// - Global async runtime for executing async BenchFS operations from C
// - Thread-local BenchFS context storage
// - Thread-local RPC server and connection pool storage for distributed mode
// - block_on() helper for async→sync conversion

use crate::api::file_ops::BenchFS;
use crate::rpc::connection::ConnectionPool;
use crate::rpc::server::RpcServer;
use pluvio_runtime::executor::Runtime;
use std::cell::RefCell;
use std::rc::Rc;

// Thread-local runtime
//
// Since Runtime contains RefCell which is not Sync, we cannot use it as a global static.
// Instead, each thread gets its own runtime instance.
// For distributed mode, this is initialized in benchfs_init() with reactors registered.
thread_local! {
    static LOCAL_RUNTIME: RefCell<Option<Rc<Runtime>>> = RefCell::new(None);
}

// Thread-local BenchFS context
//
// Each thread (MPI rank) has its own BenchFS instance stored here.
// This allows concurrent access from multiple MPI processes without conflicts.
thread_local! {
    pub static BENCHFS_CTX: RefCell<Option<Rc<BenchFS>>> = RefCell::new(None);
}

// Thread-local RPC server (for server mode)
thread_local! {
    pub static RPC_SERVER: RefCell<Option<Rc<RpcServer>>> = RefCell::new(None);
}

// Thread-local connection pool (for distributed mode)
thread_local! {
    pub static CONNECTION_POOL: RefCell<Option<Rc<ConnectionPool>>> = RefCell::new(None);
}

/// Set runtime for current thread
pub fn set_runtime(runtime: Rc<Runtime>) {
    LOCAL_RUNTIME.with(|rt| {
        *rt.borrow_mut() = Some(runtime);
    });
}

/// Set BenchFS context for current thread
pub fn set_benchfs_ctx(benchfs: Rc<BenchFS>) {
    BENCHFS_CTX.with(|ctx| {
        *ctx.borrow_mut() = Some(benchfs);
    });
}

/// Set RPC server for current thread (server mode only)
pub fn set_rpc_server(server: Rc<RpcServer>) {
    RPC_SERVER.with(|srv| {
        *srv.borrow_mut() = Some(server);
    });
}

/// Set connection pool for current thread (distributed mode)
pub fn set_connection_pool(pool: Rc<ConnectionPool>) {
    CONNECTION_POOL.with(|p| {
        *p.borrow_mut() = Some(pool);
    });
}

/// Execute closure with BenchFS context
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

/// Execute async function synchronously
///
/// This is the core of async→sync conversion.
/// It spawns the future on the thread-local runtime and blocks until completion.
pub fn block_on<F>(future: F) -> F::Output
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

        // Create a holder for the result
        let result_holder = Rc::new(RefCell::new(None));
        let result_holder_clone = result_holder.clone();

        // Wrap the future to store its result
        let wrapped_future = async move {
            let result = future.await;
            *result_holder_clone.borrow_mut() = Some(result);
        };

        rt.run(wrapped_future);

        // Extract the result
        result_holder
            .borrow_mut()
            .take()
            .expect("Future did not complete")
    })
}
