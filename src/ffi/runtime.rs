// Global runtime management for FFI
//
// This module provides:
// - Global async runtime for executing async BenchFS operations from C
// - Thread-local BenchFS context storage
// - block_on() helper for async→sync conversion

use std::cell::RefCell;
use std::rc::Rc;
use pluvio_runtime::executor::Runtime;
use crate::api::file_ops::BenchFS;

/// Thread-local runtime
///
/// Since Runtime contains RefCell which is not Sync, we cannot use it as a global static.
/// Instead, each thread gets its own runtime instance.
thread_local! {
    static LOCAL_RUNTIME: Rc<Runtime> = Runtime::new(256);
}

/// Thread-local BenchFS context
///
/// Each thread (MPI rank) has its own BenchFS instance stored here.
/// This allows concurrent access from multiple MPI processes without conflicts.
thread_local! {
    pub static BENCHFS_CTX: RefCell<Option<Rc<BenchFS>>> = RefCell::new(None);
}

/// Set BenchFS context for current thread
pub fn set_benchfs_ctx(benchfs: Rc<BenchFS>) {
    BENCHFS_CTX.with(|ctx| {
        *ctx.borrow_mut() = Some(benchfs);
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
    LOCAL_RUNTIME.with(|runtime| {
        // Create a holder for the result
        let result_holder = Rc::new(RefCell::new(None));
        let result_holder_clone = result_holder.clone();

        // Wrap the future to store its result
        let wrapped_future = async move {
            let result = future.await;
            *result_holder_clone.borrow_mut() = Some(result);
        };

        runtime.run(wrapped_future);

        // Extract the result
        result_holder.borrow_mut().take().expect("Future did not complete")
    })
}
