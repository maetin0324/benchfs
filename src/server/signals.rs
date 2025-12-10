//! Signal handling utilities for BenchFS servers
//!
//! This module provides common signal handling functionality used by
//! both standalone (benchfsd) and MPI (benchfsd_mpi) server binaries.

use pluvio_runtime::executor::Runtime;
use std::cell::UnsafeCell;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use std::sync::Arc;

/// Global pointer to the running flag for signal handler access.
/// Using AtomicPtr instead of Mutex because signal handlers cannot safely lock mutexes.
static RUNNING_FLAG_PTR: AtomicPtr<AtomicBool> = AtomicPtr::new(std::ptr::null_mut());

/// Global pointer to the Runtime for signal handler access.
/// We use a raw pointer since Rc is not Send/Sync.
static RUNTIME_PTR: AtomicPtr<UnsafeCell<Option<Rc<Runtime>>>> =
    AtomicPtr::new(std::ptr::null_mut());

/// Set up signal handlers for graceful shutdown and debugging
///
/// This function registers handlers for:
/// - SIGINT and SIGTERM: graceful shutdown (uses Runtime's request_shutdown API)
/// - SIGUSR1: async task backtrace dump
///
/// # Arguments
/// * `running` - An atomic boolean flag that will be set to false on shutdown signal
///
/// # Safety
/// The `running` Arc must remain valid for the lifetime of the program.
/// This is typically ensured by storing it in the ServerState.
///
/// # Example
/// ```ignore
/// let running = Arc::new(AtomicBool::new(true));
/// setup_signal_handlers(running.clone());
///
/// while running.load(Ordering::Relaxed) {
///     // Server loop
/// }
/// ```
pub fn setup_signal_handlers(running: Arc<AtomicBool>) {
    // Store the raw pointer to the AtomicBool for signal handler access
    // Safety: The Arc keeps the AtomicBool alive, and we only store the pointer
    // The calling code must ensure the Arc is not dropped while signals may be received
    let ptr = Arc::into_raw(running);
    RUNNING_FLAG_PTR.store(ptr as *mut AtomicBool, Ordering::SeqCst);

    // Store the Runtime pointer for signal handler access
    // We need to box an UnsafeCell to store the Rc<Runtime>
    let runtime_cell = Box::new(UnsafeCell::new(pluvio_runtime::executor::get_runtime()));
    let runtime_ptr = Box::into_raw(runtime_cell);
    RUNTIME_PTR.store(runtime_ptr, Ordering::SeqCst);

    // Setup SIGINT (Ctrl+C), SIGTERM, and SIGUSR1 handlers
    #[cfg(unix)]
    {
        use libc::{SIGINT, SIGTERM, SIGUSR1};
        unsafe {
            libc::signal(SIGINT, shutdown_signal_handler as libc::sighandler_t);
            libc::signal(SIGTERM, shutdown_signal_handler as libc::sighandler_t);
            libc::signal(SIGUSR1, taskdump_signal_handler as libc::sighandler_t);
        }
        eprintln!("Signal handlers registered:");
        eprintln!("  - SIGINT/SIGTERM: graceful shutdown");
        eprintln!("  - SIGUSR1: async task backtrace dump");
    }
}

#[cfg(unix)]
extern "C" fn shutdown_signal_handler(sig: libc::c_int) {
    // Get the running flag pointer and set it to false
    let ptr = RUNNING_FLAG_PTR.load(Ordering::SeqCst);
    if !ptr.is_null() {
        // Safety: We stored a valid pointer in setup_signal_handlers
        // and the Arc is kept alive by the caller
        unsafe {
            (*ptr).store(false, Ordering::SeqCst);
        }
    }

    // Request runtime shutdown via the executor API
    let runtime_ptr = RUNTIME_PTR.load(Ordering::SeqCst);
    if !runtime_ptr.is_null() {
        // Safety: We stored a valid pointer in setup_signal_handlers
        unsafe {
            if let Some(runtime) = (*(*runtime_ptr).get()).as_ref() {
                runtime.request_shutdown();
            }
        }
    }

    // Use write() instead of eprintln!() for async-signal-safety
    let msg: &[u8] = match sig {
        libc::SIGINT => b"\nReceived SIGINT, initiating graceful shutdown...\n",
        libc::SIGTERM => b"\nReceived SIGTERM, initiating graceful shutdown...\n",
        _ => b"\nReceived shutdown signal, initiating graceful shutdown...\n",
    };
    unsafe {
        libc::write(2, msg.as_ptr() as *const libc::c_void, msg.len());
    }
}

#[cfg(unix)]
extern "C" fn taskdump_signal_handler(_: libc::c_int) {
    let msg = b"\nReceived SIGUSR1, dumping async task backtraces...\n";
    unsafe {
        libc::write(2, msg.as_ptr() as *const libc::c_void, msg.len());
    }
    crate::logging::dump_async_tasks();
}
