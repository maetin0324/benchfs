//! Signal handling utilities for BenchFS servers
//!
//! This module provides common signal handling functionality used by
//! both standalone (benchfsd) and MPI (benchfsd_mpi) server binaries.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

/// Set up signal handlers for graceful shutdown and debugging
///
/// This function registers handlers for:
/// - SIGINT and SIGTERM: graceful shutdown
/// - SIGUSR1: async task backtrace dump
///
/// # Arguments
/// * `running` - An atomic boolean flag that will be set to false on shutdown signal
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
    // Store the running flag in a static for signal handler access
    static RUNNING_FLAG: Mutex<Option<Arc<AtomicBool>>> = Mutex::new(None);
    *RUNNING_FLAG.lock().unwrap() = Some(running);

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

    #[cfg(unix)]
    extern "C" fn shutdown_signal_handler(_: libc::c_int) {
        static RUNNING_FLAG: Mutex<Option<Arc<AtomicBool>>> = Mutex::new(None);

        if let Some(flag) = RUNNING_FLAG.lock().unwrap().as_ref() {
            eprintln!("\nReceived shutdown signal, stopping server...");
            flag.store(false, Ordering::Relaxed);
        }
    }

    #[cfg(unix)]
    extern "C" fn taskdump_signal_handler(_: libc::c_int) {
        eprintln!("\nReceived SIGUSR1, dumping async task backtraces...");
        crate::logging::dump_async_tasks();
    }
}
