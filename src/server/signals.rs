//! Signal handling utilities for BenchFS servers
//!
//! This module provides common signal handling functionality used by
//! both standalone (benchfsd) and MPI (benchfsd_mpi) server binaries.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

/// Set up signal handlers for graceful shutdown
///
/// This function registers handlers for SIGINT and SIGTERM signals
/// that will set the provided `running` flag to false when received.
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

    // Setup SIGINT (Ctrl+C) and SIGTERM handlers
    #[cfg(unix)]
    {
        use libc::{SIGINT, SIGTERM};
        unsafe {
            libc::signal(SIGINT, signal_handler as libc::sighandler_t);
            libc::signal(SIGTERM, signal_handler as libc::sighandler_t);
        }
    }

    #[cfg(unix)]
    extern "C" fn signal_handler(_: libc::c_int) {
        static RUNNING_FLAG: Mutex<Option<Arc<AtomicBool>>> = Mutex::new(None);

        if let Some(flag) = RUNNING_FLAG.lock().unwrap().as_ref() {
            eprintln!("\nReceived shutdown signal, stopping server...");
            flag.store(false, Ordering::Relaxed);
        }
    }
}
