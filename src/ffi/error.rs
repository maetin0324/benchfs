//! Error handling for C FFI
//!
//! This module provides C-compatible error handling mechanisms for BenchFS
//! operations. It includes:
//!
//! - POSIX-style error codes for common error conditions
//! - Thread-local error message storage to avoid race conditions
//! - Conversion helpers from Rust `Result` to C error codes
//!
//! # Error Codes
//!
//! Error codes follow POSIX conventions and are negative integers:
//!
//! - `BENCHFS_SUCCESS` (0): Operation succeeded
//! - `BENCHFS_ERROR` (-1): Generic error
//! - `BENCHFS_ENOENT` (-2): No such file or directory
//! - `BENCHFS_EIO` (-3): I/O error
//! - `BENCHFS_ENOMEM` (-4): Out of memory
//! - `BENCHFS_EINVAL` (-5): Invalid argument
//! - `BENCHFS_EEXIST` (-6): File exists
//! - `BENCHFS_ENOTDIR` (-20): Not a directory
//! - `BENCHFS_EISDIR` (-21): Is a directory
//! - `BENCHFS_ENOSPC` (-28): No space left on device
//!
//! # Thread Safety
//!
//! Error messages are stored in thread-local storage, making this module
//! safe for use in multi-threaded environments (e.g., MPI applications).

use std::cell::RefCell;
use std::ffi::CString;
use std::os::raw::c_char;

/// Operation succeeded
pub const BENCHFS_SUCCESS: i32 = 0;

/// Generic error (unspecified)
pub const BENCHFS_ERROR: i32 = -1;

/// No such file or directory
pub const BENCHFS_ENOENT: i32 = -2;

/// I/O error
pub const BENCHFS_EIO: i32 = -3;

/// Out of memory
pub const BENCHFS_ENOMEM: i32 = -4;

/// Invalid argument
pub const BENCHFS_EINVAL: i32 = -5;

/// File exists
pub const BENCHFS_EEXIST: i32 = -6;

/// Not a directory
pub const BENCHFS_ENOTDIR: i32 = -20;

/// Is a directory
pub const BENCHFS_EISDIR: i32 = -21;

/// No space left on device
pub const BENCHFS_ENOSPC: i32 = -28;

thread_local! {
    /// Thread-local storage for the last error message
    ///
    /// Each thread maintains its own error message to avoid race conditions
    /// in multi-threaded environments (e.g., MPI applications).
    static LAST_ERROR: RefCell<Option<CString>> = RefCell::new(None);
}

/// Set error message for the current thread
///
/// This function stores an error message in thread-local storage that can
/// be retrieved later via [`benchfs_get_error`]. The message is converted
/// to a C-compatible null-terminated string.
///
/// # Arguments
///
/// * `msg` - Error message string (will be converted to C string)
///
/// # Note
///
/// If the message contains null bytes, it will be truncated at the first null.
pub fn set_error_message(msg: &str) {
    LAST_ERROR.with(|err| {
        *err.borrow_mut() = CString::new(msg).ok();
    });
}

/// Get the last error message for the current thread (C-compatible)
///
/// Returns a pointer to a null-terminated C string containing the last error
/// message set by any BenchFS FFI function in the current thread. The returned
/// pointer is valid until the next error occurs in this thread.
///
/// # Returns
///
/// * Pointer to C string if an error message is set
/// * `NULL` if no error message is set
///
/// # Safety
///
/// The returned pointer is owned by BenchFS and must not be freed by the caller.
/// The pointer is valid until the next BenchFS operation that sets an error,
/// or until the thread exits.
///
/// # Example (C)
///
/// ```c
/// benchfs_context_t* ctx = benchfs_init("client", "/tmp/registry", NULL, 0);
/// if (!ctx) {
///     const char* error = benchfs_get_error();
///     if (error) {
///         fprintf(stderr, "Error: %s\n", error);
///     }
/// }
/// ```
#[unsafe(no_mangle)]
pub extern "C" fn benchfs_get_error() -> *const c_char {
    LAST_ERROR.with(|err| {
        err.borrow()
            .as_ref()
            .map(|s| s.as_ptr())
            .unwrap_or(std::ptr::null())
    })
}

/// Convert a Rust Result to a C error code
///
/// This helper function converts Rust-style `Result<T, E>` types to C-style
/// error codes. On success, it returns `BENCHFS_SUCCESS`. On error, it stores
/// the error message in thread-local storage and returns an appropriate error
/// code based on the error message content.
///
/// # Arguments
///
/// * `result` - Rust Result to convert
///
/// # Returns
///
/// * `BENCHFS_SUCCESS` (0) on success
/// * Negative error code on failure (see module-level error code documentation)
///
/// # Error Code Mapping
///
/// The function attempts to map error messages to specific error codes:
/// - Messages containing "not found" → `BENCHFS_ENOENT`
/// - Messages containing "exists" → `BENCHFS_EEXIST`
/// - Messages containing "Storage full" or "No space" → `BENCHFS_ENOSPC`
/// - Messages containing "Invalid" → `BENCHFS_EINVAL`
/// - All other errors → `BENCHFS_EIO`
pub fn result_to_error_code<T>(result: Result<T, impl std::fmt::Display>) -> i32 {
    match result {
        Ok(_) => BENCHFS_SUCCESS,
        Err(e) => {
            let msg = e.to_string();
            set_error_message(&msg);

            // Try to map to specific error codes based on error message
            if msg.contains("not found") || msg.contains("No such") {
                BENCHFS_ENOENT
            } else if msg.contains("exists") {
                BENCHFS_EEXIST
            } else if msg.contains("Storage full") || msg.contains("No space") {
                BENCHFS_ENOSPC
            } else if msg.contains("Invalid") || msg.contains("invalid") {
                BENCHFS_EINVAL
            } else {
                BENCHFS_EIO
            }
        }
    }
}
