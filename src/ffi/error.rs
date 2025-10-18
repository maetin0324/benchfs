// Error handling for C FFI
//
// This module provides:
// - C-compatible error codes
// - Thread-local error message storage
// - Conversion helpers from Result to error codes

use std::cell::RefCell;
use std::ffi::CString;
use std::os::raw::c_char;

// Error code definitions (following POSIX conventions)
pub const BENCHFS_SUCCESS: i32 = 0;
pub const BENCHFS_ERROR: i32 = -1;
pub const BENCHFS_ENOENT: i32 = -2;   // No such file or directory
pub const BENCHFS_EIO: i32 = -3;       // I/O error
pub const BENCHFS_ENOMEM: i32 = -4;    // Out of memory
pub const BENCHFS_EINVAL: i32 = -5;    // Invalid argument
pub const BENCHFS_EEXIST: i32 = -6;    // File exists
pub const BENCHFS_ENOTDIR: i32 = -20;  // Not a directory
pub const BENCHFS_EISDIR: i32 = -21;   // Is a directory

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = RefCell::new(None);
}

/// Set error message for current thread
pub fn set_error_message(msg: &str) {
    LAST_ERROR.with(|err| {
        *err.borrow_mut() = CString::new(msg).ok();
    });
}

/// Get error message (C-compatible)
///
/// Returns a pointer to the last error message for the current thread.
/// Returns null if no error message is set.
#[unsafe(no_mangle)]
pub extern "C" fn benchfs_get_error() -> *const c_char {
    LAST_ERROR.with(|err| {
        err.borrow()
            .as_ref()
            .map(|s| s.as_ptr())
            .unwrap_or(std::ptr::null())
    })
}

/// Convert Result to error code
///
/// On success: returns BENCHFS_SUCCESS
/// On error: stores error message and returns appropriate error code
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
            } else if msg.contains("Invalid") || msg.contains("invalid") {
                BENCHFS_EINVAL
            } else {
                BENCHFS_EIO
            }
        }
    }
}
