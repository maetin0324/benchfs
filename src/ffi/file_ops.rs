// File operations FFI
//
// This module provides C-compatible file operation functions that wrap
// BenchFS async operations with sync conversion.

use std::ffi::CStr;
use std::os::raw::c_char;
use std::slice;

use super::runtime::{block_on, with_benchfs_ctx};
use super::error::*;
use crate::api::types::{FileHandle, OpenFlags};
use crate::api::file_ops::BenchFS;

// Opaque type for file handle
#[repr(C)]
pub struct benchfs_file_t {
    _private: [u8; 0],
}

// File open flags (matching Linux O_* flags)
pub const BENCHFS_O_RDONLY: i32 = 0x0000;
pub const BENCHFS_O_WRONLY: i32 = 0x0001;
pub const BENCHFS_O_RDWR: i32 = 0x0002;
pub const BENCHFS_O_CREAT: i32 = 0x0040;
pub const BENCHFS_O_EXCL: i32 = 0x0080;
pub const BENCHFS_O_TRUNC: i32 = 0x0200;
pub const BENCHFS_O_APPEND: i32 = 0x0400;

/// Convert C flags to OpenFlags
fn c_flags_to_open_flags(flags: i32) -> OpenFlags {
    let mut open_flags = OpenFlags {
        read: false,
        write: false,
        create: false,
        truncate: false,
        append: false,
    };

    // Access mode
    let access_mode = flags & 0x3;
    match access_mode {
        BENCHFS_O_RDONLY => {
            open_flags.read = true;
            open_flags.write = false;
        }
        BENCHFS_O_WRONLY => {
            open_flags.read = false;
            open_flags.write = true;
        }
        BENCHFS_O_RDWR => {
            open_flags.read = true;
            open_flags.write = true;
        }
        _ => {}
    }

    // File creation flags
    if flags & BENCHFS_O_CREAT != 0 {
        open_flags.create = true;
    }
    if flags & BENCHFS_O_TRUNC != 0 {
        open_flags.truncate = true;
    }
    if flags & BENCHFS_O_APPEND != 0 {
        open_flags.append = true;
    }
    // Note: EXCL flag is handled implicitly by the create flag in BenchFS

    open_flags
}

/// Create a new file
///
/// # Arguments
/// * `ctx` - BenchFS context (unused, uses thread-local context)
/// * `path` - File path
/// * `flags` - Open flags (O_CREAT, O_WRONLY, etc.)
/// * `mode` - File permissions (Unix-style, currently unused)
///
/// # Returns
/// File handle pointer, or NULL on error
#[unsafe(no_mangle)]
pub extern "C" fn benchfs_create(
    _ctx: *mut super::init::benchfs_context_t,
    path: *const c_char,
    flags: i32,
    _mode: u32,
) -> *mut benchfs_file_t {
    if path.is_null() {
        set_error_message("path must not be null");
        return std::ptr::null_mut();
    }

    let path_str = unsafe {
        match CStr::from_ptr(path).to_str() {
            Ok(s) => s,
            Err(_) => {
                set_error_message("Invalid UTF-8 in path");
                return std::ptr::null_mut();
            }
        }
    };

    // Convert flags to OpenFlags
    let mut open_flags = c_flags_to_open_flags(flags);
    open_flags.create = true; // Ensure create flag is set

    // Open file using thread-local context
    let result = with_benchfs_ctx(|fs| {
        fs.benchfs_open(path_str, open_flags)
    });

    match result {
        Ok(Ok(handle)) => {
            // Box the handle and return as opaque pointer
            Box::into_raw(Box::new(handle)) as *mut benchfs_file_t
        }
        Ok(Err(e)) => {
            set_error_message(&e.to_string());
            std::ptr::null_mut()
        }
        Err(e) => {
            set_error_message(&e);
            std::ptr::null_mut()
        }
    }
}

/// Open an existing file
///
/// # Arguments
/// * `ctx` - BenchFS context (unused)
/// * `path` - File path
/// * `flags` - Open flags
///
/// # Returns
/// File handle pointer, or NULL on error
#[unsafe(no_mangle)]
pub extern "C" fn benchfs_open(
    _ctx: *mut super::init::benchfs_context_t,
    path: *const c_char,
    flags: i32,
) -> *mut benchfs_file_t {
    if path.is_null() {
        set_error_message("path must not be null");
        return std::ptr::null_mut();
    }

    let path_str = unsafe {
        match CStr::from_ptr(path).to_str() {
            Ok(s) => s,
            Err(_) => {
                set_error_message("Invalid UTF-8 in path");
                return std::ptr::null_mut();
            }
        }
    };

    let open_flags = c_flags_to_open_flags(flags);

    let result = with_benchfs_ctx(|fs| {
        fs.benchfs_open(path_str, open_flags)
    });

    match result {
        Ok(Ok(handle)) => {
            Box::into_raw(Box::new(handle)) as *mut benchfs_file_t
        }
        Ok(Err(e)) => {
            set_error_message(&e.to_string());
            std::ptr::null_mut()
        }
        Err(e) => {
            set_error_message(&e);
            std::ptr::null_mut()
        }
    }
}

/// Write data to a file
///
/// # Arguments
/// * `file` - File handle
/// * `buffer` - Data to write
/// * `size` - Number of bytes to write
/// * `offset` - File offset (currently unused, position is managed by handle)
///
/// # Returns
/// Number of bytes written, or negative error code
#[unsafe(no_mangle)]
pub extern "C" fn benchfs_write(
    file: *mut benchfs_file_t,
    buffer: *const u8,
    size: usize,
    _offset: i64,
) -> i64 {
    if file.is_null() || buffer.is_null() {
        set_error_message("file and buffer must not be null");
        return BENCHFS_EINVAL as i64;
    }

    unsafe {
        let handle = &*(file as *const FileHandle);
        let handle_clone = handle.clone();
        let buf = slice::from_raw_parts(buffer, size).to_vec();

        // Execute async write synchronously
        // We need to work around the lifetime issue by using raw pointers
        let result = with_benchfs_ctx(|fs| {
            // Get raw pointer to BenchFS (unsafe but necessary for FFI)
            let fs_ptr = fs as *const BenchFS;

            // Execute block_on with the pointer
            unsafe {
                let fs_ref = &*fs_ptr;
                block_on(async move {
                    fs_ref.benchfs_write(&handle_clone, &buf).await
                })
            }
        });

        match result {
            Ok(Ok(n)) => n as i64,
            Ok(Err(e)) => {
                set_error_message(&e.to_string());
                BENCHFS_EIO as i64
            }
            Err(e) => {
                set_error_message(&e);
                BENCHFS_ERROR as i64
            }
        }
    }
}

/// Read data from a file
///
/// # Arguments
/// * `file` - File handle
/// * `buffer` - Buffer to read into
/// * `size` - Number of bytes to read
/// * `offset` - File offset (currently unused, position is managed by handle)
///
/// # Returns
/// Number of bytes read, or negative error code
#[unsafe(no_mangle)]
pub extern "C" fn benchfs_read(
    file: *mut benchfs_file_t,
    buffer: *mut u8,
    size: usize,
    _offset: i64,
) -> i64 {
    if file.is_null() || buffer.is_null() {
        set_error_message("file and buffer must not be null");
        return BENCHFS_EINVAL as i64;
    }

    unsafe {
        let handle = &*(file as *const FileHandle);
        let handle_clone = handle.clone();
        let buf_ptr = buffer;
        let buf_size = size;

        // Execute async read synchronously
        // We need to work around the lifetime issue by using raw pointers
        let mut temp_buf = vec![0u8; buf_size];
        let result = with_benchfs_ctx(|fs| {
            // Get raw pointer to BenchFS
            let fs_ptr = fs as *const BenchFS;
            let temp_buf_ptr = temp_buf.as_mut_ptr();

            unsafe {
                let fs_ref = &*fs_ptr;
                let mut local_buf = std::slice::from_raw_parts_mut(temp_buf_ptr, buf_size);

                let n = block_on(async move {
                    fs_ref.benchfs_read(&handle_clone, &mut local_buf).await
                });

                match n {
                    Ok(bytes_read) => {
                        // Copy data back to C buffer
                        std::ptr::copy_nonoverlapping(
                            temp_buf_ptr,
                            buf_ptr,
                            bytes_read
                        );
                        Ok(bytes_read)
                    }
                    Err(e) => Err(e)
                }
            }
        });

        match result {
            Ok(Ok(n)) => n as i64,
            Ok(Err(e)) => {
                set_error_message(&e.to_string());
                BENCHFS_EIO as i64
            }
            Err(e) => {
                set_error_message(&e);
                BENCHFS_ERROR as i64
            }
        }
    }
}

/// Close a file
///
/// # Arguments
/// * `file` - File handle
///
/// # Returns
/// 0 on success, negative error code on failure
#[unsafe(no_mangle)]
pub extern "C" fn benchfs_close(file: *mut benchfs_file_t) -> i32 {
    if file.is_null() {
        return BENCHFS_EINVAL;
    }

    unsafe {
        // Take ownership of the handle and drop it
        let handle = Box::from_raw(file as *mut FileHandle);

        let result = with_benchfs_ctx(|fs| {
            fs.benchfs_close(&handle).map_err(|e| e.to_string())
        });

        result_to_error_code(result.and_then(|r| r))
    }
}

/// Synchronize file data to storage
///
/// # Arguments
/// * `file` - File handle
///
/// # Returns
/// 0 on success, negative error code on failure
#[unsafe(no_mangle)]
pub extern "C" fn benchfs_fsync(file: *mut benchfs_file_t) -> i32 {
    if file.is_null() {
        return BENCHFS_EINVAL;
    }

    unsafe {
        let handle = &*(file as *const FileHandle);

        let result = with_benchfs_ctx(|fs| {
            let fs_ptr = fs as *const BenchFS;
            unsafe {
                let fs_ref = &*fs_ptr;
                block_on(async move {
                    fs_ref.benchfs_fsync(handle).await.map_err(|e| e.to_string())
                })
            }
        });

        result_to_error_code(result.and_then(|r| r))
    }
}

/// Remove (delete) a file
///
/// # Arguments
/// * `ctx` - BenchFS context (unused)
/// * `path` - File path
///
/// # Returns
/// 0 on success, negative error code on failure
#[unsafe(no_mangle)]
pub extern "C" fn benchfs_remove(
    _ctx: *mut super::init::benchfs_context_t,
    path: *const c_char,
) -> i32 {
    if path.is_null() {
        set_error_message("path must not be null");
        return BENCHFS_EINVAL;
    }

    let path_str = unsafe {
        match CStr::from_ptr(path).to_str() {
            Ok(s) => s,
            Err(_) => {
                set_error_message("Invalid UTF-8 in path");
                return BENCHFS_EINVAL;
            }
        }
    };

    let result = with_benchfs_ctx(|fs| {
        let fs_ptr = fs as *const BenchFS;
        unsafe {
            let fs_ref = &*fs_ptr;
            block_on(async move {
                fs_ref.benchfs_unlink(path_str).await.map_err(|e| e.to_string())
            })
        }
    });

    result_to_error_code(result.and_then(|r| r))
}

/// Seek to a position in a file
///
/// # Arguments
/// * `file` - File handle
/// * `offset` - Offset from whence
/// * `whence` - Seek mode (0=SET, 1=CUR, 2=END)
///
/// # Returns
/// New file position, or negative error code
#[unsafe(no_mangle)]
pub extern "C" fn benchfs_lseek(
    file: *mut benchfs_file_t,
    offset: i64,
    whence: i32,
) -> i64 {
    if file.is_null() {
        set_error_message("file must not be null");
        return BENCHFS_EINVAL as i64;
    }

    unsafe {
        let handle = &*(file as *const FileHandle);

        let result = with_benchfs_ctx(|fs| {
            fs.benchfs_seek(handle, offset, whence).map_err(|e| e.to_string())
        });

        match result {
            Ok(Ok(pos)) => pos as i64,
            Ok(Err(e)) => {
                set_error_message(&e);
                BENCHFS_ERROR as i64
            }
            Err(e) => {
                set_error_message(&e);
                BENCHFS_ERROR as i64
            }
        }
    }
}
