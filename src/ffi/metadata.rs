// Metadata operations FFI
//
// This module provides C-compatible metadata operations:
// - stat (file/directory status)
// - mkdir/rmdir (directory operations)
// - get_file_size
// - rename
// - truncate

use std::ffi::CStr;
use std::os::raw::c_char;

use super::error::*;
use super::runtime::{block_on, with_benchfs_ctx};

// C-compatible stat structure
// This matches the basic fields of Unix struct stat
#[repr(C)]
pub struct benchfs_stat_t {
    pub st_ino: u64,     // Inode number
    pub st_mode: u32,    // File mode
    pub st_nlink: u64,   // Number of hard links
    pub st_size: i64,    // File size in bytes
    pub st_blocks: i64,  // Number of 512B blocks allocated
    pub st_blksize: i64, // Preferred I/O block size
}

impl Default for benchfs_stat_t {
    fn default() -> Self {
        Self {
            st_ino: 0,
            st_mode: 0,
            st_nlink: 1,
            st_size: 0,
            st_blocks: 0,
            st_blksize: 4096,
        }
    }
}

/// Get file or directory status
///
/// # Arguments
/// * `ctx` - BenchFS context (unused)
/// * `path` - File or directory path
/// * `buf` - Output buffer for stat information
///
/// # Returns
/// 0 on success, negative error code on failure
#[unsafe(no_mangle)]
pub extern "C" fn benchfs_stat(
    _ctx: *mut super::init::benchfs_context_t,
    path: *const c_char,
    buf: *mut benchfs_stat_t,
) -> i32 {
    if path.is_null() || buf.is_null() {
        set_error_message("path and buf must not be null");
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
        let fs_ptr = fs as *const crate::api::file_ops::BenchFS;
        unsafe {
            let fs_ref = &*fs_ptr;
            block_on(async move { fs_ref.benchfs_stat(path_str).await })
        }
    });

    match result {
        Ok(Ok(stat)) => {
            unsafe {
                (*buf).st_ino = stat.inode;
                (*buf).st_mode = if stat.is_dir() {
                    0o040755 // S_IFDIR | 0755
                } else {
                    0o100644 // S_IFREG | 0644
                };
                (*buf).st_nlink = 1;
                (*buf).st_size = stat.size as i64;
                (*buf).st_blocks = ((stat.size + 511) / 512) as i64;
                (*buf).st_blksize = 4096;
            }
            BENCHFS_SUCCESS
        }
        Ok(Err(e)) => {
            set_error_message(&e.to_string());
            BENCHFS_ENOENT
        }
        Err(e) => {
            set_error_message(&e);
            BENCHFS_ERROR
        }
    }
}

/// Get file size
///
/// # Arguments
/// * `ctx` - BenchFS context (unused)
/// * `path` - File path
///
/// # Returns
/// File size in bytes, or negative error code
#[unsafe(no_mangle)]
pub extern "C" fn benchfs_get_file_size(
    _ctx: *mut super::init::benchfs_context_t,
    path: *const c_char,
) -> i64 {
    if path.is_null() {
        set_error_message("path must not be null");
        return BENCHFS_EINVAL as i64;
    }

    let path_str = unsafe {
        match CStr::from_ptr(path).to_str() {
            Ok(s) => s,
            Err(_) => {
                set_error_message("Invalid UTF-8 in path");
                return BENCHFS_EINVAL as i64;
            }
        }
    };

    let result = with_benchfs_ctx(|fs| {
        let fs_ptr = fs as *const crate::api::file_ops::BenchFS;
        unsafe {
            let fs_ref = &*fs_ptr;
            block_on(async move { fs_ref.benchfs_stat(path_str).await })
        }
    });

    match result {
        Ok(Ok(stat)) => stat.size as i64,
        Ok(Err(e)) => {
            set_error_message(&e.to_string());
            BENCHFS_ENOENT as i64
        }
        Err(e) => {
            set_error_message(&e);
            BENCHFS_ERROR as i64
        }
    }
}

/// Create a directory
///
/// # Arguments
/// * `ctx` - BenchFS context (unused)
/// * `path` - Directory path
/// * `mode` - Directory permissions (Unix-style)
///
/// # Returns
/// 0 on success, negative error code on failure
#[unsafe(no_mangle)]
pub extern "C" fn benchfs_mkdir(
    _ctx: *mut super::init::benchfs_context_t,
    path: *const c_char,
    mode: u32,
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
        let fs_ptr = fs as *const crate::api::file_ops::BenchFS;
        unsafe {
            let fs_ref = &*fs_ptr;
            block_on(async move {
                fs_ref
                    .benchfs_mkdir(path_str, mode)
                    .await
                    .map_err(|e| e.to_string())
            })
        }
    });

    result_to_error_code(result.and_then(|r| r))
}

/// Remove a directory
///
/// # Arguments
/// * `ctx` - BenchFS context (unused)
/// * `path` - Directory path
///
/// # Returns
/// 0 on success, negative error code on failure
#[unsafe(no_mangle)]
pub extern "C" fn benchfs_rmdir(
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

    let result = with_benchfs_ctx(|fs| fs.benchfs_rmdir(path_str).map_err(|e| e.to_string()));

    result_to_error_code(result.and_then(|r| r))
}

/// Rename a file or directory
///
/// # Arguments
/// * `ctx` - BenchFS context (unused)
/// * `oldpath` - Current path
/// * `newpath` - New path
///
/// # Returns
/// 0 on success, negative error code on failure
#[unsafe(no_mangle)]
pub extern "C" fn benchfs_rename(
    _ctx: *mut super::init::benchfs_context_t,
    oldpath: *const c_char,
    newpath: *const c_char,
) -> i32 {
    if oldpath.is_null() || newpath.is_null() {
        set_error_message("oldpath and newpath must not be null");
        return BENCHFS_EINVAL;
    }

    let oldpath_str = unsafe {
        match CStr::from_ptr(oldpath).to_str() {
            Ok(s) => s,
            Err(_) => {
                set_error_message("Invalid UTF-8 in oldpath");
                return BENCHFS_EINVAL;
            }
        }
    };

    let newpath_str = unsafe {
        match CStr::from_ptr(newpath).to_str() {
            Ok(s) => s,
            Err(_) => {
                set_error_message("Invalid UTF-8 in newpath");
                return BENCHFS_EINVAL;
            }
        }
    };

    let result = with_benchfs_ctx(|fs| {
        fs.benchfs_rename(oldpath_str, newpath_str)
            .map_err(|e| e.to_string())
    });

    result_to_error_code(result.and_then(|r| r))
}

/// Truncate a file to a specified size
///
/// # Arguments
/// * `ctx` - BenchFS context (unused)
/// * `path` - File path
/// * `size` - New file size
///
/// # Returns
/// 0 on success, negative error code on failure
#[unsafe(no_mangle)]
pub extern "C" fn benchfs_truncate(
    _ctx: *mut super::init::benchfs_context_t,
    path: *const c_char,
    size: i64,
) -> i32 {
    if path.is_null() {
        set_error_message("path must not be null");
        return BENCHFS_EINVAL;
    }

    if size < 0 {
        set_error_message("size must be non-negative");
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
        let fs_ptr = fs as *const crate::api::file_ops::BenchFS;
        unsafe {
            let fs_ref = &*fs_ptr;
            block_on(async move {
                fs_ref
                    .benchfs_truncate(path_str, size as u64)
                    .await
                    .map_err(|e| e.to_string())
            })
        }
    });

    result_to_error_code(result.and_then(|r| r))
}

/// Check file access permissions
///
/// # Arguments
/// * `ctx` - BenchFS context (unused)
/// * `path` - File path
/// * `mode` - Access mode to check (R_OK, W_OK, X_OK, F_OK)
///
/// # Returns
/// 0 if access is permitted, negative error code otherwise
#[unsafe(no_mangle)]
pub extern "C" fn benchfs_access(
    _ctx: *mut super::init::benchfs_context_t,
    path: *const c_char,
    _mode: i32,
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

    // Check if file exists
    let result = with_benchfs_ctx(|fs| {
        let fs_ptr = fs as *const crate::api::file_ops::BenchFS;
        unsafe {
            let fs_ref = &*fs_ptr;
            block_on(async move { fs_ref.benchfs_stat(path_str).await })
        }
    });

    match result {
        Ok(Ok(_)) => BENCHFS_SUCCESS,
        Ok(Err(_)) => BENCHFS_ENOENT,
        Err(e) => {
            set_error_message(&e);
            BENCHFS_ERROR
        }
    }
}
