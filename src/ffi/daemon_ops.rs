//! Daemon mode FFI operations.
//!
//! This module provides C-compatible wrappers for daemon mode operations.
//! When daemon mode is enabled, IOR processes communicate with a client daemon
//! via shared memory instead of establishing direct UCX connections to servers.
//!
//! # Architecture
//!
//! ```text
//! IOR Process
//!     │ FFI calls (benchfs_daemon_*)
//!     ▼
//! DaemonClientStub ──► Shared Memory ──► Client Daemon ──► BenchFS Servers
//! ```
//!
//! # Usage
//!
//! 1. Call `benchfs_daemon_init()` instead of `benchfs_init()`
//! 2. Use `benchfs_daemon_open/read/write/close()` for file operations
//! 3. Call `benchfs_daemon_finalize()` when done

use std::ffi::CStr;
use std::os::raw::c_char;
use std::path::PathBuf;
use std::slice;

use super::error::*;
use crate::daemon::client_stub::DaemonClientStub;
use crate::daemon::launcher::LauncherConfig;
use crate::daemon::shm::ShmConfig;

std::thread_local! {
    static DAEMON_STUB: std::cell::RefCell<Option<DaemonClientStub>> = const { std::cell::RefCell::new(None) };
}

/// Opaque BenchFS context type for C code (daemon mode)
#[allow(non_camel_case_types)]
#[repr(C)]
pub struct benchfs_context_t {
    _private: [u8; 0],
}

/// Opaque file handle type for C code (daemon mode)
#[allow(non_camel_case_types)]
#[repr(C)]
pub struct benchfs_file_t {
    fd: u64,
}

/// Initialize BenchFS in daemon mode.
///
/// This function connects to an existing client daemon or spawns one if necessary.
/// Unlike direct mode, the daemon manages all UCX connections, reducing the total
/// connection count when many IOR processes run on the same node.
///
/// # Arguments
///
/// * `node_id` - Unique identifier for this client process
/// * `registry_dir` - Path to shared directory for server discovery
/// * `data_dir` - Path to local data storage directory
/// * `shm_name` - Shared memory name (NULL for auto-generated based on hostname)
///
/// # Returns
///
/// * Pointer to opaque context on success
/// * NULL on error (call `benchfs_get_error` for details)
///
/// # Safety
///
/// - `node_id` and `registry_dir` must be valid, null-terminated C strings
/// - `data_dir` and `shm_name` may be NULL
#[unsafe(no_mangle)]
pub extern "C" fn benchfs_init(
    node_id: *const c_char,
    registry_dir: *const c_char,
    data_dir: *const c_char,
    shm_name: *const c_char,
) -> *mut benchfs_context_t {
    crate::logging::init_with_hostname("info");

    // Validate required pointers
    if node_id.is_null() || registry_dir.is_null() {
        set_error_message("node_id and registry_dir must not be null");
        return std::ptr::null_mut();
    }

    // Convert C strings to Rust strings
    let _node_id_str = unsafe {
        match CStr::from_ptr(node_id).to_str() {
            Ok(s) => s,
            Err(_) => {
                set_error_message("Invalid UTF-8 in node_id");
                return std::ptr::null_mut();
            }
        }
    };

    let registry_dir_str = unsafe {
        match CStr::from_ptr(registry_dir).to_str() {
            Ok(s) => s,
            Err(_) => {
                set_error_message("Invalid UTF-8 in registry_dir");
                return std::ptr::null_mut();
            }
        }
    };

    let data_dir_str = if data_dir.is_null() {
        None
    } else {
        unsafe {
            match CStr::from_ptr(data_dir).to_str() {
                Ok(s) => Some(s),
                Err(_) => {
                    set_error_message("Invalid UTF-8 in data_dir");
                    return std::ptr::null_mut();
                }
            }
        }
    };

    let shm_name_str = if shm_name.is_null() {
        None
    } else {
        unsafe {
            match CStr::from_ptr(shm_name).to_str() {
                Ok(s) => Some(s.to_string()),
                Err(_) => {
                    set_error_message("Invalid UTF-8 in shm_name");
                    return std::ptr::null_mut();
                }
            }
        }
    };

    tracing::info!(
        "benchfs_daemon_init: registry_dir={}, data_dir={:?}, shm_name={:?}",
        registry_dir_str,
        data_dir_str,
        shm_name_str
    );

    // Get daemon binary path from environment variable or use default
    let daemon_binary = match std::env::var("BENCHFS_DAEMON_BINARY") {
        Ok(path) => {
            tracing::info!("Using daemon binary from BENCHFS_DAEMON_BINARY: {}", path);
            PathBuf::from(path)
        }
        Err(_) => {
            // Fallback: try to find the daemon binary relative to the library
            // This works when both libbenchfs.so and benchfs_daemon are in target/release
            let fallback = PathBuf::from("benchfs_daemon");
            tracing::warn!(
                "BENCHFS_DAEMON_BINARY not set, using default: {:?}. \
                 Set BENCHFS_DAEMON_BINARY to the absolute path of benchfs_daemon binary.",
                fallback
            );
            fallback
        }
    };

    // Check if the binary exists
    if !daemon_binary.exists() && !daemon_binary.is_absolute() {
        tracing::warn!(
            "Daemon binary {:?} not found in PATH. Consider setting BENCHFS_DAEMON_BINARY.",
            daemon_binary
        );
    }

    tracing::info!("Daemon binary path: {:?}", daemon_binary);

    // Create launcher config
    let config = LauncherConfig {
        shm_name: shm_name_str.unwrap_or_else(crate::daemon::default_shm_name),
        shm_config: ShmConfig::default(),
        daemon_binary,
        registry_dir: PathBuf::from(registry_dir_str),
        data_dir: PathBuf::from(data_dir_str.unwrap_or("/tmp/benchfs_daemon_data")),
        startup_timeout: std::time::Duration::from_secs(30),
        env_vars: Vec::new(),
    };

    // Connect to daemon
    match DaemonClientStub::connect(&config) {
        Ok(stub) => {
            tracing::info!("Connected to daemon, slot_id={}", stub.slot_id());

            // Store in thread-local storage
            DAEMON_STUB.with(|s| {
                *s.borrow_mut() = Some(stub);
            });

            // Return a non-null dummy pointer as context handle
            // We use thread-local storage, so the context is just a marker
            Box::into_raw(Box::new(1u8)) as *mut benchfs_context_t
        }
        Err(e) => {
            set_error_message(&format!("Failed to connect to daemon: {:?}", e));
            std::ptr::null_mut()
        }
    }
}

/// Finalize BenchFS daemon mode.
///
/// This function disconnects from the client daemon and cleans up resources.
///
/// # Safety
///
/// - `ctx` must be a valid pointer from `benchfs_init`
/// - `ctx` must not be used after this call
#[unsafe(no_mangle)]
pub extern "C" fn benchfs_finalize(ctx: *mut benchfs_context_t) {
    if ctx.is_null() {
        return;
    }

    tracing::info!("benchfs_daemon_finalize: cleaning up");

    // Clear thread-local storage (this will send disconnect to daemon via Drop)
    DAEMON_STUB.with(|s| {
        *s.borrow_mut() = None;
    });

    // Free the dummy context pointer
    unsafe {
        let _ = Box::from_raw(ctx as *mut u8);
    }

    tracing::info!("Daemon finalize complete");
}

/// Helper to access daemon stub
fn with_daemon_stub<F, R>(f: F) -> Result<R, String>
where
    F: FnOnce(&DaemonClientStub) -> R,
{
    DAEMON_STUB.with(|s| {
        s.borrow()
            .as_ref()
            .map(|stub| f(stub))
            .ok_or_else(|| "Daemon not initialized".to_string())
    })
}

/// Open a file in daemon mode.
///
/// # Arguments
///
/// * `ctx` - BenchFS context (unused, uses thread-local)
/// * `path` - File path
/// * `flags` - Open flags (same as POSIX)
///
/// # Returns
///
/// * File handle pointer on success
/// * NULL on error
#[unsafe(no_mangle)]
pub extern "C" fn benchfs_open(
    _ctx: *mut benchfs_context_t,
    path: *const c_char,
    flags: u32,
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

    tracing::debug!("benchfs_daemon_open: path={}, flags={:#x}", path_str, flags);

    let result = with_daemon_stub(|stub| stub.open(path_str, flags));

    match result {
        Ok(Ok(fd)) => Box::into_raw(Box::new(benchfs_file_t { fd })),
        Ok(Err(e)) => {
            set_error_message(&format!("open failed: {:?}", e));
            std::ptr::null_mut()
        }
        Err(e) => {
            set_error_message(&e);
            std::ptr::null_mut()
        }
    }
}

/// Create a file in daemon mode.
///
/// This is similar to `benchfs_open` but ensures the O_CREAT flag is set.
///
/// # Arguments
///
/// * `ctx` - BenchFS context (unused, uses thread-local)
/// * `path` - File path
/// * `flags` - Open flags (same as POSIX)
/// * `mode` - File mode (unused, for POSIX compatibility)
///
/// # Returns
///
/// * File handle pointer on success
/// * NULL on error
#[unsafe(no_mangle)]
pub extern "C" fn benchfs_create(
    _ctx: *mut benchfs_context_t,
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

    // Ensure O_CREAT flag is set
    const O_CREAT: u32 = 0x0040;
    let flags_with_create = (flags as u32) | O_CREAT;

    tracing::debug!("benchfs_create: path={}, flags={:#x}", path_str, flags_with_create);

    let result = with_daemon_stub(|stub| stub.open(path_str, flags_with_create));

    match result {
        Ok(Ok(fd)) => Box::into_raw(Box::new(benchfs_file_t { fd })),
        Ok(Err(e)) => {
            set_error_message(&format!("create failed: {:?}", e));
            std::ptr::null_mut()
        }
        Err(e) => {
            set_error_message(&e);
            std::ptr::null_mut()
        }
    }
}

/// Close a file in daemon mode.
///
/// # Arguments
///
/// * `file` - File handle from `benchfs_open`
///
/// # Returns
///
/// * 0 on success
/// * Negative error code on failure
#[unsafe(no_mangle)]
pub extern "C" fn benchfs_close(file: *mut benchfs_file_t) -> i32 {
    if file.is_null() {
        return BENCHFS_EINVAL;
    }

    let fd = unsafe { (*file).fd };
    tracing::debug!("benchfs_daemon_close: fd={}", fd);

    let result = with_daemon_stub(|stub| stub.close(fd));

    // Free the file handle
    unsafe {
        let _ = Box::from_raw(file);
    }

    match result {
        Ok(Ok(())) => 0,
        Ok(Err(e)) => {
            set_error_message(&format!("close failed: {:?}", e));
            BENCHFS_EIO
        }
        Err(e) => {
            set_error_message(&e);
            BENCHFS_ERROR
        }
    }
}

/// Read from a file in daemon mode.
///
/// # Arguments
///
/// * `file` - File handle
/// * `buffer` - Buffer to read into
/// * `size` - Number of bytes to read
/// * `offset` - File offset
///
/// # Returns
///
/// * Number of bytes read on success
/// * Negative error code on failure
#[unsafe(no_mangle)]
pub extern "C" fn benchfs_read(
    file: *mut benchfs_file_t,
    buffer: *mut u8,
    size: usize,
    offset: i64,
) -> i64 {
    if file.is_null() || buffer.is_null() {
        set_error_message("file and buffer must not be null");
        return BENCHFS_EINVAL as i64;
    }

    if offset < 0 {
        set_error_message("offset must be non-negative");
        return BENCHFS_EINVAL as i64;
    }

    let fd = unsafe { (*file).fd };
    let buf = unsafe { slice::from_raw_parts_mut(buffer, size) };

    tracing::trace!(
        "benchfs_daemon_read: fd={}, size={}, offset={}",
        fd,
        size,
        offset
    );

    let result = with_daemon_stub(|stub| stub.read(fd, offset as u64, buf));

    match result {
        Ok(Ok(bytes_read)) => bytes_read as i64,
        Ok(Err(e)) => {
            set_error_message(&format!("read failed: {:?}", e));
            BENCHFS_EIO as i64
        }
        Err(e) => {
            set_error_message(&e);
            BENCHFS_ERROR as i64
        }
    }
}

/// Write to a file in daemon mode.
///
/// # Arguments
///
/// * `file` - File handle
/// * `buffer` - Data to write
/// * `size` - Number of bytes to write
/// * `offset` - File offset
///
/// # Returns
///
/// * Number of bytes written on success
/// * Negative error code on failure
#[unsafe(no_mangle)]
pub extern "C" fn benchfs_write(
    file: *mut benchfs_file_t,
    buffer: *const u8,
    size: usize,
    offset: i64,
) -> i64 {
    if file.is_null() || buffer.is_null() {
        set_error_message("file and buffer must not be null");
        return BENCHFS_EINVAL as i64;
    }

    if offset < 0 {
        set_error_message("offset must be non-negative");
        return BENCHFS_EINVAL as i64;
    }

    let fd = unsafe { (*file).fd };
    let data = unsafe { slice::from_raw_parts(buffer, size) };

    tracing::trace!(
        "benchfs_daemon_write: fd={}, size={}, offset={}",
        fd,
        size,
        offset
    );

    let result = with_daemon_stub(|stub| stub.write(fd, offset as u64, data));

    match result {
        Ok(Ok(bytes_written)) => bytes_written as i64,
        Ok(Err(e)) => {
            set_error_message(&format!("write failed: {:?}", e));
            BENCHFS_EIO as i64
        }
        Err(e) => {
            set_error_message(&e);
            BENCHFS_ERROR as i64
        }
    }
}

/// Delete a file in daemon mode.
///
/// # Arguments
///
/// * `ctx` - BenchFS context (unused)
/// * `path` - File path
///
/// # Returns
///
/// * 0 on success
/// * Negative error code on failure
#[unsafe(no_mangle)]
pub extern "C" fn benchfs_remove(
    _ctx: *mut benchfs_context_t,
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

    tracing::debug!("benchfs_daemon_remove: path={}", path_str);

    let result = with_daemon_stub(|stub| stub.unlink(path_str));

    match result {
        Ok(Ok(())) => 0,
        Ok(Err(e)) => {
            set_error_message(&format!("unlink failed: {:?}", e));
            BENCHFS_EIO
        }
        Err(e) => {
            set_error_message(&e);
            BENCHFS_ERROR
        }
    }
}

/// Synchronize file data in daemon mode.
///
/// # Arguments
///
/// * `file` - File handle
///
/// # Returns
///
/// * 0 on success
/// * Negative error code on failure
#[unsafe(no_mangle)]
pub extern "C" fn benchfs_fsync(file: *mut benchfs_file_t) -> i32 {
    if file.is_null() {
        return BENCHFS_EINVAL;
    }

    let fd = unsafe { (*file).fd };
    tracing::debug!("benchfs_daemon_fsync: fd={}", fd);

    let result = with_daemon_stub(|stub| stub.fsync(fd));

    match result {
        Ok(Ok(())) => 0,
        Ok(Err(e)) => {
            set_error_message(&format!("fsync failed: {:?}", e));
            BENCHFS_EIO
        }
        Err(e) => {
            set_error_message(&e);
            BENCHFS_ERROR
        }
    }
}

/// Get file size in daemon mode.
///
/// # Arguments
///
/// * `ctx` - BenchFS context (unused)
/// * `path` - File path
///
/// # Returns
///
/// * File size on success
/// * Negative error code on failure
#[unsafe(no_mangle)]
pub extern "C" fn benchfs_stat(
    _ctx: *mut benchfs_context_t,
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

    tracing::debug!("benchfs_daemon_stat: path={}", path_str);

    let result = with_daemon_stub(|stub| stub.stat(path_str));

    match result {
        Ok(Ok(size)) => size as i64,
        Ok(Err(e)) => {
            set_error_message(&format!("stat failed: {:?}", e));
            BENCHFS_EIO as i64
        }
        Err(e) => {
            set_error_message(&e);
            BENCHFS_ERROR as i64
        }
    }
}

/// Get file size in daemon mode.
///
/// # Arguments
///
/// * `ctx` - BenchFS context (unused)
/// * `path` - File path
///
/// # Returns
///
/// * File size in bytes on success
/// * Negative error code on failure
#[unsafe(no_mangle)]
pub extern "C" fn benchfs_get_file_size(
    _ctx: *mut benchfs_context_t,
    path: *const c_char,
) -> i64 {
    // benchfs_get_file_size is just an alias for benchfs_stat
    benchfs_stat(_ctx, path)
}

/// Create directory in daemon mode.
///
/// # Arguments
///
/// * `ctx` - BenchFS context (unused)
/// * `path` - Directory path
///
/// # Returns
///
/// * 0 on success
/// * Negative error code on failure
#[unsafe(no_mangle)]
pub extern "C" fn benchfs_mkdir(
    _ctx: *mut benchfs_context_t,
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

    tracing::debug!("benchfs_daemon_mkdir: path={}", path_str);

    let result = with_daemon_stub(|stub| stub.mkdir(path_str));

    match result {
        Ok(Ok(())) => 0,
        Ok(Err(e)) => {
            set_error_message(&format!("mkdir failed: {:?}", e));
            BENCHFS_EIO
        }
        Err(e) => {
            set_error_message(&e);
            BENCHFS_ERROR
        }
    }
}

/// Seek to position in daemon mode.
///
/// # Arguments
///
/// * `file` - File handle
/// * `offset` - Seek offset
/// * `whence` - Seek mode (0=SET, 1=CUR, 2=END)
///
/// # Returns
///
/// * New file position on success
/// * Negative error code on failure
#[unsafe(no_mangle)]
pub extern "C" fn benchfs_lseek(
    file: *mut benchfs_file_t,
    offset: i64,
    _whence: i32,
) -> i64 {
    if file.is_null() {
        set_error_message("file must not be null");
        return BENCHFS_EINVAL as i64;
    }

    let fd = unsafe { (*file).fd };
    tracing::trace!("benchfs_daemon_lseek: fd={}, offset={}", fd, offset);

    // For now, we only support SEEK_SET (whence=0)
    let result = with_daemon_stub(|stub| stub.seek(fd, offset));

    match result {
        Ok(Ok(pos)) => pos as i64,
        Ok(Err(e)) => {
            set_error_message(&format!("seek failed: {:?}", e));
            BENCHFS_EIO as i64
        }
        Err(e) => {
            set_error_message(&e);
            BENCHFS_ERROR as i64
        }
    }
}
