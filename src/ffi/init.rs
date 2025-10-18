// Initialization and finalization for C FFI
//
// This module provides benchfs_init() and benchfs_finalize() functions
// that are called from IOR to setup and teardown BenchFS instances.

use std::ffi::CStr;
use std::os::raw::c_char;
use std::rc::Rc;

use super::runtime::set_benchfs_ctx;
use super::error::*;
use crate::api::file_ops::BenchFS;

// Opaque type for BenchFS context
// This prevents C code from accessing internal structure
#[repr(C)]
pub struct benchfs_context_t {
    _private: [u8; 0],
}

/// Initialize BenchFS instance
///
/// # Arguments
/// * `node_id` - Node identifier (e.g., "client_1", "server")
/// * `registry_dir` - Shared directory for service discovery (required)
/// * `data_dir` - Data directory for server nodes (optional for clients)
/// * `is_server` - 1 if this is a server node, 0 for client
///
/// # Returns
/// Opaque pointer to BenchFS context, or NULL on error
///
/// # Safety
/// - `node_id` and `registry_dir` must be valid C strings
/// - The returned pointer must be freed with benchfs_finalize()
#[unsafe(no_mangle)]
pub extern "C" fn benchfs_init(
    node_id: *const c_char,
    registry_dir: *const c_char,
    data_dir: *const c_char,
    is_server: i32,
) -> *mut benchfs_context_t {
    // Validate pointers
    if node_id.is_null() || registry_dir.is_null() {
        set_error_message("node_id and registry_dir must not be null");
        return std::ptr::null_mut();
    }

    // Convert C strings to Rust strings
    let node_id_str = unsafe {
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

    // Create BenchFS instance
    // TODO: Implement distributed mode initialization
    // For now, create a simple local instance
    let benchfs = if is_server != 0 {
        // Server mode
        tracing::info!("Initializing BenchFS server: node_id={}, registry_dir={}, data_dir={:?}",
                      node_id_str, registry_dir_str, data_dir_str);

        // TODO: Start server with UCX endpoint, register in registry
        // For now, just create a local BenchFS instance
        Rc::new(BenchFS::new(node_id_str.to_string()))
    } else {
        // Client mode
        tracing::info!("Initializing BenchFS client: node_id={}, registry_dir={}",
                      node_id_str, registry_dir_str);

        // TODO: Connect to server via registry discovery
        // For now, just create a local BenchFS instance
        Rc::new(BenchFS::new(node_id_str.to_string()))
    };

    // Store in thread-local context
    set_benchfs_ctx(benchfs.clone());

    // Return opaque pointer
    // We box the Rc to pass ownership to C
    let boxed = Box::new(benchfs);
    Box::into_raw(boxed) as *mut benchfs_context_t
}

/// Finalize BenchFS instance
///
/// # Arguments
/// * `ctx` - BenchFS context from benchfs_init()
///
/// # Safety
/// - `ctx` must be a valid pointer from benchfs_init()
/// - `ctx` must not be used after this call
#[unsafe(no_mangle)]
pub extern "C" fn benchfs_finalize(ctx: *mut benchfs_context_t) {
    if ctx.is_null() {
        return;
    }

    // Convert back to Rc<BenchFS> and drop it
    unsafe {
        let _ = Box::from_raw(ctx as *mut Rc<BenchFS>);
    }

    tracing::info!("BenchFS finalized");
}
