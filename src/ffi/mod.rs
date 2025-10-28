//! FFI (Foreign Function Interface) module for C API integration
//!
//! This module provides a C-compatible API for BenchFS operations, enabling
//! integration with IOR and other C-based benchmark tools. The FFI layer handles:
//!
//! - **Async to Sync Conversion**: Executes async Rust operations synchronously
//!   for C compatibility using a thread-local runtime
//! - **Error Handling**: Maps Rust errors to C-style error codes and provides
//!   thread-local error message storage
//! - **Opaque Types**: Uses opaque pointers to hide Rust types from C code
//! - **Memory Safety**: Manages ownership and lifetimes across the FFI boundary
//!
//! # Architecture
//!
//! The FFI layer consists of several modules:
//!
//! - [`init`]: Initialization and finalization (`benchfs_init`, `benchfs_finalize`)
//! - [`file_ops`]: File operations (`benchfs_open`, `benchfs_read`, `benchfs_write`, etc.)
//! - [`metadata`]: Metadata operations (stat, mkdir, etc.)
//! - [`error`]: Error handling (error codes and message storage)
//! - [`runtime`]: Global runtime management for async execution
//!
//! # Usage from C
//!
//! ```c
//! #include <benchfs.h>
//!
//! // Initialize BenchFS
//! benchfs_context_t* ctx = benchfs_init(
//!     "client_1",              // node_id
//!     "/tmp/benchfs_registry", // registry_dir
//!     NULL,                    // data_dir (optional for clients)
//!     0                        // is_server (0 = client, 1 = server)
//! );
//!
//! if (!ctx) {
//!     const char* err = benchfs_get_error();
//!     fprintf(stderr, "Init failed: %s\n", err);
//!     return -1;
//! }
//!
//! // Open a file
//! benchfs_file_t* file = benchfs_create(
//!     ctx,
//!     "/test.txt",
//!     BENCHFS_O_CREAT | BENCHFS_O_WRONLY,
//!     0644
//! );
//!
//! // Write data
//! char buffer[1024] = "Hello, BenchFS!";
//! ssize_t written = benchfs_write(file, buffer, strlen(buffer), 0);
//!
//! // Close file
//! benchfs_close(file);
//!
//! // Finalize BenchFS
//! benchfs_finalize(ctx);
//! ```
//!
//! # Safety Considerations
//!
//! All FFI functions are marked as `unsafe extern "C"` and require careful
//! validation of inputs:
//!
//! - Null pointer checks are performed before dereferencing
//! - String inputs are validated for UTF-8 encoding
//! - Buffer sizes are validated to prevent overflows
//! - Error messages are stored in thread-local storage
//!
//! # Thread Safety
//!
//! Each thread maintains its own:
//! - BenchFS context (stored in thread-local storage)
//! - Async runtime instance
//! - Error message buffer
//!
//! This design allows multiple MPI ranks to use BenchFS concurrently without
//! contention, as each MPI process typically runs in its own thread/process.

pub mod error;
pub mod file_ops;
pub mod init;
pub mod metadata;
pub mod runtime;

// Re-exports for convenience
pub use error::*;
pub use file_ops::*;
pub use init::*;
pub use metadata::*;
