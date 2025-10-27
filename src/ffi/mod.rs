// FFI module for C API integration with IOR benchmark
//
// This module provides C-compatible functions for BenchFS operations.
// It handles async→sync conversion, global runtime management, and error propagation.

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
