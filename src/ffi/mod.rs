// FFI module for C API integration with IOR benchmark
//
// This module provides C-compatible functions for BenchFS operations.
// It handles async→sync conversion, global runtime management, and error propagation.

pub mod runtime;
pub mod error;
pub mod init;
pub mod file_ops;
pub mod metadata;

// Re-exports for convenience
pub use error::*;
pub use init::*;
pub use file_ops::*;
pub use metadata::*;
