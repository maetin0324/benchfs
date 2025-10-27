/// BenchFS Filesystem API
///
/// This module provides POSIX-like filesystem operations for BenchFS.
/// All operations work with the distributed metadata and data storage.
pub mod file_ops;
pub mod types;

// Re-export main types
pub use file_ops::*;
pub use types::*;
