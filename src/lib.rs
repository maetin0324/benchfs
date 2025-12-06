//! BenchFS - A Distributed Filesystem Benchmark Tool with IOR Integration
//!
//! BenchFS is a high-performance distributed filesystem designed for benchmarking
//! I/O workloads with integration for the IOR benchmark tool. It features:
//!
//! - **Distributed Architecture**: Metadata and data are distributed across nodes
//!   using consistent hashing for load balancing
//! - **High-Performance I/O**: Uses io_uring for local storage operations and
//!   UCX/RDMA for efficient remote data transfers
//! - **Path-based KV Design**: Full file paths are used as keys, eliminating
//!   the need for traditional directory hierarchies
//! - **Chunk-based Storage**: Files are split into configurable chunks (default 4MB)
//!   for parallel access and efficient distribution
//! - **FFI Interface**: C-compatible API for integration with IOR and other benchmarks
//!
//! # Architecture
//!
//! BenchFS consists of several key components:
//!
//! - **Metadata Management** ([`metadata`]): Handles file and directory metadata
//!   using a path-based key-value design with consistent hashing for distribution
//! - **Storage Layer** ([`storage`]): Local chunk storage using io_uring for
//!   high-performance async I/O operations
//! - **RPC Layer** ([`rpc`]): Remote procedure calls using UCX for metadata
//!   and data operations across nodes
//! - **Cache Layer** ([`cache`]): LRU caching for both metadata and data chunks
//!   to reduce remote access overhead
//! - **API Layer** ([`api`]): High-level POSIX-like file operations
//! - **FFI Layer** ([`ffi`]): C-compatible interface for IOR integration
//!
//! # Example
//!
//! ```rust,no_run
//! use benchfs::api::file_ops::{BenchFS, BenchFSBuilder};
//! use benchfs::api::types::OpenFlags;
//! use benchfs::storage::{IOUringBackend, IOUringChunkStore};
//! use std::rc::Rc;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create chunk store
//! # let allocator = todo!(); // Assume allocator is available
//! let io_backend = Rc::new(IOUringBackend::new(allocator));
//! let chunk_store = Rc::new(IOUringChunkStore::new("/tmp/benchfs/chunks", io_backend)?);
//!
//! // Create BenchFS instance
//! let fs = BenchFS::new("node1".to_string(), chunk_store);
//!
//! // Create and write to a file
//! let handle = fs.benchfs_open("/test.txt", OpenFlags::create()).await?;
//! let data = b"Hello, BenchFS!";
//! fs.benchfs_write(&handle, data).await?;
//! fs.benchfs_close(&handle).await?;
//!
//! // Read from the file
//! let handle = fs.benchfs_open("/test.txt", OpenFlags::read_only()).await?;
//! let mut buf = vec![0u8; 100];
//! let bytes_read = fs.benchfs_read(&handle, &mut buf).await?;
//! fs.benchfs_close(&handle).await?;
//! # Ok(())
//! # }
//! ```

pub mod api;
pub mod cache;
pub mod config;
pub mod constants;
pub mod data;
pub mod ffi;
pub mod logging;
pub mod metadata;
pub mod perfetto;
pub mod rpc;
pub mod server;
pub mod storage;
