//! Global constants for BenchFS
//!
//! This module centralizes commonly used constants across the codebase
//! to improve maintainability and reduce duplication.

/// Worker address buffer size for RPC operations
///
/// This is the standard size for receiving worker addresses in RPC requests.
/// Worker addresses are used for direct response communication.
pub const WORKER_ADDRESS_BUFFER_SIZE: usize = 512;

/// RDMA threshold in bytes (32KB)
///
/// Data transfers larger than this will use Rendezvous (RDMA) protocol.
/// Smaller transfers will use Eager protocol for lower latency.
///
/// This matches the threshold used in CHFS for consistency.
pub const RDMA_THRESHOLD: u64 = 32768;

/// Shutdown magic number for validating shutdown requests
///
/// This magic number is used to validate shutdown RPC requests to ensure
/// they are legitimate and not accidental or malicious.
pub const SHUTDOWN_MAGIC: u64 = 0xDEADBEEF_CAFEBABE;

/// Maximum path length for file operations (4KB)
///
/// This is a reasonable limit that prevents excessive memory allocation
/// while supporting very long paths if needed.
pub const MAX_PATH_LENGTH: usize = 4096;

/// Maximum single transfer size (1GB)
///
/// This is the maximum size for a single RPC data transfer operation.
pub const MAX_TRANSFER_SIZE: usize = 1 << 30;

/// Determine whether to use RDMA based on data size
///
/// # Arguments
///
/// * `data_size` - The size of data to transfer in bytes
///
/// # Returns
///
/// `true` if RDMA should be used, `false` for Eager protocol
#[inline]
pub fn should_use_rdma(data_size: u64) -> bool {
    data_size >= RDMA_THRESHOLD
}
