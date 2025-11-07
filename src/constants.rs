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
    data_size >= RDMA_THRESHOLD && rdma_allowed()
}

fn rdma_allowed() -> bool {
    use std::env;
    use std::sync::OnceLock;

    static CACHE: OnceLock<bool> = OnceLock::new();
    *CACHE.get_or_init(|| {
        if is_env_truthy("BENCHFS_FORCE_RDMA") {
            tracing::info!("RDMA forced ON via BENCHFS_FORCE_RDMA");
            return true;
        }

        if is_env_truthy("BENCHFS_DISABLE_RDMA") {
            tracing::warn!("RDMA disabled via BENCHFS_DISABLE_RDMA");
            return false;
        }

        if let Ok(tls) = env::var("UCX_TLS") {
            let has_rdma = tls
                .split(',')
                .map(|v| v.trim().to_ascii_lowercase())
                .any(|t| t.starts_with("rc") || t.starts_with("dc"));

            if has_rdma {
                tracing::debug!("RDMA enabled (UCX_TLS={})", tls);
                return true;
            } else {
                tracing::warn!(
                    "UCX_TLS ({}) has no RC/DC transports; using eager protocol",
                    tls
                );
                return false;
            }
        }

        tracing::warn!(
            "UCX_TLS not set; defaulting to eager protocol (set BENCHFS_FORCE_RDMA=1 to override)"
        );
        false
    })
}

fn is_env_truthy(key: &str) -> bool {
    use std::env;

    match env::var(key) {
        Ok(value) => matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        Err(_) => false,
    }
}
