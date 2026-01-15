use std::cell::RefCell;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use pluvio_timer::{timeout, TimeoutError};
use tracing::instrument;

use crate::rpc::{AmRpc, Connection, RpcError};

// ============================================================================
// Global retry statistics counters
// ============================================================================

static TOTAL_RPC_REQUESTS: AtomicU64 = AtomicU64::new(0);
static TOTAL_RETRIES: AtomicU64 = AtomicU64::new(0);
static TOTAL_RETRY_SUCCESSES: AtomicU64 = AtomicU64::new(0);
static TOTAL_RETRY_FAILURES: AtomicU64 = AtomicU64::new(0);

/// Statistics for RPC retry operations
#[derive(Debug, Clone, Default)]
pub struct RetryStats {
    /// Total number of RPC requests attempted
    pub total_requests: u64,
    /// Total number of retry attempts (not including initial attempt)
    pub total_retries: u64,
    /// Number of requests that succeeded after retry
    pub retry_successes: u64,
    /// Number of requests that failed after all retries
    pub retry_failures: u64,
}

impl RetryStats {
    /// Get the retry rate (retries / total_requests)
    pub fn retry_rate(&self) -> f64 {
        if self.total_requests == 0 {
            0.0
        } else {
            self.total_retries as f64 / self.total_requests as f64
        }
    }

    /// Get the retry success rate (retry_successes / (retry_successes + retry_failures))
    pub fn retry_success_rate(&self) -> f64 {
        let total_retried = self.retry_successes + self.retry_failures;
        if total_retried == 0 {
            0.0
        } else {
            self.retry_successes as f64 / total_retried as f64
        }
    }
}

/// Get current retry statistics
pub fn get_retry_stats() -> RetryStats {
    RetryStats {
        total_requests: TOTAL_RPC_REQUESTS.load(Ordering::Relaxed),
        total_retries: TOTAL_RETRIES.load(Ordering::Relaxed),
        retry_successes: TOTAL_RETRY_SUCCESSES.load(Ordering::Relaxed),
        retry_failures: TOTAL_RETRY_FAILURES.load(Ordering::Relaxed),
    }
}

/// Reset retry statistics (useful for testing)
pub fn reset_retry_stats() {
    TOTAL_RPC_REQUESTS.store(0, Ordering::Relaxed);
    TOTAL_RETRIES.store(0, Ordering::Relaxed);
    TOTAL_RETRY_SUCCESSES.store(0, Ordering::Relaxed);
    TOTAL_RETRY_FAILURES.store(0, Ordering::Relaxed);
}

/// Write retry statistics to a CSV file
///
/// The CSV format is:
/// ```csv
/// node_id,total_requests,total_retries,retry_successes,retry_failures,retry_rate
/// ```
pub fn write_retry_stats_to_csv(path: &str, node_id: &str) -> std::io::Result<()> {
    use std::fs::File;
    use std::io::Write;

    let stats = get_retry_stats();

    // Create parent directory if needed
    if let Some(parent) = std::path::Path::new(path).parent() {
        if !parent.exists() {
            std::fs::create_dir_all(parent)?;
        }
    }

    let mut file = File::create(path)?;
    writeln!(file, "node_id,total_requests,total_retries,retry_successes,retry_failures,retry_rate")?;
    writeln!(
        file,
        "{},{},{},{},{},{:.4}",
        node_id,
        stats.total_requests,
        stats.total_retries,
        stats.retry_successes,
        stats.retry_failures,
        stats.retry_rate()
    )?;

    tracing::info!(
        "Retry stats written to {}: requests={}, retries={}, successes={}, failures={}, rate={:.4}",
        path,
        stats.total_requests,
        stats.total_retries,
        stats.retry_successes,
        stats.retry_failures,
        stats.retry_rate()
    );

    Ok(())
}

// Helper functions for incrementing counters
fn increment_request_count() {
    TOTAL_RPC_REQUESTS.fetch_add(1, Ordering::Relaxed);
}

fn increment_retry_count() {
    TOTAL_RETRIES.fetch_add(1, Ordering::Relaxed);
}

fn increment_retry_success() {
    TOTAL_RETRY_SUCCESSES.fetch_add(1, Ordering::Relaxed);
}

fn increment_retry_failure() {
    TOTAL_RETRY_FAILURES.fetch_add(1, Ordering::Relaxed);
}

/// Default RPC timeout duration (30 seconds)
const DEFAULT_RPC_TIMEOUT_SECS: u64 = 30;

/// Default retry count
const DEFAULT_RPC_RETRY_COUNT: u32 = 3;

/// Default initial retry delay (100ms)
const DEFAULT_RPC_RETRY_DELAY_MS: u64 = 100;

/// Default backoff multiplier
const DEFAULT_RPC_RETRY_BACKOFF: f64 = 2.0;

/// Retry configuration for RPC calls
struct RetryConfig {
    max_retries: u32,
    initial_delay: Duration,
    backoff_multiplier: f64,
}

/// Get the RPC timeout from environment variable or use default
fn get_rpc_timeout() -> Duration {
    std::env::var("BENCHFS_RPC_TIMEOUT")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(DEFAULT_RPC_TIMEOUT_SECS))
}

/// Get retry configuration from environment variables
fn get_retry_config() -> RetryConfig {
    RetryConfig {
        max_retries: std::env::var("BENCHFS_RPC_RETRY_COUNT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_RPC_RETRY_COUNT),
        initial_delay: Duration::from_millis(
            std::env::var("BENCHFS_RPC_RETRY_DELAY_MS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(DEFAULT_RPC_RETRY_DELAY_MS),
        ),
        backoff_multiplier: std::env::var("BENCHFS_RPC_RETRY_BACKOFF")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_RPC_RETRY_BACKOFF),
    }
}

/// RPC client for making RPC calls
///
/// The client executes RPCs by taking any type that implements the `RpcCall` trait.
pub struct RpcClient {
    conn: Connection,
    // Store reply stream opaquely since pluvio_ucx may not export AmStream
    #[allow(dead_code)]
    reply_stream_id: RefCell<Option<u16>>,
    /// Client's own WorkerAddress for direct response
    worker_address: Vec<u8>,
}

impl RpcClient {
    pub fn new(conn: Connection) -> Self {
        // Get worker address from the connection
        let worker_address = conn
            .worker()
            .address()
            .map(|addr| {
                let addr_vec = addr.as_ref().to_vec();
                tracing::debug!(
                    "RpcClient: Got worker address, length={}, first_32_bytes={:?}",
                    addr_vec.len(),
                    &addr_vec.get(0..32.min(addr_vec.len())).unwrap_or(&[])
                );
                addr_vec
            })
            .unwrap_or_else(|e| {
                tracing::error!("Failed to get worker address: {:?}", e);
                tracing::error!("RpcClient: Returning empty worker address due to error");
                vec![]
            });

        Self {
            conn,
            reply_stream_id: RefCell::new(None),
            worker_address,
        }
    }

    /// Get the client's WorkerAddress
    pub fn worker_address(&self) -> &[u8] {
        &self.worker_address
    }

    /// Get the underlying connection
    pub fn connection(&self) -> &Connection {
        &self.conn
    }

    /// Initialize the reply stream for receiving RPC responses
    /// This should be called before executing any RPCs that expect replies
    ///
    /// Note: Currently simplified - full implementation would need pluvio_ucx
    /// to export AmStream or provide a different API
    pub fn init_reply_stream(&self, am_id: u16) -> Result<(), RpcError> {
        // Store the AM ID for future use
        *self.reply_stream_id.borrow_mut() = Some(am_id);
        Ok(())
    }

    /// Check if an error is retryable
    fn is_retryable(error: &RpcError) -> bool {
        matches!(error, RpcError::Timeout | RpcError::TransportError(_))
    }

    /// Execute an RPC call using a request that implements RpcCall
    ///
    /// This is the main entry point for executing RPCs. Pass any struct that implements
    /// RpcCall and it will be sent to the server, with the response being returned.
    ///
    /// The operation is subject to a timeout configured via the `BENCHFS_RPC_TIMEOUT`
    /// environment variable (in seconds). Default timeout is 30 seconds.
    ///
    /// On timeout or transport errors, the operation will be automatically retried
    /// with exponential backoff. Retry behavior is configured via environment variables.
    ///
    /// # Example
    /// ```ignore
    /// let request = ReadRequest { offset: 0, len: 4096 };
    /// let response: ReadResponse = client.execute(&request).await?;
    /// ```
    ///
    /// # Environment Variables
    /// * `BENCHFS_RPC_TIMEOUT` - Timeout in seconds (default: 30)
    /// * `BENCHFS_RPC_RETRY_COUNT` - Max retry attempts (default: 3)
    /// * `BENCHFS_RPC_RETRY_DELAY_MS` - Initial retry delay in ms (default: 100)
    /// * `BENCHFS_RPC_RETRY_BACKOFF` - Backoff multiplier (default: 2.0)
    #[async_backtrace::framed]
    #[instrument(level = "trace", name = "rpc_call", skip(self, request), fields(rpc_id = T::rpc_id()))]
    pub async fn execute<T: AmRpc>(&self, request: &T) -> Result<T::ResponseHeader, RpcError> {
        increment_request_count();

        let retry_config = get_retry_config();
        let mut attempt = 0u32;
        let mut delay = retry_config.initial_delay;

        loop {
            attempt += 1;

            match self.execute_once(request).await {
                Ok(response) => {
                    // Track if this was a retry success
                    if attempt > 1 {
                        increment_retry_success();
                    }
                    return Ok(response);
                }
                Err(e) if Self::is_retryable(&e) && attempt <= retry_config.max_retries => {
                    increment_retry_count();
                    tracing::warn!(
                        rpc_id = T::rpc_id(),
                        attempt = attempt,
                        max_retries = retry_config.max_retries,
                        delay_ms = delay.as_millis() as u64,
                        error = ?e,
                        "RPC failed, retrying..."
                    );
                    pluvio_timer::sleep(delay).await;
                    delay = Duration::from_secs_f64(
                        delay.as_secs_f64() * retry_config.backoff_multiplier
                    );
                }
                Err(e) => {
                    // Track if retries were attempted but all failed
                    if attempt > 1 {
                        increment_retry_failure();
                        tracing::error!(
                            rpc_id = T::rpc_id(),
                            attempts = attempt,
                            error = ?e,
                            "RPC failed after all retries"
                        );
                    }
                    return Err(e);
                }
            }
        }
    }

    /// Internal: Execute a single RPC attempt without retry
    #[async_backtrace::framed]
    async fn execute_once<T: AmRpc>(&self, request: &T) -> Result<T::ResponseHeader, RpcError> {
        let timeout_duration = get_rpc_timeout();
        let rpc_id = T::rpc_id();
        let reply_stream_id = T::reply_stream_id();
        let header = request.request_header();
        let data = request.request_data();
        let need_reply = request.need_reply();
        let proto = request.proto(); // TODO: Use when pluvio_ucx exports AmProto

        tracing::debug!(
            "RPC call: rpc_id={}, reply_stream_id={}, has_data={}, data_len={}, timeout_secs={}",
            rpc_id,
            reply_stream_id,
            !data.is_empty(),
            data.iter().map(|s| s.len()).sum::<usize>(),
            timeout_duration.as_secs()
        );

        let reply_stream = self.conn.worker.am_stream(reply_stream_id).map_err(|e| {
            RpcError::TransportError(format!(
                "Failed to create reply AM stream: {:?}",
                e.to_string()
            ))
        })?;

        tracing::trace!("Created reply stream: stream_id={}", reply_stream_id);

        if !need_reply {
            // No reply expected
            return Err(RpcError::HandlerError(
                "No reply expected for this RPC".to_string(),
            ));
        }

        // Send the RPC request with timeout
        tracing::debug!(
            "Sending AM request: rpc_id={}, header_size={}, need_reply={}, proto={:?}",
            rpc_id,
            std::mem::size_of_val(zerocopy::IntoBytes::as_bytes(header)),
            need_reply,
            proto
        );

        let send_future = self.conn.endpoint().am_send_vectorized(
            rpc_id as u32,
            zerocopy::IntoBytes::as_bytes(header),
            &data,
            need_reply,
            proto,
        );

        match timeout(timeout_duration, Box::pin(send_future)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                tracing::error!(
                    rpc_id = rpc_id,
                    error = ?e,
                    "Failed to send AM request"
                );
                return Err(RpcError::TransportError(format!(
                    "Failed to send AM: {:?}",
                    e
                )));
            }
            Err(TimeoutError) => {
                tracing::warn!(
                    rpc_id = rpc_id,
                    timeout_secs = timeout_duration.as_secs(),
                    "RPC send timeout: failed to send request within timeout"
                );
                return Err(RpcError::Timeout);
            }
        }

        tracing::debug!(
            "Waiting for reply on stream_id={}, rpc_id={}, timeout_secs={}",
            reply_stream_id,
            rpc_id,
            timeout_duration.as_secs()
        );

        // Wait for reply with timeout
        let wait_future = reply_stream.wait_msg();
        let mut msg = match timeout(timeout_duration, Box::pin(wait_future)).await {
            Ok(Some(msg)) => msg,
            Ok(None) => {
                tracing::warn!(
                    rpc_id = rpc_id,
                    reply_stream_id = reply_stream_id,
                    "RPC failed: no reply received (stream closed)"
                );
                return Err(RpcError::Timeout);
            }
            Err(TimeoutError) => {
                tracing::warn!(
                    rpc_id = rpc_id,
                    reply_stream_id = reply_stream_id,
                    timeout_secs = timeout_duration.as_secs(),
                    "RPC timeout: no reply received within timeout"
                );
                return Err(RpcError::Timeout);
            }
        };

        tracing::debug!(
            "Received reply message: rpc_id={}, reply_stream_id={}",
            rpc_id,
            reply_stream_id
        );

        // Deserialize the response header
        let response_header = msg
            .header()
            .get(..std::mem::size_of::<T::ResponseHeader>())
            .and_then(|bytes| zerocopy::FromBytes::read_from_bytes(bytes).ok())
            .ok_or_else(|| RpcError::InvalidHeader)?;

        // Receive response data if present (with timeout)
        let response_buffer = request.response_buffer();
        if !response_buffer.is_empty() && msg.contains_data() {
            let recv_future = msg.recv_data_vectored(response_buffer);
            match timeout(timeout_duration, Box::pin(recv_future)).await {
                Ok(Ok(_bytes_received)) => {}
                Ok(Err(e)) => {
                    tracing::error!(
                        rpc_id = rpc_id,
                        error = ?e,
                        "Failed to receive response data"
                    );
                    return Err(RpcError::TransportError(format!(
                        "Failed to recv response data: {:?}",
                        e
                    )));
                }
                Err(TimeoutError) => {
                    tracing::warn!(
                        rpc_id = rpc_id,
                        timeout_secs = timeout_duration.as_secs(),
                        "RPC recv timeout: failed to receive response data within timeout"
                    );
                    return Err(RpcError::Timeout);
                }
            }
        }

        // Note: The caller should check the status field in the response header
        // to determine if the RPC succeeded or failed on the server side
        Ok(response_header)
    }

    /// Execute an RPC without expecting a reply
    /// Useful for fire-and-forget operations
    ///
    /// The send operation is subject to a timeout configured via the `BENCHFS_RPC_TIMEOUT`
    /// environment variable (in seconds). Default timeout is 30 seconds.
    ///
    /// On timeout or transport errors, the operation will be automatically retried
    /// with exponential backoff.
    ///
    /// # Environment Variables
    /// * `BENCHFS_RPC_TIMEOUT` - Timeout in seconds (default: 30)
    /// * `BENCHFS_RPC_RETRY_COUNT` - Max retry attempts (default: 3)
    /// * `BENCHFS_RPC_RETRY_DELAY_MS` - Initial retry delay in ms (default: 100)
    /// * `BENCHFS_RPC_RETRY_BACKOFF` - Backoff multiplier (default: 2.0)
    #[async_backtrace::framed]
    pub async fn execute_no_reply<T: AmRpc>(&self, request: &T) -> Result<(), RpcError> {
        increment_request_count();

        let retry_config = get_retry_config();
        let mut attempt = 0u32;
        let mut delay = retry_config.initial_delay;

        loop {
            attempt += 1;

            match self.execute_no_reply_once(request).await {
                Ok(()) => {
                    // Track if this was a retry success
                    if attempt > 1 {
                        increment_retry_success();
                    }
                    return Ok(());
                }
                Err(e) if Self::is_retryable(&e) && attempt <= retry_config.max_retries => {
                    increment_retry_count();
                    tracing::warn!(
                        rpc_id = T::rpc_id(),
                        attempt = attempt,
                        max_retries = retry_config.max_retries,
                        delay_ms = delay.as_millis() as u64,
                        error = ?e,
                        "RPC (no_reply) failed, retrying..."
                    );
                    pluvio_timer::sleep(delay).await;
                    delay = Duration::from_secs_f64(
                        delay.as_secs_f64() * retry_config.backoff_multiplier
                    );
                }
                Err(e) => {
                    // Track if retries were attempted but all failed
                    if attempt > 1 {
                        increment_retry_failure();
                        tracing::error!(
                            rpc_id = T::rpc_id(),
                            attempts = attempt,
                            error = ?e,
                            "RPC (no_reply) failed after all retries"
                        );
                    }
                    return Err(e);
                }
            }
        }
    }

    /// Internal: Execute a single no-reply RPC attempt without retry
    #[async_backtrace::framed]
    async fn execute_no_reply_once<T: AmRpc>(&self, request: &T) -> Result<(), RpcError> {
        let timeout_duration = get_rpc_timeout();
        let rpc_id = T::rpc_id();
        let header = request.request_header();
        let data = request.request_data();
        let proto = request.proto(); // TODO: Use when pluvio_ucx exports AmProto

        let send_future = self.conn.endpoint().am_send_vectorized(
            rpc_id as u32,
            zerocopy::IntoBytes::as_bytes(header),
            &data,
            false, // need_reply = false
            proto, // proto - TODO: pass actual proto when available
        );

        match timeout(timeout_duration, Box::pin(send_future)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => {
                tracing::error!(
                    rpc_id = rpc_id,
                    error = ?e,
                    "Failed to send AM request (no_reply)"
                );
                Err(RpcError::TransportError(format!(
                    "Failed to send AM: {:?}",
                    e
                )))
            }
            Err(TimeoutError) => {
                tracing::warn!(
                    rpc_id = rpc_id,
                    timeout_secs = timeout_duration.as_secs(),
                    "RPC send timeout (no_reply): failed to send request within timeout"
                );
                Err(RpcError::Timeout)
            }
        }
    }
}
