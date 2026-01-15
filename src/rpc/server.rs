use std::cell::RefCell;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};

use pluvio_ucx::{Worker, async_ucx::ucp::WorkerAddress, am::AmStream};
use tracing::instrument;

use crate::rpc::handlers::RpcHandlerContext;
use crate::rpc::{AmRpc, RpcError, Serializable};

// ============================================================================
// Server-side RPC Statistics
// ============================================================================

/// Global counter for currently ongoing RPC requests on the server
static ONGOING_RPC_REQUESTS: AtomicUsize = AtomicUsize::new(0);
/// Global counter for total RPC requests received since process start
static TOTAL_RPC_REQUESTS_RECEIVED: AtomicUsize = AtomicUsize::new(0);
/// Global counter for total RPC requests completed since process start
static TOTAL_RPC_REQUESTS_COMPLETED: AtomicUsize = AtomicUsize::new(0);
/// Peak concurrent RPC requests observed
static PEAK_CONCURRENT_REQUESTS: AtomicUsize = AtomicUsize::new(0);

/// Statistics about server-side RPC request handling
///
/// This provides insight into the server's concurrent request handling,
/// useful for debugging connection/performance issues when connection count increases.
#[derive(Debug, Clone, Copy)]
pub struct ServerRpcStats {
    /// Number of currently ongoing (in-flight) RPC requests
    pub ongoing_requests: usize,
    /// Total RPC requests received since process start
    pub total_received: usize,
    /// Total RPC requests completed since process start
    pub total_completed: usize,
    /// Peak concurrent requests observed
    pub peak_concurrent: usize,
}

impl std::fmt::Display for ServerRpcStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ServerRpcStats {{ ongoing: {}, received: {}, completed: {}, peak: {} }}",
            self.ongoing_requests, self.total_received, self.total_completed, self.peak_concurrent
        )
    }
}

/// Get current server-side RPC statistics
pub fn get_server_rpc_stats() -> ServerRpcStats {
    ServerRpcStats {
        ongoing_requests: ONGOING_RPC_REQUESTS.load(Ordering::Relaxed),
        total_received: TOTAL_RPC_REQUESTS_RECEIVED.load(Ordering::Relaxed),
        total_completed: TOTAL_RPC_REQUESTS_COMPLETED.load(Ordering::Relaxed),
        peak_concurrent: PEAK_CONCURRENT_REQUESTS.load(Ordering::Relaxed),
    }
}

/// Get the number of currently ongoing RPC requests
pub fn get_ongoing_rpc_count() -> usize {
    ONGOING_RPC_REQUESTS.load(Ordering::Relaxed)
}

/// Increment the ongoing RPC request counter (called when spawning a handler)
fn increment_ongoing_requests() -> usize {
    let new_count = ONGOING_RPC_REQUESTS.fetch_add(1, Ordering::Relaxed) + 1;
    TOTAL_RPC_REQUESTS_RECEIVED.fetch_add(1, Ordering::Relaxed);

    // Update peak if necessary
    let mut current_peak = PEAK_CONCURRENT_REQUESTS.load(Ordering::Relaxed);
    while new_count > current_peak {
        match PEAK_CONCURRENT_REQUESTS.compare_exchange_weak(
            current_peak,
            new_count,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => break,
            Err(actual) => current_peak = actual,
        }
    }

    new_count
}

/// Decrement the ongoing RPC request counter (called when handler completes)
fn decrement_ongoing_requests() -> usize {
    TOTAL_RPC_REQUESTS_COMPLETED.fetch_add(1, Ordering::Relaxed);
    ONGOING_RPC_REQUESTS.fetch_sub(1, Ordering::Relaxed) - 1
}

/// RAII guard for tracking RPC request lifetime
struct RpcRequestGuard {
    rpc_id: u16,
}

impl RpcRequestGuard {
    fn new(rpc_id: u16) -> Self {
        let ongoing_count = increment_ongoing_requests();
        tracing::debug!(
            rpc_id = rpc_id,
            ongoing = ongoing_count,
            "RPC request started"
        );
        Self { rpc_id }
    }
}

impl Drop for RpcRequestGuard {
    fn drop(&mut self) {
        let remaining = decrement_ongoing_requests();
        tracing::debug!(
            rpc_id = self.rpc_id,
            ongoing = remaining,
            "RPC request completed"
        );
    }
}

struct SizeCheck<T, const N: usize>(PhantomData<T>);
impl<T, const N: usize> SizeCheck<T, N> {
    // ここは「const 文脈」なので assert! がコンパイル時に評価される
    const OK: () = assert!(size_of::<T>() <= N);
}

pub const MAX_HEADER_SIZE: usize = 512;

/// RPC server that receives and dispatches ActiveMessages
pub struct RpcServer {
    worker: Rc<Worker>,
    handler_context: Rc<RpcHandlerContext>,
    /// Streams registered for RPC handlers, used for graceful shutdown
    streams: Rc<RefCell<Vec<AmStream>>>,
}

impl RpcServer {
    pub fn new(worker: Rc<Worker>, handler_context: Rc<RpcHandlerContext>) -> Self {
        Self {
            worker,
            handler_context,
            streams: Rc::new(RefCell::new(Vec::new())),
        }
    }

    pub fn handler_context(&self) -> &Rc<RpcHandlerContext> {
        &self.handler_context
    }

    pub fn get_address(&self) -> Result<WorkerAddress<'_>, RpcError> {
        self.worker
            .address()
            .map_err(|e| RpcError::TransportError(format!("Failed to get worker address: {:?}", e)))
    }

    /// Start listening for RPC requests on the given AM stream ID
    ///
    /// This method uses the unified AmRpc::server_handler() interface.
    /// It automatically handles both simple header-only responses and data transfer responses.
    ///
    /// # Example
    /// ```ignore
    /// use crate::rpc::data_ops::ReadChunkRequest;
    ///
    /// server.listen::<ReadChunkRequest, _, _>(runtime.clone()).await?;
    /// ```
    #[async_backtrace::framed]
    #[instrument(level = "trace", name = "rpc_listen", skip(self))]
    pub async fn listen<Rpc, ReqH, ResH>(&self) -> Result<(), RpcError>
    where
        ResH: Serializable + 'static,
        ReqH: Serializable + 'static,
        Rpc: AmRpc<RequestHeader = ReqH, ResponseHeader = ResH> + 'static,
    {
        // header size check at compile time
        let _ = SizeCheck::<ReqH, MAX_HEADER_SIZE>::OK;
        let _ = SizeCheck::<ResH, MAX_HEADER_SIZE>::OK;

        let stream = self.worker.am_stream(Rpc::rpc_id()).map_err(|e| {
            RpcError::TransportError(format!("Failed to create AM stream: {:?}", e))
        })?;

        // Store the stream for later shutdown
        self.streams.borrow_mut().push(stream.clone());

        tracing::info!("RpcServer: Listening on AM stream ID {}", Rpc::rpc_id());

        let ctx = self.handler_context.clone();

        loop {
            // Check shutdown flag before waiting for next message
            if ctx.should_shutdown() {
                tracing::info!(
                    "RpcServer: Handler terminating due to shutdown for RPC ID {}",
                    Rpc::rpc_id()
                );
                break;
            }

            tracing::trace!("RpcServer: Waiting for message on RPC ID {}", Rpc::rpc_id());
            let msg = stream.wait_msg().await;
            if msg.is_none() {
                tracing::info!("RpcServer: Stream closed for RPC ID {}", Rpc::rpc_id());
                break;
            }

            let am_msg = msg
                .ok_or_else(|| RpcError::TransportError("Failed to receive message".to_string()))?;

            tracing::trace!(
                "RpcServer: Received message on RPC ID {}, processing",
                Rpc::rpc_id()
            );

            // Spawn handler as separate task to enable parallel request processing
            // This allows the server to immediately accept the next request while
            // the current request is being processed (storage I/O + network send)
            let ctx_clone = ctx.clone();
            let rpc_id = Rpc::rpc_id();
            pluvio_runtime::spawn(async move {
                // Track this request's lifetime with RAII guard
                let _guard = RpcRequestGuard::new(rpc_id);

                match Rpc::server_handler(ctx_clone, am_msg).await {
                    Ok((_response, _am_msg)) => {
                        // Response was already sent within server_handler via reply_ep
                        tracing::trace!(
                            "RPC handler completed successfully for RPC ID {} (response sent directly)",
                            rpc_id
                        );
                    }
                    Err((e, _am_msg)) => {
                        // エラーレスポンスもserver_handler内で送信済み（またはハンドラーがエラーを返した）
                        tracing::error!("Handler failed for RPC ID {}: {:?}", rpc_id, e);
                    }
                }
                // _guard is dropped here, decrementing the counter
            });
        }

        Ok(())
    }

    /// Register and start all standard RPC handlers
    ///
    /// This is a convenience method that starts listeners for all standard RPC types:
    /// - ReadChunkById (RPC_READ_CHUNK_BY_ID) - FileId-based read
    /// - WriteChunkById (RPC_WRITE_CHUNK_BY_ID) - FileId-based write
    /// - MetadataLookup (RPC_METADATA_LOOKUP)
    /// - MetadataCreateFile (RPC_METADATA_CREATE_FILE)
    /// - MetadataCreateDir (RPC_METADATA_CREATE_DIR)
    /// - MetadataDelete (RPC_METADATA_DELETE)
    /// - MetadataUpdate (RPC_METADATA_UPDATE)
    ///
    /// Each handler runs in its own async task spawned by the runtime.
    ///
    /// # Example
    /// ```ignore
    /// use pluvio_runtime::executor::Runtime;
    ///
    /// server.register_all_handlers().await?;
    /// ```
    #[async_backtrace::framed]
    #[instrument(level = "trace", name = "register_all_handlers", skip(self))]
    pub async fn register_all_handlers(&self) -> Result<(), RpcError> {
        use crate::rpc::data_ops::{ReadChunkByIdRequest, WriteChunkByIdRequest};
        use crate::rpc::metadata_ops::{
            MetadataCreateDirRequest, MetadataCreateFileRequest, MetadataDeleteRequest,
            MetadataLookupRequest, MetadataUpdateRequest,
        };

        tracing::info!("Registering all RPC handlers...");

        // Spawn ReadChunkById handler (FileId-based RPC)
        {
            let server = self.clone_for_handler();
            pluvio_runtime::spawn_polling_with_name(
                async move {
                    if let Err(e) = server.listen::<ReadChunkByIdRequest, _, _>().await {
                        tracing::error!("ReadChunkById handler error: {:?}", e);
                    }
                },
                "rpc_read_chunk_by_id_handler".to_string(),
            );
        }

        // Spawn WriteChunkById handler (FileId-based RPC)
        {
            let server = self.clone_for_handler();
            pluvio_runtime::spawn_polling_with_name(
                async move {
                    if let Err(e) = server.listen::<WriteChunkByIdRequest, _, _>().await {
                        tracing::error!("WriteChunkById handler error: {:?}", e);
                    }
                },
                "rpc_write_chunk_by_id_handler".to_string(),
            );
        }

        // Spawn MetadataLookup handler
        {
            let server = self.clone_for_handler();
            pluvio_runtime::spawn_with_name(
                async move {
                    if let Err(e) = server.listen::<MetadataLookupRequest, _, _>().await {
                        tracing::error!("MetadataLookup handler error: {:?}", e);
                    }
                },
                "rpc_metadata_lookup_handler".to_string(),
            );
        }

        // Spawn MetadataCreateFile handler
        {
            let server = self.clone_for_handler();
            pluvio_runtime::spawn_with_name(
                async move {
                    if let Err(e) = server.listen::<MetadataCreateFileRequest, _, _>().await {
                        tracing::error!("MetadataCreateFile handler error: {:?}", e);
                    }
                },
                "rpc_metadata_create_file_handler".to_string(),
            );
        }

        // Spawn MetadataCreateDir handler
        {
            let server = self.clone_for_handler();
            pluvio_runtime::spawn_with_name(
                async move {
                    if let Err(e) = server.listen::<MetadataCreateDirRequest, _, _>().await {
                        tracing::error!("MetadataCreateDir handler error: {:?}", e);
                    }
                },
                "rpc_metadata_create_dir_handler".to_string(),
            );
        }

        // Spawn MetadataDelete handler
        {
            let server = self.clone_for_handler();
            pluvio_runtime::spawn_with_name(
                async move {
                    if let Err(e) = server.listen::<MetadataDeleteRequest, _, _>().await {
                        tracing::error!("MetadataDelete handler error: {:?}", e);
                    }
                },
                "rpc_metadata_delete_handler".to_string(),
            );
        }

        // Spawn MetadataUpdate handler
        {
            let server = self.clone_for_handler();
            pluvio_runtime::spawn_with_name(
                async move {
                    if let Err(e) = server.listen::<MetadataUpdateRequest, _, _>().await {
                        tracing::error!("MetadataUpdate handler error: {:?}", e);
                    }
                },
                "rpc_metadata_update_handler".to_string(),
            );
        }

        // Spawn Shutdown handler
        {
            let server = self.clone_for_handler();
            pluvio_runtime::spawn_with_name(
                async move {
                    use crate::rpc::metadata_ops::ShutdownRequest;
                    if let Err(e) = server.listen::<ShutdownRequest, _, _>().await {
                        tracing::error!("Shutdown handler error: {:?}", e);
                    }
                },
                "rpc_shutdown_handler".to_string(),
            );
        }

        tracing::info!("All RPC handlers registered successfully");
        Ok(())
    }

    /// Register only benchmark RPC handlers (Ping-Pong, Throughput, etc.)
    #[async_backtrace::framed]
    #[instrument(level = "trace", name = "register_bench_handlers", skip(self))]
    pub async fn register_bench_handlers(&self) -> Result<(), RpcError> {
        use crate::rpc::bench_ops::{BenchPingRequest, BenchShutdownRequest};

        tracing::info!("Registering benchmark RPC handlers...");

        // Spawn BenchPing handler
        {
            let server = self.clone_for_handler();
            pluvio_runtime::spawn_with_name(
                async move {
                    if let Err(e) = server.listen::<BenchPingRequest, _, _>().await {
                        tracing::error!("BenchPing handler error: {:?}", e);
                    }
                },
                "bench_ping_handler".to_string(),
            );
        }

        // Spawn BenchShutdown handler
        {
            let server = self.clone_for_handler();
            pluvio_runtime::spawn_with_name(
                async move {
                    if let Err(e) = server.listen::<BenchShutdownRequest, _, _>().await {
                        tracing::error!("BenchShutdown handler error: {:?}", e);
                    }
                },
                "bench_shutdown_handler".to_string(),
            );
        }

        tracing::info!("Benchmark RPC handlers registered successfully");
        Ok(())
    }

    /// Clone the server for use in handler tasks
    /// This creates a shallow clone that shares the worker, handler context, and streams
    fn clone_for_handler(&self) -> Self {
        Self {
            worker: self.worker.clone(),
            handler_context: self.handler_context.clone(),
            streams: self.streams.clone(),
        }
    }

    /// Close all registered streams to initiate graceful shutdown of handlers.
    ///
    /// This will cause all `wait_msg()` calls to return `None`, allowing the
    /// listener tasks to exit gracefully.
    #[instrument(level = "trace", name = "shutdown_all_streams", skip(self))]
    pub fn shutdown_all_streams(&self) {
        let streams = self.streams.borrow();
        let count = streams.len();
        tracing::info!("Closing {} RPC streams for graceful shutdown", count);

        for stream in streams.iter() {
            stream.close();
        }

        tracing::info!("All {} RPC streams closed", count);
    }
}
