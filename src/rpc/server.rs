use std::cell::RefCell;
use std::marker::PhantomData;
use std::rc::Rc;

use pluvio_ucx::{Worker, async_ucx::ucp::WorkerAddress, am::AmStream};

use crate::rpc::handlers::RpcHandlerContext;
use crate::rpc::{AmRpc, RpcError, Serializable};

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
            pluvio_runtime::spawn(async move {
                // Use trace_span to avoid excessive logging at debug level
                let _handler_span =
                    tracing::trace_span!("rpc_server_handler", rpc_id = Rpc::rpc_id()).entered();

                match Rpc::server_handler(ctx_clone, am_msg).await {
                    Ok((_response, _am_msg)) => {
                        // Response was already sent within server_handler via reply_ep
                        tracing::trace!(
                            "RPC handler completed successfully for RPC ID {} (response sent directly)",
                            Rpc::rpc_id()
                        );
                    }
                    Err((e, _am_msg)) => {
                        // エラーレスポンスもserver_handler内で送信済み（またはハンドラーがエラーを返した）
                        tracing::error!("Handler failed for RPC ID {}: {:?}", Rpc::rpc_id(), e);
                    }
                }
            });
        }

        Ok(())
    }

    /// Register and start all standard RPC handlers
    ///
    /// This is a convenience method that starts listeners for all standard RPC types:
    /// - ReadChunk (RPC_READ_CHUNK)
    /// - WriteChunk (RPC_WRITE_CHUNK)
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
    pub async fn register_all_handlers(&self) -> Result<(), RpcError> {
        use crate::rpc::data_ops::{ReadChunkRequest, WriteChunkRequest};
        use crate::rpc::metadata_ops::{
            MetadataCreateDirRequest, MetadataCreateFileRequest, MetadataDeleteRequest,
            MetadataLookupRequest, MetadataUpdateRequest,
        };

        tracing::info!("Registering all RPC handlers...");

        // Spawn ReadChunk handler with polling priority
        {
            let server = self.clone_for_handler();
            pluvio_runtime::spawn_polling_with_name(
                async move {
                    if let Err(e) = server.listen::<ReadChunkRequest, _, _>().await {
                        tracing::error!("ReadChunk handler error: {:?}", e);
                    }
                },
                "rpc_read_chunk_handler".to_string(),
            );
        }

        // Spawn WriteChunk handler with polling priority
        {
            let server = self.clone_for_handler();
            pluvio_runtime::spawn_polling_with_name(
                async move {
                    if let Err(e) = server.listen::<WriteChunkRequest, _, _>().await {
                        tracing::error!("WriteChunk handler error: {:?}", e);
                    }
                },
                "rpc_write_chunk_handler".to_string(),
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
