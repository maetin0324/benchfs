use std::marker::PhantomData;
use std::rc::Rc;

use pluvio_runtime::executor::Runtime;
use pluvio_ucx::{Worker, async_ucx::ucp::WorkerAddress};

use crate::rpc::handlers::RpcHandlerContext;
use crate::rpc::{AmRpc, RpcError, Serializable};

struct SizeCheck<T, const N: usize>(PhantomData<T>);
impl<T, const N: usize> SizeCheck<T, N> {
    // ここは「const 文脈」なので assert! がコンパイル時に評価される
    const OK: () = assert!(size_of::<T>() <= N);
}

pub const MAX_HEADER_SIZE: usize = 256;

/// RPC server that receives and dispatches ActiveMessages
pub struct RpcServer {
    worker: Rc<Worker>,
    handler_context: Rc<RpcHandlerContext>,
}

impl RpcServer {
    pub fn new(worker: Rc<Worker>, handler_context: Rc<RpcHandlerContext>) -> Self {
        Self {
            worker,
            handler_context,
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
    pub async fn listen<Rpc, ReqH, ResH>(&self, _runtime: Rc<Runtime>) -> Result<(), RpcError>
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

            tracing::debug!("RpcServer: Waiting for message on RPC ID {}", Rpc::rpc_id());
            let msg = stream.wait_msg().await;
            if msg.is_none() {
                tracing::info!("RpcServer: Stream closed for RPC ID {}", Rpc::rpc_id());
                break;
            }

            let am_msg = msg
                .ok_or_else(|| RpcError::TransportError("Failed to receive message".to_string()))?;

            tracing::debug!(
                "RpcServer: Received message on RPC ID {}, calling server_handler",
                Rpc::rpc_id()
            );

            let ctx_clone = ctx.clone();

            // Call the unified server_handler with span for tracking
            let _handler_span =
                tracing::debug_span!("rpc_server_handler", rpc_id = Rpc::rpc_id()).entered();

            match Rpc::server_handler(ctx_clone, am_msg).await {
                Ok((_response, _am_msg)) => {
                    // Response已经在server_handler内部通过send_response_direct()直接送信済み
                    // reply_ep使用を回避するため、ここでは何もしない
                    tracing::debug!(
                        "RPC handler completed successfully for RPC ID {} (response sent directly)",
                        Rpc::rpc_id()
                    );
                }
                Err((e, _am_msg)) => {
                    // エラーレスポンスもserver_handler内で送信済み（またはハンドラーがエラーを返した）
                    tracing::error!("Handler failed for RPC ID {}: {:?}", Rpc::rpc_id(), e);
                }
            }

            drop(_handler_span);
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
    /// let runtime = Rc::new(Runtime::new());
    /// server.register_all_handlers(runtime.clone()).await?;
    /// ```
    pub async fn register_all_handlers(&self, runtime: Rc<Runtime>) -> Result<(), RpcError> {
        use crate::rpc::data_ops::{ReadChunkRequest, WriteChunkRequest};
        use crate::rpc::metadata_ops::{
            MetadataCreateDirRequest, MetadataCreateFileRequest, MetadataDeleteRequest,
            MetadataLookupRequest, MetadataUpdateRequest,
        };

        tracing::info!("Registering all RPC handlers...");

        // Spawn ReadChunk handler with polling priority
        {
            let server = self.clone_for_handler();
            let rt = runtime.clone();
            runtime.spawn_polling(async move {
                if let Err(e) = server.listen::<ReadChunkRequest, _, _>(rt).await {
                    tracing::error!("ReadChunk handler error: {:?}", e);
                }
            });
        }

        // Spawn WriteChunk handler with polling priority
        {
            let server = self.clone_for_handler();
            let rt = runtime.clone();
            runtime.spawn_polling(async move {
                if let Err(e) = server.listen::<WriteChunkRequest, _, _>(rt).await {
                    tracing::error!("WriteChunk handler error: {:?}", e);
                }
            });
        }

        // Spawn MetadataLookup handler
        {
            let server = self.clone_for_handler();
            let rt = runtime.clone();
            runtime.spawn(async move {
                if let Err(e) = server.listen::<MetadataLookupRequest, _, _>(rt).await {
                    tracing::error!("MetadataLookup handler error: {:?}", e);
                }
            });
        }

        // Spawn MetadataCreateFile handler
        {
            let server = self.clone_for_handler();
            let rt = runtime.clone();
            runtime.spawn(async move {
                if let Err(e) = server.listen::<MetadataCreateFileRequest, _, _>(rt).await {
                    tracing::error!("MetadataCreateFile handler error: {:?}", e);
                }
            });
        }

        // Spawn MetadataCreateDir handler
        {
            let server = self.clone_for_handler();
            let rt = runtime.clone();
            runtime.spawn(async move {
                if let Err(e) = server.listen::<MetadataCreateDirRequest, _, _>(rt).await {
                    tracing::error!("MetadataCreateDir handler error: {:?}", e);
                }
            });
        }

        // Spawn MetadataDelete handler
        {
            let server = self.clone_for_handler();
            let rt = runtime.clone();
            runtime.spawn(async move {
                if let Err(e) = server.listen::<MetadataDeleteRequest, _, _>(rt).await {
                    tracing::error!("MetadataDelete handler error: {:?}", e);
                }
            });
        }

        // Spawn MetadataUpdate handler
        {
            let server = self.clone_for_handler();
            let rt = runtime.clone();
            runtime.spawn(async move {
                if let Err(e) = server.listen::<MetadataUpdateRequest, _, _>(rt).await {
                    tracing::error!("MetadataUpdate handler error: {:?}", e);
                }
            });
        }

        // Spawn Shutdown handler
        {
            let server = self.clone_for_handler();
            let rt = runtime.clone();
            runtime.spawn(async move {
                use crate::rpc::metadata_ops::ShutdownRequest;
                if let Err(e) = server.listen::<ShutdownRequest, _, _>(rt).await {
                    tracing::error!("Shutdown handler error: {:?}", e);
                }
            });
        }

        tracing::info!("All RPC handlers registered successfully");
        Ok(())
    }

    /// Register only benchmark RPC handlers (Ping-Pong, Throughput, etc.)
    pub async fn register_bench_handlers(&self, runtime: Rc<Runtime>) -> Result<(), RpcError> {
        use crate::rpc::bench_ops::{BenchPingRequest, BenchShutdownRequest};

        tracing::info!("Registering benchmark RPC handlers...");

        // Spawn BenchPing handler
        {
            let server = self.clone_for_handler();
            let rt = runtime.clone();
            runtime.spawn(async move {
                if let Err(e) = server.listen::<BenchPingRequest, _, _>(rt).await {
                    tracing::error!("BenchPing handler error: {:?}", e);
                }
            });
        }

        // Spawn BenchShutdown handler
        {
            let server = self.clone_for_handler();
            let rt = runtime.clone();
            runtime.spawn(async move {
                if let Err(e) = server.listen::<BenchShutdownRequest, _, _>(rt).await {
                    tracing::error!("BenchShutdown handler error: {:?}", e);
                }
            });
        }

        tracing::info!("Benchmark RPC handlers registered successfully");
        Ok(())
    }

    /// Clone the server for use in handler tasks
    /// This creates a shallow clone that shares the worker and handler context
    fn clone_for_handler(&self) -> Self {
        Self {
            worker: self.worker.clone(),
            handler_context: self.handler_context.clone(),
        }
    }
}
