use std::rc::Rc;

use pluvio_runtime::executor::Runtime;
use pluvio_ucx::{async_ucx::ucp::WorkerAddress, Worker};

use crate::rpc::{AmRpc, RpcError, Serializable};
use crate::rpc::handlers::RpcHandlerContext;

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

    pub fn get_address(&self) -> Result<WorkerAddress, RpcError> {
        self.worker.address().map_err(|e| {
            RpcError::TransportError(format!("Failed to get worker address: {:?}", e))
        })
    }


    /// Start listening for RPC requests on the given AM stream ID
    ///
    /// Note: This implementation assumes clients send their requests and wait for replies
    /// on a separate reply stream. The server processes requests and sends responses
    /// using the registered connection endpoints.
    ///
    /// DEPRECATED: Use `listen_with_handler` instead for production use.
    /// This method calls the placeholder `server_handler` which is not fully implemented.
    pub async fn listen<Rpc, ReqH, ResH>(
        &self,
        runtime: Rc<Runtime>,
    ) -> Result<(), RpcError>
    where
        ResH: Serializable + 'static,
        ReqH: Serializable + 'static,
        Rpc: AmRpc<RequestHeader = ReqH, ResponseHeader = ResH> + 'static,
    {
        let stream = self.worker.am_stream(Rpc::rpc_id()).map_err(|e| {
            RpcError::TransportError(format!("Failed to create AM stream: {:?}", e))
        })?;

        tracing::info!("RpcServer: Listening on AM stream ID {}", Rpc::rpc_id());

        loop {
            let msg = stream.wait_msg().await;
            if msg.is_none() {
                tracing::info!("RpcServer: Stream closed");
                break;
            }

            let msg = msg
                .ok_or_else(|| RpcError::TransportError("Failed to receive message".to_string()))?;

            // Handle request data and surface handler failures for easier debugging
            let rpc_id = Rpc::rpc_id();
            runtime.spawn(async move {
                if let Err(e) = Rpc::server_handler(msg).await {
                    tracing::error!("RpcServer: handler task for RPC ID {} failed: {:?}", rpc_id, e);
                }
            });
        }

        Ok(())
    }

    /// Start listening for RPC requests with a custom handler function
    ///
    /// This is the recommended way to set up RPC handlers in production.
    /// The handler function receives the handler context and the incoming message,
    /// and should return a response header.
    ///
    /// # Example
    /// ```ignore
    /// use crate::rpc::data_ops::ReadChunkRequest;
    /// use crate::rpc::handlers::handle_read_chunk;
    ///
    /// server.listen_with_handler::<ReadChunkRequest, _, _>(
    ///     runtime.clone(),
    ///     handle_read_chunk
    /// ).await?;
    /// ```
    pub async fn listen_with_handler<Rpc, ReqH, ResH, F, Fut>(
        &self,
        _runtime: Rc<Runtime>,
        handler: F,
    ) -> Result<(), RpcError>
    where
        ResH: Serializable + 'static,
        ReqH: Serializable + 'static,
        Rpc: AmRpc<RequestHeader = ReqH, ResponseHeader = ResH> + 'static,
        F: Fn(Rc<RpcHandlerContext>, pluvio_ucx::async_ucx::ucp::AmMsg) -> Fut + 'static,
        Fut: std::future::Future<Output = Result<(ResH, pluvio_ucx::async_ucx::ucp::AmMsg), (RpcError, pluvio_ucx::async_ucx::ucp::AmMsg)>> + 'static,
    {
        let stream = self.worker.am_stream(Rpc::rpc_id()).map_err(|e| {
            RpcError::TransportError(format!("Failed to create AM stream: {:?}", e))
        })?;

        tracing::info!("RpcServer: Listening on AM stream ID {} with handler", Rpc::rpc_id());

        let ctx = self.handler_context.clone();

        loop {
            let msg = stream.wait_msg().await;
            if msg.is_none() {
                tracing::info!("RpcServer: Stream closed for RPC ID {}", Rpc::rpc_id());
                break;
            }

            let am_msg = msg
                .ok_or_else(|| RpcError::TransportError("Failed to receive message".to_string()))?;

            let ctx_clone = ctx.clone();

            // Call the handler function
            match handler(ctx_clone, am_msg).await {
                Ok((response_header, am_msg)) => {
                    // Send response back to client via reply when the client expects one
                    if am_msg.need_reply() {
                        let reply_stream_id = Rpc::reply_stream_id();

                        // Serialize response header
                        let response_bytes = zerocopy::IntoBytes::as_bytes(&response_header);

                        // Send reply to the client that sent the request
                        // AmMsg has a reply() method that automatically routes back to the sender
                        // SAFETY: reply() requires unsafe because it deals with raw UCX operations
                        // We ensure safety by:
                        // 1. response_bytes is a valid byte slice from zerocopy
                        // 2. The reply_stream_id is valid (from Rpc::reply_stream_id())
                        // 3. The am_msg is still valid (not dropped)
                        unsafe {
                            if let Err(e) = am_msg
                                .reply(
                                    reply_stream_id as u32,
                                    response_bytes,
                                    &[],  // No additional data payload
                                    false, // Not an eager message
                                    None,  // No special proto
                                )
                                .await
                            {
                                tracing::error!(
                                    "Failed to send reply for RPC ID {}: {:?}",
                                    Rpc::rpc_id(),
                                    e
                                );
                            } else {
                                tracing::debug!(
                                    "Successfully sent reply for RPC ID {} (reply_stream_id: {})",
                                    Rpc::rpc_id(),
                                    reply_stream_id
                                );
                            }
                        }
                    } else {
                        tracing::debug!(
                            "Skipping reply for RPC ID {} (fire-and-forget)",
                            Rpc::rpc_id()
                        );
                    }
                }
                Err((e, am_msg)) => {
                    tracing::error!("Handler failed for RPC ID {}: {:?}", Rpc::rpc_id(), e);

                    // Send an error response to the client if they expect a reply
                    if am_msg.need_reply() {
                        let reply_stream_id = Rpc::reply_stream_id();
                        let error_response = Rpc::error_response(&e);
                        let response_bytes = zerocopy::IntoBytes::as_bytes(&error_response);

                        // SAFETY: Same safety considerations as the success case above
                        unsafe {
                            if let Err(reply_err) = am_msg
                                .reply(
                                    reply_stream_id as u32,
                                    response_bytes,
                                    &[],  // No additional data payload
                                    false, // Not an eager message
                                    None,  // No special proto
                                )
                                .await
                            {
                                tracing::error!(
                                    "Failed to send error reply for RPC ID {}: {:?}",
                                    Rpc::rpc_id(),
                                    reply_err
                                );
                            } else {
                                tracing::debug!(
                                    "Successfully sent error reply for RPC ID {} (status: {:?})",
                                    Rpc::rpc_id(),
                                    e
                                );
                            }
                        }
                    }
                }
            }
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
            MetadataLookupRequest, MetadataCreateFileRequest,
            MetadataCreateDirRequest, MetadataDeleteRequest,
        };
        use crate::rpc::handlers::{
            handle_read_chunk, handle_write_chunk,
            handle_metadata_lookup, handle_metadata_create_file,
            handle_metadata_create_dir, handle_metadata_delete,
        };

        tracing::info!("Registering all RPC handlers...");

        // Spawn ReadChunk handler
        {
            let server = self.clone_for_handler();
            let rt = runtime.clone();
            runtime.spawn(async move {
                if let Err(e) = server.listen_with_handler::<ReadChunkRequest, _, _, _, _>(
                    rt,
                    handle_read_chunk,
                ).await {
                    tracing::error!("ReadChunk handler error: {:?}", e);
                }
            });
        }

        // Spawn WriteChunk handler
        {
            let server = self.clone_for_handler();
            let rt = runtime.clone();
            runtime.spawn(async move {
                if let Err(e) = server.listen_with_handler::<WriteChunkRequest, _, _, _, _>(
                    rt,
                    handle_write_chunk,
                ).await {
                    tracing::error!("WriteChunk handler error: {:?}", e);
                }
            });
        }

        // Spawn MetadataLookup handler
        {
            let server = self.clone_for_handler();
            let rt = runtime.clone();
            runtime.spawn(async move {
                if let Err(e) = server.listen_with_handler::<MetadataLookupRequest, _, _, _, _>(
                    rt,
                    handle_metadata_lookup,
                ).await {
                    tracing::error!("MetadataLookup handler error: {:?}", e);
                }
            });
        }

        // Spawn MetadataCreateFile handler
        {
            let server = self.clone_for_handler();
            let rt = runtime.clone();
            runtime.spawn(async move {
                if let Err(e) = server.listen_with_handler::<MetadataCreateFileRequest, _, _, _, _>(
                    rt,
                    handle_metadata_create_file,
                ).await {
                    tracing::error!("MetadataCreateFile handler error: {:?}", e);
                }
            });
        }

        // Spawn MetadataCreateDir handler
        {
            let server = self.clone_for_handler();
            let rt = runtime.clone();
            runtime.spawn(async move {
                if let Err(e) = server.listen_with_handler::<MetadataCreateDirRequest, _, _, _, _>(
                    rt,
                    handle_metadata_create_dir,
                ).await {
                    tracing::error!("MetadataCreateDir handler error: {:?}", e);
                }
            });
        }

        // Spawn MetadataDelete handler
        {
            let server = self.clone_for_handler();
            let rt = runtime.clone();
            runtime.spawn(async move {
                if let Err(e) = server.listen_with_handler::<MetadataDeleteRequest, _, _, _, _>(
                    rt,
                    handle_metadata_delete,
                ).await {
                    tracing::error!("MetadataDelete handler error: {:?}", e);
                }
            });
        }

        tracing::info!("All RPC handlers registered successfully");
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
