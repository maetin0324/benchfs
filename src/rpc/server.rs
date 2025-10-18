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
    /// This method uses the unified AmRpc::server_handler() interface.
    /// It automatically handles both simple header-only responses and data transfer responses.
    ///
    /// # Example
    /// ```ignore
    /// use crate::rpc::data_ops::ReadChunkRequest;
    ///
    /// server.listen::<ReadChunkRequest, _, _>(runtime.clone()).await?;
    /// ```
    pub async fn listen<Rpc, ReqH, ResH>(
        &self,
        _runtime: Rc<Runtime>,
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

            // Call the unified server_handler
            match Rpc::server_handler(ctx_clone, am_msg).await {
                Ok((response, am_msg)) => {
                    // Send response back to client via reply when the client expects one
                    if am_msg.need_reply() {
                        let reply_stream_id = Rpc::reply_stream_id();

                        // Serialize response header
                        let response_bytes = zerocopy::IntoBytes::as_bytes(&response.header);

                        // Prepare data payload if present
                        let data_payload: &[u8] = if let Some(ref data) = response.data {
                            data.as_slice()
                        } else {
                            &[]
                        };

                        // Determine protocol: use Rendezvous if we have data, otherwise None
                        let proto = if data_payload.is_empty() {
                            None
                        } else {
                            Some(pluvio_ucx::async_ucx::ucp::AmProto::Rndv)
                        };

                        // Send reply
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
                                    data_payload,
                                    false, // Not eager
                                    proto,
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
                                    "Successfully sent reply for RPC ID {} (reply_stream_id: {}, data_len: {})",
                                    Rpc::rpc_id(),
                                    reply_stream_id,
                                    data_payload.len()
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
                                    &[],   // No additional data payload
                                    false, // Not eager
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
            MetadataLookupRequest, MetadataCreateFileRequest,
            MetadataCreateDirRequest, MetadataDeleteRequest,
            MetadataUpdateRequest,
        };

        tracing::info!("Registering all RPC handlers...");

        // Spawn ReadChunk handler
        {
            let server = self.clone_for_handler();
            let rt = runtime.clone();
            runtime.spawn(async move {
                if let Err(e) = server.listen::<ReadChunkRequest, _, _>(rt).await {
                    tracing::error!("ReadChunk handler error: {:?}", e);
                }
            });
        }

        // Spawn WriteChunk handler
        {
            let server = self.clone_for_handler();
            let rt = runtime.clone();
            runtime.spawn(async move {
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
