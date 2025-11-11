//! Stream + RMA based RPC server
//!
//! Provides server-side RPC handling using UCX Stream for control messages
//! and RMA for data transfer.

use std::mem::MaybeUninit;
use std::rc::Rc;

use pluvio_ucx::async_ucx::ucp::RKey;
use pluvio_ucx::endpoint::Endpoint;
use pluvio_ucx::Worker;
use zerocopy::FromBytes;

use super::stream_helpers::{
    stream_recv, stream_recv_completion, stream_recv_header, stream_recv_rpc_id, stream_send,
    stream_send_completion, stream_send_header, stream_send_u64, MAX_HEADER_SIZE,
};
use super::stream_rpc::{ClientGetRequestMessage, ClientPutRequestMessage, RpcPattern, StreamRpc};
use super::{RpcError, Serializable};
use crate::rpc::handlers::RpcHandlerContext;

/// Stream-based RPC server
///
/// Handles incoming RPCs using Stream + RMA protocol.
pub struct StreamRpcServer {
    worker: Rc<Worker>,
    handler_context: Rc<RpcHandlerContext>,
}

impl StreamRpcServer {
    /// Create a new StreamRpcServer
    pub fn new(worker: Rc<Worker>, handler_context: Rc<RpcHandlerContext>) -> Self {
        Self {
            worker,
            handler_context,
        }
    }

    /// Get the handler context
    pub fn handler_context(&self) -> &Rc<RpcHandlerContext> {
        &self.handler_context
    }

    /// Serve a single endpoint (blocking loop)
    ///
    /// This method handles all RPCs for a given endpoint until:
    /// - The connection is closed
    /// - A shutdown signal is received
    /// - An unrecoverable error occurs
    pub async fn serve(&self, endpoint: Endpoint) -> Result<(), RpcError> {
        tracing::info!("StreamRpcServer: starting to serve endpoint");

        // Connection reset retry configuration
        const MAX_RETRY_ATTEMPTS: usize = 3;
        const RETRY_DELAY_MS: u64 = 100;

        loop {
            // Check shutdown flag
            if self.handler_context.should_shutdown() {
                tracing::info!("StreamRpcServer: shutdown requested, exiting serve loop");
                break;
            }

            // Receive RPC ID with retry on connection reset
            let rpc_id = {
                let mut retry_count = 0;
                loop {
                    match stream_recv_rpc_id(&endpoint).await {
                        Ok(id) => break id,
                        Err(e) => {
                            // Check if error is connection reset
                            let is_connection_reset = matches!(
                                e,
                                RpcError::TransportError(ref msg) if msg.contains("ConnectionReset")
                            );

                            if is_connection_reset && retry_count < MAX_RETRY_ATTEMPTS {
                                retry_count += 1;
                                tracing::warn!(
                                    "StreamRpcServer: Connection Reset detected (attempt {}/{}), retrying...",
                                    retry_count,
                                    MAX_RETRY_ATTEMPTS
                                );

                                // Brief delay before retry
                                pluvio_timer::sleep(std::time::Duration::from_millis(RETRY_DELAY_MS)).await;
                                continue;
                            } else {
                                // Non-retryable error or max retries exceeded
                                tracing::warn!(
                                    "StreamRpcServer: failed to receive RPC ID after {} attempts: {:?}",
                                    retry_count + 1,
                                    e
                                );
                                return Ok(()); // Exit serve loop gracefully
                            }
                        }
                    }
                }
            };

            tracing::debug!("StreamRpcServer: received RPC ID {}", rpc_id);

            // Dispatch to appropriate handler based on RPC ID
            use super::data_ops::{RPC_READ_CHUNK, RPC_WRITE_CHUNK};
            use super::metadata_ops::{
                RPC_METADATA_CREATE_DIR, RPC_METADATA_CREATE_FILE, RPC_METADATA_DELETE,
                RPC_METADATA_LOOKUP, RPC_METADATA_UPDATE, RPC_SHUTDOWN,
            };
            use super::stream_data_ops::{StreamReadChunkRequest, StreamWriteChunkRequest};
            use super::stream_metadata_ops::{
                StreamMetadataCreateDirRequest, StreamMetadataCreateFileRequest,
                StreamMetadataDeleteRequest, StreamMetadataLookupRequest,
                StreamMetadataUpdateRequest, StreamShutdownRequest,
            };
            use super::stream_rpc::StreamRpc;

            let result = match rpc_id {
                // Data operations
                RPC_READ_CHUNK => {
                    tracing::trace!("Dispatching to ReadChunk handler");
                    self.handle_client_get::<StreamReadChunkRequest>(&endpoint)
                        .await
                }
                RPC_WRITE_CHUNK => {
                    tracing::trace!("Dispatching to WriteChunk handler");
                    self.handle_client_put::<StreamWriteChunkRequest>(&endpoint)
                        .await
                }

                // Metadata operations
                RPC_METADATA_LOOKUP => {
                    tracing::trace!("Dispatching to MetadataLookup handler");
                    self.handle_no_rma::<StreamMetadataLookupRequest>(&endpoint)
                        .await
                }
                RPC_METADATA_CREATE_FILE => {
                    tracing::trace!("Dispatching to MetadataCreateFile handler");
                    self.handle_no_rma::<StreamMetadataCreateFileRequest>(&endpoint)
                        .await
                }
                RPC_METADATA_CREATE_DIR => {
                    tracing::trace!("Dispatching to MetadataCreateDir handler");
                    self.handle_no_rma::<StreamMetadataCreateDirRequest>(&endpoint)
                        .await
                }
                RPC_METADATA_DELETE => {
                    tracing::trace!("Dispatching to MetadataDelete handler");
                    self.handle_no_rma::<StreamMetadataDeleteRequest>(&endpoint)
                        .await
                }
                RPC_METADATA_UPDATE => {
                    tracing::trace!("Dispatching to MetadataUpdate handler");
                    self.handle_no_rma::<StreamMetadataUpdateRequest>(&endpoint)
                        .await
                }
                RPC_SHUTDOWN => {
                    tracing::trace!("Dispatching to Shutdown handler");
                    match self.handle_no_rma::<StreamShutdownRequest>(&endpoint).await {
                        Ok(()) => {
                            tracing::info!("Shutdown request processed, exiting serve loop");
                            break;
                        }
                        Err(e) => Err(e),
                    }
                }

                // Unknown RPC ID
                _ => {
                    tracing::warn!(
                        "StreamRpcServer: No handler registered for RPC ID {}",
                        rpc_id
                    );
                    continue;
                }
            };

            // Handle RPC result
            if let Err(e) = result {
                tracing::error!("StreamRpcServer: Handler error for RPC ID {}: {:?}", rpc_id, e);
                // Continue serving despite errors
            }
        }

        tracing::info!("StreamRpcServer: serve loop exited");
        Ok(())
    }

    /// Handle Pattern 1: No RMA (header-only RPC)
    ///
    /// Generic handler that works with any StreamRpc implementing NoRma pattern.
    /// Handles both path-based and path-less RPCs.
    pub async fn handle_no_rma<T: StreamRpc>(
        &self,
        endpoint: &Endpoint,
    ) -> Result<(), RpcError> {
        tracing::trace!(
            "handle_no_rma: rpc_id={}, receiving request message",
            T::rpc_id()
        );

        // 1. Receive request message (header + optional path)
        // For NoRma pattern, we receive: header_len (4) + header + path_len (4) + path
        use std::mem::MaybeUninit;
        let max_msg_size = 4096; // Should be enough for header + path
        let mut buffer = vec![MaybeUninit::<u8>::uninit(); max_msg_size];

        let msg_len = stream_recv(endpoint, &mut buffer).await?;

        // SAFETY: We just received msg_len bytes
        let msg_bytes = unsafe { std::slice::from_raw_parts(buffer.as_ptr() as *const u8, msg_len) };

        // 2. Parse message: header_len (4) + header + path_len (4) + path
        if msg_bytes.len() < 8 {
            return Err(RpcError::TransportError(
                "Message too short for NoRma pattern".to_string(),
            ));
        }

        let mut offset = 0;

        // Read header_len
        let header_len = u32::from_le_bytes([
            msg_bytes[offset],
            msg_bytes[offset + 1],
            msg_bytes[offset + 2],
            msg_bytes[offset + 3],
        ]) as usize;
        offset += 4;

        if msg_bytes.len() < offset + header_len + 4 {
            return Err(RpcError::TransportError(
                "Message too short for header and path_len".to_string(),
            ));
        }

        // Parse header
        let header: T::RequestHeader =
            T::RequestHeader::read_from_bytes(&msg_bytes[offset..offset + header_len]).map_err(
                |e| RpcError::TransportError(format!("Failed to parse request header: {:?}", e)),
            )?;
        offset += header_len;

        // Read path_len
        let path_len = u32::from_le_bytes([
            msg_bytes[offset],
            msg_bytes[offset + 1],
            msg_bytes[offset + 2],
            msg_bytes[offset + 3],
        ]) as usize;
        offset += 4;

        tracing::trace!(
            "handle_no_rma: parsed message, header_len={}, path_len={}",
            header_len,
            path_len
        );

        // 3. Call appropriate handler based on whether path is present
        let response = if path_len > 0 {
            if msg_bytes.len() < offset + path_len {
                return Err(RpcError::TransportError(
                    "Message too short for path".to_string(),
                ));
            }

            // Parse path
            let path = std::str::from_utf8(&msg_bytes[offset..offset + path_len])
                .map_err(|e| RpcError::TransportError(format!("Invalid path UTF-8: {:?}", e)))?;

            tracing::trace!("handle_no_rma: calling server_handler_with_path, path={}", path);

            // Call path-aware handler
            match T::server_handler_with_path(
                self.handler_context.clone(),
                endpoint,
                header,
                path,
            )
            .await
            {
                Ok(res) => res,
                Err(e) => {
                    tracing::error!("handle_no_rma: handler failed: {:?}", e);
                    T::error_response(&e)
                }
            }
        } else {
            tracing::trace!("handle_no_rma: calling server_handler (no path)");

            // Call path-less handler
            match T::server_handler(self.handler_context.clone(), endpoint, header).await {
                Ok(res) => res,
                Err(e) => {
                    tracing::error!("handle_no_rma: handler failed: {:?}", e);
                    T::error_response(&e)
                }
            }
        };

        tracing::trace!("handle_no_rma: sending response header");

        // 4. Send response header
        stream_send_header(endpoint, &response).await?;

        tracing::trace!("handle_no_rma: completed");

        Ok(())
    }

    /// Handle Pattern 2: Client PUT (client sends data to server)
    ///
    /// Generic handler for RPCs where client sends data.
    pub async fn handle_client_put<T: StreamRpc>(
        &self,
        endpoint: &Endpoint,
    ) -> Result<(), RpcError> {
        tracing::trace!(
            "handle_client_put: rpc_id={}, receiving request message",
            T::rpc_id()
        );

        // 1. Receive request message (header + path + rkey + data info)
        let req_msg_bytes = self.recv_client_put_request_message(endpoint).await?;
        let req_msg = ClientPutRequestMessage::from_bytes(&req_msg_bytes)?;

        tracing::trace!(
            "handle_client_put: received request message, data_size={}, path_len={}",
            req_msg.data_size,
            req_msg.path_bytes.len()
        );

        // 2. Parse request header and path
        if req_msg.header_bytes.len() > MAX_HEADER_SIZE {
            return Err(RpcError::TransportError(format!(
                "Header size {} exceeds maximum {}",
                req_msg.header_bytes.len(),
                MAX_HEADER_SIZE
            )));
        }

        let header: T::RequestHeader =
            T::RequestHeader::read_from_bytes(&req_msg.header_bytes).map_err(|e| {
                RpcError::TransportError(format!("Failed to parse request header: {:?}", e))
            })?;

        let path = std::str::from_utf8(&req_msg.path_bytes)
            .map_err(|e| RpcError::TransportError(format!("Invalid path UTF-8: {:?}", e)))?;

        // 3. Prepare buffer for receiving data
        let mut buffer = self
            .handler_context
            .allocator
            .acquire()
            .await;

        if req_msg.data_size as usize > buffer.len() {
            return Err(RpcError::TransportError(format!(
                "Data size {} exceeds buffer size {}",
                req_msg.data_size,
                buffer.len()
            )));
        }

        let buffer_addr = buffer.as_ptr() as u64;

        tracing::trace!(
            "handle_client_put: prepared buffer, addr={:#x}",
            buffer_addr
        );

        // 4. Send buffer address to client
        stream_send_u64(endpoint, buffer_addr).await?;

        tracing::trace!("handle_client_put: sent buffer address, waiting for PUT");

        // 5. Wait for client PUT completion
        stream_recv_completion(endpoint).await?;

        tracing::trace!("handle_client_put: received completion, calling handler");

        // 6. Call handler with received data and path
        // SAFETY: buffer.as_ptr() returns a valid pointer, and we know data_size bytes were written
        let data_slice = unsafe {
            std::slice::from_raw_parts(
                buffer.as_ptr(),
                req_msg.data_size as usize,
            )
        };
        let response = match T::server_handler_with_data(
            self.handler_context.clone(),
            endpoint,
            header,
            path,
            data_slice,
        )
        .await
        {
            Ok(res) => res,
            Err(e) => {
                tracing::error!("handle_client_put: handler failed: {:?}", e);
                T::error_response(&e)
            }
        };

        tracing::trace!("handle_client_put: sending response header");

        // 7. Send response header
        stream_send_header(endpoint, &response).await?;

        tracing::trace!("handle_client_put: completed");

        Ok(())
    }

    /// Handle Pattern 3: Client GET (server sends data to client)
    ///
    /// Generic handler for RPCs where server sends data.
    pub async fn handle_client_get<T: StreamRpc>(
        &self,
        endpoint: &Endpoint,
    ) -> Result<(), RpcError> {
        tracing::trace!(
            "handle_client_get: rpc_id={}, receiving request message",
            T::rpc_id()
        );

        // 1. Receive request message (header + path + rkey + buffer info)
        let req_msg_bytes = self.recv_client_get_request_message(endpoint).await?;
        let req_msg = ClientGetRequestMessage::from_bytes(&req_msg_bytes)?;

        tracing::trace!(
            "handle_client_get: received request message, buffer_size={}, path_len={}",
            req_msg.buffer_size,
            req_msg.path_bytes.len()
        );

        // 2. Parse request header and path
        if req_msg.header_bytes.len() > MAX_HEADER_SIZE {
            return Err(RpcError::TransportError(format!(
                "Header size {} exceeds maximum {}",
                req_msg.header_bytes.len(),
                MAX_HEADER_SIZE
            )));
        }

        let header: T::RequestHeader =
            T::RequestHeader::read_from_bytes(&req_msg.header_bytes).map_err(|e| {
                RpcError::TransportError(format!("Failed to parse request header: {:?}", e))
            })?;

        let path = std::str::from_utf8(&req_msg.path_bytes)
            .map_err(|e| RpcError::TransportError(format!("Invalid path UTF-8: {:?}", e)))?;

        tracing::trace!("handle_client_get: calling handler to get data");

        // 3. Call handler to get data
        let (_response, data) = match T::server_handler_get_data(
            self.handler_context.clone(),
            endpoint,
            header,
            path,
            req_msg.buffer_size,
        )
        .await
        {
            Ok(res) => res,
            Err(e) => {
                tracing::error!("handle_client_get: handler failed: {:?}", e);
                // Return empty data on error
                (T::error_response(&e), Vec::new())
            }
        };

        tracing::trace!("handle_client_get: got data, size={}", data.len());

        if data.len() > req_msg.buffer_size as usize {
            return Err(RpcError::TransportError(format!(
                "Data size {} exceeds client buffer size {}",
                data.len(),
                req_msg.buffer_size
            )));
        }

        // 4. Unpack client rkey
        let rkey = RKey::unpack(&endpoint.endpoint, &req_msg.rkey);

        // 5. PUT data to client buffer
        if !data.is_empty() {
            endpoint
                .put(&data, req_msg.buffer_addr, &rkey)
                .await
                .map_err(|e| {
                    RpcError::TransportError(format!("Failed to PUT data to client: {:?}", e))
                })?;
        }

        tracing::trace!("handle_client_get: PUT completed");

        // 6. Send data size
        stream_send_u64(endpoint, data.len() as u64).await?;

        tracing::trace!("handle_client_get: sent data size");

        // 7. Send completion notification
        stream_send_completion(endpoint).await?;

        tracing::trace!("handle_client_get: completed");

        Ok(())
    }

    /// Helper: Receive ClientPutRequestMessage
    async fn recv_client_put_request_message(&self, endpoint: &Endpoint) -> Result<Vec<u8>, RpcError> {
        // For simplicity, receive up to 4KB for the message
        // Format: rpc_id (2) + header_len (4) + header + data_addr (8) + data_size (8) + rkey_len (4) + rkey
        let max_size = 4096;
        let mut buffer = vec![MaybeUninit::<u8>::uninit(); max_size];

        let len = stream_recv(endpoint, &mut buffer).await?;

        // SAFETY: We just received len bytes
        let bytes = unsafe { std::slice::from_raw_parts(buffer.as_ptr() as *const u8, len) };

        Ok(bytes.to_vec())
    }

    /// Helper: Receive ClientGetRequestMessage
    async fn recv_client_get_request_message(&self, endpoint: &Endpoint) -> Result<Vec<u8>, RpcError> {
        // Same as ClientPutRequestMessage for now
        self.recv_client_put_request_message(endpoint).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: Actual tests require a running UCX environment with clients
    // These are placeholder tests for structure validation

    #[test]
    fn test_stream_rpc_server_structure() {
        // This test just validates that the types are correctly defined
        // Real tests would require integration testing with clients
    }
}
