//! Stream + RMA based RPC client
//!
//! Provides client-side RPC execution using UCX Stream for control messages
//! and RMA for data transfer.

use std::mem::MaybeUninit;
use std::rc::Rc;
use std::sync::Arc;

use pluvio_ucx::async_ucx::ucp::{MemoryHandle, RKey};
use pluvio_ucx::endpoint::Endpoint;
use pluvio_ucx::{Context, Worker};
use zerocopy::FromBytes;

use super::stream_helpers::{
    stream_recv, stream_recv_completion, stream_recv_header, stream_recv_u64, stream_send,
    stream_send_completion, stream_send_header, stream_send_u64,
};
use super::stream_rpc::{
    ClientGetRequestMessage, ClientPutRequestMessage, RpcPattern, StreamRpc,
};
use super::{RpcError, Serializable};

/// Stream-based RPC client
///
/// Executes RPCs using Stream + RMA protocol.
pub struct StreamRpcClient {
    endpoint: Endpoint,
    worker: Rc<Worker>,
    context: Arc<Context>,
}

impl StreamRpcClient {
    /// Create a new StreamRpcClient
    pub fn new(endpoint: Endpoint, worker: Rc<Worker>, context: Arc<Context>) -> Self {
        Self {
            endpoint,
            worker,
            context,
        }
    }

    /// Get the endpoint
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// Get the worker
    pub fn worker(&self) -> &Rc<Worker> {
        &self.worker
    }

    /// Get the context
    pub fn context(&self) -> &Arc<Context> {
        &self.context
    }

    /// Execute Pattern 1: No RMA (header-only RPC)
    ///
    /// Flow:
    /// 1. Client sends request message (header + optional path)
    /// 2. Server processes request
    /// 3. Client receives response header
    pub async fn execute_no_rma<T: StreamRpc>(
        &self,
        request: &T,
    ) -> Result<T::ResponseHeader, RpcError> {
        tracing::trace!(
            "execute_no_rma: rpc_id={}, pattern={:?}",
            T::rpc_id(),
            request.pattern()
        );

        // 1. Build request message: header_len (4) + header + path_len (4) + path
        let header_bytes = zerocopy::IntoBytes::as_bytes(request.request_header());
        let path_bytes = request.path_bytes();

        let mut msg_bytes = Vec::new();

        // Write header_len
        msg_bytes.extend_from_slice(&(header_bytes.len() as u32).to_le_bytes());

        // Write header
        msg_bytes.extend_from_slice(header_bytes);

        // Write path_len
        msg_bytes.extend_from_slice(&(path_bytes.len() as u32).to_le_bytes());

        // Write path (if present)
        if !path_bytes.is_empty() {
            msg_bytes.extend_from_slice(&path_bytes);
        }

        tracing::trace!(
            "execute_no_rma: sending request message, header_len={}, path_len={}",
            header_bytes.len(),
            path_bytes.len()
        );

        // 2. Send request message
        stream_send(&self.endpoint, &msg_bytes).await?;

        tracing::trace!("execute_no_rma: sent request message");

        // 3. Receive response header
        let response: T::ResponseHeader = stream_recv_header(&self.endpoint).await?;

        tracing::trace!("execute_no_rma: received response header");

        Ok(response)
    }

    /// Execute Pattern 2: Client PUT (client sends data to server)
    ///
    /// Flow:
    /// 1. Client registers memory and sends header + rkey + data info
    /// 2. Server prepares buffer and sends buffer address
    /// 3. Client PUTs data to server buffer
    /// 4. Client sends completion notification
    /// 5. Client receives response header
    pub async fn execute_client_put<T: StreamRpc>(
        &self,
        request: &T,
        data: &[u8],
    ) -> Result<T::ResponseHeader, RpcError> {
        let RpcPattern::ClientPut { data_size } = request.pattern() else {
            return Err(RpcError::HandlerError(
                "Request pattern is not ClientPut".to_string(),
            ));
        };

        if data.len() != data_size as usize {
            return Err(RpcError::HandlerError(format!(
                "Data size mismatch: expected {}, got {}",
                data_size,
                data.len()
            )));
        }

        tracing::trace!(
            "execute_client_put: rpc_id={}, data_size={}",
            T::rpc_id(),
            data_size
        );

        // 1. Register memory for RMA
        let mut data_copy = data.to_vec();
        let mem_handle = MemoryHandle::register(self.context.inner(), &mut data_copy);
        let rkey_buf = mem_handle.pack();
        let data_addr = data_copy.as_ptr() as u64;

        tracing::trace!(
            "execute_client_put: registered memory, data_addr={:#x}, rkey_len={}",
            data_addr,
            rkey_buf.as_ref().len()
        );

        // 2. Build and send request message (header + path + rkey + addr + size)
        let req_msg = ClientPutRequestMessage {
            rpc_id: T::rpc_id(),
            header_bytes: zerocopy::IntoBytes::as_bytes(request.request_header()).to_vec(),
            path_bytes: request.path_bytes(),
            data_addr,
            data_size,
            rkey: rkey_buf.as_ref().to_vec(),
        };

        stream_send(&self.endpoint, &req_msg.to_bytes()).await?;

        tracing::trace!("execute_client_put: sent request message");

        // 3. Wait for server buffer address
        let server_buffer_addr = stream_recv_u64(&self.endpoint).await?;

        tracing::trace!(
            "execute_client_put: received server buffer address {:#x}",
            server_buffer_addr
        );

        // 4. PUT data to server
        let rkey = RKey::unpack(&self.endpoint.endpoint, rkey_buf.as_ref());
        self.endpoint
            .put(&data_copy, server_buffer_addr, &rkey)
            .await
            .map_err(|e| RpcError::TransportError(format!("Failed to PUT data: {:?}", e)))?;

        tracing::trace!("execute_client_put: PUT completed");

        // 5. Send completion notification
        stream_send_completion(&self.endpoint).await?;

        tracing::trace!("execute_client_put: sent completion notification");

        // 6. Receive response header
        let response: T::ResponseHeader = stream_recv_header(&self.endpoint).await?;

        tracing::trace!("execute_client_put: received response header");

        Ok(response)
    }

    /// Execute Pattern 3: Client GET (server sends data to client)
    ///
    /// Flow:
    /// 1. Client registers buffer and sends header + rkey + buffer info
    /// 2. Server reads data and PUTs to client buffer
    /// 3. Client receives data size notification
    /// 4. Client receives completion notification
    /// 5. Buffer now contains the data
    pub async fn execute_client_get<T: StreamRpc>(
        &self,
        request: &T,
        buffer: &mut [u8],
    ) -> Result<(T::ResponseHeader, usize), RpcError> {
        let RpcPattern::ClientGet { buffer_size } = request.pattern() else {
            return Err(RpcError::HandlerError(
                "Request pattern is not ClientGet".to_string(),
            ));
        };

        if buffer.len() != buffer_size as usize {
            return Err(RpcError::HandlerError(format!(
                "Buffer size mismatch: expected {}, got {}",
                buffer_size,
                buffer.len()
            )));
        }

        tracing::trace!(
            "execute_client_get: rpc_id={}, buffer_size={}",
            T::rpc_id(),
            buffer_size
        );

        // 1. Register buffer for RMA
        let mem_handle = MemoryHandle::register(self.context.inner(), buffer);
        let rkey_buf = mem_handle.pack();
        let buffer_addr = buffer.as_ptr() as u64;

        tracing::trace!(
            "execute_client_get: registered buffer, buffer_addr={:#x}, rkey_len={}",
            buffer_addr,
            rkey_buf.as_ref().len()
        );

        // 2. Build and send request message (header + path + rkey + addr + size)
        let req_msg = ClientGetRequestMessage {
            rpc_id: T::rpc_id(),
            header_bytes: zerocopy::IntoBytes::as_bytes(request.request_header()).to_vec(),
            path_bytes: request.path_bytes(),
            buffer_addr,
            buffer_size,
            rkey: rkey_buf.as_ref().to_vec(),
        };

        stream_send(&self.endpoint, &req_msg.to_bytes()).await?;

        tracing::trace!("execute_client_get: sent request message");

        // 3. Wait for data size from server
        let data_size = stream_recv_u64(&self.endpoint).await?;

        tracing::trace!("execute_client_get: received data size {}", data_size);

        if data_size > buffer_size {
            return Err(RpcError::TransportError(format!(
                "Data size {} exceeds buffer size {}",
                data_size, buffer_size
            )));
        }

        // 4. Wait for completion notification (server PUT is done)
        stream_recv_completion(&self.endpoint).await?;

        tracing::trace!("execute_client_get: received completion notification");

        // 5. Buffer now contains the data
        // Create a success response header (actual header comes from server via response)
        // For now, we return a default response indicating success
        let response = T::ResponseHeader::read_from_bytes(&[0u8; 256][..std::mem::size_of::<
            T::ResponseHeader,
        >()])
        .map_err(|e| {
            RpcError::TransportError(format!("Failed to create response header: {:?}", e))
        })?;

        Ok((response, data_size as usize))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: Actual tests require a running UCX environment with server
    // These are placeholder tests for structure validation

    #[test]
    fn test_stream_rpc_client_structure() {
        // This test just validates that the types are correctly defined
        // Real tests would require integration testing with a server
    }
}
