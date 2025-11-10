//! Stream + RMA based data operations (ReadChunk/WriteChunk)
//!
//! This module provides Stream-based implementations of data operations
//! to replace the unstable Active Message based approach.

use std::rc::Rc;

use pluvio_ucx::endpoint::Endpoint;

use super::data_ops::{
    ReadChunkRequestHeader, ReadChunkResponseHeader, WriteChunkRequestHeader,
    WriteChunkResponseHeader, RPC_READ_CHUNK, RPC_WRITE_CHUNK,
};
use super::stream_client::StreamRpcClient;
use super::stream_rpc::{RpcPattern, StreamRpc};
use super::{RpcError, RpcId};
use crate::rpc::handlers::RpcHandlerContext;

// ============================================================================
// StreamReadChunk - Pattern 3: Client GET (server sends data to client)
// ============================================================================

/// Stream-based ReadChunk RPC request
///
/// Uses Pattern 3 (Client GET): Client registers buffer, server PUTs data to client.
pub struct StreamReadChunkRequest {
    header: ReadChunkRequestHeader,
    path: String,
    buffer_size: u64,
}

impl StreamReadChunkRequest {
    pub fn new(chunk_index: u64, offset: u64, length: u64, path: String) -> Self {
        let path_len = path.len() as u64;
        Self {
            header: ReadChunkRequestHeader::new(chunk_index, offset, length, path_len),
            path,
            buffer_size: length,
        }
    }

    /// Get the path
    pub fn path(&self) -> &str {
        &self.path
    }
}

impl StreamRpc for StreamReadChunkRequest {
    type RequestHeader = ReadChunkRequestHeader;
    type ResponseHeader = ReadChunkResponseHeader;

    fn rpc_id() -> RpcId {
        RPC_READ_CHUNK
    }

    fn pattern(&self) -> RpcPattern {
        RpcPattern::ClientGet {
            buffer_size: self.buffer_size,
        }
    }

    fn request_header(&self) -> &Self::RequestHeader {
        &self.header
    }

    fn path_bytes(&self) -> Vec<u8> {
        self.path.as_bytes().to_vec()
    }

    async fn call(
        &self,
        client: &StreamRpcClient,
    ) -> Result<Self::ResponseHeader, RpcError> {
        // Note: This method is not directly used for Client GET pattern
        // Instead, use execute_client_get which returns both header and data
        Err(RpcError::HandlerError(
            "Use execute_client_get for ReadChunk instead of call".to_string(),
        ))
    }

    async fn server_handler(
        _ctx: Rc<RpcHandlerContext>,
        _endpoint: &Endpoint,
        _header: Self::RequestHeader,
    ) -> Result<Self::ResponseHeader, RpcError> {
        // This method should not be called for ClientGet pattern
        // Use server_handler_get_data instead
        Err(RpcError::HandlerError(
            "Use server_handler_get_data for ReadChunk".to_string(),
        ))
    }

    async fn server_handler_get_data(
        ctx: Rc<RpcHandlerContext>,
        _endpoint: &Endpoint,
        header: Self::RequestHeader,
        path: &str,
        _buffer_size: u64,
    ) -> Result<(Self::ResponseHeader, Vec<u8>), RpcError> {
        let _span = tracing::trace_span!(
            "stream_rpc_read_chunk",
            chunk = header.chunk_index,
            offset = header.offset,
            len = header.length,
            path = path
        )
        .entered();

        tracing::debug!(
            "StreamReadChunk: path={}, chunk={}, offset={}, length={}",
            path,
            header.chunk_index,
            header.offset,
            header.length
        );

        // Read chunk from storage
        match ctx
            .chunk_store
            .read_chunk(path, header.chunk_index, header.offset, header.length)
            .await
        {
            Ok(data) => {
                let bytes_read = data.len() as u64;

                tracing::debug!(
                    "Read {} bytes from storage (path={}, chunk={})",
                    bytes_read,
                    path,
                    header.chunk_index
                );

                Ok((ReadChunkResponseHeader::success(bytes_read), data))
            }
            Err(e) => {
                tracing::error!("Failed to read chunk: {:?}", e);
                Ok((ReadChunkResponseHeader::error(-2), Vec::new())) // ENOENT
            }
        }
    }

    fn error_response(error: &RpcError) -> Self::ResponseHeader {
        let status = match error {
            RpcError::InvalidHeader => -1,
            RpcError::TransportError(_) => -2,
            RpcError::HandlerError(_) => -3,
            RpcError::ConnectionError(_) => -4,
            RpcError::Timeout => -5,
        };
        ReadChunkResponseHeader::error(status)
    }
}

// ============================================================================
// StreamWriteChunk - Pattern 2: Client PUT (client sends data to server)
// ============================================================================

/// Stream-based WriteChunk RPC request
///
/// Uses Pattern 2 (Client PUT): Client registers memory, server prepares buffer,
/// client PUTs data to server.
pub struct StreamWriteChunkRequest<'a> {
    header: WriteChunkRequestHeader,
    path: String,
    data: &'a [u8],
}

impl<'a> StreamWriteChunkRequest<'a> {
    pub fn new(chunk_index: u64, offset: u64, data: &'a [u8], path: String) -> Self {
        let length = data.len() as u64;
        let path_len = path.len() as u64;
        Self {
            header: WriteChunkRequestHeader::new(chunk_index, offset, length, path_len),
            path,
            data,
        }
    }

    /// Get the path
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Get the data
    pub fn data(&self) -> &[u8] {
        self.data
    }
}

impl StreamRpc for StreamWriteChunkRequest<'_> {
    type RequestHeader = WriteChunkRequestHeader;
    type ResponseHeader = WriteChunkResponseHeader;

    fn rpc_id() -> RpcId {
        RPC_WRITE_CHUNK
    }

    fn pattern(&self) -> RpcPattern {
        RpcPattern::ClientPut {
            data_size: self.data.len() as u64,
        }
    }

    fn request_header(&self) -> &Self::RequestHeader {
        &self.header
    }

    fn path_bytes(&self) -> Vec<u8> {
        self.path.as_bytes().to_vec()
    }

    async fn call(
        &self,
        client: &StreamRpcClient,
    ) -> Result<Self::ResponseHeader, RpcError> {
        // Note: This method is not directly used for Client PUT pattern
        // Instead, use execute_client_put which takes data parameter
        Err(RpcError::HandlerError(
            "Use execute_client_put for WriteChunk instead of call".to_string(),
        ))
    }

    async fn server_handler(
        _ctx: Rc<RpcHandlerContext>,
        _endpoint: &Endpoint,
        _header: Self::RequestHeader,
    ) -> Result<Self::ResponseHeader, RpcError> {
        // This method should not be called for ClientPut pattern
        // Use server_handler_with_data instead
        Err(RpcError::HandlerError(
            "Use server_handler_with_data for WriteChunk".to_string(),
        ))
    }

    async fn server_handler_with_data(
        ctx: Rc<RpcHandlerContext>,
        _endpoint: &Endpoint,
        header: Self::RequestHeader,
        path: &str,
        data: &[u8],
    ) -> Result<Self::ResponseHeader, RpcError> {
        let _span = tracing::trace_span!(
            "stream_rpc_write_chunk",
            chunk = header.chunk_index,
            offset = header.offset,
            len = header.length,
            path = path
        )
        .entered();

        tracing::debug!(
            "StreamWriteChunk: path={}, chunk={}, offset={}, length={}",
            path,
            header.chunk_index,
            header.offset,
            header.length
        );

        // Validate data length
        if data.len() != header.length as usize {
            tracing::error!(
                "Data length mismatch: expected {}, got {}",
                header.length,
                data.len()
            );
            return Ok(WriteChunkResponseHeader::error(-22)); // EINVAL
        }

        // Write chunk to storage
        match ctx
            .chunk_store
            .write_chunk(path, header.chunk_index, header.offset, data)
            .await
        {
            Ok(bytes_written) => {
                tracing::debug!(
                    "Wrote {} bytes to storage (path={}, chunk={})",
                    bytes_written,
                    path,
                    header.chunk_index
                );
                Ok(WriteChunkResponseHeader::success(bytes_written as u64))
            }
            Err(e) => {
                tracing::error!("Failed to write chunk: {:?}", e);
                Ok(WriteChunkResponseHeader::error(-5)) // EIO
            }
        }
    }

    fn error_response(error: &RpcError) -> Self::ResponseHeader {
        let status = match error {
            RpcError::InvalidHeader => -1,
            RpcError::TransportError(_) => -2,
            RpcError::HandlerError(_) => -3,
            RpcError::ConnectionError(_) => -4,
            RpcError::Timeout => -5,
        };
        WriteChunkResponseHeader::error(status)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stream_read_chunk_request() {
        let request = StreamReadChunkRequest::new(0, 0, 1024, "/test/file.txt".to_string());
        assert_eq!(request.header.chunk_index, 0);
        assert_eq!(request.header.offset, 0);
        assert_eq!(request.header.length, 1024);
        assert_eq!(request.header.path_len, 14); // "/test/file.txt".len()
        assert_eq!(request.path(), "/test/file.txt");
        assert_eq!(request.buffer_size, 1024);
    }

    #[test]
    fn test_stream_read_chunk_pattern() {
        let request = StreamReadChunkRequest::new(0, 0, 1024, "/test/file.txt".to_string());
        match request.pattern() {
            RpcPattern::ClientGet { buffer_size } => {
                assert_eq!(buffer_size, 1024);
            }
            _ => panic!("Expected ClientGet pattern"),
        }
    }

    #[test]
    fn test_stream_write_chunk_request() {
        let data = vec![0xAA; 512];
        let request = StreamWriteChunkRequest::new(1, 0, &data[..], "/test/file.txt".to_string());

        assert_eq!(request.header.chunk_index, 1);
        assert_eq!(request.header.offset, 0);
        assert_eq!(request.header.length, 512);
        assert_eq!(request.header.path_len, 14); // "/test/file.txt".len()
        assert_eq!(request.path(), "/test/file.txt");
        assert_eq!(request.data(), &data[..]);
    }

    #[test]
    fn test_stream_write_chunk_pattern() {
        let data = vec![0xAA; 512];
        let request = StreamWriteChunkRequest::new(1, 0, &data[..], "/test/file.txt".to_string());
        match request.pattern() {
            RpcPattern::ClientPut { data_size } => {
                assert_eq!(data_size, 512);
            }
            _ => panic!("Expected ClientPut pattern"),
        }
    }

    #[test]
    fn test_rpc_ids() {
        assert_eq!(StreamReadChunkRequest::rpc_id(), RPC_READ_CHUNK);
        assert_eq!(StreamWriteChunkRequest::rpc_id(), RPC_WRITE_CHUNK);
    }
}
