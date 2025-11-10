//! Stream + RMA based RPC framework
//!
//! This module provides a new RPC framework using UCX Stream for control messages
//! and RMA (Remote Memory Access) for data transfer, replacing the unstable
//! Active Message (AM) based approach.
//!
//! # Architecture
//!
//! - **Stream**: Used for control messages (headers, completion notifications)
//! - **RMA**: Used for high-performance data transfer (PUT/GET operations)
//!
//! # RPC Patterns
//!
//! 1. **Pattern 1: No RMA** - Header-only RPCs (Ping, Stat, Mkdir)
//!    - Client sends request header via stream
//!    - Server sends response header via stream
//!
//! 2. **Pattern 2: Client PUT** - Client sends data to server (Write)
//!    - Client registers memory and sends header + rkey
//!    - Server prepares buffer and sends address
//!    - Client PUTs data to server
//!    - Client sends completion notification
//!    - Server sends response
//!
//! 3. **Pattern 3: Client GET** - Server sends data to client (Read)
//!    - Client registers buffer and sends header + rkey
//!    - Server reads data and PUTs to client buffer
//!    - Server sends data size and completion notification
//!
//! 4. **Pattern 4: Bidirectional** - Both directions (future extension)

use std::rc::Rc;

use pluvio_ucx::endpoint::Endpoint;

use super::{RpcError, RpcId, Serializable};
use crate::rpc::handlers::RpcHandlerContext;

/// RPC pattern types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RpcPattern {
    /// Pattern 1: Header-only communication (no data transfer)
    NoRma,

    /// Pattern 2: Client PUT - client sends data to server
    /// Used for Write operations
    ClientPut {
        /// Expected data size in bytes
        data_size: u64,
    },

    /// Pattern 3: Client GET - server sends data to client
    /// Used for Read operations
    ClientGet {
        /// Expected buffer size in bytes
        buffer_size: u64,
    },

    /// Pattern 4: Bidirectional - both PUT operations
    /// Reserved for future use
    #[allow(dead_code)]
    ClientPutServerPut {
        /// Request data size
        request_size: u64,
        /// Response data size
        response_size: u64,
    },
}

/// Stream-based RPC trait
///
/// Implement this trait to define a new RPC operation using Stream + RMA.
///
/// # Example
///
/// ```ignore
/// pub struct PingRequest {
///     header: PingRequestHeader,
/// }
///
/// impl StreamRpc for PingRequest {
///     type RequestHeader = PingRequestHeader;
///     type ResponseHeader = PingResponseHeader;
///
///     fn rpc_id() -> RpcId {
///         RPC_PING
///     }
///
///     fn pattern(&self) -> RpcPattern {
///         RpcPattern::NoRma
///     }
///
///     fn request_header(&self) -> &Self::RequestHeader {
///         &self.header
///     }
///
///     async fn call(&self, client: &StreamRpcClient) -> Result<Self::ResponseHeader, RpcError> {
///         client.execute_no_rma(self).await
///     }
///
///     async fn server_handler(
///         ctx: Rc<RpcHandlerContext>,
///         endpoint: &Endpoint,
///         header: Self::RequestHeader,
///     ) -> Result<Self::ResponseHeader, RpcError> {
///         // Process the request and return response
///         Ok(PingResponseHeader::success())
///     }
/// }
/// ```
#[allow(async_fn_in_trait)]
pub trait StreamRpc {
    /// Request header type (must be serializable)
    type RequestHeader: Serializable;

    /// Response header type (must be serializable)
    type ResponseHeader: Serializable;

    /// Get the RPC ID for this operation
    fn rpc_id() -> RpcId;

    /// Get the RPC pattern for this operation
    fn pattern(&self) -> RpcPattern;

    /// Get the request header
    fn request_header(&self) -> &Self::RequestHeader;

    /// Get the path bytes for this RPC
    ///
    /// Override this method if your RPC needs to send path information.
    /// Returns empty vector by default.
    fn path_bytes(&self) -> Vec<u8> {
        Vec::new()
    }

    /// Client-side RPC execution
    ///
    /// This method should call the appropriate execute method on the client
    /// based on the pattern type.
    async fn call(
        &self,
        client: &crate::rpc::stream_client::StreamRpcClient,
    ) -> Result<Self::ResponseHeader, RpcError>;

    /// Server-side RPC handler
    ///
    /// Process the RPC request and return the response.
    /// This is called by the server after receiving the request header.
    async fn server_handler(
        ctx: Rc<RpcHandlerContext>,
        endpoint: &Endpoint,
        header: Self::RequestHeader,
    ) -> Result<Self::ResponseHeader, RpcError>;

    /// Server-side RPC handler with path information
    ///
    /// Process the RPC request and return the response.
    /// This is called by the server after receiving the request header and path.
    /// Default implementation calls the regular server_handler, ignoring the path.
    async fn server_handler_with_path(
        ctx: Rc<RpcHandlerContext>,
        endpoint: &Endpoint,
        header: Self::RequestHeader,
        path: &str,
    ) -> Result<Self::ResponseHeader, RpcError> {
        Self::server_handler(ctx, endpoint, header).await
    }

    /// Server-side RPC handler with path and data
    ///
    /// Process the RPC request with received data and return the response.
    /// This is called by the server for Client PUT pattern.
    /// Default implementation calls the regular server_handler_with_path, ignoring the data.
    async fn server_handler_with_data(
        ctx: Rc<RpcHandlerContext>,
        endpoint: &Endpoint,
        header: Self::RequestHeader,
        path: &str,
        data: &[u8],
    ) -> Result<Self::ResponseHeader, RpcError> {
        Self::server_handler_with_path(ctx, endpoint, header, path).await
    }

    /// Server-side RPC handler that returns data
    ///
    /// Process the RPC request and return both the response and data.
    /// This is called by the server for Client GET pattern.
    /// Default implementation returns empty data.
    async fn server_handler_get_data(
        ctx: Rc<RpcHandlerContext>,
        endpoint: &Endpoint,
        header: Self::RequestHeader,
        path: &str,
        buffer_size: u64,
    ) -> Result<(Self::ResponseHeader, Vec<u8>), RpcError> {
        let response = Self::server_handler_with_path(ctx, endpoint, header, path).await?;
        Ok((response, Vec::new()))
    }

    /// Create an error response from RpcError
    ///
    /// This allows the server to send proper error responses to clients.
    fn error_response(error: &RpcError) -> Self::ResponseHeader;
}

/// Request message for Client PUT pattern
///
/// Contains the request header, path, memory location info, and rkey for RMA.
/// Note: RPC ID is sent separately before this message.
#[derive(Debug, Clone)]
pub struct ClientPutRequestMessage {
    /// Request header bytes
    pub header_bytes: Vec<u8>,

    /// Path bytes (for file operations)
    pub path_bytes: Vec<u8>,

    /// Client data address (virtual address)
    pub data_addr: u64,

    /// Data size in bytes
    pub data_size: u64,

    /// Packed rkey for RMA access
    pub rkey: Vec<u8>,
}

impl ClientPutRequestMessage {
    /// Serialize to bytes for stream transmission
    /// Note: RPC ID is sent separately before this message
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        // Header length (4 bytes)
        bytes.extend_from_slice(&(self.header_bytes.len() as u32).to_le_bytes());

        // Header bytes
        bytes.extend_from_slice(&self.header_bytes);

        // Path length (4 bytes)
        bytes.extend_from_slice(&(self.path_bytes.len() as u32).to_le_bytes());

        // Path bytes
        bytes.extend_from_slice(&self.path_bytes);

        // Data address (8 bytes)
        bytes.extend_from_slice(&self.data_addr.to_le_bytes());

        // Data size (8 bytes)
        bytes.extend_from_slice(&self.data_size.to_le_bytes());

        // RKey length (4 bytes)
        bytes.extend_from_slice(&(self.rkey.len() as u32).to_le_bytes());

        // RKey bytes
        bytes.extend_from_slice(&self.rkey);

        bytes
    }

    /// Deserialize from bytes
    /// Note: RPC ID is received separately before this message
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, RpcError> {
        if bytes.len() < 4 {
            return Err(RpcError::TransportError(
                "Message too short for ClientPutRequestMessage".to_string(),
            ));
        }

        let mut offset = 0;

        // Header length
        let header_len = u32::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]) as usize;
        offset += 4;

        if bytes.len() < offset + header_len + 4 {
            return Err(RpcError::TransportError(
                "Message too short for header and path length".to_string(),
            ));
        }

        // Header bytes
        let header_bytes = bytes[offset..offset + header_len].to_vec();
        offset += header_len;

        // Path length
        let path_len = u32::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]) as usize;
        offset += 4;

        if bytes.len() < offset + path_len + 8 + 8 + 4 {
            return Err(RpcError::TransportError(
                "Message too short for path and metadata".to_string(),
            ));
        }

        // Path bytes
        let path_bytes = bytes[offset..offset + path_len].to_vec();
        offset += path_len;

        // Data address
        let data_addr = u64::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
            bytes[offset + 4],
            bytes[offset + 5],
            bytes[offset + 6],
            bytes[offset + 7],
        ]);
        offset += 8;

        // Data size
        let data_size = u64::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
            bytes[offset + 4],
            bytes[offset + 5],
            bytes[offset + 6],
            bytes[offset + 7],
        ]);
        offset += 8;

        // RKey length
        let rkey_len = u32::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]) as usize;
        offset += 4;

        if bytes.len() < offset + rkey_len {
            return Err(RpcError::TransportError(
                "Message too short for rkey".to_string(),
            ));
        }

        // RKey bytes
        let rkey = bytes[offset..offset + rkey_len].to_vec();

        Ok(Self {
            header_bytes,
            path_bytes,
            data_addr,
            data_size,
            rkey,
        })
    }
}

/// Request message for Client GET pattern
///
/// Contains the request header, path, buffer location info, and rkey for RMA.
/// Note: RPC ID is sent separately before this message.
#[derive(Debug, Clone)]
pub struct ClientGetRequestMessage {
    /// Request header bytes
    pub header_bytes: Vec<u8>,

    /// Path bytes (for file operations)
    pub path_bytes: Vec<u8>,

    /// Client buffer address (virtual address)
    pub buffer_addr: u64,

    /// Buffer size in bytes
    pub buffer_size: u64,

    /// Packed rkey for RMA access
    pub rkey: Vec<u8>,
}

impl ClientGetRequestMessage {
    /// Serialize to bytes for stream transmission
    /// Note: RPC ID is sent separately before this message
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        // Header length (4 bytes)
        bytes.extend_from_slice(&(self.header_bytes.len() as u32).to_le_bytes());

        // Header bytes
        bytes.extend_from_slice(&self.header_bytes);

        // Path length (4 bytes)
        bytes.extend_from_slice(&(self.path_bytes.len() as u32).to_le_bytes());

        // Path bytes
        bytes.extend_from_slice(&self.path_bytes);

        // Buffer address (8 bytes)
        bytes.extend_from_slice(&self.buffer_addr.to_le_bytes());

        // Buffer size (8 bytes)
        bytes.extend_from_slice(&self.buffer_size.to_le_bytes());

        // RKey length (4 bytes)
        bytes.extend_from_slice(&(self.rkey.len() as u32).to_le_bytes());

        // RKey bytes
        bytes.extend_from_slice(&self.rkey);

        bytes
    }

    /// Deserialize from bytes
    /// Note: RPC ID is received separately before this message
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, RpcError> {
        if bytes.len() < 4 {
            return Err(RpcError::TransportError(
                "Message too short for ClientGetRequestMessage".to_string(),
            ));
        }

        let mut offset = 0;

        // Header length
        let header_len = u32::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]) as usize;
        offset += 4;

        if bytes.len() < offset + header_len + 4 {
            return Err(RpcError::TransportError(
                "Message too short for header and path length".to_string(),
            ));
        }

        // Header bytes
        let header_bytes = bytes[offset..offset + header_len].to_vec();
        offset += header_len;

        // Path length
        let path_len = u32::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]) as usize;
        offset += 4;

        if bytes.len() < offset + path_len + 8 + 8 + 4 {
            return Err(RpcError::TransportError(
                "Message too short for path and metadata".to_string(),
            ));
        }

        // Path bytes
        let path_bytes = bytes[offset..offset + path_len].to_vec();
        offset += path_len;

        // Buffer address
        let buffer_addr = u64::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
            bytes[offset + 4],
            bytes[offset + 5],
            bytes[offset + 6],
            bytes[offset + 7],
        ]);
        offset += 8;

        // Buffer size
        let buffer_size = u64::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
            bytes[offset + 4],
            bytes[offset + 5],
            bytes[offset + 6],
            bytes[offset + 7],
        ]);
        offset += 8;

        // RKey length
        let rkey_len = u32::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]) as usize;
        offset += 4;

        if bytes.len() < offset + rkey_len {
            return Err(RpcError::TransportError(
                "Message too short for rkey".to_string(),
            ));
        }

        // RKey bytes
        let rkey = bytes[offset..offset + rkey_len].to_vec();

        Ok(Self {
            header_bytes,
            path_bytes,
            buffer_addr,
            buffer_size,
            rkey,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_put_request_message_serialization() {
        let msg = ClientPutRequestMessage {
            header_bytes: vec![1, 2, 3, 4],
            path_bytes: b"/test/file.txt".to_vec(),
            data_addr: 0x1000,
            data_size: 4096,
            rkey: vec![0xAA, 0xBB, 0xCC],
        };

        let bytes = msg.to_bytes();
        let deserialized = ClientPutRequestMessage::from_bytes(&bytes).unwrap();

        assert_eq!(deserialized.header_bytes, vec![1, 2, 3, 4]);
        assert_eq!(deserialized.path_bytes, b"/test/file.txt".to_vec());
        assert_eq!(deserialized.data_addr, 0x1000);
        assert_eq!(deserialized.data_size, 4096);
        assert_eq!(deserialized.rkey, vec![0xAA, 0xBB, 0xCC]);
    }

    #[test]
    fn test_client_get_request_message_serialization() {
        let msg = ClientGetRequestMessage {
            header_bytes: vec![5, 6, 7, 8],
            path_bytes: b"/test/read.txt".to_vec(),
            buffer_addr: 0x2000,
            buffer_size: 8192,
            rkey: vec![0xDD, 0xEE, 0xFF],
        };

        let bytes = msg.to_bytes();
        let deserialized = ClientGetRequestMessage::from_bytes(&bytes).unwrap();

        assert_eq!(deserialized.header_bytes, vec![5, 6, 7, 8]);
        assert_eq!(deserialized.path_bytes, b"/test/read.txt".to_vec());
        assert_eq!(deserialized.buffer_addr, 0x2000);
        assert_eq!(deserialized.buffer_size, 8192);
        assert_eq!(deserialized.rkey, vec![0xDD, 0xEE, 0xFF]);
    }
}
