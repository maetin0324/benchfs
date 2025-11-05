use std::rc::Rc;

use pluvio_ucx::async_ucx::ucp::AmMsg;
use pluvio_ucx::{Worker, endpoint::Endpoint};

use crate::rpc::client::RpcClient;
use crate::rpc::handlers::RpcHandlerContext;

pub mod address_registry;
pub mod bench_ops;
pub mod client;
pub mod connection;
pub mod data_ops;
pub mod handlers;
pub mod helpers;
pub mod metadata_ops;
pub mod server;

/// RPC ID type for identifying different RPC operations
pub type RpcId = u16;

// Re-export commonly used constants from the constants module
pub use crate::constants::{should_use_rdma, RDMA_THRESHOLD, SHUTDOWN_MAGIC, WORKER_ADDRESS_BUFFER_SIZE};

/// Unified server response structure
/// Contains response header and optional data payload
pub struct ServerResponse<H> {
    pub header: H,
    pub data: Option<Vec<u8>>,
}

impl<H> ServerResponse<H> {
    pub fn new(header: H) -> Self {
        Self { header, data: None }
    }

    pub fn with_data(header: H, data: Vec<u8>) -> Self {
        Self {
            header,
            data: Some(data),
        }
    }
}

pub trait Serializable:
    zerocopy::FromBytes
    + zerocopy::IntoBytes
    + zerocopy::KnownLayout
    + zerocopy::Immutable
    + std::fmt::Debug
{
}

impl<T> Serializable for T where
    T: zerocopy::FromBytes
        + zerocopy::IntoBytes
        + zerocopy::KnownLayout
        + zerocopy::Immutable
        + std::fmt::Debug
{
}

/// Connection wrapper for UCX endpoint with worker
pub struct Connection {
    worker: Rc<Worker>,
    endpoint: Endpoint,
}

impl Connection {
    pub fn new(worker: Rc<Worker>, endpoint: Endpoint) -> Self {
        Self { worker, endpoint }
    }

    pub fn worker(&self) -> &Rc<Worker> {
        &self.worker
    }

    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }
}

pub enum AmRpcCallType {
    Put,
    Get,
    PutGet,
    None,
}

/// RPC call trait for client-side operations
///
/// Implement this trait on your request structures to make them executable as RPCs.
///
/// # Example
/// ```ignore
/// struct ReadRequest {
///     offset: u64,
///     len: u64,
/// }
///
/// #[repr(C)]
/// #[derive(zerocopy::FromBytes, zerocopy::IntoBytes, Debug)]
/// struct ReadResponse {
///     bytes_read: u64,
/// }
///
/// impl RpcCall for ReadRequest {
///     type Response = ReadResponse;
///     
///     fn rpc_id(&self) -> RpcId {
///         1 // RPC_READ
///     }
///     
///     fn header(&self) -> RpcHeader {
///         RpcHeader::new(self.offset, self.len, 0)
///     }
/// }
///
/// // Execute the RPC
/// let request = ReadRequest { offset: 0, len: 4096 };
/// let response = client.execute(&request).await?;
/// ```
#[allow(async_fn_in_trait)]
pub trait AmRpc {
    type RequestHeader: Serializable;

    type ResponseHeader: Serializable;

    /// Get the RPC ID for this operation
    fn rpc_id() -> RpcId;

    /// Get the reply stream ID for this operation
    /// By default, this is rpc_id() + 100 to avoid conflicts with request stream
    fn reply_stream_id() -> u16 {
        Self::rpc_id() + 100
    }

    /// Get the RPC call type
    /// Override this method if your RPC uses RDMA operations
    fn call_type(&self) -> AmRpcCallType {
        AmRpcCallType::None
    }

    /// Get the RPC request header
    fn request_header(&self) -> &Self::RequestHeader;

    /// Get the request data payload (if any) as IoSlice for zero-copy
    /// Override this method if your RPC needs to send additional data beyond the header
    fn request_data(&self) -> &[std::io::IoSlice<'_>] {
        &[]
    }

    /// Get the response header
    // fn response_header(&self) -> Self::ResponseHeader;

    /// Get the response data payload (if any) as IoSliceMut for zero-copy
    /// response data may be rdma-writed by the server
    fn response_buffer(&self) -> &[std::io::IoSliceMut<'_>] {
        &[]
    }

    /// Whether this RPC expects a reply
    /// Override to return false for fire-and-forget operations
    fn need_reply(&self) -> bool {
        true
    }

    /// Optional: Customize the AM protocol (Eager or Rendezvous)
    /// Note: This requires pluvio_ucx to re-export AmProto
    fn proto(&self) -> Option<pluvio_ucx::async_ucx::ucp::AmProto> {
        None
    }

    async fn call(&self, client: &RpcClient) -> Result<Self::ResponseHeader, RpcError>;

    async fn call_no_reply(&self, client: &RpcClient) -> Result<(), RpcError>;

    /// Server-side handler for this RPC
    ///
    /// This is called by RpcServer::listen() when a request is received.
    /// Returns (ServerResponse, AmMsg) on success, or (RpcError, AmMsg) on failure.
    /// The AmMsg must be returned so the server can send a reply.
    async fn server_handler(
        ctx: Rc<RpcHandlerContext>,
        am_msg: AmMsg,
    ) -> Result<(ServerResponse<Self::ResponseHeader>, AmMsg), (RpcError, AmMsg)>;

    /// Create an error response from an RpcError
    /// This allows the server to send proper error responses to clients
    fn error_response(error: &RpcError) -> Self::ResponseHeader;
}

/// RPC error types
#[derive(Debug)]
pub enum RpcError {
    InvalidHeader,
    TransportError(String),
    HandlerError(String),
    ConnectionError(String),
    Timeout,
}

impl std::fmt::Display for RpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RpcError::InvalidHeader => write!(f, "Invalid RPC header"),
            RpcError::TransportError(msg) => write!(f, "Transport error: {}", msg),
            RpcError::HandlerError(msg) => write!(f, "Handler error: {}", msg),
            RpcError::ConnectionError(msg) => write!(f, "Connection error: {}", msg),
            RpcError::Timeout => write!(f, "RPC timeout"),
        }
    }
}

impl std::error::Error for RpcError {}
