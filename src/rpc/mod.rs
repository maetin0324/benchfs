use std::rc::Rc;

use pluvio_ucx::{Worker, endpoint::Endpoint};
use pluvio_ucx::async_ucx::ucp::AmMsg;

use crate::rpc::client::RpcClient;

pub mod file_ops;
pub mod data_ops;
pub mod metadata_ops;
pub mod handlers;
pub mod server;
pub mod client;

/// RPC ID type for identifying different RPC operations
pub type RpcId = u16;

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

    async fn server_handler(
        am_msg: AmMsg,
    ) -> Result<Self::ResponseHeader, RpcError>;
}





/// RPC error types
#[derive(Debug)]
pub enum RpcError {
    InvalidHeader,
    TransportError(String),
    HandlerError(String),
    Timeout,
}

impl std::fmt::Display for RpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RpcError::InvalidHeader => write!(f, "Invalid RPC header"),
            RpcError::TransportError(msg) => write!(f, "Transport error: {}", msg),
            RpcError::HandlerError(msg) => write!(f, "Handler error: {}", msg),
            RpcError::Timeout => write!(f, "RPC timeout"),
        }
    }
}

impl std::error::Error for RpcError {}
