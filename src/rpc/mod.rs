use std::rc::Rc;
use std::collections::HashMap;
use std::cell::RefCell;

use pluvio_ucx::{endpoint::Endpoint, Worker};

pub mod file_ops;

/// RPC ID type for identifying different RPC operations
pub type RpcId = u16;

pub trait Serializable: zerocopy::FromBytes
        + zerocopy::IntoBytes 
        + zerocopy::KnownLayout
        + zerocopy::Immutable
        + std::fmt::Debug {}

impl<T> Serializable for T where T: zerocopy::FromBytes
        + zerocopy::IntoBytes
        + zerocopy::KnownLayout
        + zerocopy::Immutable
        + std::fmt::Debug {}

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

/// Fixed-size RPC header containing metadata like offset, length, etc.
/// This header is sent in the ActiveMessage header field.
#[repr(C)]
#[derive(Debug, Clone, Copy, zerocopy::FromBytes, zerocopy::IntoBytes, zerocopy::Immutable, zerocopy::KnownLayout)]
pub struct RpcHeader {
    /// File offset for I/O operations
    pub offset: u64,
    /// Length of data
    pub len: u64,
    /// Operation-specific flags
    pub flags: u32,
    /// Reserved for future use
    pub reserved: u32,
}

impl RpcHeader {
    pub fn new(offset: u64, len: u64, flags: u32) -> Self {
        Self {
            offset,
            len,
            flags,
            reserved: 0,
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        zerocopy::IntoBytes::as_bytes(self)
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<&Self> {
        zerocopy::FromBytes::ref_from_bytes(bytes).ok()
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
pub trait RpcCall {
    type RequestHeader: Serializable;

    type ResponseHeader: Serializable;

    /// Get the RPC ID for this operation
    fn rpc_id(&self) -> RpcId;

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
    fn proto(&self) -> Option<()> {
        None
    }

    async fn call(&self, client: &RpcClient) -> Result<Self::ResponseHeader, RpcError>;

}

/// Type alias for RPC handler function
pub type RpcHandlerFn = Box<dyn Fn(RpcHeader, Option<Vec<u8>>, Endpoint) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<u8>, RpcError>>>>>;

/// RPC server that receives and dispatches ActiveMessages
pub struct RpcServer {
    worker: Rc<Worker>,
    handlers: RefCell<HashMap<RpcId, RpcHandlerFn>>,
}

impl RpcServer {
    pub fn new(worker: Rc<Worker>) -> Self {
        Self {
            worker,
            handlers: RefCell::new(HashMap::new()),
        }
    }

    /// Register an RPC handler for a specific RPC ID
    pub fn register_handler(&self, rpc_id: RpcId, handler: RpcHandlerFn) {
        self.handlers.borrow_mut().insert(rpc_id, handler);
    }

    /// Start listening for RPC requests on the given AM stream ID
    pub async fn listen(&self, am_id: u16) -> Result<(), RpcError> {
        let stream = self.worker
            .am_stream(am_id)
            .map_err(|e| RpcError::TransportError(format!("Failed to create AM stream: {:?}", e)))?;

        loop {
            let msg = stream.wait_msg().await;
            if msg.is_none() {
                break;
            }

            let mut msg = msg.unwrap();
            let header_bytes = msg.header();
            
            let _header = RpcHeader::from_bytes(header_bytes)
                .ok_or_else(|| RpcError::InvalidHeader)?;

            let rpc_id = msg.id();

            // Receive data if present
            let _data = if msg.contains_data() {
                Some(msg.recv_data().await
                    .map_err(|e| RpcError::TransportError(format!("Failed to recv data: {:?}", e)))?)
            } else {
                None
            };

            // Dispatch to handler
            // Note: We need to get the endpoint from somewhere - for now skip this part
            // In practice, you'd maintain endpoint information per connection
            let handlers = self.handlers.borrow();
            if let Some(_handler) = handlers.get(&rpc_id) {
                // TODO: Call handler and send reply
                // This requires restructuring to avoid lifetime issues
                eprintln!("Handler execution needs to be implemented with proper async support");
            } else {
                eprintln!("No handler registered for RPC ID: {}", rpc_id);
            }
        }

        Ok(())
    }
}

/// RPC client for making RPC calls
/// 
/// The client executes RPCs by taking any type that implements the `RpcCall` trait.
pub struct RpcClient {
    conn: Connection,
    // Store reply stream opaquely since pluvio_ucx may not export AmStream
    #[allow(dead_code)]
    reply_stream_id: RefCell<Option<u16>>,
}

impl RpcClient {
    pub fn new(conn: Connection) -> Self {
        Self { 
            conn,
            reply_stream_id: RefCell::new(None),
        }
    }

    /// Initialize the reply stream for receiving RPC responses
    /// This should be called before executing any RPCs that expect replies
    /// 
    /// Note: Currently simplified - full implementation would need pluvio_ucx
    /// to export AmStream or provide a different API
    pub fn init_reply_stream(&self, am_id: u16) -> Result<(), RpcError> {
        // Store the AM ID for future use
        *self.reply_stream_id.borrow_mut() = Some(am_id);
        Ok(())
    }

    /// Execute an RPC call using a request that implements RpcCall
    /// 
    /// This is the main entry point for executing RPCs. Pass any struct that implements
    /// RpcCall and it will be sent to the server, with the response being returned.
    /// 
    /// # Example
    /// ```ignore
    /// let request = ReadRequest { offset: 0, len: 4096 };
    /// let response: ReadResponse = client.execute(&request).await?;
    /// ```
    pub async fn execute<T: RpcCall>(&self, request: &T) -> Result<T::ResponseHeader, RpcError> {
        let rpc_id = request.rpc_id();
        let header = request.request_header();
        let data = request.request_data();
        let need_reply = request.need_reply();
        let _proto = request.proto();  // TODO: Use when pluvio_ucx exports AmProto

        let reply_stream = self.conn.worker.am_stream(rpc_id)
            .map_err(|e| RpcError::TransportError(format!("Failed to create reply AM stream: {:?}", e.to_string())))?;

        if !need_reply {
            // No reply expected
            return Err(RpcError::HandlerError("No reply expected for this RPC".to_string()));
        }

        // Send the RPC request (proto is set to None for now)
        self.conn.endpoint()
            .am_send_vectorized(
                rpc_id as u32,
                zerocopy::IntoBytes::as_bytes(header),
                &data,
                need_reply,
                None,  // proto - TODO: pass actual proto when available
            )
            .await
            .map_err(|e| RpcError::TransportError(format!("Failed to send AM: {:?}", e)))?;

        // Wait for reply
        // TODO: Implement proper reply reception
        // This requires either:
        // 1. pluvio_ucx to export AmStream publicly
        // 2. A different API design where reply handling is done separately
        // 3. Using a callback-based approach

        let mut msg = reply_stream.wait_msg().await
            .ok_or_else(|| RpcError::Timeout)?;

        let response_header = msg.header()
            .get(..std::mem::size_of::<T::ResponseHeader>())
            .and_then(|bytes| zerocopy::FromBytes::read_from_bytes(bytes).ok())
            .ok_or_else(|| RpcError::InvalidHeader)?;

        let response_buffer = request.response_buffer();
        if !response_buffer.is_empty() && msg.contains_data() {
            msg.recv_data_vectored(response_buffer)
                .await
                .map_err(|e| RpcError::TransportError(format!("Failed to recv response data: {:?}", e)))?;
        }

        Ok(response_header)
    }

    /// Execute an RPC without expecting a reply
    /// Useful for fire-and-forget operations
    pub async fn execute_no_reply<T: RpcCall>(&self, request: &T) -> Result<(), RpcError> {
        let rpc_id = request.rpc_id();
        let header = request.request_header();
        let data = request.request_data();
        let _proto = request.proto();  // TODO: Use when pluvio_ucx exports AmProto

        self.conn.endpoint()
            .am_send_vectorized(
                rpc_id as u32,
                zerocopy::IntoBytes::as_bytes(header),
                &data,
                false,  // need_reply = false
                None,   // proto - TODO: pass actual proto when available
            )
            .await
            .map_err(|e| RpcError::TransportError(format!("Failed to send AM: {:?}", e)))?;

        Ok(())
    }
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