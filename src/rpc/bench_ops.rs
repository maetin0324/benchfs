use std::io::IoSlice;
use std::rc::Rc;

use pluvio_ucx::async_ucx::ucp::AmMsg;

use crate::rpc::helpers::{parse_header, send_rpc_response_via_reply};
use crate::rpc::{AmRpc, AmRpcCallType, RpcClient, RpcError, RpcId, SHUTDOWN_MAGIC};

/// RPC IDs for benchmark operations
pub const RPC_BENCH_PING: RpcId = 30;
pub const RPC_BENCH_THROUGHPUT: RpcId = 31;
pub const RPC_BENCH_SHUTDOWN: RpcId = 32;

// ============================================================================
// Ping-Pong RPC (for latency measurement)
// ============================================================================

/// Ping request header
#[repr(C)]
#[derive(
    Debug,
    Clone,
    Copy,
    zerocopy::FromBytes,
    zerocopy::IntoBytes,
    zerocopy::KnownLayout,
    zerocopy::Immutable,
)]
pub struct BenchPingRequestHeader {
    /// Sequence number for matching requests/responses
    pub sequence_number: u64,

    /// Client timestamp (nanoseconds)
    pub client_timestamp_ns: u64,
}

impl BenchPingRequestHeader {
    pub fn new(sequence_number: u64, timestamp_ns: u64) -> Self {
        Self {
            sequence_number,
            client_timestamp_ns: timestamp_ns,
        }
    }
}

/// Ping response header
#[repr(C)]
#[derive(
    Debug,
    Clone,
    Copy,
    zerocopy::FromBytes,
    zerocopy::IntoBytes,
    zerocopy::KnownLayout,
    zerocopy::Immutable,
)]
pub struct BenchPingResponseHeader {
    /// Sequence number (echoed from request)
    pub sequence_number: u64,

    /// Server timestamp when request was received (nanoseconds)
    pub server_timestamp_ns: u64,
}

impl BenchPingResponseHeader {
    pub fn new(sequence_number: u64, server_timestamp_ns: u64) -> Self {
        Self {
            sequence_number,
            server_timestamp_ns,
        }
    }
}

/// Ping-Pong RPC request
pub struct BenchPingRequest {
    header: BenchPingRequestHeader,
}

impl BenchPingRequest {
    pub fn new(sequence_number: u64, timestamp_ns: u64) -> Self {
        Self {
            header: BenchPingRequestHeader::new(sequence_number, timestamp_ns),
        }
    }
}

impl AmRpc for BenchPingRequest {
    type RequestHeader = BenchPingRequestHeader;
    type ResponseHeader = BenchPingResponseHeader;

    fn rpc_id() -> RpcId {
        RPC_BENCH_PING
    }

    fn call_type(&self) -> AmRpcCallType {
        AmRpcCallType::None
    }

    fn request_header(&self) -> &Self::RequestHeader {
        &self.header
    }

    fn request_data(&self) -> &[IoSlice<'_>] {
        &[] // No data payload - reply via reply_ep
    }

    #[async_backtrace::framed]
    async fn call(&self, client: &RpcClient) -> Result<Self::ResponseHeader, RpcError> {
        client.execute(self).await
    }

    #[async_backtrace::framed]
    async fn call_no_reply(&self, _client: &RpcClient) -> Result<(), RpcError> {
        Err(RpcError::HandlerError(
            "BenchPing requires a reply".to_string(),
        ))
    }

    #[async_backtrace::framed]
    async fn server_handler(
        _ctx: Rc<crate::rpc::handlers::RpcHandlerContext>,
        am_msg: AmMsg,
    ) -> Result<(crate::rpc::ServerResponse<Self::ResponseHeader>, AmMsg), (RpcError, AmMsg)> {
        // Parse request header
        let header: BenchPingRequestHeader = match parse_header(&am_msg) {
            Ok(h) => h,
            Err(e) => return Err((e, am_msg)),
        };

        // Get server timestamp
        let server_timestamp_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;

        let response_header =
            BenchPingResponseHeader::new(header.sequence_number, server_timestamp_ns);

        // Send response using reply_ep
        send_rpc_response_via_reply(Self::reply_stream_id(), &response_header, None, am_msg).await
    }

    fn error_response(error: &RpcError) -> Self::ResponseHeader {
        let _ = error;
        BenchPingResponseHeader::new(0, 0)
    }
}

// ============================================================================
// Shutdown RPC (for graceful server termination)
// ============================================================================

/// Shutdown request header
#[repr(C)]
#[derive(
    Debug,
    Clone,
    Copy,
    zerocopy::FromBytes,
    zerocopy::IntoBytes,
    zerocopy::KnownLayout,
    zerocopy::Immutable,
)]
pub struct BenchShutdownRequestHeader {
    /// Magic number to verify request integrity
    pub magic: u64,
}

impl BenchShutdownRequestHeader {
    pub fn new() -> Self {
        Self {
            magic: SHUTDOWN_MAGIC,
        }
    }
}

/// Shutdown response header
#[repr(C)]
#[derive(
    Debug,
    Clone,
    Copy,
    zerocopy::FromBytes,
    zerocopy::IntoBytes,
    zerocopy::KnownLayout,
    zerocopy::Immutable,
)]
pub struct BenchShutdownResponseHeader {
    /// 1 if shutdown accepted, 0 otherwise
    pub success: u64,
}

impl BenchShutdownResponseHeader {
    pub fn new(success: bool) -> Self {
        Self {
            success: if success { 1 } else { 0 },
        }
    }
}

/// Shutdown RPC request
pub struct BenchShutdownRequest {
    header: BenchShutdownRequestHeader,
}

impl BenchShutdownRequest {
    pub fn new() -> Self {
        Self {
            header: BenchShutdownRequestHeader::new(),
        }
    }
}

impl AmRpc for BenchShutdownRequest {
    type RequestHeader = BenchShutdownRequestHeader;
    type ResponseHeader = BenchShutdownResponseHeader;

    fn rpc_id() -> RpcId {
        RPC_BENCH_SHUTDOWN
    }

    fn call_type(&self) -> AmRpcCallType {
        AmRpcCallType::None
    }

    fn request_header(&self) -> &Self::RequestHeader {
        &self.header
    }

    fn request_data(&self) -> &[IoSlice<'_>] {
        &[] // No data payload - reply via reply_ep
    }

    #[async_backtrace::framed]
    async fn call(&self, client: &RpcClient) -> Result<Self::ResponseHeader, RpcError> {
        client.execute(self).await
    }

    #[async_backtrace::framed]
    async fn call_no_reply(&self, _client: &RpcClient) -> Result<(), RpcError> {
        Err(RpcError::HandlerError(
            "BenchShutdown requires a reply".to_string(),
        ))
    }

    #[async_backtrace::framed]
    async fn server_handler(
        ctx: Rc<crate::rpc::handlers::RpcHandlerContext>,
        am_msg: AmMsg,
    ) -> Result<(crate::rpc::ServerResponse<Self::ResponseHeader>, AmMsg), (RpcError, AmMsg)> {
        // Parse request header
        let header: BenchShutdownRequestHeader = match parse_header(&am_msg) {
            Ok(h) => h,
            Err(e) => return Err((e, am_msg)),
        };

        // Verify magic number
        if header.magic != SHUTDOWN_MAGIC {
            tracing::warn!("Invalid shutdown magic: {:#x}", header.magic);
            return Err((RpcError::InvalidHeader, am_msg));
        }

        tracing::info!("Received shutdown request");

        // Set shutdown flag
        ctx.set_shutdown_flag();

        let response_header = BenchShutdownResponseHeader::new(true);

        // Send response using reply_ep
        send_rpc_response_via_reply(Self::reply_stream_id(), &response_header, None, am_msg).await
    }

    fn error_response(error: &RpcError) -> Self::ResponseHeader {
        let _ = error;
        BenchShutdownResponseHeader::new(false)
    }
}
