use std::cell::UnsafeCell;
use std::io::IoSlice;
use std::rc::Rc;

use pluvio_ucx::async_ucx::ucp::AmMsg;
use zerocopy::FromBytes;

use crate::rpc::{AmRpc, AmRpcCallType, RpcClient, RpcError, RpcId};

/// RPC IDs for benchmark operations
pub const RPC_BENCH_PING: RpcId = 30;
pub const RPC_BENCH_THROUGHPUT: RpcId = 31;

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
    /// Client's WorkerAddress for direct response
    worker_address: Vec<u8>,
    /// IoSlice for worker_address
    request_ioslice: UnsafeCell<IoSlice<'static>>,
}

// SAFETY: BenchPingRequest is only used in single-threaded context (Pluvio runtime)
unsafe impl Send for BenchPingRequest {}

impl BenchPingRequest {
    pub fn new(sequence_number: u64, timestamp_ns: u64, worker_address: Vec<u8>) -> Self {
        // SAFETY: We're creating 'static IoSlice by transmuting the lifetime.
        // This is safe because:
        // 1. The buffer lives as long as the BenchPingRequest
        // 2. The IoSlice is only accessed through request_data()
        // 3. The RPC client will only use it during the RPC call
        let ioslice = unsafe {
            let addr_slice: &'static [u8] = std::mem::transmute(worker_address.as_slice());
            IoSlice::new(addr_slice)
        };

        Self {
            header: BenchPingRequestHeader::new(sequence_number, timestamp_ns),
            worker_address,
            request_ioslice: UnsafeCell::new(ioslice),
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
        // SAFETY: We're returning a slice containing the IoSlice we created in new()
        // This is safe because the IoSlice lifetime is tied to self
        unsafe { std::slice::from_ref(&*self.request_ioslice.get()) }
    }

    async fn call(&self, client: &RpcClient) -> Result<Self::ResponseHeader, RpcError> {
        client.execute(self).await
    }

    async fn call_no_reply(&self, _client: &RpcClient) -> Result<(), RpcError> {
        Err(RpcError::HandlerError(
            "BenchPing requires a reply".to_string(),
        ))
    }

    async fn server_handler(
        ctx: Rc<crate::rpc::handlers::RpcHandlerContext>,
        mut am_msg: AmMsg,
    ) -> Result<(crate::rpc::ServerResponse<Self::ResponseHeader>, AmMsg), (RpcError, AmMsg)> {
        // Parse request header
        let header = match am_msg
            .header()
            .get(..std::mem::size_of::<BenchPingRequestHeader>())
            .and_then(|bytes| {
                BenchPingRequestHeader::read_from_prefix(bytes)
                    .ok()
                    .map(|(h, _)| h.clone())
            }) {
            Some(h) => h,
            None => return Err((RpcError::InvalidHeader, am_msg)),
        };

        // Receive WorkerAddress
        let mut worker_addr_bytes = vec![0u8; 512];

        if am_msg.contains_data() {
            if let Err(_e) = am_msg.recv_data_vectored(&[
                std::io::IoSliceMut::new(&mut worker_addr_bytes),
            ]).await {
                return Err((RpcError::TransportError("Failed to receive worker address".to_string()), am_msg));
            }
        }

        // Get server timestamp
        let server_timestamp_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;

        let response_header = BenchPingResponseHeader::new(
            header.sequence_number,
            server_timestamp_ns,
        );

        // Send response directly using WorkerAddress
        if let Err(e) = ctx.send_response_direct(
            &worker_addr_bytes,
            Self::reply_stream_id(),
            Self::rpc_id(),
            &response_header,
            None,
        ).await {
            tracing::error!("Failed to send direct response: {:?}", e);
            return Err((e, am_msg));
        }

        // Return empty response since we already sent the response directly
        Ok((
            crate::rpc::ServerResponse::new(response_header),
            am_msg,
        ))
    }

    fn error_response(error: &RpcError) -> Self::ResponseHeader {
        let _ = error;
        BenchPingResponseHeader::new(0, 0)
    }
}
