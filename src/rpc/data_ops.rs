use std::cell::UnsafeCell;
use std::io::{IoSlice, IoSliceMut};
use std::rc::Rc;

use pluvio_ucx::async_ucx::ucp::AmMsg;

use crate::metadata::NodeId;
use crate::rpc::helpers::parse_header;
use crate::rpc::{AmRpc, AmRpcCallType, RpcClient, RpcError, RpcId};

/// RPC IDs for data operations
pub const RPC_READ_CHUNK: RpcId = 10;
pub const RPC_WRITE_CHUNK: RpcId = 11;

// ============================================================================
// ReadChunk RPC
// ============================================================================

/// ReadChunk request header
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
pub struct ReadChunkRequestHeader {
    /// Chunk index to read
    pub chunk_index: u64,

    /// Offset within the chunk
    pub offset: u64,

    /// Length to read
    pub length: u64,

    /// Path length (path is sent in data section)
    pub path_len: u64,

    /// WorkerAddress length (sent before path in data section)
    pub worker_address_len: u64,
}

impl ReadChunkRequestHeader {
    pub fn new(chunk_index: u64, offset: u64, length: u64, path_len: u64, worker_address_len: u64) -> Self {
        Self {
            chunk_index,
            offset,
            length,
            path_len,
            worker_address_len,
        }
    }
}

/// ReadChunk response header
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
pub struct ReadChunkResponseHeader {
    /// Number of bytes actually read
    pub bytes_read: u64,

    /// Status code (0 = success, non-zero = error)
    pub status: i32,

    /// Padding for alignment
    _padding: [u8; 4],
}

impl ReadChunkResponseHeader {
    pub fn success(bytes_read: u64) -> Self {
        Self {
            bytes_read,
            status: 0,
            _padding: [0; 4],
        }
    }

    pub fn error(status: i32) -> Self {
        Self {
            bytes_read: 0,
            status,
            _padding: [0; 4],
        }
    }

    pub fn is_success(&self) -> bool {
        self.status == 0
    }
}

/// ReadChunk RPC request
///
/// IMPORTANT: This struct must be kept alive for the entire duration of the RPC call.
/// The IoSlices reference the internal data directly, so dropping this struct
/// while the RPC is in flight will cause use-after-free errors.
pub struct ReadChunkRequest {
    header: ReadChunkRequestHeader,
    path: String,
    /// Data buffers - these must be stored directly in the struct
    worker_address: Vec<u8>,
    path_bytes: Vec<u8>,
    response_buffer: Vec<u8>,
    /// Cached IoSlices - lazily initialized on first call
    cached_request_ioslices: UnsafeCell<Option<[IoSlice<'static>; 2]>>,
    cached_response_ioslice: UnsafeCell<Option<IoSliceMut<'static>>>,
}

// SAFETY: ReadChunkRequest is Send because all its fields are Send
unsafe impl Send for ReadChunkRequest {}

impl ReadChunkRequest {
    pub fn new(chunk_index: u64, offset: u64, length: u64, path: String, worker_address: Vec<u8>) -> Self {
        let path_bytes = path.as_bytes().to_vec();
        let path_len = path_bytes.len() as u64;
        let worker_address_len = worker_address.len() as u64;
        let response_buffer = vec![0u8; length as usize];

        Self {
            header: ReadChunkRequestHeader::new(chunk_index, offset, length, path_len, worker_address_len),
            path,
            worker_address,
            path_bytes,
            response_buffer,
            cached_request_ioslices: UnsafeCell::new(None),
            cached_response_ioslice: UnsafeCell::new(None),
        }
    }

    /// Get the data buffer after the RPC completes
    pub fn take_data(mut self) -> Vec<u8> {
        std::mem::take(&mut self.response_buffer)
    }

    /// Get a reference to the data buffer
    pub fn data(&self) -> &[u8] {
        &self.response_buffer
    }
}

impl AmRpc for ReadChunkRequest {
    type RequestHeader = ReadChunkRequestHeader;
    type ResponseHeader = ReadChunkResponseHeader;

    fn rpc_id() -> RpcId {
        RPC_READ_CHUNK
    }

    fn call_type(&self) -> AmRpcCallType {
        // Server will RDMA-write data to client
        AmRpcCallType::Get
    }

    fn request_header(&self) -> &Self::RequestHeader {
        &self.header
    }

    fn request_data(&self) -> &[std::io::IoSlice<'_>] {
        // Lazily initialize the cached IoSlices on first call
        // IMPORTANT: Order is [Path, WorkerAddress] to avoid Rendezvous protocol issues
        unsafe {
            let cache = &mut *self.cached_request_ioslices.get();

            if cache.is_none() {
                let ioslices = [
                    IoSlice::new(std::mem::transmute::<&[u8], &'static [u8]>(&self.path_bytes)),
                    IoSlice::new(std::mem::transmute::<&[u8], &'static [u8]>(&self.worker_address)),
                ];
                *cache = Some(ioslices);
            }

            std::mem::transmute::<&[IoSlice<'static>], &[IoSlice<'_>]>(
                cache.as_ref().unwrap()
            )
        }
    }

    fn response_buffer(&self) -> &[IoSliceMut<'_>] {
        // Lazily initialize the cached IoSliceMut on first call
        unsafe {
            let cache = &mut *self.cached_response_ioslice.get();

            if cache.is_none() {
                // Create mutable slice from the response buffer
                let ptr = self.response_buffer.as_ptr() as *mut u8;
                let len = self.response_buffer.len();
                let slice = std::slice::from_raw_parts_mut(ptr, len);
                *cache = Some(IoSliceMut::new(std::mem::transmute::<&mut [u8], &'static mut [u8]>(slice)));
            }

            // Return as a slice
            std::slice::from_ref(std::mem::transmute::<&IoSliceMut<'static>, &IoSliceMut<'_>>(
                cache.as_ref().unwrap()
            ))
        }
    }

    fn proto(&self) -> Option<pluvio_ucx::async_ucx::ucp::AmProto> {
        // ReadChunk request has no data payload (only header)
        // Rendezvous protocol will be used for the RESPONSE (server->client),
        // not for the request. The server will use Rendezvous when replying
        // with data via am_msg.reply().
        None
    }

    async fn call(&self, client: &RpcClient) -> Result<Self::ResponseHeader, RpcError> {
        client.execute(self).await
    }

    async fn call_no_reply(&self, _client: &RpcClient) -> Result<(), RpcError> {
        Err(RpcError::HandlerError(
            "ReadChunk requires a reply".to_string(),
        ))
    }

    async fn server_handler(
        ctx: Rc<crate::rpc::handlers::RpcHandlerContext>,
        mut am_msg: AmMsg,
    ) -> Result<(crate::rpc::ServerResponse<Self::ResponseHeader>, AmMsg), (RpcError, AmMsg)> {
        // Parse request header
        let header: ReadChunkRequestHeader = match parse_header(&am_msg) {
            Ok(h) => h,
            Err(e) => return Err((e, am_msg)),
        };

        let _span = tracing::trace_span!("rpc_read_chunk", chunk = header.chunk_index, offset = header.offset, len = header.length).entered();

        // Receive Path and WorkerAddress from request data
        // IMPORTANT: Order is [Path, WorkerAddress] to avoid Rendezvous protocol issues
        if !am_msg.contains_data() {
            return Err((RpcError::TransportError("Request contains no data".to_string()), am_msg));
        }

        let mut path_bytes = vec![0u8; header.path_len as usize];
        let mut worker_addr_bytes = vec![0u8; header.worker_address_len as usize];

        if let Err(e) = am_msg
            .recv_data_vectored(&[
                std::io::IoSliceMut::new(&mut path_bytes),
                std::io::IoSliceMut::new(&mut worker_addr_bytes),
            ])
            .await
        {
            return Err((RpcError::TransportError(format!("Failed to receive data: {:?}", e)), am_msg));
        }

        // Log received worker address for debugging
        tracing::debug!(
            "ReadChunkRequest: Received worker_addr_len={}, first_32_bytes={:?}",
            worker_addr_bytes.len(),
            &worker_addr_bytes.get(0..32.min(worker_addr_bytes.len())).unwrap_or(&[])
        );

        let path = match String::from_utf8(path_bytes) {
            Ok(p) => p,
            Err(e) => return Err((RpcError::TransportError(format!("Invalid UTF-8 in path: {:?}", e)), am_msg)),
        };

        tracing::trace!("Reading from path: {}", path);

        // Read chunk data from storage
        let (response_header, response_data) = match ctx
            .chunk_store
            .read_chunk(&path, header.chunk_index, header.offset, header.length)
            .await
        {
            Ok(data) => {
                let bytes_read = data.len() as u64;
                tracing::trace!("Read {} bytes from storage", bytes_read);
                (ReadChunkResponseHeader::success(bytes_read), Some(data))
            }
            Err(e) => {
                tracing::error!("Failed to read chunk: {:?}", e);
                (ReadChunkResponseHeader::error(-2), None)
            }
        };

        // Send response directly using WorkerAddress
        if let Err(e) = ctx.send_response_direct(
            &worker_addr_bytes,
            Self::reply_stream_id(),
            Self::rpc_id(),
            &response_header,
            response_data.as_deref(),
        ).await {
            tracing::error!("Failed to send direct response: {:?}", e);
            return Err((e, am_msg));
        }

        Ok((crate::rpc::ServerResponse::new(response_header), am_msg))
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
// WriteChunk RPC
// ============================================================================

/// WriteChunk request header
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
pub struct WriteChunkRequestHeader {
    /// Chunk index to write
    pub chunk_index: u64,

    /// Offset within the chunk
    pub offset: u64,

    /// Length to write
    pub length: u64,

    /// Path length (path is sent in data section)
    pub path_len: u64,

    /// WorkerAddress length (sent before path in data section)
    pub worker_address_len: u64,
}

impl WriteChunkRequestHeader {
    pub fn new(chunk_index: u64, offset: u64, length: u64, path_len: u64, worker_address_len: u64) -> Self {
        Self {
            chunk_index,
            offset,
            length,
            path_len,
            worker_address_len,
        }
    }
}

/// WriteChunk response header
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
pub struct WriteChunkResponseHeader {
    /// Number of bytes actually written
    pub bytes_written: u64,

    /// Status code (0 = success, non-zero = error)
    pub status: i32,

    /// Padding for alignment
    _padding: [u8; 4],
}

impl WriteChunkResponseHeader {
    pub fn success(bytes_written: u64) -> Self {
        Self {
            bytes_written,
            status: 0,
            _padding: [0; 4],
        }
    }

    pub fn error(status: i32) -> Self {
        Self {
            bytes_written: 0,
            status,
            _padding: [0; 4],
        }
    }

    pub fn is_success(&self) -> bool {
        self.status == 0
    }
}

/// WriteChunk RPC request
///
/// IMPORTANT: This struct must be kept alive for the entire duration of the RPC call.
/// The IoSlices reference the internal data directly, so dropping this struct
/// while the RPC is in flight will cause use-after-free errors.
pub struct WriteChunkRequest<'a> {
    header: WriteChunkRequestHeader,
    /// File path (for reference)
    path: String,
    /// Data buffers - these must be stored directly in the struct
    worker_address: Vec<u8>,
    path_bytes: Vec<u8>,
    data: &'a [u8],
    /// Cached IoSlices - lazily initialized on first call to request_data()
    cached_ioslices: UnsafeCell<Option<[IoSlice<'static>; 3]>>,
}

// SAFETY: WriteChunkRequest is Send because all its fields are Send
unsafe impl Send for WriteChunkRequest<'_> {}

impl<'a> WriteChunkRequest<'a> {
    pub fn new(chunk_index: u64, offset: u64, data: &'a [u8], path: String, worker_address: Vec<u8>) -> Self {
        let length = data.len() as u64;
        let path_len = path.len() as u64;
        let worker_address_len = worker_address.len() as u64;
        let path_bytes = path.as_bytes().to_vec();

        Self {
            header: WriteChunkRequestHeader::new(chunk_index, offset, length, path_len, worker_address_len),
            path,
            worker_address,
            path_bytes,
            data,
            cached_ioslices: UnsafeCell::new(None),
        }
    }

    /// Get the data buffer
    pub fn data(&self) -> &[u8] {
        &self.data
    }
}

impl AmRpc for WriteChunkRequest<'_> {
    type RequestHeader = WriteChunkRequestHeader;
    type ResponseHeader = WriteChunkResponseHeader;

    fn rpc_id() -> RpcId {
        RPC_WRITE_CHUNK
    }

    fn call_type(&self) -> AmRpcCallType {
        // Client will RDMA-write data to server
        AmRpcCallType::Put
    }

    fn request_header(&self) -> &Self::RequestHeader {
        &self.header
    }

    fn request_data(&self) -> &[IoSlice<'_>] {
        // Lazily initialize the cached IoSlices on first call
        // IMPORTANT: Order is [Path, Data, WorkerAddress] to avoid Rendezvous protocol issues
        unsafe {
            let cache = &mut *self.cached_ioslices.get();

            if cache.is_none() {
                // Create IoSlices from our internal buffers
                // We transmute to 'static lifetime, which is safe because:
                // 1. The data is owned by self and won't move
                // 2. The IoSlices are cached in self, so they live as long as self
                // 3. self must outlive any use of these IoSlices (ensured by API contract)
                let ioslices = [
                    IoSlice::new(std::mem::transmute::<&[u8], &'static [u8]>(&self.path_bytes)),
                    IoSlice::new(std::mem::transmute::<&[u8], &'static [u8]>(&self.data)),
                    IoSlice::new(std::mem::transmute::<&[u8], &'static [u8]>(&self.worker_address)),
                ];
                *cache = Some(ioslices);
            }

            // Return the cached IoSlices
            // The lifetime is tied to &self, which is correct
            std::mem::transmute::<&[IoSlice<'static>], &[IoSlice<'_>]>(
                cache.as_ref().unwrap()
            )
        }
    }

    fn proto(&self) -> Option<pluvio_ucx::async_ucx::ucp::AmProto> {
        // Use Rendezvous protocol for RDMA transfer if data is large enough
        // Otherwise use Eager protocol for better latency on small transfers
        if crate::rpc::should_use_rdma(self.data().len() as u64) {
            Some(pluvio_ucx::async_ucx::ucp::AmProto::Rndv)
        } else {
            None // Eager protocol
        }
    }

    async fn call(&self, client: &RpcClient) -> Result<Self::ResponseHeader, RpcError> {
        client.execute(self).await
    }

    async fn call_no_reply(&self, client: &RpcClient) -> Result<(), RpcError> {
        client.execute_no_reply(self).await
    }

    async fn server_handler(
        ctx: Rc<crate::rpc::handlers::RpcHandlerContext>,
        mut am_msg: AmMsg,
    ) -> Result<(crate::rpc::ServerResponse<Self::ResponseHeader>, AmMsg), (RpcError, AmMsg)> {
        // Parse request header
        let header: WriteChunkRequestHeader = match parse_header(&am_msg) {
            Ok(h) => h,
            Err(e) => return Err((e, am_msg)),
        };

        let _span = tracing::trace_span!("rpc_write_chunk", chunk = header.chunk_index, offset = header.offset, len = header.length).entered();

        // Receive WorkerAddress, path and data from client
        if !am_msg.contains_data() {
            tracing::error!("WriteChunk request contains no data");
            return Ok((
                crate::rpc::ServerResponse::new(WriteChunkResponseHeader::error(-22)),
                am_msg,
            ));
        }

        // Receive WorkerAddress first using actual length from header
        let mut worker_addr_bytes = vec![0u8; header.worker_address_len as usize];

        // Check if chunk_store is IOUringChunkStore to use zero-copy path
        use crate::storage::chunk_store::IOUringChunkStore;
        use std::any::Any;

        let data_len = header.length as usize;
        let chunk_store_any = &*ctx.chunk_store as &dyn Any;

        tracing::trace!(
            "Processing WriteChunk: chunk={}, offset={}, len={}, worker_address_len={}",
            header.chunk_index,
            header.offset,
            header.length,
            header.worker_address_len
        );

        let response_header = if let Some(io_uring_store) = chunk_store_any.downcast_ref::<IOUringChunkStore>() {
            tracing::debug!("Using zero-copy path for WriteChunk");
            // Zero-copy path: Use registered buffer
            let mut fixed_buffer = ctx.allocator.acquire().await;

            // Ensure the data fits in the registered buffer
            if data_len > fixed_buffer.len() {
                tracing::error!(
                    "Data size {} exceeds registered buffer size {}",
                    data_len,
                    fixed_buffer.len()
                );
                WriteChunkResponseHeader::error(-22)
            } else {
                // Receive Path, Data, and WorkerAddress in one vectored call (zero-copy RDMA)
                // IMPORTANT: Order is [Path, Data, WorkerAddress] to avoid Rendezvous protocol issues
                let mut path_bytes = vec![0u8; header.path_len as usize];
                let buffer_slice = &mut fixed_buffer.as_mut_slice()[..data_len];

                tracing::debug!(
                    "WriteChunk: receiving data - worker_addr_len={}, path_len={}, data_len={}",
                    header.worker_address_len,
                    header.path_len,
                    data_len
                );
                tracing::trace!("WriteChunk: calling recv_data_vectored (zero-copy)...");

                if let Err(e) = am_msg
                    .recv_data_vectored(&[
                        std::io::IoSliceMut::new(&mut path_bytes),
                        std::io::IoSliceMut::new(buffer_slice),
                        std::io::IoSliceMut::new(&mut worker_addr_bytes),
                    ])
                    .await
                {
                    tracing::error!("WriteChunk: failed to receive request data (zero-copy): {:?}", e);
                    WriteChunkResponseHeader::error(-5)
                } else {
                    tracing::trace!("WriteChunk: recv_data_vectored completed (zero-copy)");

                    // Log received worker address for debugging
                    tracing::debug!(
                        "WriteChunkRequest (zero-copy): Received worker_addr_len={}, first_32_bytes={:?}",
                        worker_addr_bytes.len(),
                        &worker_addr_bytes.get(0..32.min(worker_addr_bytes.len())).unwrap_or(&[])
                    );

                    let path = match String::from_utf8(path_bytes) {
                        Ok(p) => p,
                        Err(e) => {
                            tracing::error!("Failed to decode path: {:?}", e);
                            return Ok((
                                crate::rpc::ServerResponse::new(WriteChunkResponseHeader::error(-22)),
                                am_msg,
                            ));
                        }
                    };

                    tracing::debug!(
                        "WriteChunk request (zero-copy): path={}, chunk={}, offset={}, length={}",
                        path,
                        header.chunk_index,
                        header.offset,
                        header.length
                    );

                    // Use zero-copy write with registered buffer
                    match io_uring_store
                        .write_chunk_fixed(&path, header.chunk_index, header.offset, fixed_buffer, data_len)
                        .await
                    {
                        Ok(bytes_written) => {
                            tracing::debug!(
                                "Wrote {} bytes (zero-copy) to storage (path={}, chunk={})",
                                bytes_written,
                                path,
                                header.chunk_index
                            );
                            WriteChunkResponseHeader::success(header.length)
                        }
                        Err(e) => {
                            tracing::error!("Failed to write chunk (zero-copy): {:?}", e);
                            WriteChunkResponseHeader::error(-5)
                        }
                    }
                }
            }
        } else {
            // Fallback path: Use Vec<u8> for non-IOUring backends
            tracing::debug!("Using fallback path for WriteChunk (chunk_store is not IOUringChunkStore)");
            let mut path_bytes = vec![0u8; header.path_len as usize];
            let mut data = vec![0u8; data_len];

            // IMPORTANT: Order is [Path, Data, WorkerAddress] to avoid Rendezvous protocol issues
            if let Err(e) = am_msg
                .recv_data_vectored(&[
                    std::io::IoSliceMut::new(&mut path_bytes),
                    std::io::IoSliceMut::new(&mut data),
                    std::io::IoSliceMut::new(&mut worker_addr_bytes),
                ])
                .await
            {
                tracing::error!("Failed to receive request data: {:?}", e);
                WriteChunkResponseHeader::error(-5)
            } else {
                // Log received worker address for debugging
                tracing::debug!(
                    "WriteChunkRequest (fallback): Received worker_addr_len={}, first_32_bytes={:?}",
                    worker_addr_bytes.len(),
                    &worker_addr_bytes.get(0..32.min(worker_addr_bytes.len())).unwrap_or(&[])
                );

                let path = match String::from_utf8(path_bytes) {
                    Ok(p) => p,
                    Err(e) => {
                        tracing::error!("Failed to decode path: {:?}", e);
                        return Ok((
                            crate::rpc::ServerResponse::new(WriteChunkResponseHeader::error(-22)),
                            am_msg,
                        ));
                    }
                };

                tracing::debug!(
                    "WriteChunk request: path={}, chunk={}, offset={}, length={}",
                    path,
                    header.chunk_index,
                    header.offset,
                    header.length
                );

                match ctx
                    .chunk_store
                    .write_chunk(&path, header.chunk_index, header.offset, &data)
                    .await
                {
                    Ok(_bytes_written) => {
                        tracing::debug!(
                            "Wrote {} bytes to storage (path={}, chunk={})",
                            data.len(),
                            path,
                            header.chunk_index
                        );
                        WriteChunkResponseHeader::success(header.length)
                    }
                    Err(e) => {
                        tracing::error!("Failed to write chunk: {:?}", e);
                        WriteChunkResponseHeader::error(-5)
                    }
                }
            }
        };

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

        Ok((crate::rpc::ServerResponse::new(response_header), am_msg))
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

// ============================================================================
// Chunk location query (for metadata integration)
// ============================================================================

/// Query which node stores a specific chunk
pub fn get_chunk_node(chunk_index: u64, chunk_locations: &[NodeId]) -> Option<&NodeId> {
    chunk_locations.get(chunk_index as usize)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_chunk_request_header() {
        let header = ReadChunkRequestHeader::new(5, 1024, 4096, 42, 256);
        assert_eq!(header.chunk_index, 5);
        assert_eq!(header.offset, 1024);
        assert_eq!(header.length, 4096);
        assert_eq!(header.path_len, 42);
        assert_eq!(header.worker_address_len, 256);

        // Verify it can be serialized
        let bytes = zerocopy::IntoBytes::as_bytes(&header);
        assert_eq!(bytes.len(), std::mem::size_of::<ReadChunkRequestHeader>());

        // Verify it can be deserialized
        let deserialized: ReadChunkRequestHeader =
            zerocopy::FromBytes::read_from_bytes(bytes).unwrap();
        assert_eq!(deserialized.chunk_index, 5);
    }

    #[test]
    fn test_read_chunk_response_header() {
        let success = ReadChunkResponseHeader::success(4096);
        assert!(success.is_success());
        assert_eq!(success.bytes_read, 4096);

        let error = ReadChunkResponseHeader::error(-1);
        assert!(!error.is_success());
        assert_eq!(error.bytes_read, 0);
    }

    #[test]
    fn test_read_chunk_request() {
        let dummy_worker_addr = vec![0u8; 512];
        let request = ReadChunkRequest::new(0, 0, 1024, "/test/file.txt".to_string(), dummy_worker_addr);
        assert_eq!(request.header.chunk_index, 0);
        assert_eq!(request.header.length, 1024);
        assert_eq!(request.header.path_len, 14); // "/test/file.txt".len()

        // Buffer should be allocated
        assert_eq!(request.data().len(), 1024);
    }

    #[test]
    fn test_write_chunk_request_header() {
        let header = WriteChunkRequestHeader::new(3, 512, 2048, 99, 256);
        assert_eq!(header.chunk_index, 3);
        assert_eq!(header.offset, 512);
        assert_eq!(header.length, 2048);
        assert_eq!(header.path_len, 99);
        assert_eq!(header.worker_address_len, 256);
    }

    #[test]
    fn test_write_chunk_response_header() {
        let success = WriteChunkResponseHeader::success(2048);
        assert!(success.is_success());
        assert_eq!(success.bytes_written, 2048);

        let error = WriteChunkResponseHeader::error(-2);
        assert!(!error.is_success());
        assert_eq!(error.bytes_written, 0);
    }

    #[test]
    fn test_write_chunk_request() {
        let data = vec![0xAA; 512];
        let dummy_worker_addr = vec![0u8; 512];
        let request = WriteChunkRequest::new(1, 0, &data[..], "/test/file.txt".to_string(), dummy_worker_addr);

        assert_eq!(request.header.chunk_index, 1);
        assert_eq!(request.header.length, 512);
        assert_eq!(request.header.path_len, 14); // "/test/file.txt".len()
        assert_eq!(request.data(), &data[..]);
    }

    #[test]
    fn test_get_chunk_node() {
        let locations = vec![
            "node1".to_string(),
            "node2".to_string(),
            "node3".to_string(),
        ];

        assert_eq!(get_chunk_node(0, &locations), Some(&"node1".to_string()));
        assert_eq!(get_chunk_node(1, &locations), Some(&"node2".to_string()));
        assert_eq!(get_chunk_node(2, &locations), Some(&"node3".to_string()));
        assert_eq!(get_chunk_node(3, &locations), None);
    }

    #[test]
    fn test_rpc_ids() {
        assert_eq!(ReadChunkRequest::rpc_id(), RPC_READ_CHUNK);
        assert_eq!(WriteChunkRequest::rpc_id(), RPC_WRITE_CHUNK);

        // Reply stream IDs should be different
        assert_eq!(ReadChunkRequest::reply_stream_id(), RPC_READ_CHUNK + 100);
        assert_eq!(WriteChunkRequest::reply_stream_id(), RPC_WRITE_CHUNK + 100);
    }
}
