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

/// Maximum path length for RPC operations
pub const MAX_RPC_PATH_LENGTH: usize = 256;

/// ReadChunk request header with embedded path
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
#[deprecated(since = "0.2.0", note = "Use ReadChunkByIdRequest instead")]
pub struct ReadChunkRequestHeader {
    /// Chunk index to read
    pub chunk_index: u64,

    /// Offset within the chunk
    pub offset: u64,

    /// Length to read
    pub length: u64,

    /// Actual path length (valid bytes in path_buffer)
    pub path_len: u64,

    /// Fixed-size path buffer (256 bytes)
    pub path_buffer: [u8; MAX_RPC_PATH_LENGTH],
}

#[allow(deprecated)]
impl ReadChunkRequestHeader {
    pub fn new(chunk_index: u64, offset: u64, length: u64, path: &str) -> Self {
        let mut path_buffer = [0u8; MAX_RPC_PATH_LENGTH];
        let path_bytes = path.as_bytes();
        let path_len = path_bytes.len().min(MAX_RPC_PATH_LENGTH);
        path_buffer[..path_len].copy_from_slice(&path_bytes[..path_len]);

        Self {
            chunk_index,
            offset,
            length,
            path_len: path_len as u64,
            path_buffer,
        }
    }

    /// Get the path as a string slice
    pub fn path(&self) -> Result<&str, std::str::Utf8Error> {
        let path_len = self.path_len as usize;
        std::str::from_utf8(&self.path_buffer[..path_len])
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

/// ReadChunk RPC request with external buffer
///
/// This version takes an external buffer reference to receive data directly,
/// avoiding an extra copy from internal Vec to user buffer.
///
/// **DEPRECATED**: Use `ReadChunkByIdRequest` instead. Path-based RPCs have been
/// replaced by FileId-based RPCs for reduced header overhead (32 bytes vs 288 bytes).
#[allow(deprecated)]
#[deprecated(since = "0.2.0", note = "Use ReadChunkByIdRequest instead")]
pub struct ReadChunkRequest<'a> {
    header: ReadChunkRequestHeader,
    /// External buffer to receive response data directly
    response_buffer: &'a mut [u8],
    /// Cached IoSliceMut for response - lazily initialized on first call
    cached_response_ioslice: UnsafeCell<Option<IoSliceMut<'static>>>,
}

// SAFETY: ReadChunkRequest is Send because all its fields are Send
#[allow(deprecated)]
unsafe impl<'a> Send for ReadChunkRequest<'a> {}

#[allow(deprecated)]
impl<'a> ReadChunkRequest<'a> {
    /// Create a new ReadChunkRequest with an external buffer
    ///
    /// The buffer must be at least `length` bytes long.
    /// Data will be written directly to this buffer during the RPC call.
    pub fn new(
        chunk_index: u64,
        offset: u64,
        length: u64,
        path: String,
        buffer: &'a mut [u8],
    ) -> Self {
        debug_assert!(
            buffer.len() >= length as usize,
            "Buffer too small: {} < {}",
            buffer.len(),
            length
        );

        Self {
            header: ReadChunkRequestHeader::new(chunk_index, offset, length, &path),
            response_buffer: buffer,
            cached_response_ioslice: UnsafeCell::new(None),
        }
    }

    /// Get a reference to the data buffer
    pub fn data(&self) -> &[u8] {
        self.response_buffer
    }

    /// Get the length of data requested
    pub fn requested_length(&self) -> usize {
        self.header.length as usize
    }
}

#[allow(deprecated)]
impl<'a> AmRpc for ReadChunkRequest<'a> {
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
        // No request data - path is embedded in header
        &[]
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
                *cache = Some(IoSliceMut::new(std::mem::transmute::<
                    &mut [u8],
                    &'static mut [u8],
                >(slice)));
            }

            // Return as a slice
            std::slice::from_ref(
                std::mem::transmute::<&IoSliceMut<'static>, &IoSliceMut<'_>>(
                    cache.as_ref().unwrap(),
                ),
            )
        }
    }

    fn proto(&self) -> Option<pluvio_ucx::async_ucx::ucp::AmProto> {
        // ReadChunk request has no data payload (only header)
        // Rendezvous protocol will be used for the RESPONSE (server->client),
        // not for the request. The server will use Rendezvous when replying
        // with data via am_msg.reply().
        None
    }

    #[async_backtrace::framed]
    async fn call(&self, client: &RpcClient) -> Result<Self::ResponseHeader, RpcError> {
        client.execute(self).await
    }

    #[async_backtrace::framed]
    async fn call_no_reply(&self, _client: &RpcClient) -> Result<(), RpcError> {
        Err(RpcError::HandlerError(
            "ReadChunk requires a reply".to_string(),
        ))
    }

    #[async_backtrace::framed]
    #[tracing::instrument(level = "trace", name = "rpc_read_chunk_handler", skip(ctx, am_msg))]
    async fn server_handler(
        ctx: Rc<crate::rpc::handlers::RpcHandlerContext>,
        am_msg: AmMsg,
    ) -> Result<(crate::rpc::ServerResponse<Self::ResponseHeader>, AmMsg), (RpcError, AmMsg)> {
        // Parse request header
        let header: ReadChunkRequestHeader = match parse_header(&am_msg) {
            Ok(h) => h,
            Err(e) => return Err((e, am_msg)),
        };

        tracing::trace!(
            chunk = header.chunk_index,
            offset = header.offset,
            len = header.length,
            "ReadChunk request received"
        );

        // Extract path from header
        let path = match header.path() {
            Ok(p) => p.to_owned(),
            Err(e) => {
                return Err((
                    RpcError::TransportError(format!("Invalid UTF-8 in path: {:?}", e)),
                    am_msg,
                ));
            }
        };

        // Register path in FileIdRegistry for FileId-based RPC support
        ctx.file_id_registry().register(&path);

        tracing::trace!("Reading from path: {}", path);

        // Check if chunk_store is IOUringChunkStore to use zero-copy path
        use crate::storage::chunk_store::IOUringChunkStore;
        use std::any::Any;

        let chunk_store_any = &*ctx.chunk_store as &dyn Any;

        // Try zero-copy path first for IOUringChunkStore
        let (response_header, response_data, fixed_buffer_opt) = if let Some(io_uring_store) =
            chunk_store_any.downcast_ref::<IOUringChunkStore>()
        {
            tracing::trace!("Using zero-copy path for ReadChunk");

            // Acquire a registered buffer from the allocator
            let fixed_buffer = ctx.allocator.acquire().await;

            // Ensure the requested length fits in the buffer
            let read_len = (header.length as usize).min(fixed_buffer.len());

            // RPCの性能テストのために即時に返す実装
            // (
            //     ReadChunkResponseHeader::success(read_len as u64),
            //     None,
            //     Some((fixed_buffer, read_len)),
            // )

            match io_uring_store
                .read_chunk_fixed(&path, header.chunk_index, header.offset, fixed_buffer)
                .await
            {
                Ok((bytes_read, fixed_buffer)) => {
                    let actual_bytes = bytes_read.min(read_len);
                    tracing::trace!(
                        "Read {} bytes (zero-copy) from storage (path={}, chunk={})",
                        actual_bytes,
                        path,
                        header.chunk_index
                    );
                    (
                        ReadChunkResponseHeader::success(actual_bytes as u64),
                        None,
                        Some((fixed_buffer, actual_bytes)),
                    )
                }
                Err(e) => {
                    tracing::error!("Failed to read chunk (zero-copy): {:?}", e);
                    (ReadChunkResponseHeader::error(-2), None, None)
                }
            }
        } else {
            // Fallback path: Use Vec<u8> for non-IOUring backends
            tracing::trace!(
                "Using fallback path for ReadChunk (chunk_store is not IOUringChunkStore)"
            );

            match ctx
                .chunk_store
                .read_chunk(&path, header.chunk_index, header.offset, header.length)
                .await
            {
                Ok(data) => {
                    let bytes_read = data.len() as u64;
                    tracing::trace!("Read {} bytes from storage", bytes_read);
                    (ReadChunkResponseHeader::success(bytes_read), Some(data), None)
                }
                Err(e) => {
                    tracing::error!("Failed to read chunk: {:?}", e);
                    (ReadChunkResponseHeader::error(-2), None, None)
                }
            }
        };

        // Send response using reply_vectorized
        if !am_msg.need_reply() {
            tracing::error!("Message does not support reply");
            return Err((
                RpcError::HandlerError("Message does not support reply".to_string()),
                am_msg,
            ));
        }

        // Copy header bytes into an owned buffer so they live for the entire async send.
        let header_vec = zerocopy::IntoBytes::as_bytes(&response_header).to_vec();
        let header_bytes: &[u8] = &header_vec;

        // Use reply_vectorized with IoSlice
        let result = if let Some((mut fixed_buffer, bytes_read)) = fixed_buffer_opt {
            // Zero-copy path: send directly from registered buffer
            let data_slice = &fixed_buffer.as_mut_slice()[..bytes_read];
            let data_slices = [std::io::IoSlice::new(data_slice)];

            // Determine protocol based on data size
            let proto = if crate::rpc::should_use_rdma(bytes_read as u64) {
                Some(pluvio_ucx::async_ucx::ucp::AmProto::Rndv)
            } else {
                None
            };

            tracing::debug!(
                "ReadChunk: sending response (zero-copy), bytes={}, proto={:?}",
                bytes_read, proto
            );

            let send_result = unsafe {
                am_msg
                    .reply_vectorized(
                        Self::reply_stream_id() as u32,
                        header_bytes,
                        &data_slices,
                        false, // need_reply
                        proto,
                    )
                    .await
            };

            tracing::debug!("ReadChunk: response sent (zero-copy), success={}", send_result.is_ok());
            send_result
        } else if let Some(ref data) = response_data {
            // Fallback path: send from Vec<u8>
            let data_slices = [std::io::IoSlice::new(data)];

            // Determine protocol based on data size
            let proto = if crate::rpc::should_use_rdma(data.len() as u64) {
                Some(pluvio_ucx::async_ucx::ucp::AmProto::Rndv)
            } else {
                None
            };

            tracing::debug!(
                "ReadChunk: sending response (fallback), bytes={}, proto={:?}",
                data.len(), proto
            );

            let send_result = unsafe {
                am_msg
                    .reply_vectorized(
                        Self::reply_stream_id() as u32,
                        header_bytes,
                        &data_slices,
                        false, // need_reply
                        proto,
                    )
                    .await
            };

            tracing::debug!("ReadChunk: response sent (fallback), success={}", send_result.is_ok());
            send_result
        } else {
            // No data, just send header (error case)
            tracing::debug!("ReadChunk: sending error response (no data)");

            let send_result = unsafe {
                am_msg
                    .reply_vectorized(
                        Self::reply_stream_id() as u32,
                        header_bytes,
                        &[],
                        false, // need_reply
                        None,
                    )
                    .await
            };

            tracing::debug!("ReadChunk: error response sent, success={}", send_result.is_ok());
            send_result
        };

        if let Err(e) = result {
            tracing::error!("Failed to send reply: {:?}", e);
            return Err((
                RpcError::TransportError(format!("Failed to send reply: {:?}", e)),
                am_msg,
            ));
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

/// WriteChunk request header with embedded path
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
#[deprecated(since = "0.2.0", note = "Use WriteChunkByIdRequest instead")]
pub struct WriteChunkRequestHeader {
    /// Chunk index to write
    pub chunk_index: u64,

    /// Offset within the chunk
    pub offset: u64,

    /// Length to write
    pub length: u64,

    /// Actual path length (valid bytes in path_buffer)
    pub path_len: u64,

    /// Fixed-size path buffer (256 bytes)
    pub path_buffer: [u8; MAX_RPC_PATH_LENGTH],
}

#[allow(deprecated)]
impl WriteChunkRequestHeader {
    pub fn new(chunk_index: u64, offset: u64, length: u64, path: &str) -> Self {
        let mut path_buffer = [0u8; MAX_RPC_PATH_LENGTH];
        let path_bytes = path.as_bytes();
        let path_len = path_bytes.len().min(MAX_RPC_PATH_LENGTH);
        path_buffer[..path_len].copy_from_slice(&path_bytes[..path_len]);

        Self {
            chunk_index,
            offset,
            length,
            path_len: path_len as u64,
            path_buffer,
        }
    }

    /// Get the path as a string slice
    pub fn path(&self) -> Result<&str, std::str::Utf8Error> {
        let path_len = self.path_len as usize;
        std::str::from_utf8(&self.path_buffer[..path_len])
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
/// Path is now embedded in the header, so only data payload is sent.
///
/// **DEPRECATED**: Use `WriteChunkByIdRequest` instead. Path-based RPCs have been
/// replaced by FileId-based RPCs for reduced header overhead (32 bytes vs 288 bytes).
#[allow(deprecated)]
#[deprecated(since = "0.2.0", note = "Use WriteChunkByIdRequest instead")]
pub struct WriteChunkRequest<'a> {
    header: WriteChunkRequestHeader,
    data: &'a [u8],
    /// Cached IoSlice for data - lazily initialized on first call to request_data()
    cached_ioslice: UnsafeCell<Option<IoSlice<'static>>>,
}

// SAFETY: WriteChunkRequest is Send because all its fields are Send
#[allow(deprecated)]
unsafe impl Send for WriteChunkRequest<'_> {}

#[allow(deprecated)]
impl<'a> WriteChunkRequest<'a> {
    pub fn new(chunk_index: u64, offset: u64, data: &'a [u8], path: String) -> Self {
        let length = data.len() as u64;

        Self {
            header: WriteChunkRequestHeader::new(chunk_index, offset, length, &path),
            data,
            cached_ioslice: UnsafeCell::new(None),
        }
    }

    /// Get the data buffer
    pub fn data(&self) -> &[u8] {
        &self.data
    }
}

#[allow(deprecated)]
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
        // Lazily initialize the cached IoSlice for data
        unsafe {
            let cache = &mut *self.cached_ioslice.get();

            if cache.is_none() {
                // Create IoSlice from data buffer only (path is in header)
                *cache = Some(IoSlice::new(std::mem::transmute::<&[u8], &'static [u8]>(
                    &self.data,
                )));
            }

            // Return as a slice
            std::slice::from_ref(std::mem::transmute::<&IoSlice<'static>, &IoSlice<'_>>(
                cache.as_ref().unwrap(),
            ))
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

    #[async_backtrace::framed]
    async fn call(&self, client: &RpcClient) -> Result<Self::ResponseHeader, RpcError> {
        client.execute(self).await
    }

    #[async_backtrace::framed]
    async fn call_no_reply(&self, client: &RpcClient) -> Result<(), RpcError> {
        client.execute_no_reply(self).await
    }

    #[async_backtrace::framed]
    #[tracing::instrument(level = "trace", name = "rpc_write_chunk_handler", skip(ctx, am_msg))]
    async fn server_handler(
        ctx: Rc<crate::rpc::handlers::RpcHandlerContext>,
        mut am_msg: AmMsg,
    ) -> Result<(crate::rpc::ServerResponse<Self::ResponseHeader>, AmMsg), (RpcError, AmMsg)> {
        // Parse request header
        let header: WriteChunkRequestHeader = match parse_header(&am_msg) {
            Ok(h) => h,
            Err(e) => return Err((e, am_msg)),
        };

        tracing::trace!(
            chunk = header.chunk_index,
            offset = header.offset,
            len = header.length,
            "WriteChunk request received"
        );

        // Receive path and data from client
        if !am_msg.contains_data() {
            tracing::error!("WriteChunk request contains no data");
            return Ok((
                crate::rpc::ServerResponse::new(WriteChunkResponseHeader::error(-22)),
                am_msg,
            ));
        }

        // Check if chunk_store is IOUringChunkStore to use zero-copy path
        use crate::storage::chunk_store::IOUringChunkStore;
        use std::any::Any;

        let data_len = header.length as usize;
        let chunk_store_any = &*ctx.chunk_store as &dyn Any;

        tracing::trace!(
            "Processing WriteChunk: chunk={}, offset={}, len={}",
            header.chunk_index,
            header.offset,
            header.length
        );

        let response_header = if let Some(io_uring_store) =
            chunk_store_any.downcast_ref::<IOUringChunkStore>()
        {
            tracing::trace!("Using zero-copy path for WriteChunk");

            // Extract path from header
            let path = match header.path() {
                Ok(p) => p.to_owned(),
                Err(e) => {
                    tracing::error!("Failed to decode path: {:?}", e);
                    return Ok((
                        crate::rpc::ServerResponse::new(WriteChunkResponseHeader::error(-22)),
                        am_msg,
                    ));
                }
            };

            // Register path in FileIdRegistry for FileId-based RPC support
            ctx.file_id_registry().register(&path);

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
                let buffer_slice = &mut fixed_buffer.as_mut_slice()[..data_len];

                tracing::trace!(
                    "WriteChunk: receiving data - data_len={}",
                    data_len
                );
                tracing::trace!("WriteChunk: calling recv_data_single (zero-copy)...");

                // Receive data into FixedBuffer
                if let Err(e) = am_msg
                    .recv_data_single(buffer_slice)
                    .await
                {
                    tracing::error!(
                        "WriteChunk: failed to receive data (zero-copy): {:?}",
                        e
                    );
                    WriteChunkResponseHeader::error(-5)
                } else {
                    tracing::trace!("WriteChunk: recv_data_single completed (zero-copy)");

                    tracing::trace!(
                        "WriteChunk request (zero-copy): path={}, chunk={}, offset={}, length={}",
                        path,
                        header.chunk_index,
                        header.offset,
                        header.length
                    );

                    // Use zero-copy write with registered buffer
                    match io_uring_store
                        .write_chunk_fixed(
                            &path,
                            header.chunk_index,
                            header.offset,
                            fixed_buffer,
                            data_len,
                        )
                        .await
                    {
                        Ok(bytes_written) => {
                            tracing::trace!(
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
            tracing::trace!(
                "Using fallback path for WriteChunk (chunk_store is not IOUringChunkStore)"
            );

            // Extract path from header
            let path = match header.path() {
                Ok(p) => p.to_owned(),
                Err(e) => {
                    tracing::error!("Failed to decode path: {:?}", e);
                    return Ok((
                        crate::rpc::ServerResponse::new(WriteChunkResponseHeader::error(-22)),
                        am_msg,
                    ));
                }
            };

            // Register path in FileIdRegistry for FileId-based RPC support
            ctx.file_id_registry().register(&path);

            let mut data = vec![0u8; data_len];

            // Receive data
            if let Err(e) = am_msg
                .recv_data_single(&mut data)
                .await
            {
                tracing::error!("Failed to receive data: {:?}", e);
                WriteChunkResponseHeader::error(-5)
            } else {
                tracing::trace!(
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
                        tracing::trace!(
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

        // Send response using reply_vectorized
        if !am_msg.need_reply() {
            tracing::error!("Message does not support reply");
            return Err((
                RpcError::HandlerError("Message does not support reply".to_string()),
                am_msg,
            ));
        }

        let header_vec = zerocopy::IntoBytes::as_bytes(&response_header).to_vec();
        let header_bytes: &[u8] = &header_vec;

        tracing::debug!(
            "WriteChunk: sending response, status={}, bytes_written={}",
            response_header.status, response_header.bytes_written
        );

        // Send header only (no data payload for write response)
        let result = unsafe {
            am_msg
                .reply_vectorized(
                    Self::reply_stream_id() as u32,
                    header_bytes,
                    &[],
                    false, // need_reply
                    None,  // No data, so no protocol needed
                )
                .await
        };

        tracing::debug!("WriteChunk: response sent, success={}", result.is_ok());

        if let Err(e) = result {
            tracing::error!("Failed to send reply: {:?}", e);
            return Err((
                RpcError::TransportError(format!("Failed to send reply: {:?}", e)),
                am_msg,
            ));
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

// ============================================================================
// FileId-based RPCs (Compact headers - 32 bytes vs 288 bytes)
// ============================================================================

/// RPC IDs for FileId-based data operations
pub const RPC_READ_CHUNK_BY_ID: RpcId = 12;
pub const RPC_WRITE_CHUNK_BY_ID: RpcId = 13;

/// Compact ReadChunk request header using FileId (32 bytes total)
///
/// This replaces the 288-byte path-based header with a compact 32-byte header:
/// - file_id: 8 bytes (contains both path_hash and chunk_id)
/// - offset: 8 bytes (offset within chunk)
/// - length: 8 bytes (bytes to read)
/// - _reserved: 8 bytes (for future use, alignment)
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
pub struct ReadChunkByIdRequestHeader {
    /// FileId containing path_hash (lower 32 bits) and chunk_id (upper 32 bits)
    pub file_id: u64,

    /// Offset within the chunk
    pub offset: u64,

    /// Length to read
    pub length: u64,

    /// Reserved for future use (alignment)
    pub _reserved: u64,
}

impl ReadChunkByIdRequestHeader {
    pub fn new(file_id: u64, offset: u64, length: u64) -> Self {
        Self {
            file_id,
            offset,
            length,
            _reserved: 0,
        }
    }

    /// Create from FileId type
    pub fn from_file_id(file_id: crate::rpc::file_id::FileId, offset: u64, length: u64) -> Self {
        Self::new(file_id.as_raw(), offset, length)
    }

    /// Get the path hash (lower 32 bits of file_id)
    #[inline]
    pub const fn path_hash(&self) -> u32 {
        (self.file_id & 0xFFFF_FFFF) as u32
    }

    /// Get the chunk ID (upper 32 bits of file_id)
    #[inline]
    pub const fn chunk_id(&self) -> u32 {
        (self.file_id >> 32) as u32
    }
}

/// Compact WriteChunk request header using FileId (32 bytes total)
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
pub struct WriteChunkByIdRequestHeader {
    /// FileId containing path_hash (lower 32 bits) and chunk_id (upper 32 bits)
    pub file_id: u64,

    /// Offset within the chunk
    pub offset: u64,

    /// Length to write
    pub length: u64,

    /// Reserved for future use (alignment)
    pub _reserved: u64,
}

impl WriteChunkByIdRequestHeader {
    pub fn new(file_id: u64, offset: u64, length: u64) -> Self {
        Self {
            file_id,
            offset,
            length,
            _reserved: 0,
        }
    }

    /// Create from FileId type
    pub fn from_file_id(file_id: crate::rpc::file_id::FileId, offset: u64, length: u64) -> Self {
        Self::new(file_id.as_raw(), offset, length)
    }

    /// Get the path hash (lower 32 bits of file_id)
    #[inline]
    pub const fn path_hash(&self) -> u32 {
        (self.file_id & 0xFFFF_FFFF) as u32
    }

    /// Get the chunk ID (upper 32 bits of file_id)
    #[inline]
    pub const fn chunk_id(&self) -> u32 {
        (self.file_id >> 32) as u32
    }
}

/// ReadChunkById RPC request with external buffer
pub struct ReadChunkByIdRequest<'a> {
    header: ReadChunkByIdRequestHeader,
    /// External buffer to receive response data directly
    response_buffer: &'a mut [u8],
    /// Cached IoSliceMut for response - lazily initialized on first call
    cached_response_ioslice: UnsafeCell<Option<IoSliceMut<'static>>>,
}

// SAFETY: ReadChunkByIdRequest is Send because all its fields are Send
unsafe impl<'a> Send for ReadChunkByIdRequest<'a> {}

impl<'a> ReadChunkByIdRequest<'a> {
    /// Create a new ReadChunkByIdRequest with an external buffer
    pub fn new(file_id: u64, offset: u64, length: u64, buffer: &'a mut [u8]) -> Self {
        debug_assert!(
            buffer.len() >= length as usize,
            "Buffer too small: {} < {}",
            buffer.len(),
            length
        );

        Self {
            header: ReadChunkByIdRequestHeader::new(file_id, offset, length),
            response_buffer: buffer,
            cached_response_ioslice: UnsafeCell::new(None),
        }
    }

    /// Create from FileId type
    pub fn from_file_id(
        file_id: crate::rpc::file_id::FileId,
        offset: u64,
        length: u64,
        buffer: &'a mut [u8],
    ) -> Self {
        Self::new(file_id.as_raw(), offset, length, buffer)
    }

    /// Get a reference to the data buffer
    pub fn data(&self) -> &[u8] {
        self.response_buffer
    }

    /// Get the length of data requested
    pub fn requested_length(&self) -> usize {
        self.header.length as usize
    }

    /// Get the path hash
    pub fn path_hash(&self) -> u32 {
        self.header.path_hash()
    }

    /// Get the chunk ID
    pub fn chunk_id(&self) -> u32 {
        self.header.chunk_id()
    }
}

impl<'a> AmRpc for ReadChunkByIdRequest<'a> {
    type RequestHeader = ReadChunkByIdRequestHeader;
    type ResponseHeader = ReadChunkResponseHeader;

    fn rpc_id() -> RpcId {
        RPC_READ_CHUNK_BY_ID
    }

    fn call_type(&self) -> AmRpcCallType {
        AmRpcCallType::Get
    }

    fn request_header(&self) -> &Self::RequestHeader {
        &self.header
    }

    fn request_data(&self) -> &[std::io::IoSlice<'_>] {
        &[]
    }

    fn response_buffer(&self) -> &[IoSliceMut<'_>] {
        unsafe {
            let cache = &mut *self.cached_response_ioslice.get();

            if cache.is_none() {
                let ptr = self.response_buffer.as_ptr() as *mut u8;
                let len = self.response_buffer.len();
                let slice = std::slice::from_raw_parts_mut(ptr, len);
                *cache = Some(IoSliceMut::new(std::mem::transmute::<
                    &mut [u8],
                    &'static mut [u8],
                >(slice)));
            }

            std::slice::from_ref(
                std::mem::transmute::<&IoSliceMut<'static>, &IoSliceMut<'_>>(
                    cache.as_ref().unwrap(),
                ),
            )
        }
    }

    fn proto(&self) -> Option<pluvio_ucx::async_ucx::ucp::AmProto> {
        None
    }

    #[async_backtrace::framed]
    async fn call(&self, client: &RpcClient) -> Result<Self::ResponseHeader, RpcError> {
        client.execute(self).await
    }

    #[async_backtrace::framed]
    async fn call_no_reply(&self, _client: &RpcClient) -> Result<(), RpcError> {
        Err(RpcError::HandlerError(
            "ReadChunkById requires a reply".to_string(),
        ))
    }

    #[async_backtrace::framed]
    #[tracing::instrument(level = "trace", name = "rpc_read_chunk_by_id_handler", skip(ctx, am_msg))]
    async fn server_handler(
        ctx: Rc<crate::rpc::handlers::RpcHandlerContext>,
        am_msg: AmMsg,
    ) -> Result<(crate::rpc::ServerResponse<Self::ResponseHeader>, AmMsg), (RpcError, AmMsg)> {
        // Parse request header
        let header: ReadChunkByIdRequestHeader = match parse_header(&am_msg) {
            Ok(h) => h,
            Err(e) => return Err((e, am_msg)),
        };

        tracing::trace!(
            file_id = header.file_id,
            path_hash = header.path_hash(),
            chunk_id = header.chunk_id(),
            offset = header.offset,
            len = header.length,
            "ReadChunkById request received"
        );

        // Look up path from FileIdRegistry, or use synthetic path if not registered
        let path = match ctx.file_id_registry().lookup(header.path_hash()) {
            Some(p) => p,
            None => {
                // Generate synthetic path from path_hash for FileId-only access
                // This allows FileId-based RPCs to work without prior path registration
                let synthetic_path = format!("/benchfs/{:08x}", header.path_hash());
                tracing::trace!(
                    "Using synthetic path {} for unregistered path_hash={:#x}",
                    synthetic_path,
                    header.path_hash()
                );
                synthetic_path
            }
        };

        tracing::trace!("Resolved path: {} for path_hash={:#x}", path, header.path_hash());

        // Check if chunk_store is IOUringChunkStore to use zero-copy path
        use crate::storage::chunk_store::IOUringChunkStore;
        use std::any::Any;

        let chunk_store_any = &*ctx.chunk_store as &dyn Any;
        let chunk_index = header.chunk_id() as u64;

        let (response_header, response_data, fixed_buffer_opt) = if let Some(io_uring_store) =
            chunk_store_any.downcast_ref::<IOUringChunkStore>()
        {
            tracing::trace!("Using zero-copy path for ReadChunkById");

            let fixed_buffer = ctx.allocator.acquire().await;
            let read_len = (header.length as usize).min(fixed_buffer.len());

            match io_uring_store
                .read_chunk_fixed(&path, chunk_index, header.offset, fixed_buffer)
                .await
            {
                Ok((bytes_read, fixed_buffer)) => {
                    let actual_bytes = bytes_read.min(read_len);
                    tracing::trace!(
                        "Read {} bytes (zero-copy) from storage (path={}, chunk={})",
                        actual_bytes,
                        path,
                        chunk_index
                    );
                    (
                        ReadChunkResponseHeader::success(actual_bytes as u64),
                        None,
                        Some((fixed_buffer, actual_bytes)),
                    )
                }
                Err(e) => {
                    tracing::error!("Failed to read chunk (zero-copy): {:?}", e);
                    (ReadChunkResponseHeader::error(-2), None, None)
                }
            }
        } else {
            // Fallback path
            tracing::trace!("Using fallback path for ReadChunkById");

            match ctx
                .chunk_store
                .read_chunk(&path, chunk_index, header.offset, header.length)
                .await
            {
                Ok(data) => {
                    let bytes_read = data.len() as u64;
                    tracing::trace!("Read {} bytes from storage", bytes_read);
                    (ReadChunkResponseHeader::success(bytes_read), Some(data), None)
                }
                Err(e) => {
                    tracing::error!("Failed to read chunk: {:?}", e);
                    (ReadChunkResponseHeader::error(-2), None, None)
                }
            }
        };

        // Send response
        if !am_msg.need_reply() {
            tracing::error!("Message does not support reply");
            return Err((
                RpcError::HandlerError("Message does not support reply".to_string()),
                am_msg,
            ));
        }

        let header_vec = zerocopy::IntoBytes::as_bytes(&response_header).to_vec();
        let header_bytes: &[u8] = &header_vec;

        let result = if let Some((mut fixed_buffer, bytes_read)) = fixed_buffer_opt {
            let data_slice = &fixed_buffer.as_mut_slice()[..bytes_read];
            let data_slices = [std::io::IoSlice::new(data_slice)];

            let proto = if crate::rpc::should_use_rdma(bytes_read as u64) {
                Some(pluvio_ucx::async_ucx::ucp::AmProto::Rndv)
            } else {
                None
            };

            unsafe {
                am_msg
                    .reply_vectorized(
                        Self::reply_stream_id() as u32,
                        header_bytes,
                        &data_slices,
                        false,
                        proto,
                    )
                    .await
            }
        } else if let Some(ref data) = response_data {
            let data_slices = [std::io::IoSlice::new(data)];

            let proto = if crate::rpc::should_use_rdma(data.len() as u64) {
                Some(pluvio_ucx::async_ucx::ucp::AmProto::Rndv)
            } else {
                None
            };

            unsafe {
                am_msg
                    .reply_vectorized(
                        Self::reply_stream_id() as u32,
                        header_bytes,
                        &data_slices,
                        false,
                        proto,
                    )
                    .await
            }
        } else {
            unsafe {
                am_msg
                    .reply_vectorized(
                        Self::reply_stream_id() as u32,
                        header_bytes,
                        &[],
                        false,
                        None,
                    )
                    .await
            }
        };

        if let Err(e) = result {
            tracing::error!("Failed to send reply: {:?}", e);
            return Err((
                RpcError::TransportError(format!("Failed to send reply: {:?}", e)),
                am_msg,
            ));
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

/// WriteChunkById RPC request
pub struct WriteChunkByIdRequest<'a> {
    header: WriteChunkByIdRequestHeader,
    data: &'a [u8],
    /// Cached IoSlice for data - lazily initialized on first call
    cached_ioslice: UnsafeCell<Option<IoSlice<'static>>>,
}

// SAFETY: WriteChunkByIdRequest is Send because all its fields are Send
unsafe impl Send for WriteChunkByIdRequest<'_> {}

impl<'a> WriteChunkByIdRequest<'a> {
    pub fn new(file_id: u64, offset: u64, data: &'a [u8]) -> Self {
        let length = data.len() as u64;

        Self {
            header: WriteChunkByIdRequestHeader::new(file_id, offset, length),
            data,
            cached_ioslice: UnsafeCell::new(None),
        }
    }

    /// Create from FileId type
    pub fn from_file_id(
        file_id: crate::rpc::file_id::FileId,
        offset: u64,
        data: &'a [u8],
    ) -> Self {
        Self::new(file_id.as_raw(), offset, data)
    }

    /// Get the data buffer
    pub fn data(&self) -> &[u8] {
        self.data
    }

    /// Get the path hash
    pub fn path_hash(&self) -> u32 {
        self.header.path_hash()
    }

    /// Get the chunk ID
    pub fn chunk_id(&self) -> u32 {
        self.header.chunk_id()
    }
}

impl AmRpc for WriteChunkByIdRequest<'_> {
    type RequestHeader = WriteChunkByIdRequestHeader;
    type ResponseHeader = WriteChunkResponseHeader;

    fn rpc_id() -> RpcId {
        RPC_WRITE_CHUNK_BY_ID
    }

    fn call_type(&self) -> AmRpcCallType {
        AmRpcCallType::Put
    }

    fn request_header(&self) -> &Self::RequestHeader {
        &self.header
    }

    fn request_data(&self) -> &[IoSlice<'_>] {
        unsafe {
            let cache = &mut *self.cached_ioslice.get();

            if cache.is_none() {
                *cache = Some(IoSlice::new(std::mem::transmute::<&[u8], &'static [u8]>(
                    self.data,
                )));
            }

            std::slice::from_ref(std::mem::transmute::<&IoSlice<'static>, &IoSlice<'_>>(
                cache.as_ref().unwrap(),
            ))
        }
    }

    fn proto(&self) -> Option<pluvio_ucx::async_ucx::ucp::AmProto> {
        if crate::rpc::should_use_rdma(self.data().len() as u64) {
            Some(pluvio_ucx::async_ucx::ucp::AmProto::Rndv)
        } else {
            None
        }
    }

    #[async_backtrace::framed]
    async fn call(&self, client: &RpcClient) -> Result<Self::ResponseHeader, RpcError> {
        client.execute(self).await
    }

    #[async_backtrace::framed]
    async fn call_no_reply(&self, client: &RpcClient) -> Result<(), RpcError> {
        client.execute_no_reply(self).await
    }

    #[async_backtrace::framed]
    #[tracing::instrument(level = "trace", name = "rpc_write_chunk_by_id_handler", skip(ctx, am_msg))]
    async fn server_handler(
        ctx: Rc<crate::rpc::handlers::RpcHandlerContext>,
        mut am_msg: AmMsg,
    ) -> Result<(crate::rpc::ServerResponse<Self::ResponseHeader>, AmMsg), (RpcError, AmMsg)> {
        // Parse request header
        let header: WriteChunkByIdRequestHeader = match parse_header(&am_msg) {
            Ok(h) => h,
            Err(e) => return Err((e, am_msg)),
        };

        tracing::trace!(
            file_id = header.file_id,
            path_hash = header.path_hash(),
            chunk_id = header.chunk_id(),
            offset = header.offset,
            len = header.length,
            "WriteChunkById request received"
        );

        if !am_msg.contains_data() {
            tracing::error!("WriteChunkById request contains no data");
            return Ok((
                crate::rpc::ServerResponse::new(WriteChunkResponseHeader::error(-22)),
                am_msg,
            ));
        }

        // Look up path from FileIdRegistry, or use synthetic path if not registered
        let path = match ctx.file_id_registry().lookup(header.path_hash()) {
            Some(p) => p,
            None => {
                // Generate synthetic path from path_hash for FileId-only access
                // This allows FileId-based RPCs to work without prior path registration
                let synthetic_path = format!("/benchfs/{:08x}", header.path_hash());
                tracing::trace!(
                    "Using synthetic path {} for unregistered path_hash={:#x}",
                    synthetic_path,
                    header.path_hash()
                );
                synthetic_path
            }
        };

        tracing::trace!("Resolved path: {} for path_hash={:#x}", path, header.path_hash());

        // Check if chunk_store is IOUringChunkStore
        use crate::storage::chunk_store::IOUringChunkStore;
        use std::any::Any;

        let data_len = header.length as usize;
        let chunk_store_any = &*ctx.chunk_store as &dyn Any;
        let chunk_index = header.chunk_id() as u64;

        let response_header = if let Some(io_uring_store) =
            chunk_store_any.downcast_ref::<IOUringChunkStore>()
        {
            tracing::trace!("Using zero-copy path for WriteChunkById");

            let mut fixed_buffer = ctx.allocator.acquire().await;

            if data_len > fixed_buffer.len() {
                tracing::error!(
                    "Data size {} exceeds registered buffer size {}",
                    data_len,
                    fixed_buffer.len()
                );
                WriteChunkResponseHeader::error(-22)
            } else {
                let buffer_slice = &mut fixed_buffer.as_mut_slice()[..data_len];

                if let Err(e) = am_msg.recv_data_single(buffer_slice).await {
                    tracing::error!("Failed to receive data (zero-copy): {:?}", e);
                    WriteChunkResponseHeader::error(-5)
                } else {
                    match io_uring_store
                        .write_chunk_fixed(&path, chunk_index, header.offset, fixed_buffer, data_len)
                        .await
                    {
                        Ok(bytes_written) => {
                            tracing::trace!(
                                "Wrote {} bytes (zero-copy) to storage (path={}, chunk={})",
                                bytes_written,
                                path,
                                chunk_index
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
            // Fallback path
            tracing::trace!("Using fallback path for WriteChunkById");

            let mut data = vec![0u8; data_len];

            if let Err(e) = am_msg.recv_data_single(&mut data).await {
                tracing::error!("Failed to receive data: {:?}", e);
                WriteChunkResponseHeader::error(-5)
            } else {
                match ctx
                    .chunk_store
                    .write_chunk(&path, chunk_index, header.offset, &data)
                    .await
                {
                    Ok(_bytes_written) => {
                        tracing::trace!(
                            "Wrote {} bytes to storage (path={}, chunk={})",
                            data.len(),
                            path,
                            chunk_index
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

        // Send response
        if !am_msg.need_reply() {
            tracing::error!("Message does not support reply");
            return Err((
                RpcError::HandlerError("Message does not support reply".to_string()),
                am_msg,
            ));
        }

        let header_vec = zerocopy::IntoBytes::as_bytes(&response_header).to_vec();
        let header_bytes: &[u8] = &header_vec;

        let result = unsafe {
            am_msg
                .reply_vectorized(
                    Self::reply_stream_id() as u32,
                    header_bytes,
                    &[],
                    false,
                    None,
                )
                .await
        };

        if let Err(e) = result {
            tracing::error!("Failed to send reply: {:?}", e);
            return Err((
                RpcError::TransportError(format!("Failed to send reply: {:?}", e)),
                am_msg,
            ));
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
// FsyncChunk RPC
// ============================================================================

/// RPC ID for fsync chunk operation
pub const RPC_FSYNC_CHUNK: RpcId = 14;

/// FsyncChunk request header with embedded path
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
pub struct FsyncChunkRequestHeader {
    /// Chunk index to fsync
    pub chunk_index: u64,

    /// Actual path length (valid bytes in path_buffer)
    pub path_len: u64,

    /// Fixed-size path buffer (256 bytes)
    pub path_buffer: [u8; MAX_RPC_PATH_LENGTH],
}

impl FsyncChunkRequestHeader {
    pub fn new(chunk_index: u64, path: &str) -> Self {
        let mut path_buffer = [0u8; MAX_RPC_PATH_LENGTH];
        let path_bytes = path.as_bytes();
        let path_len = path_bytes.len().min(MAX_RPC_PATH_LENGTH);
        path_buffer[..path_len].copy_from_slice(&path_bytes[..path_len]);

        Self {
            chunk_index,
            path_len: path_len as u64,
            path_buffer,
        }
    }

    /// Get the path as a string slice
    pub fn path(&self) -> Result<&str, std::str::Utf8Error> {
        let path_len = self.path_len as usize;
        std::str::from_utf8(&self.path_buffer[..path_len])
    }
}

/// FsyncChunk response header
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
pub struct FsyncChunkResponseHeader {
    /// Status code (0 = success, non-zero = error)
    pub status: i32,

    /// Padding for alignment
    _padding: [u8; 4],
}

impl FsyncChunkResponseHeader {
    pub fn success() -> Self {
        Self {
            status: 0,
            _padding: [0; 4],
        }
    }

    pub fn error(status: i32) -> Self {
        Self {
            status,
            _padding: [0; 4],
        }
    }

    pub fn is_success(&self) -> bool {
        self.status == 0
    }
}

/// FsyncChunk RPC request
pub struct FsyncChunkRequest {
    header: FsyncChunkRequestHeader,
}

impl FsyncChunkRequest {
    pub fn new(path: String, chunk_index: u64) -> Self {
        Self {
            header: FsyncChunkRequestHeader::new(chunk_index, &path),
        }
    }
}

impl AmRpc for FsyncChunkRequest {
    type RequestHeader = FsyncChunkRequestHeader;
    type ResponseHeader = FsyncChunkResponseHeader;

    fn rpc_id() -> RpcId {
        RPC_FSYNC_CHUNK
    }

    fn call_type(&self) -> AmRpcCallType {
        AmRpcCallType::Put // Fsync is a write-like operation
    }

    fn request_header(&self) -> &Self::RequestHeader {
        &self.header
    }

    fn request_data(&self) -> &[IoSlice<'_>] {
        &[]
    }

    #[async_backtrace::framed]
    async fn call(&self, client: &RpcClient) -> Result<Self::ResponseHeader, RpcError> {
        client.execute(self).await
    }

    #[async_backtrace::framed]
    async fn call_no_reply(&self, client: &RpcClient) -> Result<(), RpcError> {
        client.execute_no_reply(self).await
    }

    #[async_backtrace::framed]
    #[tracing::instrument(level = "trace", name = "rpc_fsync_chunk_handler", skip(ctx, am_msg))]
    async fn server_handler(
        ctx: Rc<crate::rpc::handlers::RpcHandlerContext>,
        am_msg: AmMsg,
    ) -> Result<(crate::rpc::ServerResponse<Self::ResponseHeader>, AmMsg), (RpcError, AmMsg)> {
        let header: Self::RequestHeader = match parse_header(&am_msg) {
            Ok(h) => h,
            Err(e) => {
                tracing::error!("Failed to parse FsyncChunk request header: {:?}", e);
                return Err((RpcError::InvalidHeader, am_msg));
            }
        };

        let path = match header.path() {
            Ok(p) => p.to_string(),
            Err(_) => {
                tracing::error!("Invalid path in FsyncChunk request");
                return Err((RpcError::InvalidHeader, am_msg));
            }
        };

        let chunk_index = header.chunk_index;

        tracing::trace!(
            "FsyncChunk RPC received: path={}, chunk={}",
            path,
            chunk_index
        );

        // Fsync the chunk using the chunk store
        use crate::storage::chunk_store::IOUringChunkStore;
        use std::any::Any;

        let chunk_store_any = &*ctx.chunk_store as &dyn Any;

        let response_header = if let Some(io_uring_store) =
            chunk_store_any.downcast_ref::<IOUringChunkStore>()
        {
            match io_uring_store.fsync_chunk(&path, chunk_index).await {
                Ok(()) => {
                    tracing::trace!(
                        "Fsynced chunk (path={}, chunk={})",
                        path,
                        chunk_index
                    );
                    FsyncChunkResponseHeader::success()
                }
                Err(e) => {
                    tracing::error!("Failed to fsync chunk: {:?}", e);
                    FsyncChunkResponseHeader::error(-5)
                }
            }
        } else {
            // No io_uring store, fsync is a no-op for in-memory store
            tracing::trace!(
                "FsyncChunk: No io_uring store, returning success (path={}, chunk={})",
                path,
                chunk_index
            );
            FsyncChunkResponseHeader::success()
        };

        // Send response
        if !am_msg.need_reply() {
            tracing::error!("Message does not support reply");
            return Err((
                RpcError::HandlerError("Message does not support reply".to_string()),
                am_msg,
            ));
        }

        let header_vec = zerocopy::IntoBytes::as_bytes(&response_header).to_vec();
        let header_bytes: &[u8] = &header_vec;

        let result = unsafe {
            am_msg
                .reply_vectorized(
                    Self::rpc_id() as u32,
                    header_bytes,
                    &[],
                    false,
                    None,
                )
                .await
        };

        if let Err(e) = result {
            tracing::error!("Failed to send FsyncChunk reply: {:?}", e);
            return Err((
                RpcError::TransportError(format!("Failed to send reply: {:?}", e)),
                am_msg,
            ));
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
        FsyncChunkResponseHeader::error(status)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_chunk_request_header() {
        let header = ReadChunkRequestHeader::new(5, 1024, 4096, "/test/file.txt");
        assert_eq!(header.chunk_index, 5);
        assert_eq!(header.offset, 1024);
        assert_eq!(header.length, 4096);
        assert_eq!(header.path_len, 14); // "/test/file.txt".len()
        assert_eq!(header.path().unwrap(), "/test/file.txt");

        // Verify it can be serialized
        let bytes = zerocopy::IntoBytes::as_bytes(&header);
        assert_eq!(bytes.len(), std::mem::size_of::<ReadChunkRequestHeader>());

        // Verify it can be deserialized
        let deserialized: ReadChunkRequestHeader =
            zerocopy::FromBytes::read_from_bytes(bytes).unwrap();
        assert_eq!(deserialized.chunk_index, 5);
        assert_eq!(deserialized.path().unwrap(), "/test/file.txt");
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
    #[allow(deprecated)]
    fn test_read_chunk_request() {
        let mut buffer = vec![0u8; 1024];
        let request = ReadChunkRequest::new(0, 0, 1024, "/test/file.txt".to_string(), &mut buffer);
        assert_eq!(request.header.chunk_index, 0);
        assert_eq!(request.header.length, 1024);
        assert_eq!(request.header.path_len, 14); // "/test/file.txt".len()
        assert_eq!(request.header.path().unwrap(), "/test/file.txt");

        // Buffer should be the external buffer
        assert_eq!(request.data().len(), 1024);
        assert_eq!(request.requested_length(), 1024);
    }

    #[test]
    #[allow(deprecated)]
    fn test_write_chunk_request_header() {
        let header = WriteChunkRequestHeader::new(3, 512, 2048, "/my/test/path.dat");
        assert_eq!(header.chunk_index, 3);
        assert_eq!(header.offset, 512);
        assert_eq!(header.length, 2048);
        assert_eq!(header.path_len, 17); // "/my/test/path.dat".len()
        assert_eq!(header.path().unwrap(), "/my/test/path.dat");
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
    #[allow(deprecated)]
    fn test_write_chunk_request() {
        let data = vec![0xAA; 512];
        let request = WriteChunkRequest::new(1, 0, &data[..], "/test/file.txt".to_string());

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
    #[allow(deprecated)]
    fn test_rpc_ids() {
        assert_eq!(ReadChunkRequest::rpc_id(), RPC_READ_CHUNK);
        assert_eq!(WriteChunkRequest::rpc_id(), RPC_WRITE_CHUNK);

        // Reply stream IDs should be different
        assert_eq!(ReadChunkRequest::reply_stream_id(), RPC_READ_CHUNK + 100);
        assert_eq!(WriteChunkRequest::reply_stream_id(), RPC_WRITE_CHUNK + 100);
    }
}
