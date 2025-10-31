use std::cell::UnsafeCell;
use std::io::IoSlice;
use std::path::Path;
use std::rc::Rc;

use pluvio_ucx::async_ucx::ucp::AmMsg;
use zerocopy::FromBytes;

use crate::metadata::{DirectoryMetadata, FileMetadata};
use crate::rpc::{AmRpc, AmRpcCallType, RpcClient, RpcError, RpcId};

/// RPC IDs for metadata operations
pub const RPC_METADATA_LOOKUP: RpcId = 20;
pub const RPC_METADATA_CREATE_FILE: RpcId = 21;
pub const RPC_METADATA_CREATE_DIR: RpcId = 22;
pub const RPC_METADATA_DELETE: RpcId = 23;
pub const RPC_METADATA_UPDATE: RpcId = 24;

// Maximum path length for RPC messages
const _MAX_PATH_LEN: usize = 256;

// ============================================================================
// MetadataLookup RPC
// ============================================================================

/// MetadataLookup request header
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
pub struct MetadataLookupRequestHeader {
    /// Path length
    pub path_len: u32,

    /// Padding for alignment
    _padding: [u8; 4],
}

impl MetadataLookupRequestHeader {
    pub fn new(path_len: usize) -> Self {
        Self {
            path_len: path_len as u32,
            _padding: [0; 4],
        }
    }
}

/// MetadataLookup response header
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
pub struct MetadataLookupResponseHeader {
    /// Inode number
    pub inode: u64,

    /// File size (0 for directories)
    pub size: u64,

    /// Status code (0 = success, non-zero = error)
    pub status: i32,

    /// Entry type: 0 = not found, 1 = file, 2 = directory
    pub entry_type: u8,

    /// Padding for alignment
    _padding: [u8; 3],
}

impl MetadataLookupResponseHeader {
    pub fn file(size: u64) -> Self {
        Self {
            inode: 0, // Dummy value for path-based KV design
            size,
            entry_type: 1,
            status: 0,
            _padding: [0; 3],
        }
    }

    pub fn directory() -> Self {
        Self {
            inode: 0, // Dummy value for path-based KV design
            size: 0,
            entry_type: 2,
            status: 0,
            _padding: [0; 3],
        }
    }

    pub fn not_found() -> Self {
        Self {
            inode: 0,
            size: 0,
            entry_type: 0,
            status: -1, // ENOENT
            _padding: [0; 3],
        }
    }

    pub fn error(status: i32) -> Self {
        Self {
            inode: 0,
            size: 0,
            entry_type: 0,
            status,
            _padding: [0; 3],
        }
    }

    pub fn is_success(&self) -> bool {
        self.status == 0
    }

    pub fn is_file(&self) -> bool {
        self.entry_type == 1
    }

    pub fn is_directory(&self) -> bool {
        self.entry_type == 2
    }
}

/// MetadataLookup RPC request
pub struct MetadataLookupRequest {
    header: MetadataLookupRequestHeader,
    path: String,
    /// Client's WorkerAddress for direct response
    worker_address: Vec<u8>,
    /// IoSlices: [worker_address, path]
    request_ioslice: UnsafeCell<[IoSlice<'static>; 2]>,
}

// SAFETY: MetadataLookupRequest is only used in single-threaded context (Pluvio runtime)
unsafe impl Send for MetadataLookupRequest {}

impl MetadataLookupRequest {
    pub fn new(path: String, worker_address: Vec<u8>) -> Self {
        // SAFETY: We're creating 'static IoSlices by transmuting the lifetime.
        // This is safe because:
        // 1. The buffers live as long as the MetadataLookupRequest
        // 2. The IoSlices are only accessed through request_data()
        // 3. The RPC client will only use them during the RPC call
        let ioslices = unsafe {
            let addr_slice: &'static [u8] = std::mem::transmute(worker_address.as_slice());
            let path_slice: &'static [u8] = std::mem::transmute(path.as_bytes());
            [IoSlice::new(addr_slice), IoSlice::new(path_slice)]
        };

        Self {
            header: MetadataLookupRequestHeader::new(path.len()),
            path,
            worker_address,
            request_ioslice: UnsafeCell::new(ioslices),
        }
    }

    pub fn path(&self) -> &str {
        &self.path
    }
}

impl AmRpc for MetadataLookupRequest {
    type RequestHeader = MetadataLookupRequestHeader;
    type ResponseHeader = MetadataLookupResponseHeader;

    fn rpc_id() -> RpcId {
        RPC_METADATA_LOOKUP
    }

    fn call_type(&self) -> AmRpcCallType {
        AmRpcCallType::None
    }

    fn request_header(&self) -> &Self::RequestHeader {
        &self.header
    }

    fn request_data(&self) -> &[IoSlice<'_>] {
        // SAFETY: We're returning a slice containing the IoSlices we created in new()
        // This is safe because the IoSlice lifetime is tied to self
        unsafe { &*self.request_ioslice.get() }
    }

    async fn call(&self, client: &RpcClient) -> Result<Self::ResponseHeader, RpcError> {
        client.execute(self).await
    }

    async fn call_no_reply(&self, _client: &RpcClient) -> Result<(), RpcError> {
        Err(RpcError::HandlerError(
            "MetadataLookup requires a reply".to_string(),
        ))
    }

    async fn server_handler(
        ctx: Rc<crate::rpc::handlers::RpcHandlerContext>,
        mut am_msg: AmMsg,
    ) -> Result<(crate::rpc::ServerResponse<Self::ResponseHeader>, AmMsg), (RpcError, AmMsg)> {
        // Parse request header
        let header = match am_msg
            .header()
            .get(..std::mem::size_of::<MetadataLookupRequestHeader>())
            .and_then(|bytes| {
                MetadataLookupRequestHeader::read_from_prefix(bytes)
                    .ok()
                    .map(|(h, _)| h.clone())
            }) {
            Some(h) => h,
            None => return Err((RpcError::InvalidHeader, am_msg)),
        };

        // Receive WorkerAddress and path data
        let mut worker_addr_bytes = vec![0u8; 512];
        let mut path_bytes = vec![0u8; header.path_len as usize];

        if am_msg.contains_data() {
            if let Err(_e) = am_msg.recv_data_vectored(&[
                std::io::IoSliceMut::new(&mut worker_addr_bytes),
                std::io::IoSliceMut::new(&mut path_bytes),
            ]).await {
                return Ok((
                    crate::rpc::ServerResponse::new(MetadataLookupResponseHeader::error(-2)),
                    am_msg,
                ));
            }
        }

        let path_str = String::from_utf8_lossy(&path_bytes);
        let path = Path::new(path_str.as_ref());

        // Determine response
        let response_header = if let Ok(file_meta) = ctx.metadata_manager.get_file_metadata(path) {
            MetadataLookupResponseHeader::file(file_meta.size)
        } else if let Ok(_dir_meta) = ctx.metadata_manager.get_dir_metadata(path) {
            MetadataLookupResponseHeader::directory()
        } else {
            MetadataLookupResponseHeader::not_found()
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

        // Return empty response since we already sent the response directly
        Ok((
            crate::rpc::ServerResponse::new(response_header),
            am_msg,
        ))
    }

    fn error_response(error: &RpcError) -> Self::ResponseHeader {
        let status = match error {
            RpcError::InvalidHeader => -1,
            RpcError::TransportError(_) => -2,
            RpcError::HandlerError(_) => -3,
            RpcError::ConnectionError(_) => -4,
            RpcError::Timeout => -5,
        };
        MetadataLookupResponseHeader::error(status)
    }
}

// ============================================================================
// MetadataCreateFile RPC
// ============================================================================

/// MetadataCreateFile request header
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
pub struct MetadataCreateFileRequestHeader {
    /// Initial file size
    pub size: u64,

    /// File mode (permissions)
    pub mode: u32,

    /// Path length
    pub path_len: u32,
}

impl MetadataCreateFileRequestHeader {
    pub fn new(size: u64, mode: u32, path_len: usize) -> Self {
        Self {
            size,
            mode,
            path_len: path_len as u32,
        }
    }
}

/// MetadataCreateFile response header
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
pub struct MetadataCreateFileResponseHeader {
    /// Assigned inode number
    pub inode: u64,

    /// Status code (0 = success, non-zero = error)
    pub status: i32,

    /// Padding for alignment
    _padding: [u8; 4],
}

impl MetadataCreateFileResponseHeader {
    pub fn success(inode: u64) -> Self {
        Self {
            inode,
            status: 0,
            _padding: [0; 4],
        }
    }

    pub fn error(status: i32) -> Self {
        Self {
            inode: 0,
            status,
            _padding: [0; 4],
        }
    }

    pub fn is_success(&self) -> bool {
        self.status == 0
    }
}

/// MetadataCreateFile RPC request
pub struct MetadataCreateFileRequest {
    header: MetadataCreateFileRequestHeader,
    path: String,
    /// Client's WorkerAddress for direct response
    worker_address: Vec<u8>,
    /// IoSlices: [worker_address, path]
    request_ioslice: UnsafeCell<[IoSlice<'static>; 2]>,
}

// SAFETY: MetadataCreateFileRequest is only used in single-threaded context (Pluvio runtime)
unsafe impl Send for MetadataCreateFileRequest {}

impl MetadataCreateFileRequest {
    pub fn new(path: String, size: u64, mode: u32, worker_address: Vec<u8>) -> Self {
        let ioslices = unsafe {
            let addr_slice: &'static [u8] = std::mem::transmute(worker_address.as_slice());
            let path_slice: &'static [u8] = std::mem::transmute(path.as_bytes());
            [IoSlice::new(addr_slice), IoSlice::new(path_slice)]
        };

        Self {
            header: MetadataCreateFileRequestHeader::new(size, mode, path.len()),
            path,
            worker_address,
            request_ioslice: UnsafeCell::new(ioslices),
        }
    }

    pub fn path(&self) -> &str {
        &self.path
    }
}

impl AmRpc for MetadataCreateFileRequest {
    type RequestHeader = MetadataCreateFileRequestHeader;
    type ResponseHeader = MetadataCreateFileResponseHeader;

    fn rpc_id() -> RpcId {
        RPC_METADATA_CREATE_FILE
    }

    fn call_type(&self) -> AmRpcCallType {
        AmRpcCallType::None
    }

    fn request_header(&self) -> &Self::RequestHeader {
        &self.header
    }

    fn request_data(&self) -> &[IoSlice<'_>] {
        unsafe { &*self.request_ioslice.get() }
    }

    async fn call(&self, client: &RpcClient) -> Result<Self::ResponseHeader, RpcError> {
        client.execute(self).await
    }

    async fn call_no_reply(&self, _client: &RpcClient) -> Result<(), RpcError> {
        Err(RpcError::HandlerError(
            "MetadataCreateFile requires a reply".to_string(),
        ))
    }

    async fn server_handler(
        ctx: Rc<crate::rpc::handlers::RpcHandlerContext>,
        mut am_msg: AmMsg,
    ) -> Result<(crate::rpc::ServerResponse<Self::ResponseHeader>, AmMsg), (RpcError, AmMsg)> {
        // Parse request header
        let header = match am_msg
            .header()
            .get(..std::mem::size_of::<MetadataCreateFileRequestHeader>())
            .and_then(|bytes| {
                MetadataCreateFileRequestHeader::read_from_prefix(bytes)
                    .ok()
                    .map(|(h, _)| h.clone())
            }) {
            Some(h) => h,
            None => return Err((RpcError::InvalidHeader, am_msg)),
        };

        // Receive WorkerAddress and path data
        let mut worker_addr_bytes = vec![0u8; 512];
        let mut path_bytes = vec![0u8; header.path_len as usize];

        if am_msg.contains_data() {
            if let Err(_e) = am_msg.recv_data_vectored(&[
                std::io::IoSliceMut::new(&mut worker_addr_bytes),
                std::io::IoSliceMut::new(&mut path_bytes),
            ]).await {
                return Ok((
                    crate::rpc::ServerResponse::new(MetadataCreateFileResponseHeader::error(-2)),
                    am_msg,
                ));
            }
        }

        let path_str = String::from_utf8_lossy(&path_bytes);

        // Create file metadata (no inode in path-based KV design)
        let file_meta = FileMetadata::new(path_str.to_string(), header.size);

        // Store file metadata and prepare response
        let response_header = match ctx.metadata_manager.store_file_metadata(file_meta) {
            Ok(()) => MetadataCreateFileResponseHeader::success(0), // Dummy inode
            Err(crate::metadata::manager::MetadataError::AlreadyExists(ref path)) => {
                tracing::warn!("File already exists: {}", path);
                MetadataCreateFileResponseHeader::error(-17) // EEXIST
            }
            Err(e) => {
                tracing::error!("Failed to store file metadata: {:?}", e);
                MetadataCreateFileResponseHeader::error(-5) // EIO
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

        // Return empty response since we already sent the response directly
        Ok((
            crate::rpc::ServerResponse::new(response_header),
            am_msg,
        ))
    }

    fn error_response(error: &RpcError) -> Self::ResponseHeader {
        let status = match error {
            RpcError::InvalidHeader => -1,
            RpcError::TransportError(_) => -2,
            RpcError::HandlerError(_) => -3,
            RpcError::ConnectionError(_) => -4,
            RpcError::Timeout => -5,
        };
        MetadataCreateFileResponseHeader::error(status)
    }
}

// ============================================================================
// MetadataCreateDir RPC
// ============================================================================

/// MetadataCreateDir request header
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
pub struct MetadataCreateDirRequestHeader {
    /// Directory mode (permissions)
    pub mode: u32,

    /// Path length
    pub path_len: u32,
}

impl MetadataCreateDirRequestHeader {
    pub fn new(mode: u32, path_len: usize) -> Self {
        Self {
            mode,
            path_len: path_len as u32,
        }
    }
}

/// MetadataCreateDir response header (same as CreateFile)
pub type MetadataCreateDirResponseHeader = MetadataCreateFileResponseHeader;

/// MetadataCreateDir RPC request
pub struct MetadataCreateDirRequest {
    header: MetadataCreateDirRequestHeader,
    path: String,
    /// Client's WorkerAddress for direct response
    worker_address: Vec<u8>,
    /// IoSlices: [worker_address, path]
    request_ioslice: UnsafeCell<[IoSlice<'static>; 2]>,
}

// SAFETY: MetadataCreateDirRequest is only used in single-threaded context (Pluvio runtime)
unsafe impl Send for MetadataCreateDirRequest {}

impl MetadataCreateDirRequest {
    pub fn new(path: String, mode: u32, worker_address: Vec<u8>) -> Self {
        // SAFETY: Same as MetadataLookupRequest
        let ioslices = unsafe {
            let addr_slice: &'static [u8] = std::mem::transmute(worker_address.as_slice());
            let path_slice: &'static [u8] = std::mem::transmute(path.as_bytes());
            [IoSlice::new(addr_slice), IoSlice::new(path_slice)]
        };

        Self {
            header: MetadataCreateDirRequestHeader::new(mode, path.len()),
            path,
            worker_address,
            request_ioslice: UnsafeCell::new(ioslices),
        }
    }

    pub fn path(&self) -> &str {
        &self.path
    }
}

impl AmRpc for MetadataCreateDirRequest {
    type RequestHeader = MetadataCreateDirRequestHeader;
    type ResponseHeader = MetadataCreateDirResponseHeader;

    fn rpc_id() -> RpcId {
        RPC_METADATA_CREATE_DIR
    }

    fn call_type(&self) -> AmRpcCallType {
        AmRpcCallType::None
    }

    fn request_header(&self) -> &Self::RequestHeader {
        &self.header
    }

    fn request_data(&self) -> &[IoSlice<'_>] {
        // SAFETY: Same as MetadataLookupRequest
        unsafe { &*self.request_ioslice.get() }
    }

    async fn call(&self, client: &RpcClient) -> Result<Self::ResponseHeader, RpcError> {
        client.execute(self).await
    }

    async fn call_no_reply(&self, _client: &RpcClient) -> Result<(), RpcError> {
        Err(RpcError::HandlerError(
            "MetadataCreateDir requires a reply".to_string(),
        ))
    }

    async fn server_handler(
        ctx: Rc<crate::rpc::handlers::RpcHandlerContext>,
        mut am_msg: AmMsg,
    ) -> Result<(crate::rpc::ServerResponse<Self::ResponseHeader>, AmMsg), (RpcError, AmMsg)> {
        // Parse request header
        let header = match am_msg
            .header()
            .get(..std::mem::size_of::<MetadataCreateDirRequestHeader>())
            .and_then(|bytes| {
                MetadataCreateDirRequestHeader::read_from_prefix(bytes)
                    .ok()
                    .map(|(h, _)| h.clone())
            }) {
            Some(h) => h,
            None => return Err((RpcError::InvalidHeader, am_msg)),
        };

        // Receive WorkerAddress and path data
        let mut worker_addr_bytes = vec![0u8; 512];
        let mut path_bytes = vec![0u8; header.path_len as usize];
        if am_msg.contains_data() {
            if let Err(_e) = am_msg.recv_data_vectored(&[
                std::io::IoSliceMut::new(&mut worker_addr_bytes),
                std::io::IoSliceMut::new(&mut path_bytes),
            ]).await {
                let response_header = MetadataCreateDirResponseHeader::error(-2);
                // Send error response directly
                let _ = ctx.send_response_direct(
                    &worker_addr_bytes,
                    Self::reply_stream_id(),
                    Self::rpc_id(),
                    &response_header,
                    None,
                ).await;
                return Ok((crate::rpc::ServerResponse::new(response_header), am_msg));
            }
        }
        let path_str = String::from_utf8_lossy(&path_bytes);

        // Generate inode and create directory metadata
        let inode = ctx.metadata_manager.generate_inode();
        let dir_meta = DirectoryMetadata::new(inode, path_str.to_string());

        // Store directory metadata
        let response_header = match ctx.metadata_manager.store_dir_metadata(dir_meta) {
            Ok(()) => MetadataCreateDirResponseHeader::success(inode),
            Err(crate::metadata::manager::MetadataError::AlreadyExists(ref path)) => {
                tracing::warn!("Directory already exists: {}", path);
                MetadataCreateDirResponseHeader::error(-17) // EEXIST
            }
            Err(e) => {
                tracing::error!("Failed to store directory metadata: {:?}", e);
                MetadataCreateDirResponseHeader::error(-5) // EIO
            }
        };

        // Send response directly to client
        if let Err(e) = ctx.send_response_direct(
            &worker_addr_bytes,
            Self::reply_stream_id(),
            Self::rpc_id(),
            &response_header,
            None,
        ).await {
            tracing::error!("Failed to send MetadataCreateDir response: {:?}", e);
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
        MetadataCreateDirResponseHeader::error(status)
    }
}

// ============================================================================
// MetadataDelete RPC
// ============================================================================

/// MetadataDelete request header
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
pub struct MetadataDeleteRequestHeader {
    /// Path length
    pub path_len: u32,

    /// Entry type: 1 = file, 2 = directory
    pub entry_type: u8,

    /// Padding for alignment
    _padding: [u8; 3],
}

impl MetadataDeleteRequestHeader {
    pub fn file(path_len: usize) -> Self {
        Self {
            path_len: path_len as u32,
            entry_type: 1,
            _padding: [0; 3],
        }
    }

    pub fn directory(path_len: usize) -> Self {
        Self {
            path_len: path_len as u32,
            entry_type: 2,
            _padding: [0; 3],
        }
    }
}

/// MetadataDelete response header
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
pub struct MetadataDeleteResponseHeader {
    /// Status code (0 = success, non-zero = error)
    pub status: i32,

    /// Padding for alignment
    _padding: [u8; 4],
}

impl MetadataDeleteResponseHeader {
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

/// MetadataDelete RPC request
pub struct MetadataDeleteRequest {
    header: MetadataDeleteRequestHeader,
    path: String,
    /// Client's WorkerAddress for direct response
    worker_address: Vec<u8>,
    /// IoSlices: [worker_address, path]
    request_ioslice: UnsafeCell<[IoSlice<'static>; 2]>,
}

// SAFETY: MetadataDeleteRequest is only used in single-threaded context (Pluvio runtime)
unsafe impl Send for MetadataDeleteRequest {}

impl MetadataDeleteRequest {
    pub fn delete_file(path: String, worker_address: Vec<u8>) -> Self {
        // SAFETY: Same as MetadataLookupRequest
        let ioslices = unsafe {
            let addr_slice: &'static [u8] = std::mem::transmute(worker_address.as_slice());
            let path_slice: &'static [u8] = std::mem::transmute(path.as_bytes());
            [IoSlice::new(addr_slice), IoSlice::new(path_slice)]
        };

        Self {
            header: MetadataDeleteRequestHeader::file(path.len()),
            path,
            worker_address,
            request_ioslice: UnsafeCell::new(ioslices),
        }
    }

    pub fn delete_directory(path: String, worker_address: Vec<u8>) -> Self {
        // SAFETY: Same as MetadataLookupRequest
        let ioslices = unsafe {
            let addr_slice: &'static [u8] = std::mem::transmute(worker_address.as_slice());
            let path_slice: &'static [u8] = std::mem::transmute(path.as_bytes());
            [IoSlice::new(addr_slice), IoSlice::new(path_slice)]
        };

        Self {
            header: MetadataDeleteRequestHeader::directory(path.len()),
            path,
            worker_address,
            request_ioslice: UnsafeCell::new(ioslices),
        }
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn is_file(&self) -> bool {
        self.header.entry_type == 1
    }

    pub fn is_directory(&self) -> bool {
        self.header.entry_type == 2
    }
}

impl AmRpc for MetadataDeleteRequest {
    type RequestHeader = MetadataDeleteRequestHeader;
    type ResponseHeader = MetadataDeleteResponseHeader;

    fn rpc_id() -> RpcId {
        RPC_METADATA_DELETE
    }

    fn call_type(&self) -> AmRpcCallType {
        AmRpcCallType::None
    }

    fn request_header(&self) -> &Self::RequestHeader {
        &self.header
    }

    fn request_data(&self) -> &[IoSlice<'_>] {
        // SAFETY: Same as MetadataLookupRequest
        unsafe { &*self.request_ioslice.get() }
    }

    async fn call(&self, client: &RpcClient) -> Result<Self::ResponseHeader, RpcError> {
        client.execute(self).await
    }

    async fn call_no_reply(&self, _client: &RpcClient) -> Result<(), RpcError> {
        Err(RpcError::HandlerError(
            "MetadataDelete requires a reply".to_string(),
        ))
    }

    async fn server_handler(
        ctx: Rc<crate::rpc::handlers::RpcHandlerContext>,
        mut am_msg: AmMsg,
    ) -> Result<(crate::rpc::ServerResponse<Self::ResponseHeader>, AmMsg), (RpcError, AmMsg)> {
        // Parse request header
        let header = match am_msg
            .header()
            .get(..std::mem::size_of::<MetadataDeleteRequestHeader>())
            .and_then(|bytes| {
                MetadataDeleteRequestHeader::read_from_prefix(bytes)
                    .ok()
                    .map(|(h, _)| h.clone())
            }) {
            Some(h) => h,
            None => return Err((RpcError::InvalidHeader, am_msg)),
        };

        // Receive WorkerAddress and path data
        let mut worker_addr_bytes = vec![0u8; 512];
        let mut path_bytes = vec![0u8; header.path_len as usize];
        if am_msg.contains_data() {
            if let Err(_e) = am_msg.recv_data_vectored(&[
                std::io::IoSliceMut::new(&mut worker_addr_bytes),
                std::io::IoSliceMut::new(&mut path_bytes),
            ]).await {
                let response_header = MetadataDeleteResponseHeader::error(-2);
                let _ = ctx.send_response_direct(
                    &worker_addr_bytes,
                    Self::reply_stream_id(),
                    Self::rpc_id(),
                    &response_header,
                    None,
                ).await;
                return Ok((crate::rpc::ServerResponse::new(response_header), am_msg));
            }
        }
        let path_str = String::from_utf8_lossy(&path_bytes);
        let path = Path::new(path_str.as_ref());

        // Delete based on entry type
        let response_header = if header.entry_type == 1 {
            // Delete file
            match ctx.metadata_manager.remove_file_metadata(path) {
                Ok(()) => MetadataDeleteResponseHeader::success(),
                Err(_e) => MetadataDeleteResponseHeader::error(-2), // ENOENT
            }
        } else if header.entry_type == 2 {
            // Delete directory
            match ctx.metadata_manager.remove_dir_metadata(path) {
                Ok(()) => MetadataDeleteResponseHeader::success(),
                Err(_e) => MetadataDeleteResponseHeader::error(-2), // ENOENT
            }
        } else {
            MetadataDeleteResponseHeader::error(-22) // EINVAL
        };

        // Send response directly to client
        if let Err(e) = ctx.send_response_direct(
            &worker_addr_bytes,
            Self::reply_stream_id(),
            Self::rpc_id(),
            &response_header,
            None,
        ).await {
            tracing::error!("Failed to send MetadataDelete response: {:?}", e);
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
        MetadataDeleteResponseHeader::error(status)
    }
}

// ============================================================================
// MetadataUpdate RPC
// ============================================================================

/// Update mask bits for MetadataUpdate
const UPDATE_SIZE: u8 = 1 << 0; // Update file size
const UPDATE_MODE: u8 = 1 << 1; // Update file mode/permissions

/// MetadataUpdate request header
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
pub struct MetadataUpdateRequestHeader {
    /// New file size (if UPDATE_SIZE is set)
    pub new_size: u64,

    /// New file mode (if UPDATE_MODE is set)
    pub new_mode: u32,

    /// Path length
    pub path_len: u32,

    /// Update mask (which fields to update)
    pub update_mask: u8,

    /// Padding for alignment
    _padding: [u8; 7],
}

impl MetadataUpdateRequestHeader {
    pub fn new(path_len: usize) -> Self {
        Self {
            new_size: 0,
            new_mode: 0,
            path_len: path_len as u32,
            update_mask: 0,
            _padding: [0; 7],
        }
    }

    pub fn with_size(mut self, size: u64) -> Self {
        self.new_size = size;
        self.update_mask |= UPDATE_SIZE;
        self
    }

    pub fn with_mode(mut self, mode: u32) -> Self {
        self.new_mode = mode;
        self.update_mask |= UPDATE_MODE;
        self
    }

    pub fn should_update_size(&self) -> bool {
        self.update_mask & UPDATE_SIZE != 0
    }

    pub fn should_update_mode(&self) -> bool {
        self.update_mask & UPDATE_MODE != 0
    }
}

/// MetadataUpdate response header
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
pub struct MetadataUpdateResponseHeader {
    /// Status code (0 = success, non-zero = error)
    pub status: i32,

    /// Padding for alignment
    _padding: [u8; 4],
}

impl MetadataUpdateResponseHeader {
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

/// MetadataUpdate RPC request
pub struct MetadataUpdateRequest {
    header: MetadataUpdateRequestHeader,
    path: String,
    /// IoSlice for the path data
    path_ioslice: UnsafeCell<IoSlice<'static>>,
}

// SAFETY: MetadataUpdateRequest is only used in single-threaded context (Pluvio runtime)
unsafe impl Send for MetadataUpdateRequest {}

impl MetadataUpdateRequest {
    pub fn new(path: String) -> Self {
        // SAFETY: Same as MetadataLookupRequest
        let ioslice = unsafe {
            let slice: &'static [u8] = std::mem::transmute(path.as_bytes());
            IoSlice::new(slice)
        };

        Self {
            header: MetadataUpdateRequestHeader::new(path.len()),
            path,
            path_ioslice: UnsafeCell::new(ioslice),
        }
    }

    pub fn with_size(mut self, size: u64) -> Self {
        self.header = self.header.with_size(size);
        self
    }

    pub fn with_mode(mut self, mode: u32) -> Self {
        self.header = self.header.with_mode(mode);
        self
    }

    pub fn path(&self) -> &str {
        &self.path
    }
}

impl AmRpc for MetadataUpdateRequest {
    type RequestHeader = MetadataUpdateRequestHeader;
    type ResponseHeader = MetadataUpdateResponseHeader;

    fn rpc_id() -> RpcId {
        RPC_METADATA_UPDATE
    }

    fn call_type(&self) -> AmRpcCallType {
        AmRpcCallType::None
    }

    fn request_header(&self) -> &Self::RequestHeader {
        &self.header
    }

    fn request_data(&self) -> &[IoSlice<'_>] {
        unsafe { std::slice::from_ref(&*self.path_ioslice.get()) }
    }

    async fn call(&self, client: &RpcClient) -> Result<Self::ResponseHeader, RpcError> {
        client.execute(self).await
    }

    async fn call_no_reply(&self, _client: &RpcClient) -> Result<(), RpcError> {
        Err(RpcError::HandlerError(
            "MetadataUpdate requires a reply".to_string(),
        ))
    }

    async fn server_handler(
        ctx: Rc<crate::rpc::handlers::RpcHandlerContext>,
        mut am_msg: AmMsg,
    ) -> Result<(crate::rpc::ServerResponse<Self::ResponseHeader>, AmMsg), (RpcError, AmMsg)> {
        // Parse request header
        let header = match am_msg
            .header()
            .get(..std::mem::size_of::<MetadataUpdateRequestHeader>())
            .and_then(|bytes| {
                MetadataUpdateRequestHeader::read_from_prefix(bytes)
                    .ok()
                    .map(|(h, _)| h.clone())
            }) {
            Some(h) => h,
            None => return Err((RpcError::InvalidHeader, am_msg)),
        };

        // Receive path data
        let mut path_bytes = vec![0u8; header.path_len as usize];
        if am_msg.contains_data() {
            let mut ioslices = vec![std::io::IoSliceMut::new(&mut path_bytes)];
            if let Err(_e) = am_msg.recv_data_vectored(&mut ioslices).await {
                return Ok((
                    crate::rpc::ServerResponse::new(MetadataUpdateResponseHeader::error(-2)),
                    am_msg,
                ));
            }
        }
        let path_str = String::from_utf8_lossy(&path_bytes);
        let path = Path::new(path_str.as_ref());

        // Get current file metadata
        let mut file_meta = match ctx.metadata_manager.get_file_metadata(path) {
            Ok(meta) => meta,
            Err(_) => {
                return Ok((
                    crate::rpc::ServerResponse::new(MetadataUpdateResponseHeader::error(-2)), // ENOENT
                    am_msg,
                ));
            }
        };

        // Update size if requested
        if header.should_update_size() {
            file_meta.size = header.new_size;
            // chunk_count is calculated on demand via calculate_chunk_count()
        }

        // Note: Mode update would be handled here if FileMetadata supported it

        // Store updated metadata
        match ctx.metadata_manager.update_file_metadata(file_meta) {
            Ok(()) => Ok((
                crate::rpc::ServerResponse::new(MetadataUpdateResponseHeader::success()),
                am_msg,
            )),
            Err(_e) => Ok((
                crate::rpc::ServerResponse::new(MetadataUpdateResponseHeader::error(-5)), // EIO
                am_msg,
            )),
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
        MetadataUpdateResponseHeader::error(status)
    }
}

// ============================================================================
// Helper functions
// ============================================================================

/// Convert FileMetadata to lookup response
pub fn file_metadata_to_lookup_response(metadata: &FileMetadata) -> MetadataLookupResponseHeader {
    MetadataLookupResponseHeader::file(metadata.size)
}

/// Convert DirectoryMetadata to lookup response
pub fn dir_metadata_to_lookup_response(
    _metadata: &DirectoryMetadata,
) -> MetadataLookupResponseHeader {
    MetadataLookupResponseHeader::directory()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metadata_lookup_request_header() {
        let header = MetadataLookupRequestHeader::new(10);
        assert_eq!(header.path_len, 10);

        // Verify it can be serialized
        let bytes = zerocopy::IntoBytes::as_bytes(&header);
        assert_eq!(
            bytes.len(),
            std::mem::size_of::<MetadataLookupRequestHeader>()
        );
    }

    #[test]
    fn test_metadata_lookup_response_header() {
        let file_resp = MetadataLookupResponseHeader::file(1024);
        assert!(file_resp.is_success());
        assert!(file_resp.is_file());
        assert!(!file_resp.is_directory());
        assert_eq!(file_resp.size, 1024);
        // inode is dummy value in path-based KV design

        let dir_resp = MetadataLookupResponseHeader::directory();
        assert!(dir_resp.is_success());
        assert!(!dir_resp.is_file());
        assert!(dir_resp.is_directory());
        // inode is dummy value in path-based KV design

        let not_found = MetadataLookupResponseHeader::not_found();
        assert!(!not_found.is_success());
        assert_eq!(not_found.entry_type, 0);
    }

    #[test]
    fn test_metadata_lookup_request() {
        let dummy_worker_addr = vec![0u8; 512]; // Dummy WorkerAddress for testing
        let request = MetadataLookupRequest::new("/foo/bar.txt".to_string(), dummy_worker_addr);
        assert_eq!(request.path(), "/foo/bar.txt");
        assert_eq!(request.header.path_len, 12);
    }

    #[test]
    fn test_metadata_create_file_request() {
        let dummy_worker_addr = vec![0u8; 512]; // Dummy WorkerAddress for testing
        let request = MetadataCreateFileRequest::new("/new.txt".to_string(), 0, 0o644, dummy_worker_addr);
        assert_eq!(request.path(), "/new.txt");
        assert_eq!(request.header.size, 0);
        assert_eq!(request.header.mode, 0o644);
        assert_eq!(request.header.path_len, 8);
    }

    #[test]
    fn test_metadata_create_file_response() {
        let success = MetadataCreateFileResponseHeader::success(0); // Dummy inode in path-based KV
        assert!(success.is_success());
        // inode is dummy value in path-based KV design

        let error = MetadataCreateFileResponseHeader::error(-1);
        assert!(!error.is_success());
    }

    #[test]
    fn test_metadata_create_dir_request() {
        let request = MetadataCreateDirRequest::new("/newdir".to_string(), 0o755, vec![1, 2, 3, 4]);
        assert_eq!(request.path(), "/newdir");
        assert_eq!(request.header.mode, 0o755);
        assert_eq!(request.header.path_len, 7);
    }

    #[test]
    fn test_metadata_delete_request() {
        let file_req = MetadataDeleteRequest::delete_file("/file.txt".to_string(), vec![1, 2, 3, 4]);
        assert_eq!(file_req.path(), "/file.txt");
        assert!(file_req.is_file());
        assert!(!file_req.is_directory());

        let dir_req = MetadataDeleteRequest::delete_directory("/dir".to_string(), vec![1, 2, 3, 4]);
        assert_eq!(dir_req.path(), "/dir");
        assert!(!dir_req.is_file());
        assert!(dir_req.is_directory());
    }

    #[test]
    fn test_metadata_delete_response() {
        let success = MetadataDeleteResponseHeader::success();
        assert!(success.is_success());

        let error = MetadataDeleteResponseHeader::error(-2);
        assert!(!error.is_success());
        assert_eq!(error.status, -2);
    }

    #[test]
    fn test_rpc_ids() {
        assert_eq!(MetadataLookupRequest::rpc_id(), RPC_METADATA_LOOKUP);
        assert_eq!(
            MetadataCreateFileRequest::rpc_id(),
            RPC_METADATA_CREATE_FILE
        );
        assert_eq!(MetadataCreateDirRequest::rpc_id(), RPC_METADATA_CREATE_DIR);
        assert_eq!(MetadataDeleteRequest::rpc_id(), RPC_METADATA_DELETE);
    }

    #[test]
    fn test_helper_functions() {
        let file_meta = FileMetadata::new("/test.txt".to_string(), 2048);
        let resp = file_metadata_to_lookup_response(&file_meta);
        assert!(resp.is_file());
        assert_eq!(resp.size, 2048);
        // inode is dummy value in path-based KV design

        let dir_meta = DirectoryMetadata::new(2, "/testdir".to_string());
        let resp = dir_metadata_to_lookup_response(&dir_meta);
        assert!(resp.is_directory());
        // inode is dummy value in path-based KV design
    }
}
