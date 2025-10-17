use pluvio_ucx::async_ucx::ucp::AmMsg;

use crate::metadata::{FileMetadata, DirectoryMetadata};
use crate::rpc::{AmRpc, AmRpcCallType, RpcClient, RpcError, RpcId};

/// RPC IDs for metadata operations
pub const RPC_METADATA_LOOKUP: RpcId = 20;
pub const RPC_METADATA_CREATE_FILE: RpcId = 21;
pub const RPC_METADATA_CREATE_DIR: RpcId = 22;
pub const RPC_METADATA_DELETE: RpcId = 23;
pub const RPC_METADATA_UPDATE: RpcId = 24;

// Maximum path length for RPC messages
const MAX_PATH_LEN: usize = 256;

// ============================================================================
// MetadataLookup RPC
// ============================================================================

/// MetadataLookup request header
#[repr(C)]
#[derive(Debug, Clone, Copy, zerocopy::FromBytes, zerocopy::IntoBytes, zerocopy::KnownLayout, zerocopy::Immutable)]
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
#[derive(Debug, Clone, Copy, zerocopy::FromBytes, zerocopy::IntoBytes, zerocopy::KnownLayout, zerocopy::Immutable)]
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
    pub fn file(inode: u64, size: u64) -> Self {
        Self {
            inode,
            size,
            entry_type: 1,
            status: 0,
            _padding: [0; 3],
        }
    }

    pub fn directory(inode: u64) -> Self {
        Self {
            inode,
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
}

impl MetadataLookupRequest {
    pub fn new(path: String) -> Self {
        Self {
            header: MetadataLookupRequestHeader::new(path.len()),
            path,
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

    async fn call(&self, client: &RpcClient) -> Result<Self::ResponseHeader, RpcError> {
        client.execute(self).await
    }

    async fn call_no_reply(&self, _client: &RpcClient) -> Result<(), RpcError> {
        Err(RpcError::HandlerError(
            "MetadataLookup requires a reply".to_string(),
        ))
    }

    async fn server_handler(_am_msg: AmMsg) -> Result<Self::ResponseHeader, RpcError> {
        Err(RpcError::HandlerError(
            "Server handler not implemented yet".to_string(),
        ))
    }
}

// ============================================================================
// MetadataCreateFile RPC
// ============================================================================

/// MetadataCreateFile request header
#[repr(C)]
#[derive(Debug, Clone, Copy, zerocopy::FromBytes, zerocopy::IntoBytes, zerocopy::KnownLayout, zerocopy::Immutable)]
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
#[derive(Debug, Clone, Copy, zerocopy::FromBytes, zerocopy::IntoBytes, zerocopy::KnownLayout, zerocopy::Immutable)]
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
}

impl MetadataCreateFileRequest {
    pub fn new(path: String, size: u64, mode: u32) -> Self {
        Self {
            header: MetadataCreateFileRequestHeader::new(size, mode, path.len()),
            path,
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

    async fn call(&self, client: &RpcClient) -> Result<Self::ResponseHeader, RpcError> {
        client.execute(self).await
    }

    async fn call_no_reply(&self, _client: &RpcClient) -> Result<(), RpcError> {
        Err(RpcError::HandlerError(
            "MetadataCreateFile requires a reply".to_string(),
        ))
    }

    async fn server_handler(_am_msg: AmMsg) -> Result<Self::ResponseHeader, RpcError> {
        Err(RpcError::HandlerError(
            "Server handler not implemented yet".to_string(),
        ))
    }
}

// ============================================================================
// MetadataCreateDir RPC
// ============================================================================

/// MetadataCreateDir request header
#[repr(C)]
#[derive(Debug, Clone, Copy, zerocopy::FromBytes, zerocopy::IntoBytes, zerocopy::KnownLayout, zerocopy::Immutable)]
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
}

impl MetadataCreateDirRequest {
    pub fn new(path: String, mode: u32) -> Self {
        Self {
            header: MetadataCreateDirRequestHeader::new(mode, path.len()),
            path,
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

    async fn call(&self, client: &RpcClient) -> Result<Self::ResponseHeader, RpcError> {
        client.execute(self).await
    }

    async fn call_no_reply(&self, _client: &RpcClient) -> Result<(), RpcError> {
        Err(RpcError::HandlerError(
            "MetadataCreateDir requires a reply".to_string(),
        ))
    }

    async fn server_handler(_am_msg: AmMsg) -> Result<Self::ResponseHeader, RpcError> {
        Err(RpcError::HandlerError(
            "Server handler not implemented yet".to_string(),
        ))
    }
}

// ============================================================================
// MetadataDelete RPC
// ============================================================================

/// MetadataDelete request header
#[repr(C)]
#[derive(Debug, Clone, Copy, zerocopy::FromBytes, zerocopy::IntoBytes, zerocopy::KnownLayout, zerocopy::Immutable)]
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
#[derive(Debug, Clone, Copy, zerocopy::FromBytes, zerocopy::IntoBytes, zerocopy::KnownLayout, zerocopy::Immutable)]
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
}

impl MetadataDeleteRequest {
    pub fn delete_file(path: String) -> Self {
        Self {
            header: MetadataDeleteRequestHeader::file(path.len()),
            path,
        }
    }

    pub fn delete_directory(path: String) -> Self {
        Self {
            header: MetadataDeleteRequestHeader::directory(path.len()),
            path,
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

    async fn call(&self, client: &RpcClient) -> Result<Self::ResponseHeader, RpcError> {
        client.execute(self).await
    }

    async fn call_no_reply(&self, _client: &RpcClient) -> Result<(), RpcError> {
        Err(RpcError::HandlerError(
            "MetadataDelete requires a reply".to_string(),
        ))
    }

    async fn server_handler(_am_msg: AmMsg) -> Result<Self::ResponseHeader, RpcError> {
        Err(RpcError::HandlerError(
            "Server handler not implemented yet".to_string(),
        ))
    }
}

// ============================================================================
// Helper functions
// ============================================================================

/// Convert FileMetadata to lookup response
pub fn file_metadata_to_lookup_response(metadata: &FileMetadata) -> MetadataLookupResponseHeader {
    MetadataLookupResponseHeader::file(metadata.inode, metadata.size)
}

/// Convert DirectoryMetadata to lookup response
pub fn dir_metadata_to_lookup_response(metadata: &DirectoryMetadata) -> MetadataLookupResponseHeader {
    MetadataLookupResponseHeader::directory(metadata.inode)
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
        assert_eq!(bytes.len(), std::mem::size_of::<MetadataLookupRequestHeader>());
    }

    #[test]
    fn test_metadata_lookup_response_header() {
        let file_resp = MetadataLookupResponseHeader::file(42, 1024);
        assert!(file_resp.is_success());
        assert!(file_resp.is_file());
        assert!(!file_resp.is_directory());
        assert_eq!(file_resp.inode, 42);
        assert_eq!(file_resp.size, 1024);

        let dir_resp = MetadataLookupResponseHeader::directory(99);
        assert!(dir_resp.is_success());
        assert!(!dir_resp.is_file());
        assert!(dir_resp.is_directory());
        assert_eq!(dir_resp.inode, 99);

        let not_found = MetadataLookupResponseHeader::not_found();
        assert!(!not_found.is_success());
        assert_eq!(not_found.entry_type, 0);
    }

    #[test]
    fn test_metadata_lookup_request() {
        let request = MetadataLookupRequest::new("/foo/bar.txt".to_string());
        assert_eq!(request.path(), "/foo/bar.txt");
        assert_eq!(request.header.path_len, 12);
    }

    #[test]
    fn test_metadata_create_file_request() {
        let request = MetadataCreateFileRequest::new("/new.txt".to_string(), 0, 0o644);
        assert_eq!(request.path(), "/new.txt");
        assert_eq!(request.header.size, 0);
        assert_eq!(request.header.mode, 0o644);
        assert_eq!(request.header.path_len, 8);
    }

    #[test]
    fn test_metadata_create_file_response() {
        let success = MetadataCreateFileResponseHeader::success(123);
        assert!(success.is_success());
        assert_eq!(success.inode, 123);

        let error = MetadataCreateFileResponseHeader::error(-1);
        assert!(!error.is_success());
    }

    #[test]
    fn test_metadata_create_dir_request() {
        let request = MetadataCreateDirRequest::new("/newdir".to_string(), 0o755);
        assert_eq!(request.path(), "/newdir");
        assert_eq!(request.header.mode, 0o755);
        assert_eq!(request.header.path_len, 7);
    }

    #[test]
    fn test_metadata_delete_request() {
        let file_req = MetadataDeleteRequest::delete_file("/file.txt".to_string());
        assert_eq!(file_req.path(), "/file.txt");
        assert!(file_req.is_file());
        assert!(!file_req.is_directory());

        let dir_req = MetadataDeleteRequest::delete_directory("/dir".to_string());
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
        assert_eq!(MetadataCreateFileRequest::rpc_id(), RPC_METADATA_CREATE_FILE);
        assert_eq!(MetadataCreateDirRequest::rpc_id(), RPC_METADATA_CREATE_DIR);
        assert_eq!(MetadataDeleteRequest::rpc_id(), RPC_METADATA_DELETE);
    }

    #[test]
    fn test_helper_functions() {
        let file_meta = FileMetadata::new(1, "/test.txt".to_string(), 2048);
        let resp = file_metadata_to_lookup_response(&file_meta);
        assert!(resp.is_file());
        assert_eq!(resp.inode, 1);
        assert_eq!(resp.size, 2048);

        let dir_meta = DirectoryMetadata::new(2, "/testdir".to_string());
        let resp = dir_metadata_to_lookup_response(&dir_meta);
        assert!(resp.is_directory());
        assert_eq!(resp.inode, 2);
    }
}
