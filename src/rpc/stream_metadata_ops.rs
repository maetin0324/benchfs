//! Stream + RMA based metadata operations
//!
//! This module provides Stream-based implementations of metadata operations
//! to replace the unstable Active Message based approach.
//!
//! All metadata operations use Pattern 1 (No RMA) - header-only communication.

use std::rc::Rc;

use pluvio_ucx::endpoint::Endpoint;

use super::metadata_ops::{
    MetadataCreateDirRequestHeader, MetadataCreateDirResponseHeader, MetadataCreateFileRequestHeader,
    MetadataCreateFileResponseHeader, MetadataDeleteRequestHeader, MetadataDeleteResponseHeader,
    MetadataLookupRequestHeader, MetadataLookupResponseHeader, MetadataUpdateRequestHeader,
    MetadataUpdateResponseHeader, ShutdownRequestHeader, ShutdownResponseHeader,
    RPC_METADATA_CREATE_DIR, RPC_METADATA_CREATE_FILE, RPC_METADATA_DELETE, RPC_METADATA_LOOKUP,
    RPC_METADATA_UPDATE, RPC_SHUTDOWN,
};
use super::stream_client::StreamRpcClient;
use super::stream_rpc::{RpcPattern, StreamRpc};
use super::{RpcError, RpcId};
use crate::rpc::handlers::RpcHandlerContext;

// ============================================================================
// StreamMetadataLookup - Pattern 1: No RMA
// ============================================================================

/// Stream-based MetadataLookup RPC request
pub struct StreamMetadataLookupRequest {
    header: MetadataLookupRequestHeader,
    path: String,
}

impl StreamMetadataLookupRequest {
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

impl StreamRpc for StreamMetadataLookupRequest {
    type RequestHeader = MetadataLookupRequestHeader;
    type ResponseHeader = MetadataLookupResponseHeader;

    fn rpc_id() -> RpcId {
        RPC_METADATA_LOOKUP
    }

    fn pattern(&self) -> RpcPattern {
        RpcPattern::NoRma
    }

    fn request_header(&self) -> &Self::RequestHeader {
        &self.header
    }

    fn path_bytes(&self) -> Vec<u8> {
        self.path.as_bytes().to_vec()
    }

    async fn call(
        &self,
        client: &StreamRpcClient,
    ) -> Result<Self::ResponseHeader, RpcError> {
        client.execute_no_rma(self).await
    }

    async fn server_handler(
        _ctx: Rc<RpcHandlerContext>,
        _endpoint: &Endpoint,
        _header: Self::RequestHeader,
    ) -> Result<Self::ResponseHeader, RpcError> {
        // This method should not be called for NoRma pattern with path
        // Use server_handler_with_path instead
        Err(RpcError::HandlerError(
            "Use server_handler_with_path for MetadataLookup".to_string(),
        ))
    }

    async fn server_handler_with_path(
        ctx: Rc<RpcHandlerContext>,
        _endpoint: &Endpoint,
        _header: Self::RequestHeader,
        path: &str,
    ) -> Result<Self::ResponseHeader, RpcError> {
        tracing::debug!("StreamMetadataLookup: path={}", path);

        use std::path::Path;
        let path_ref = Path::new(path);

        // Look up file metadata first
        if let Ok(file_meta) = ctx.metadata_manager.get_file_metadata(path_ref) {
            tracing::debug!(
                "Found file: path={}, size={}",
                file_meta.path,
                file_meta.size
            );
            return Ok(MetadataLookupResponseHeader::file(file_meta.size));
        }

        // Look up directory metadata
        if let Ok(dir_meta) = ctx.metadata_manager.get_dir_metadata(path_ref) {
            tracing::debug!("Found directory: path={}", dir_meta.path);
            return Ok(MetadataLookupResponseHeader::directory());
        }

        tracing::debug!("Path not found: {}", path);
        Ok(MetadataLookupResponseHeader::not_found())
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
// StreamMetadataCreateFile - Pattern 1: No RMA
// ============================================================================

/// Stream-based MetadataCreateFile RPC request
pub struct StreamMetadataCreateFileRequest {
    header: MetadataCreateFileRequestHeader,
    path: String,
}

impl StreamMetadataCreateFileRequest {
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

impl StreamRpc for StreamMetadataCreateFileRequest {
    type RequestHeader = MetadataCreateFileRequestHeader;
    type ResponseHeader = MetadataCreateFileResponseHeader;

    fn rpc_id() -> RpcId {
        RPC_METADATA_CREATE_FILE
    }

    fn pattern(&self) -> RpcPattern {
        RpcPattern::NoRma
    }

    fn request_header(&self) -> &Self::RequestHeader {
        &self.header
    }

    fn path_bytes(&self) -> Vec<u8> {
        self.path.as_bytes().to_vec()
    }

    async fn call(
        &self,
        client: &StreamRpcClient,
    ) -> Result<Self::ResponseHeader, RpcError> {
        client.execute_no_rma(self).await
    }

    async fn server_handler(
        _ctx: Rc<RpcHandlerContext>,
        _endpoint: &Endpoint,
        _header: Self::RequestHeader,
    ) -> Result<Self::ResponseHeader, RpcError> {
        // This method should not be called for NoRma pattern with path
        // Use server_handler_with_path instead
        Err(RpcError::HandlerError(
            "Use server_handler_with_path for MetadataCreateFile".to_string(),
        ))
    }

    async fn server_handler_with_path(
        ctx: Rc<RpcHandlerContext>,
        _endpoint: &Endpoint,
        header: Self::RequestHeader,
        path: &str,
    ) -> Result<Self::ResponseHeader, RpcError> {
        tracing::debug!(
            "StreamMetadataCreateFile: path={}, size={}, mode={:#o}",
            path,
            header.size,
            header.mode
        );

        // Create file metadata
        use crate::metadata::FileMetadata;
        let file_meta = FileMetadata::new(path.to_string(), header.size);

        // Store file metadata
        match ctx.metadata_manager.store_file_metadata(file_meta) {
            Ok(()) => {
                tracing::debug!("Created file metadata: path={}", path);
                Ok(MetadataCreateFileResponseHeader::success(0)) // Dummy inode
            }
            Err(e) => {
                tracing::error!("Failed to store file metadata: {:?}", e);
                Ok(MetadataCreateFileResponseHeader::error(-5)) // EIO
            }
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
        MetadataCreateFileResponseHeader::error(status)
    }
}

// ============================================================================
// StreamMetadataCreateDir - Pattern 1: No RMA
// ============================================================================

/// Stream-based MetadataCreateDir RPC request
pub struct StreamMetadataCreateDirRequest {
    header: MetadataCreateDirRequestHeader,
    path: String,
}

impl StreamMetadataCreateDirRequest {
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

impl StreamRpc for StreamMetadataCreateDirRequest {
    type RequestHeader = MetadataCreateDirRequestHeader;
    type ResponseHeader = MetadataCreateDirResponseHeader;

    fn rpc_id() -> RpcId {
        RPC_METADATA_CREATE_DIR
    }

    fn pattern(&self) -> RpcPattern {
        RpcPattern::NoRma
    }

    fn request_header(&self) -> &Self::RequestHeader {
        &self.header
    }

    fn path_bytes(&self) -> Vec<u8> {
        self.path.as_bytes().to_vec()
    }

    async fn call(
        &self,
        client: &StreamRpcClient,
    ) -> Result<Self::ResponseHeader, RpcError> {
        client.execute_no_rma(self).await
    }

    async fn server_handler(
        _ctx: Rc<RpcHandlerContext>,
        _endpoint: &Endpoint,
        _header: Self::RequestHeader,
    ) -> Result<Self::ResponseHeader, RpcError> {
        // This method should not be called for NoRma pattern with path
        // Use server_handler_with_path instead
        Err(RpcError::HandlerError(
            "Use server_handler_with_path for MetadataCreateDir".to_string(),
        ))
    }

    async fn server_handler_with_path(
        ctx: Rc<RpcHandlerContext>,
        _endpoint: &Endpoint,
        header: Self::RequestHeader,
        path: &str,
    ) -> Result<Self::ResponseHeader, RpcError> {
        tracing::debug!("StreamMetadataCreateDir: path={}, mode={:#o}", path, header.mode);

        // Create directory metadata
        use crate::metadata::DirectoryMetadata;
        let dir_meta = DirectoryMetadata::new(
            ctx.metadata_manager.generate_inode(),
            path.to_string(),
        );

        let inode = dir_meta.inode;

        // Store directory metadata
        match ctx.metadata_manager.store_dir_metadata(dir_meta) {
            Ok(()) => {
                tracing::debug!("Created directory metadata: path={}, inode={}", path, inode);
                Ok(MetadataCreateDirResponseHeader::success(inode))
            }
            Err(e) => {
                tracing::error!("Failed to store directory metadata: {:?}", e);
                Ok(MetadataCreateDirResponseHeader::error(-5)) // EIO
            }
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
        MetadataCreateDirResponseHeader::error(status)
    }
}

// ============================================================================
// StreamMetadataDelete - Pattern 1: No RMA
// ============================================================================

/// Stream-based MetadataDelete RPC request
pub struct StreamMetadataDeleteRequest {
    header: MetadataDeleteRequestHeader,
    path: String,
}

impl StreamMetadataDeleteRequest {
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

impl StreamRpc for StreamMetadataDeleteRequest {
    type RequestHeader = MetadataDeleteRequestHeader;
    type ResponseHeader = MetadataDeleteResponseHeader;

    fn rpc_id() -> RpcId {
        RPC_METADATA_DELETE
    }

    fn pattern(&self) -> RpcPattern {
        RpcPattern::NoRma
    }

    fn request_header(&self) -> &Self::RequestHeader {
        &self.header
    }

    fn path_bytes(&self) -> Vec<u8> {
        self.path.as_bytes().to_vec()
    }

    async fn call(
        &self,
        client: &StreamRpcClient,
    ) -> Result<Self::ResponseHeader, RpcError> {
        client.execute_no_rma(self).await
    }

    async fn server_handler(
        _ctx: Rc<RpcHandlerContext>,
        _endpoint: &Endpoint,
        _header: Self::RequestHeader,
    ) -> Result<Self::ResponseHeader, RpcError> {
        // This method should not be called for NoRma pattern with path
        // Use server_handler_with_path instead
        Err(RpcError::HandlerError(
            "Use server_handler_with_path for MetadataDelete".to_string(),
        ))
    }

    async fn server_handler_with_path(
        ctx: Rc<RpcHandlerContext>,
        _endpoint: &Endpoint,
        header: Self::RequestHeader,
        path: &str,
    ) -> Result<Self::ResponseHeader, RpcError> {
        tracing::debug!(
            "StreamMetadataDelete: path={}, entry_type={}",
            path,
            header.entry_type
        );

        use std::path::Path;
        let path_ref = Path::new(path);

        // Delete based on entry type
        let result = if header.entry_type == 1 {
            // Delete file
            ctx.metadata_manager.remove_file_metadata(path_ref)
        } else if header.entry_type == 2 {
            // Delete directory
            ctx.metadata_manager.remove_dir_metadata(path_ref)
        } else {
            tracing::error!("Invalid entry_type: {}", header.entry_type);
            return Ok(MetadataDeleteResponseHeader::error(-22)); // EINVAL
        };

        match result {
            Ok(()) => {
                tracing::debug!("Deleted metadata: path={}", path);
                Ok(MetadataDeleteResponseHeader::success())
            }
            Err(e) => {
                tracing::error!("Failed to delete metadata: {:?}", e);
                Ok(MetadataDeleteResponseHeader::error(-2)) // ENOENT
            }
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
        MetadataDeleteResponseHeader::error(status)
    }
}

// ============================================================================
// StreamMetadataUpdate - Pattern 1: No RMA
// ============================================================================

/// Stream-based MetadataUpdate RPC request
pub struct StreamMetadataUpdateRequest {
    header: MetadataUpdateRequestHeader,
    path: String,
}

impl StreamMetadataUpdateRequest {
    pub fn new(path: String) -> Self {
        Self {
            header: MetadataUpdateRequestHeader::new(path.len()),
            path,
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

impl StreamRpc for StreamMetadataUpdateRequest {
    type RequestHeader = MetadataUpdateRequestHeader;
    type ResponseHeader = MetadataUpdateResponseHeader;

    fn rpc_id() -> RpcId {
        RPC_METADATA_UPDATE
    }

    fn pattern(&self) -> RpcPattern {
        RpcPattern::NoRma
    }

    fn request_header(&self) -> &Self::RequestHeader {
        &self.header
    }

    fn path_bytes(&self) -> Vec<u8> {
        self.path.as_bytes().to_vec()
    }

    async fn call(
        &self,
        client: &StreamRpcClient,
    ) -> Result<Self::ResponseHeader, RpcError> {
        client.execute_no_rma(self).await
    }

    async fn server_handler(
        _ctx: Rc<RpcHandlerContext>,
        _endpoint: &Endpoint,
        _header: Self::RequestHeader,
    ) -> Result<Self::ResponseHeader, RpcError> {
        // This method should not be called for NoRma pattern with path
        // Use server_handler_with_path instead
        Err(RpcError::HandlerError(
            "Use server_handler_with_path for MetadataUpdate".to_string(),
        ))
    }

    async fn server_handler_with_path(
        ctx: Rc<RpcHandlerContext>,
        _endpoint: &Endpoint,
        header: Self::RequestHeader,
        path: &str,
    ) -> Result<Self::ResponseHeader, RpcError> {
        tracing::debug!(
            "StreamMetadataUpdate: path={}, update_mask={:#b}",
            path,
            header.update_mask
        );

        use std::path::Path;
        let path_ref = Path::new(path);

        // Get current file metadata
        let mut file_meta = match ctx.metadata_manager.get_file_metadata(path_ref) {
            Ok(meta) => meta,
            Err(_) => {
                tracing::debug!("File not found: {}", path);
                return Ok(MetadataUpdateResponseHeader::error(-2)); // ENOENT
            }
        };

        // Update size if requested
        if header.should_update_size() {
            let old_size = file_meta.size;
            file_meta.size = header.new_size;

            tracing::debug!(
                "Updated file size: {} -> {} (path={})",
                old_size,
                header.new_size,
                path
            );
        }

        // Update mode if requested (logged for now)
        if header.should_update_mode() {
            tracing::debug!("Updated file mode: {:#o} (path={})", header.new_mode, path);
            // Note: BenchFS doesn't currently use mode field in FileMetadata
        }

        // Store updated metadata
        match ctx.metadata_manager.update_file_metadata(file_meta) {
            Ok(()) => {
                tracing::debug!("Successfully updated metadata: path={}", path);
                Ok(MetadataUpdateResponseHeader::success())
            }
            Err(e) => {
                tracing::error!("Failed to update metadata: {:?}", e);
                Ok(MetadataUpdateResponseHeader::error(-5)) // EIO
            }
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
// StreamShutdown - Pattern 1: No RMA
// ============================================================================

/// Stream-based Shutdown RPC request
pub struct StreamShutdownRequest {
    header: ShutdownRequestHeader,
}

impl StreamShutdownRequest {
    pub fn new() -> Self {
        Self {
            header: ShutdownRequestHeader::new(),
        }
    }
}

impl Default for StreamShutdownRequest {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamRpc for StreamShutdownRequest {
    type RequestHeader = ShutdownRequestHeader;
    type ResponseHeader = ShutdownResponseHeader;

    fn rpc_id() -> RpcId {
        RPC_SHUTDOWN
    }

    fn pattern(&self) -> RpcPattern {
        RpcPattern::NoRma
    }

    fn request_header(&self) -> &Self::RequestHeader {
        &self.header
    }

    async fn call(
        &self,
        client: &StreamRpcClient,
    ) -> Result<Self::ResponseHeader, RpcError> {
        client.execute_no_rma(self).await
    }

    async fn server_handler(
        ctx: Rc<RpcHandlerContext>,
        _endpoint: &Endpoint,
        header: Self::RequestHeader,
    ) -> Result<Self::ResponseHeader, RpcError> {
        // Verify magic number
        if header.magic != crate::constants::SHUTDOWN_MAGIC {
            tracing::warn!("Invalid shutdown magic: {:#x}", header.magic);
            return Err(RpcError::InvalidHeader);
        }

        tracing::info!("Received shutdown request");

        // Set shutdown flag
        ctx.set_shutdown_flag();

        Ok(ShutdownResponseHeader::new(true))
    }

    fn error_response(_error: &RpcError) -> Self::ResponseHeader {
        ShutdownResponseHeader::new(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stream_metadata_lookup_request() {
        let request = StreamMetadataLookupRequest::new("/foo/bar.txt".to_string());
        assert_eq!(request.path(), "/foo/bar.txt");
        assert_eq!(request.header.path_len, 12);
        assert!(matches!(request.pattern(), RpcPattern::NoRma));
    }

    #[test]
    fn test_stream_metadata_create_file_request() {
        let request = StreamMetadataCreateFileRequest::new("/new.txt".to_string(), 0, 0o644);
        assert_eq!(request.path(), "/new.txt");
        assert_eq!(request.header.size, 0);
        assert_eq!(request.header.mode, 0o644);
        assert_eq!(request.header.path_len, 8);
        assert!(matches!(request.pattern(), RpcPattern::NoRma));
    }

    #[test]
    fn test_stream_metadata_create_dir_request() {
        let request = StreamMetadataCreateDirRequest::new("/newdir".to_string(), 0o755);
        assert_eq!(request.path(), "/newdir");
        assert_eq!(request.header.mode, 0o755);
        assert_eq!(request.header.path_len, 7);
        assert!(matches!(request.pattern(), RpcPattern::NoRma));
    }

    #[test]
    fn test_stream_metadata_delete_request() {
        let file_req = StreamMetadataDeleteRequest::delete_file("/file.txt".to_string());
        assert_eq!(file_req.path(), "/file.txt");
        assert!(file_req.is_file());
        assert!(!file_req.is_directory());
        assert!(matches!(file_req.pattern(), RpcPattern::NoRma));

        let dir_req = StreamMetadataDeleteRequest::delete_directory("/dir".to_string());
        assert_eq!(dir_req.path(), "/dir");
        assert!(!dir_req.is_file());
        assert!(dir_req.is_directory());
        assert!(matches!(dir_req.pattern(), RpcPattern::NoRma));
    }

    #[test]
    fn test_stream_metadata_update_request() {
        let request = StreamMetadataUpdateRequest::new("/test.txt".to_string())
            .with_size(2048)
            .with_mode(0o600);
        assert_eq!(request.path(), "/test.txt");
        assert!(request.header.should_update_size());
        assert!(request.header.should_update_mode());
        assert_eq!(request.header.new_size, 2048);
        assert_eq!(request.header.new_mode, 0o600);
        assert!(matches!(request.pattern(), RpcPattern::NoRma));
    }

    #[test]
    fn test_stream_shutdown_request() {
        let request = StreamShutdownRequest::new();
        assert_eq!(request.header.magic, crate::constants::SHUTDOWN_MAGIC);
        assert!(matches!(request.pattern(), RpcPattern::NoRma));
    }

    #[test]
    fn test_rpc_ids() {
        assert_eq!(
            StreamMetadataLookupRequest::rpc_id(),
            RPC_METADATA_LOOKUP
        );
        assert_eq!(
            StreamMetadataCreateFileRequest::rpc_id(),
            RPC_METADATA_CREATE_FILE
        );
        assert_eq!(
            StreamMetadataCreateDirRequest::rpc_id(),
            RPC_METADATA_CREATE_DIR
        );
        assert_eq!(StreamMetadataDeleteRequest::rpc_id(), RPC_METADATA_DELETE);
        assert_eq!(StreamMetadataUpdateRequest::rpc_id(), RPC_METADATA_UPDATE);
        assert_eq!(StreamShutdownRequest::rpc_id(), RPC_SHUTDOWN);
    }
}
