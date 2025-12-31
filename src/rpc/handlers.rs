use std::cell::RefCell;
use std::rc::Rc;

use pluvio_ucx::async_ucx::ucp::AmMsg;

use crate::metadata::MetadataManager;
use crate::rpc::buffer_pool::{PathBufferLease, PathBufferPool};
use crate::rpc::file_id::FileIdRegistry;
use crate::rpc::{RpcError, data_ops::*, metadata_ops::*};
use crate::storage::ChunkStore;

/// RPC Handler context
///
/// Contains references to the metadata manager and chunk store
/// that handlers need to access.
pub struct RpcHandlerContext {
    pub metadata_manager: Rc<MetadataManager>,
    pub chunk_store: Rc<dyn ChunkStore>,
    pub allocator: Rc<pluvio_uring::allocator::FixedBufferAllocator>,
    path_buffer_pool: Rc<PathBufferPool>,
    /// FileId to path mapping registry for compact RPC
    file_id_registry: Rc<FileIdRegistry>,
    /// Shutdown flag for graceful termination
    shutdown_flag: RefCell<bool>,
}

impl RpcHandlerContext {
    pub fn new(
        metadata_manager: Rc<MetadataManager>,
        chunk_store: Rc<dyn ChunkStore>,
        allocator: Rc<pluvio_uring::allocator::FixedBufferAllocator>,
    ) -> Self {
        Self {
            metadata_manager,
            chunk_store,
            allocator,
            path_buffer_pool: Rc::new(PathBufferPool::new(64)),
            file_id_registry: Rc::new(FileIdRegistry::with_capacity(1024)),
            shutdown_flag: RefCell::new(false),
        }
    }

    /// Create a context with a custom FileIdRegistry
    pub fn with_file_id_registry(
        metadata_manager: Rc<MetadataManager>,
        chunk_store: Rc<dyn ChunkStore>,
        allocator: Rc<pluvio_uring::allocator::FixedBufferAllocator>,
        file_id_registry: Rc<FileIdRegistry>,
    ) -> Self {
        Self {
            metadata_manager,
            chunk_store,
            allocator,
            path_buffer_pool: Rc::new(PathBufferPool::new(64)),
            file_id_registry,
            shutdown_flag: RefCell::new(false),
        }
    }

    /// Create a minimal context for benchmark (no storage/metadata)
    pub fn new_bench() -> Self {
        use crate::metadata::MetadataManager;
        use crate::storage::InMemoryChunkStore;

        // Create dummy metadata manager
        let metadata_manager = Rc::new(MetadataManager::new("bench".to_string()));

        // Create dummy chunk store
        let chunk_store: Rc<dyn ChunkStore> = Rc::new(InMemoryChunkStore::new());

        // Create dummy allocator (won't be used in benchmark)
        // We need to create a minimal IoUring instance to initialize the allocator
        // Leak the ring to keep it alive for the lifetime of the program
        // This is acceptable for benchmark programs
        let ring = Box::new(io_uring::IoUring::new(1).expect("Failed to create placeholder ring"));
        let ring: &'static mut io_uring::IoUring = Box::leak(ring);
        let allocator = pluvio_uring::allocator::FixedBufferAllocator::new(1, 4096, ring);

        Self {
            metadata_manager,
            chunk_store,
            allocator,
            path_buffer_pool: Rc::new(PathBufferPool::new(16)),
            file_id_registry: Rc::new(FileIdRegistry::new()),
            shutdown_flag: RefCell::new(false),
        }
    }

    /// Set the shutdown flag to signal graceful termination
    pub fn set_shutdown_flag(&self) {
        *self.shutdown_flag.borrow_mut() = true;
        tracing::info!("Shutdown flag set");
    }

    /// Check if shutdown has been requested
    pub fn should_shutdown(&self) -> bool {
        *self.shutdown_flag.borrow()
    }

    pub fn acquire_path_buffer(&self) -> PathBufferLease {
        self.path_buffer_pool.acquire()
    }

    /// Get a reference to the FileIdRegistry
    ///
    /// This registry maps path_hash (32-bit) to full file path for
    /// FileId-based RPC operations.
    pub fn file_id_registry(&self) -> &FileIdRegistry {
        &self.file_id_registry
    }

    /// Get the shared FileIdRegistry reference
    pub fn file_id_registry_rc(&self) -> Rc<FileIdRegistry> {
        Rc::clone(&self.file_id_registry)
    }
}

// ============================================================================
// Data RPC Handlers
// ============================================================================

/// Response for ReadChunk that includes both header and data
pub struct ReadChunkHandlerResponse {
    pub header: ReadChunkResponseHeader,
    pub data: Option<Vec<u8>>,
}

/// Handle ReadChunk RPC request
///
/// Reads chunk data from local storage and returns it to the client via RDMA.
///
/// **DEPRECATED**: This handler is deprecated. Use `ReadChunkByIdRequest::server_handler` instead.
#[allow(deprecated)]
#[async_backtrace::framed]
pub async fn handle_read_chunk(
    ctx: Rc<RpcHandlerContext>,
    mut am_msg: AmMsg,
) -> Result<(ReadChunkHandlerResponse, AmMsg), (RpcError, AmMsg)> {
    // Parse request header
    let header: ReadChunkRequestHeader = match am_msg
        .header()
        .get(..std::mem::size_of::<ReadChunkRequestHeader>())
        .and_then(|bytes| zerocopy::FromBytes::read_from_bytes(bytes).ok())
    {
        Some(h) => h,
        None => return Err((RpcError::InvalidHeader, am_msg)),
    };

    // Receive path from request data
    let path = if header.path_len > 0 && am_msg.contains_data() {
        let mut path_bytes = vec![0u8; header.path_len as usize];
        if let Err(e) = am_msg
            .recv_data_vectored(&[std::io::IoSliceMut::new(&mut path_bytes)])
            .await
        {
            tracing::error!("Failed to receive path data: {:?}", e);
            return Ok((
                ReadChunkHandlerResponse {
                    header: ReadChunkResponseHeader::error(-5), // EIO
                    data: None,
                },
                am_msg,
            ));
        }

        match String::from_utf8(path_bytes) {
            Ok(p) => p,
            Err(e) => {
                tracing::error!("Failed to decode path: {:?}", e);
                return Ok((
                    ReadChunkHandlerResponse {
                        header: ReadChunkResponseHeader::error(-22), // EINVAL
                        data: None,
                    },
                    am_msg,
                ));
            }
        }
    } else {
        return Ok((
            ReadChunkHandlerResponse {
                header: ReadChunkResponseHeader::error(-22), // EINVAL
                data: None,
            },
            am_msg,
        ));
    };

    tracing::debug!(
        "ReadChunk: path={}, chunk={}, offset={}, length={}",
        path,
        header.chunk_index,
        header.offset,
        header.length
    );

    // Read chunk from storage
    match ctx
        .chunk_store
        .read_chunk(&path, header.chunk_index, header.offset, header.length)
        .await
    {
        Ok(data) => {
            let bytes_read = data.len() as u64;

            tracing::debug!(
                "Read {} bytes from storage (path={}, chunk={})",
                bytes_read,
                path,
                header.chunk_index
            );

            Ok((
                ReadChunkHandlerResponse {
                    header: ReadChunkResponseHeader::success(bytes_read),
                    data: Some(data),
                },
                am_msg,
            ))
        }
        Err(e) => {
            tracing::error!("Failed to read chunk: {:?}", e);
            Ok((
                ReadChunkHandlerResponse {
                    header: ReadChunkResponseHeader::error(-2), // ENOENT
                    data: None,
                },
                am_msg,
            ))
        }
    }
}

/// Handle WriteChunk RPC request
///
/// Receives chunk data from the client via RDMA and writes it to local storage.
/// Uses registered buffers for zero-copy DMA writes.
///
/// **DEPRECATED**: This handler is deprecated. Use `WriteChunkByIdRequest::server_handler` instead.
#[allow(deprecated)]
#[async_backtrace::framed]
pub async fn handle_write_chunk(
    ctx: Rc<RpcHandlerContext>,
    mut am_msg: AmMsg,
) -> Result<(WriteChunkResponseHeader, AmMsg), (RpcError, AmMsg)> {
    // Parse request header
    let header: WriteChunkRequestHeader = match am_msg
        .header()
        .get(..std::mem::size_of::<WriteChunkRequestHeader>())
        .and_then(|bytes| zerocopy::FromBytes::read_from_bytes(bytes).ok())
    {
        Some(h) => h,
        None => return Err((RpcError::InvalidHeader, am_msg)),
    };

    // Receive path and data from client via RDMA-read
    // Data section layout: [path][chunk_data]
    if !am_msg.contains_data() {
        tracing::error!("WriteChunk request contains no data");
        return Ok((WriteChunkResponseHeader::error(-22), am_msg)); // EINVAL
    }

    // Receive path first (small buffer)
    let mut path_bytes = vec![0u8; header.path_len as usize];
    if let Err(e) = am_msg
        .recv_data_vectored(&[std::io::IoSliceMut::new(&mut path_bytes)])
        .await
    {
        tracing::error!("Failed to receive path: {:?}", e);
        return Ok((WriteChunkResponseHeader::error(-5), am_msg)); // EIO
    }

    let path = match String::from_utf8(path_bytes) {
        Ok(p) => p,
        Err(e) => {
            tracing::error!("Failed to decode path: {:?}", e);
            return Ok((WriteChunkResponseHeader::error(-22), am_msg)); // EINVAL
        }
    };

    // Acquire a registered buffer for zero-copy DMA write
    let mut fixed_buffer = ctx.allocator.acquire().await;
    let data_len = header.length as usize;

    // Ensure the data fits in the registered buffer
    if data_len > fixed_buffer.len() {
        tracing::error!(
            "Data size {} exceeds registered buffer size {}",
            data_len,
            fixed_buffer.len()
        );
        return Ok((WriteChunkResponseHeader::error(-22), am_msg)); // EINVAL
    }

    // Receive chunk data directly into registered buffer
    let buffer_slice = &mut fixed_buffer.as_mut_slice()[..data_len];
    if let Err(e) = am_msg
        .recv_data_vectored(&[std::io::IoSliceMut::new(buffer_slice)])
        .await
    {
        tracing::error!("Failed to receive chunk data: {:?}", e);
        return Ok((WriteChunkResponseHeader::error(-5), am_msg)); // EIO
    }

    tracing::debug!(
        "WriteChunk (zero-copy): path={}, chunk={}, offset={}, length={}",
        path,
        header.chunk_index,
        header.offset,
        header.length
    );

    // Check if chunk_store supports zero-copy write
    // Try downcasting to IOUringChunkStore to use write_chunk_fixed
    use crate::storage::chunk_store::IOUringChunkStore;
    use std::any::Any;

    let chunk_store_any = &*ctx.chunk_store as &dyn Any;
    if let Some(io_uring_store) = chunk_store_any.downcast_ref::<IOUringChunkStore>() {
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
                tracing::debug!(
                    "Wrote {} bytes (zero-copy) to storage (path={}, chunk={})",
                    bytes_written,
                    path,
                    header.chunk_index
                );
                return Ok((
                    WriteChunkResponseHeader::success(bytes_written as u64),
                    am_msg,
                ));
            }
            Err(e) => {
                tracing::error!("Failed to write chunk (zero-copy): {:?}", e);
                return Ok((WriteChunkResponseHeader::error(-5), am_msg)); // EIO
            }
        }
    }

    // Fallback: copy to Vec<u8> and use normal write
    let data = fixed_buffer.as_mut_slice()[..data_len].to_vec();
    match ctx
        .chunk_store
        .write_chunk(&path, header.chunk_index, header.offset, &data)
        .await
    {
        Ok(bytes_written) => {
            tracing::debug!(
                "Wrote {} bytes to storage (path={}, chunk={})",
                bytes_written,
                path,
                header.chunk_index
            );
            Ok((
                WriteChunkResponseHeader::success(bytes_written as u64),
                am_msg,
            ))
        }
        Err(e) => {
            tracing::error!("Failed to write chunk: {:?}", e);
            Ok((WriteChunkResponseHeader::error(-5), am_msg)) // EIO
        }
    }
}

// ============================================================================
// Metadata RPC Handlers
// ============================================================================

/// Handle MetadataLookup RPC request
///
/// Looks up file or directory metadata and returns it to the client.
#[async_backtrace::framed]
pub async fn handle_metadata_lookup(
    ctx: Rc<RpcHandlerContext>,
    mut am_msg: AmMsg,
) -> Result<(MetadataLookupResponseHeader, AmMsg), (RpcError, AmMsg)> {
    // Parse request header
    let header: MetadataLookupRequestHeader = match am_msg
        .header()
        .get(..std::mem::size_of::<MetadataLookupRequestHeader>())
        .and_then(|bytes| zerocopy::FromBytes::read_from_bytes(bytes).ok())
    {
        Some(h) => h,
        None => return Err((RpcError::InvalidHeader, am_msg)),
    };

    // Receive path from request data if available
    let path = if header.path_len > 0 && am_msg.contains_data() {
        let mut path_bytes = vec![0u8; header.path_len as usize];
        if let Err(e) = am_msg
            .recv_data_vectored(&[std::io::IoSliceMut::new(&mut path_bytes)])
            .await
        {
            tracing::error!("Failed to receive path data: {:?}", e);
            return Ok((MetadataLookupResponseHeader::error(-5), am_msg)); // EIO
        }

        match String::from_utf8(path_bytes) {
            Ok(p) => p,
            Err(e) => {
                tracing::error!("Failed to parse path as UTF-8: {:?}", e);
                return Ok((MetadataLookupResponseHeader::error(-22), am_msg)); // EINVAL
            }
        }
    } else {
        "/".to_string()
    };

    tracing::debug!("MetadataLookup: path={}", path);

    use std::path::Path;
    let path_ref = Path::new(&path);

    // Look up file metadata first
    if let Ok(file_meta) = ctx.metadata_manager.get_file_metadata(path_ref) {
        tracing::debug!(
            "Found file: path={}, size={}",
            file_meta.path,
            file_meta.size
        );
        return Ok((MetadataLookupResponseHeader::file(file_meta.size), am_msg));
    }

    // Look up directory metadata (dummy in path-based KV design)
    if let Ok(dir_meta) = ctx.metadata_manager.get_dir_metadata(path_ref) {
        tracing::debug!("Found directory: path={}", dir_meta.path);
        return Ok((MetadataLookupResponseHeader::directory(), am_msg));
    }

    tracing::debug!("Path not found: {}", path);
    Ok((MetadataLookupResponseHeader::not_found(), am_msg))
}

/// Handle MetadataCreateFile RPC request
///
/// Creates a new file metadata entry.
#[async_backtrace::framed]
pub async fn handle_metadata_create_file(
    ctx: Rc<RpcHandlerContext>,
    mut am_msg: AmMsg,
) -> Result<(MetadataCreateFileResponseHeader, AmMsg), (RpcError, AmMsg)> {
    // Parse request header
    let header: MetadataCreateFileRequestHeader = match am_msg
        .header()
        .get(..std::mem::size_of::<MetadataCreateFileRequestHeader>())
        .and_then(|bytes| zerocopy::FromBytes::read_from_bytes(bytes).ok())
    {
        Some(h) => h,
        None => return Err((RpcError::InvalidHeader, am_msg)),
    };

    // Receive path from request data
    let path = if header.path_len > 0 && am_msg.contains_data() {
        let mut path_bytes = vec![0u8; header.path_len as usize];
        if let Err(e) = am_msg
            .recv_data_vectored(&[std::io::IoSliceMut::new(&mut path_bytes)])
            .await
        {
            tracing::error!("Failed to receive path data: {:?}", e);
            return Ok((MetadataCreateFileResponseHeader::error(-5), am_msg)); // EIO
        }

        match String::from_utf8(path_bytes) {
            Ok(p) => p,
            Err(e) => {
                tracing::error!("Failed to parse path as UTF-8: {:?}", e);
                return Ok((MetadataCreateFileResponseHeader::error(-22), am_msg)); // EINVAL
            }
        }
    } else {
        tracing::error!("MetadataCreateFile: missing path");
        return Ok((MetadataCreateFileResponseHeader::error(-22), am_msg)); // EINVAL
    };

    tracing::debug!(
        "MetadataCreateFile: path={}, size={}, mode={:#o}",
        path,
        header.size,
        header.mode
    );

    // Create file metadata (no inode in path-based KV design)
    use crate::metadata::FileMetadata;
    let file_meta = FileMetadata::new(path.clone(), header.size);

    // Store file metadata
    match ctx.metadata_manager.store_file_metadata(file_meta) {
        Ok(()) => {
            tracing::debug!("Created file metadata: path={}", path);
            Ok((MetadataCreateFileResponseHeader::success(0), am_msg)) // Dummy inode
        }
        Err(e) => {
            tracing::error!("Failed to store file metadata: {:?}", e);
            Ok((MetadataCreateFileResponseHeader::error(-5), am_msg)) // EIO
        }
    }
}

/// Handle MetadataCreateDir RPC request
///
/// Creates a new directory metadata entry.
#[async_backtrace::framed]
pub async fn handle_metadata_create_dir(
    ctx: Rc<RpcHandlerContext>,
    mut am_msg: AmMsg,
) -> Result<(MetadataCreateDirResponseHeader, AmMsg), (RpcError, AmMsg)> {
    // Parse request header
    let header: MetadataCreateDirRequestHeader = match am_msg
        .header()
        .get(..std::mem::size_of::<MetadataCreateDirRequestHeader>())
        .and_then(|bytes| zerocopy::FromBytes::read_from_bytes(bytes).ok())
    {
        Some(h) => h,
        None => return Err((RpcError::InvalidHeader, am_msg)),
    };

    // Receive path from request data
    let path = if header.path_len > 0 && am_msg.contains_data() {
        let mut path_bytes = vec![0u8; header.path_len as usize];
        if let Err(e) = am_msg
            .recv_data_vectored(&[std::io::IoSliceMut::new(&mut path_bytes)])
            .await
        {
            tracing::error!("Failed to receive path data: {:?}", e);
            return Ok((MetadataCreateDirResponseHeader::error(-5), am_msg)); // EIO
        }

        match String::from_utf8(path_bytes) {
            Ok(p) => p,
            Err(e) => {
                tracing::error!("Failed to parse path as UTF-8: {:?}", e);
                return Ok((MetadataCreateDirResponseHeader::error(-22), am_msg)); // EINVAL
            }
        }
    } else {
        tracing::error!("MetadataCreateDir: missing path");
        return Ok((MetadataCreateDirResponseHeader::error(-22), am_msg)); // EINVAL
    };

    tracing::debug!("MetadataCreateDir: path={}, mode={:#o}", path, header.mode);

    // Create directory metadata
    use crate::metadata::DirectoryMetadata;
    let dir_meta = DirectoryMetadata::new(ctx.metadata_manager.generate_inode(), path.clone());

    let inode = dir_meta.inode;

    // Store directory metadata
    match ctx.metadata_manager.store_dir_metadata(dir_meta) {
        Ok(()) => {
            tracing::debug!("Created directory metadata: path={}, inode={}", path, inode);
            Ok((MetadataCreateDirResponseHeader::success(inode), am_msg))
        }
        Err(e) => {
            tracing::error!("Failed to store directory metadata: {:?}", e);
            Ok((MetadataCreateDirResponseHeader::error(-5), am_msg)) // EIO
        }
    }
}

/// Handle MetadataDelete RPC request
///
/// Deletes a file or directory metadata entry.
#[async_backtrace::framed]
pub async fn handle_metadata_delete(
    ctx: Rc<RpcHandlerContext>,
    mut am_msg: AmMsg,
) -> Result<(MetadataDeleteResponseHeader, AmMsg), (RpcError, AmMsg)> {
    // Parse request header
    let header: MetadataDeleteRequestHeader = match am_msg
        .header()
        .get(..std::mem::size_of::<MetadataDeleteRequestHeader>())
        .and_then(|bytes| zerocopy::FromBytes::read_from_bytes(bytes).ok())
    {
        Some(h) => h,
        None => return Err((RpcError::InvalidHeader, am_msg)),
    };

    // Receive path from request data
    let path = if header.path_len > 0 && am_msg.contains_data() {
        let mut path_bytes = vec![0u8; header.path_len as usize];
        if let Err(e) = am_msg
            .recv_data_vectored(&[std::io::IoSliceMut::new(&mut path_bytes)])
            .await
        {
            tracing::error!("Failed to receive path data: {:?}", e);
            return Ok((MetadataDeleteResponseHeader::error(-5), am_msg)); // EIO
        }

        match String::from_utf8(path_bytes) {
            Ok(p) => p,
            Err(e) => {
                tracing::error!("Failed to parse path as UTF-8: {:?}", e);
                return Ok((MetadataDeleteResponseHeader::error(-22), am_msg)); // EINVAL
            }
        }
    } else {
        tracing::error!("MetadataDelete: missing path");
        return Ok((MetadataDeleteResponseHeader::error(-22), am_msg)); // EINVAL
    };

    tracing::debug!(
        "MetadataDelete: path={}, entry_type={}",
        path,
        header.entry_type
    );

    use std::path::Path;
    let path_ref = Path::new(&path);

    // Delete based on entry type
    let result = if header.entry_type == 1 {
        // Delete file
        ctx.metadata_manager.remove_file_metadata(path_ref)
    } else if header.entry_type == 2 {
        // Delete directory
        ctx.metadata_manager.remove_dir_metadata(path_ref)
    } else {
        tracing::error!("Invalid entry_type: {}", header.entry_type);
        return Ok((MetadataDeleteResponseHeader::error(-22), am_msg)); // EINVAL
    };

    match result {
        Ok(()) => {
            tracing::debug!("Deleted metadata: path={}", path);
            Ok((MetadataDeleteResponseHeader::success(), am_msg))
        }
        Err(e) => {
            tracing::error!("Failed to delete metadata: {:?}", e);
            Ok((MetadataDeleteResponseHeader::error(-2), am_msg)) // ENOENT
        }
    }
}

/// Handle MetadataUpdate RPC request
///
/// Updates file metadata (size, mode/permissions).
#[async_backtrace::framed]
pub async fn handle_metadata_update(
    ctx: Rc<RpcHandlerContext>,
    mut am_msg: AmMsg,
) -> Result<(MetadataUpdateResponseHeader, AmMsg), (RpcError, AmMsg)> {
    // Parse request header
    let header: MetadataUpdateRequestHeader = match am_msg
        .header()
        .get(..std::mem::size_of::<MetadataUpdateRequestHeader>())
        .and_then(|bytes| zerocopy::FromBytes::read_from_bytes(bytes).ok())
    {
        Some(h) => h,
        None => return Err((RpcError::InvalidHeader, am_msg)),
    };

    // Receive path from request data
    let path = if header.path_len > 0 && am_msg.contains_data() {
        let mut path_bytes = vec![0u8; header.path_len as usize];
        if let Err(e) = am_msg
            .recv_data_vectored(&[std::io::IoSliceMut::new(&mut path_bytes)])
            .await
        {
            tracing::error!("Failed to receive path data: {:?}", e);
            return Ok((MetadataUpdateResponseHeader::error(-5), am_msg)); // EIO
        }

        match String::from_utf8(path_bytes) {
            Ok(p) => p,
            Err(e) => {
                tracing::error!("Failed to parse path as UTF-8: {:?}", e);
                return Ok((MetadataUpdateResponseHeader::error(-22), am_msg)); // EINVAL
            }
        }
    } else {
        tracing::error!("MetadataUpdate: missing path");
        return Ok((MetadataUpdateResponseHeader::error(-22), am_msg)); // EINVAL
    };

    tracing::debug!(
        "MetadataUpdate: path={}, update_mask={:#b}",
        path,
        header.update_mask
    );

    use std::path::Path;
    let path_ref = Path::new(&path);

    // Get current file metadata
    let mut file_meta = match ctx.metadata_manager.get_file_metadata(path_ref) {
        Ok(meta) => meta,
        Err(_) => {
            tracing::debug!("File not found: {}", path);
            return Ok((MetadataUpdateResponseHeader::error(-2), am_msg)); // ENOENT
        }
    };

    // Update size if requested
    if header.should_update_size() {
        let old_size = file_meta.size;
        file_meta.size = header.new_size;
        // chunk_count is calculated on demand via calculate_chunk_count()

        tracing::debug!(
            "Updated file size: {} -> {} (path={})",
            old_size,
            header.new_size,
            path
        );

        // Note: In path-based KV design, chunk_locations are not tracked
        // Chunks are identified directly by (path, chunk_index)
    }

    // Update mode if requested
    // Note: BenchFS doesn't currently use mode field in FileMetadata,
    // so we just log it for now
    if header.should_update_mode() {
        tracing::debug!("Updated file mode: {:#o} (path={})", header.new_mode, path);
        // In the future, store mode in FileMetadata
    }

    // Store updated metadata
    match ctx.metadata_manager.update_file_metadata(file_meta) {
        Ok(()) => {
            tracing::debug!("Successfully updated metadata: path={}", path);
            Ok((MetadataUpdateResponseHeader::success(), am_msg))
        }
        Err(e) => {
            tracing::error!("Failed to update metadata: {:?}", e);
            Ok((MetadataUpdateResponseHeader::error(-5), am_msg)) // EIO
        }
    }
}

#[cfg(test)]
mod tests {

    // Note: Testing with IOUringChunkStore requires async runtime setup
    // These tests are disabled as they would need complex setup with io_uring reactor

    // Note: Testing the actual handlers requires creating AmMsg instances,
    // which is difficult without a real UCX worker. These would be integration tests.
}
