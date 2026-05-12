use std::cell::RefCell;
use std::rc::Rc;

use pluvio_ucx::async_ucx::ucp::AmMsg;

use crate::metadata::MetadataManager;
use crate::rpc::buffer_pool::{PathBufferLease, PathBufferPool};
use crate::rpc::file_id::FileIdRegistry;
use crate::rpc::{RpcError, data_ops::*, metadata_ops::*};
use crate::storage::{ChunkStore, IOUringChunkStore};

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

    /// Borrow the chunk store as `&IOUringChunkStore` if that's the
    /// concrete backend in use. Used by the CHFS POSIX-style metadata
    /// handlers, which require the chunk-0 header / extension helpers
    /// only present on `IOUringChunkStore`.
    fn iouring_chunk_store(&self) -> Option<&IOUringChunkStore> {
        self.chunk_store
            .as_any()
            .downcast_ref::<IOUringChunkStore>()
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

    // Fast path: in-memory cache (populated by create/update and previous
    // lookups). Skipping the disk hit here is critical at scale — at 640
    // concurrent ranks each MetadataLookup that fell through to chunk-0
    // open/read/close was enough to stall the WriteChunk pipeline. The
    // cache is a write-through layer; the on-disk chunk-0 extension is
    // still the durable source of truth for recovery.
    if let Ok(file_meta) = ctx.metadata_manager.get_file_metadata(path_ref) {
        tracing::trace!(
            "MetadataLookup cache hit: path={}, size={}",
            file_meta.path,
            file_meta.size
        );
        return Ok((MetadataLookupResponseHeader::file(file_meta.size), am_msg));
    }

    // Slow path: read from chunk-0 ext on disk, then populate the cache so
    // subsequent lookups hit memory.
    if let Some(iouring) = ctx.iouring_chunk_store() {
        match iouring.read_chunk0_extension(&path).await {
            Ok(Some(ext)) => {
                tracing::debug!(
                    "MetadataLookup chunk-0 ext hit: path={}, size={}",
                    path,
                    ext.logical_size
                );
                // Repopulate the in-memory cache for next time.
                let _ =
                    ctx.metadata_manager
                        .store_file_metadata(crate::metadata::FileMetadata::new(
                            path.clone(),
                            ext.logical_size,
                        ));
                return Ok((MetadataLookupResponseHeader::file(ext.logical_size), am_msg));
            }
            Ok(None) => { /* fall through to directory check */ }
            Err(e) => {
                tracing::warn!("chunk-0 stat failed for {}: {:?}", path, e);
            }
        }
    }

    // Directories are still in-memory; CHFS uses real POSIX directories,
    // which is out of scope for this phase.
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

    // CHFS POSIX-style write-through: persist on chunk-0 (durable) first,
    // then update the in-memory cache so subsequent lookups skip the disk.
    if let Some(iouring) = ctx.iouring_chunk_store() {
        if let Err(e) = iouring
            .write_chunk0_extension(
                &path,
                crate::storage::BenchfsChunk0Extension::with_size(header.size),
            )
            .await
        {
            tracing::error!("Failed to write chunk-0 extension for {}: {:?}", path, e);
            return Ok((MetadataCreateFileResponseHeader::error(-5), am_msg)); // EIO
        }
    } else {
        tracing::error!("MetadataCreate: no IOUringChunkStore backend available");
        return Ok((MetadataCreateFileResponseHeader::error(-5), am_msg)); // EIO
    }

    // Populate in-memory cache. Duplicate creates are benign; just refresh
    // the cached size to whatever the latest write produced.
    let cache_entry = crate::metadata::FileMetadata::new(path.clone(), header.size);
    if ctx
        .metadata_manager
        .update_file_metadata(cache_entry.clone())
        .is_err()
    {
        let _ = ctx.metadata_manager.store_file_metadata(cache_entry);
    }

    tracing::debug!("Created file (chunk-0 backed): path={}", path);
    Ok((MetadataCreateFileResponseHeader::success(0), am_msg)) // Dummy inode
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
    if header.entry_type == 1 {
        // For files, deleting the chunk-0 (and other chunk) files removes
        // both data and metadata in one shot — CHFS POSIX-style.
        if let Some(iouring) = ctx.iouring_chunk_store() {
            if let Err(e) = iouring.delete_file_chunks(&path).await {
                tracing::warn!("delete_file_chunks failed for {}: {:?}", path, e);
            }
        }
        // Invalidate the in-memory cache.
        let _ = ctx.metadata_manager.remove_file_metadata(path_ref);
        tracing::debug!("Deleted file (chunk-0 backed): path={}", path);
        Ok((MetadataDeleteResponseHeader::success(), am_msg))
    } else if header.entry_type == 2 {
        match ctx.metadata_manager.remove_dir_metadata(path_ref) {
            Ok(()) => {
                tracing::debug!("Deleted directory metadata: path={}", path);
                Ok((MetadataDeleteResponseHeader::success(), am_msg))
            }
            Err(e) => {
                tracing::error!("Failed to delete directory metadata: {:?}", e);
                Ok((MetadataDeleteResponseHeader::error(-2), am_msg)) // ENOENT
            }
        }
    } else {
        tracing::error!("Invalid entry_type: {}", header.entry_type);
        Ok((MetadataDeleteResponseHeader::error(-22), am_msg)) // EINVAL
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

    // Update size on chunk-0's BenchFS extension, max-extend semantics.
    //
    // Use extend (max) rather than overwrite so concurrent writers to a
    // shared file don't trample each other's metadata. Without this, the
    // last-arriving close from a low-rank IOR client (whose local cache only
    // saw its own offset range) could shrink the recorded size below what
    // higher-rank clients had already written, and the read phase would see
    // file_meta.size=0 / short reads.
    //
    // Truncate (e.g. on open with O_TRUNC) deliberately uses 0; treat 0 as a
    // truncate intent and pass it through.
    if header.should_update_size() {
        let updated = if let Some(iouring) = ctx.iouring_chunk_store() {
            match iouring.update_logical_size(&path, header.new_size).await {
                Ok(updated) => {
                    tracing::debug!(
                        "Updated chunk-0 logical_size -> {} (requested={}, path={})",
                        updated,
                        header.new_size,
                        path
                    );
                    updated
                }
                Err(e) => {
                    tracing::error!("Failed to update chunk-0 logical_size: {:?}", e);
                    return Ok((MetadataUpdateResponseHeader::error(-5), am_msg)); // EIO
                }
            }
        } else {
            tracing::error!("MetadataUpdate: no IOUringChunkStore backend available");
            return Ok((MetadataUpdateResponseHeader::error(-5), am_msg)); // EIO
        };

        // Refresh the in-memory cache so future MetadataLookup hits don't
        // need to re-read the chunk-0 extension.
        let cache_entry = crate::metadata::FileMetadata::new(path.clone(), updated);
        if ctx
            .metadata_manager
            .update_file_metadata(cache_entry.clone())
            .is_err()
        {
            let _ = ctx.metadata_manager.store_file_metadata(cache_entry);
        }
    }

    if header.should_update_mode() {
        tracing::debug!("Updated file mode: {:#o} (path={})", header.new_mode, path);
        // BenchFS doesn't currently persist mode; CHFS records it via POSIX
        // file mode bits, which would require chmod() at the chunk file
        // level — out of scope for this phase.
    }

    Ok((MetadataUpdateResponseHeader::success(), am_msg))
}

#[cfg(test)]
mod tests {

    // Note: Testing with IOUringChunkStore requires async runtime setup
    // These tests are disabled as they would need complex setup with io_uring reactor

    // Note: Testing the actual handlers requires creating AmMsg instances,
    // which is difficult without a real UCX worker. These would be integration tests.
}
