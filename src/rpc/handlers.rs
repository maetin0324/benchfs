use std::rc::Rc;
use std::cell::RefCell;
use std::collections::HashMap;

use pluvio_ucx::{Worker, async_ucx::ucp::AmMsg};

use crate::metadata::MetadataManager;
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
    pub worker: Rc<Worker>,
    /// Endpoint cache for reusing connections to clients
    /// Key: client WorkerAddress bytes, Value: Rc<Endpoint>
    client_endpoints: RefCell<HashMap<Vec<u8>, Rc<pluvio_ucx::endpoint::Endpoint>>>,
    /// Shutdown flag for graceful termination
    shutdown_flag: RefCell<bool>,
}

impl RpcHandlerContext {
    pub fn new(
        metadata_manager: Rc<MetadataManager>,
        chunk_store: Rc<dyn ChunkStore>,
        allocator: Rc<pluvio_uring::allocator::FixedBufferAllocator>,
        worker: Rc<Worker>,
    ) -> Self {
        Self {
            metadata_manager,
            chunk_store,
            allocator,
            worker,
            client_endpoints: RefCell::new(HashMap::new()),
            shutdown_flag: RefCell::new(false),
        }
    }

    /// Create a minimal context for benchmark (no storage/metadata)
    pub fn new_bench(worker: Rc<Worker>) -> Self {
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
            worker,
            client_endpoints: RefCell::new(HashMap::new()),
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

    /// Send a response directly to the client using WorkerAddress
    /// instead of reply_ep to avoid UCX lifetime issues
    #[tracing::instrument(skip(self, client_addr, header, data), fields(
        client_addr_len = client_addr.len(),
        stream_id = stream_id,
        rpc_id = rpc_id,
        data_len = data.map(|d| d.len()).unwrap_or(0)
    ))]
    pub async fn send_response_direct<H: crate::rpc::Serializable>(
        &self,
        client_addr: &[u8],
        stream_id: u16,
        rpc_id: u16,
        header: &H,
        data: Option<&[u8]>,
    ) -> Result<(), RpcError> {
        use pluvio_ucx::async_ucx::ucp::WorkerAddressInner;

        // Log client address bytes for debugging
        tracing::trace!(
            "Client address bytes: {:?} (len={})",
            &client_addr[..client_addr.len().min(32)],
            client_addr.len()
        );

        // Try to get endpoint from cache, or create new one
        // IMPORTANT: Split the borrow scopes to avoid holding RefCell borrow
        // across potentially blocking operations (connect_addr)
        let endpoint = {
            // First, try to get from cache with a short read-only borrow
            let cache = self.client_endpoints.borrow();
            if let Some(ep) = cache.get(client_addr) {
                // Cache hit - reuse existing endpoint
                tracing::debug!(
                    "Endpoint cache hit (stream_id={}, rpc_id={}, cache_size={})",
                    stream_id,
                    rpc_id,
                    cache.len()
                );
                Some(Rc::clone(ep))
            } else {
                // Cache miss - log and prepare to create
                tracing::debug!(
                    "Endpoint cache miss (stream_id={}, rpc_id={}, cache_size={})",
                    stream_id,
                    rpc_id,
                    cache.len()
                );
                None
            }
        };

        // If not in cache, create new endpoint OUTSIDE the borrow scope
        let endpoint = if let Some(ep) = endpoint {
            ep
        } else {
            // Create endpoint without holding any RefCell borrow
            tracing::trace!("Creating endpoint for client address");
            let worker_address = WorkerAddressInner::from(client_addr);
            let ep = self
                .worker
                .connect_addr(&worker_address)
                .map_err(|e| RpcError::TransportError(format!("Failed to connect to client: {:?}", e)))?;

            // Wrap in Rc
            let ep_rc = Rc::new(ep);

            // Insert into cache with a short-lived exclusive borrow
            {
                let mut cache = self.client_endpoints.borrow_mut();
                cache.insert(client_addr.to_vec(), Rc::clone(&ep_rc));
                tracing::debug!(
                    "Endpoint created and cached (stream_id={}, rpc_id={}, new_cache_size={})",
                    stream_id,
                    rpc_id,
                    cache.len()
                );
            } // borrow_mut is released here immediately

            ep_rc
        };

        // Serialize header
        let header_bytes = zerocopy::IntoBytes::as_bytes(header);
        tracing::trace!("Header size: {} bytes", header_bytes.len());

        // Prepare data payload
        let data_slice = data.unwrap_or(&[]);

        // Prepare IoSlice for data
        let data_ioslice = if data_slice.is_empty() {
            vec![]
        } else {
            vec![std::io::IoSlice::new(data_slice)]
        };

        // Determine protocol
        let proto = if data_slice.is_empty() {
            None
        } else {
            Some(pluvio_ucx::async_ucx::ucp::AmProto::Rndv)
        };

        tracing::debug!(
            "Sending AM response: stream_id={}, header_size={}, data_size={}, proto={:?}",
            stream_id,
            header_bytes.len(),
            data_slice.len(),
            proto
        );

        // Send response via AM (stream_id is the reply stream)
        endpoint
            .am_send_vectorized(
                stream_id as u32,
                header_bytes,
                &data_ioslice,
                false, // Not need_reply
                proto,
            )
            .await
            .map_err(|e| {
                tracing::error!(
                    "Failed to send AM response: stream_id={}, error={:?}",
                    stream_id,
                    e
                );
                RpcError::TransportError(format!("Failed to send response: {:?}", e))
            })?;

        tracing::debug!(
            "Successfully sent direct response to client (stream_id={}, rpc_id={})",
            stream_id,
            rpc_id
        );

        Ok(())
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
