use std::cell::RefCell;
use std::collections::HashMap;
/// File operations for BenchFS
///
/// This module provides POSIX-like file operations that work with
/// the distributed metadata and data storage.
use std::rc::Rc;
use std::time::Duration;

use futures::FutureExt;
use futures_timer::Delay;
use pluvio_runtime::spawn_with_name;
use tracing::instrument;

use crate::api::types::{ApiError, ApiResult, FileHandle, OpenFlags};
use crate::cache::{ChunkCache, ChunkId};
use crate::data::{ChunkManager, PlacementStrategy, RoundRobinPlacement};
use crate::metadata::{
    ConsistentHashRing, DirectoryMetadata, FileMetadata, InodeType, MetadataManager,
};
use crate::rpc::AmRpc;
use crate::rpc::connection::ConnectionPool;
use crate::rpc::data_ops::{ReadChunkByIdRequest, WriteChunkByIdRequest};
use crate::rpc::file_id::FileId;
use crate::rpc::metadata_ops::{MetadataCreateFileRequest, MetadataLookupRequest};
use crate::storage::IOUringChunkStore;

/// BenchFS Filesystem Client
///
/// This is the main entry point for filesystem operations.
/// It maintains connections to metadata and data servers.
pub struct BenchFS {
    /// Node ID for this client
    node_id: String,

    /// Metadata manager (for local metadata)
    metadata_manager: Rc<MetadataManager>,

    /// Data node consistent hash ring (for chunk distribution across storage nodes)
    data_ring: Option<Rc<ConsistentHashRing>>,

    /// Metadata consistent hash ring (for distributed metadata)
    metadata_ring: Option<Rc<ConsistentHashRing>>,

    /// Chunk store (for local operations)
    chunk_store: Rc<IOUringChunkStore>,

    /// Chunk cache
    chunk_cache: ChunkCache,

    /// Chunk manager
    chunk_manager: ChunkManager,

    /// Placement strategy
    #[allow(dead_code)]
    placement: Rc<dyn PlacementStrategy>,

    /// Open file descriptors
    open_files: RefCell<HashMap<u64, FileHandle>>,

    /// Next file descriptor
    next_fd: RefCell<u64>,

    /// Connection pool for remote RPC calls
    connection_pool: Option<Rc<ConnectionPool>>,
}

impl BenchFS {
    /// Create a new BenchFS client (local only)
    ///
    /// # Arguments
    /// * `node_id` - Node identifier
    /// * `chunk_store` - Chunk store for local data storage (using io_uring)
    pub fn new(node_id: String, chunk_store: Rc<IOUringChunkStore>) -> Self {
        BenchFSBuilder::new(node_id, chunk_store).build()
    }

    /// Create a new BenchFS client with connection pool (distributed mode)
    ///
    /// # Arguments
    /// * `node_id` - Node identifier
    /// * `chunk_store` - Chunk store for local data storage (using io_uring)
    /// * `connection_pool` - Connection pool for RPC communication
    pub fn with_connection_pool(
        node_id: String,
        chunk_store: Rc<IOUringChunkStore>,
        connection_pool: Rc<ConnectionPool>,
    ) -> Self {
        BenchFSBuilder::new(node_id, chunk_store)
            .with_connection_pool(connection_pool)
            .build()
    }

    /// Create a new BenchFS client with connection pool and custom target nodes (distributed mode)
    ///
    /// # Arguments
    /// * `node_id` - This client's node ID
    /// * `chunk_store` - Chunk store for local data storage (using io_uring)
    /// * `connection_pool` - Connection pool for RPC communication
    /// * `target_nodes` - List of target nodes for chunk placement
    pub fn with_connection_pool_and_targets(
        node_id: String,
        chunk_store: Rc<IOUringChunkStore>,
        connection_pool: Rc<ConnectionPool>,
        target_nodes: Vec<String>,
    ) -> Self {
        BenchFSBuilder::new(node_id, chunk_store)
            .with_connection_pool(connection_pool)
            .with_data_nodes(target_nodes)
            .build()
    }

    /// Create a new BenchFS client with distributed metadata support
    ///
    /// # Arguments
    /// * `node_id` - This client's node ID
    /// * `chunk_store` - Chunk store for local data storage (using io_uring)
    /// * `connection_pool` - Connection pool for RPC communication
    /// * `data_nodes` - List of data nodes for chunk placement
    /// * `metadata_nodes` - List of metadata server nodes
    pub fn with_distributed_metadata(
        node_id: String,
        chunk_store: Rc<IOUringChunkStore>,
        connection_pool: Rc<ConnectionPool>,
        data_nodes: Vec<String>,
        metadata_nodes: Vec<String>,
    ) -> Self {
        BenchFSBuilder::new(node_id, chunk_store)
            .with_connection_pool(connection_pool)
            .with_data_nodes(data_nodes)
            .with_metadata_nodes(metadata_nodes)
            .build()
    }

    /// Check if distributed mode is enabled
    pub fn is_distributed(&self) -> bool {
        self.connection_pool.is_some()
    }

    /// Get the metadata server node for a given path
    ///
    /// Returns the node ID responsible for this path's metadata.
    /// If distributed metadata is not enabled, returns this node's ID.
    fn get_metadata_node(&self, path: &str) -> String {
        if let Some(ring) = &self.metadata_ring {
            ring.get_node(path).unwrap_or_else(|| self.node_id.clone())
        } else {
            self.node_id.clone()
        }
    }

    /// Check if metadata for this path is stored locally
    fn is_local_metadata(&self, path: &str) -> bool {
        self.get_metadata_node(path) == self.node_id
    }

    /// Get the node responsible for storing a chunk (using consistent hashing)
    ///
    /// This method uses the data_ring (if available) to distribute chunks across storage nodes.
    /// If data_ring is not available, it falls back to metadata_ring for backward compatibility,
    /// and ultimately to the local node ID.
    fn get_chunk_node(&self, chunk_key: &str) -> String {
        // Priority 1: Use data node ring for chunk distribution
        if let Some(ring) = &self.data_ring {
            return ring
                .get_node(chunk_key)
                .unwrap_or_else(|| self.node_id.clone());
        }

        // Priority 2: Fall back to metadata ring (backward compatibility)
        if let Some(ring) = &self.metadata_ring {
            return ring
                .get_node(chunk_key)
                .unwrap_or_else(|| self.node_id.clone());
        }

        // Priority 3: Default to local node
        self.node_id.clone()
    }

    /// Get file metadata with automatic caching for distributed mode
    ///
    /// This helper function tries to get metadata locally first.
    /// If not found locally and in distributed mode, it fetches from remote server and caches locally.
    async fn get_file_metadata_cached(&self, path: &str) -> ApiResult<FileMetadata> {
        use std::path::Path;
        let path_ref = Path::new(path);

        // Try local first
        if let Ok(meta) = self.metadata_manager.get_file_metadata(path_ref) {
            return Ok(meta);
        }

        // If not local and in distributed mode, fetch from remote
        let metadata_node = self.get_metadata_node(path);
        if let Some(pool) = &self.connection_pool {
            match pool.get_or_connect(&metadata_node).await {
                Ok(client) => {
                    let request = MetadataLookupRequest::new(path.to_string());
                    match request.call(&*client).await {
                        Ok(response) if response.is_success() && response.is_file() => {
                            let meta = FileMetadata::new(path.to_string(), response.size);
                            // Note: In path-based KV design, chunk locations are not tracked in metadata

                            // Cache locally for future access
                            if let Err(e) = self.metadata_manager.store_file_metadata(meta.clone())
                            {
                                tracing::warn!("Failed to cache metadata locally: {:?}", e);
                            } else {
                                tracing::debug!("Cached metadata for {} locally", path);
                            }
                            Ok(meta)
                        }
                        Ok(_) => Err(ApiError::NotFound(path.to_string())),
                        Err(e) => Err(ApiError::Internal(format!("Remote lookup failed: {:?}", e))),
                    }
                }
                Err(e) => Err(ApiError::Internal(format!("Connection failed: {:?}", e))),
            }
        } else {
            Err(ApiError::NotFound(path.to_string()))
        }
    }

    /// Open a file
    ///
    /// # Arguments
    /// * `path` - File path
    /// * `flags` - Open flags
    ///
    /// # Returns
    /// File handle
    #[async_backtrace::framed]
    pub async fn benchfs_open(&self, path: &str, flags: OpenFlags) -> ApiResult<FileHandle> {
        let _span = tracing::debug_span!("api_open", path, ?flags).entered();

        use std::path::Path;
        let path_ref = Path::new(path);

        // Determine metadata server for this path
        let metadata_node = self.get_metadata_node(path);
        let is_local = self.is_local_metadata(path);
        tracing::debug!("Metadata node: {}, is_local: {}", metadata_node, is_local);

        // Lookup file metadata (local or remote)
        let file_meta = if is_local {
            // Local metadata lookup
            match self.metadata_manager.get_file_metadata(path_ref) {
                Ok(meta) => Some(meta),
                Err(_) => None,
            }
        } else {
            // Remote metadata lookup via RPC
            if let Some(pool) = &self.connection_pool {
                match pool.get_or_connect(&metadata_node).await {
                    Ok(client) => {
                        let request = MetadataLookupRequest::new(path.to_string());
                        match request.call(&*client).await {
                            Ok(response) if response.is_success() && response.is_file() => {
                                // File found on remote server - create local cache entry
                                let meta = FileMetadata::new(path.to_string(), response.size);
                                // Cache locally for future access
                                if let Err(e) =
                                    self.metadata_manager.store_file_metadata(meta.clone())
                                {
                                    tracing::warn!(
                                        "Failed to cache metadata in benchfs_open: {:?}",
                                        e
                                    );
                                } else {
                                    tracing::debug!("Cached metadata for {} in benchfs_open", path);
                                }
                                Some(meta)
                            }
                            Ok(_) => None, // Not found or is directory
                            Err(e) => {
                                tracing::warn!("Remote metadata lookup failed: {:?}", e);
                                None
                            }
                        }
                    }
                    Err(e) => {
                        return Err(ApiError::Internal(format!(
                            "Failed to connect to metadata server {}: {:?}",
                            metadata_node, e
                        )));
                    }
                }
            } else {
                return Err(ApiError::Internal(
                    "Distributed mode not enabled but metadata is remote".to_string(),
                ));
            }
        };

        let inode = if let Some(meta) = file_meta {
            // File exists
            if flags.create && !flags.truncate {
                return Err(ApiError::AlreadyExists(path.to_string()));
            }

            if flags.truncate {
                // Truncate file
                if is_local {
                    // Local truncate
                    let mut new_meta = meta.clone();
                    new_meta.size = 0;
                    // chunk_count and chunk_locations are not tracked in path-based KV design
                    self.metadata_manager
                        .update_file_metadata(new_meta)
                        .map_err(|e| ApiError::Internal(format!("Failed to truncate: {:?}", e)))?;
                } else {
                    // Remote truncate via RPC
                    if let Some(pool) = &self.connection_pool {
                        match pool.get_or_connect(&metadata_node).await {
                            Ok(client) => {
                                use crate::rpc::metadata_ops::MetadataUpdateRequest;
                                let request =
                                    MetadataUpdateRequest::new(path.to_string()).with_size(0);
                                match request.call(&*client).await {
                                    Ok(response) if response.is_success() => {
                                        tracing::debug!("Remote truncate succeeded for {}", path);

                                        // Update local cache after successful remote truncate
                                        let mut truncated_meta = meta.clone();
                                        truncated_meta.size = 0;
                                        // chunk_count and chunk_locations are not tracked in path-based KV design
                                        if let Err(e) = self
                                            .metadata_manager
                                            .update_file_metadata(truncated_meta)
                                        {
                                            tracing::warn!(
                                                "Failed to update local cache after remote truncate: {:?}",
                                                e
                                            );
                                        } else {
                                            tracing::debug!(
                                                "Updated local cache after remote truncate for {}",
                                                path
                                            );
                                        }
                                    }
                                    Ok(response) => {
                                        return Err(ApiError::Internal(format!(
                                            "Remote truncate failed with status {}",
                                            response.status
                                        )));
                                    }
                                    Err(e) => {
                                        return Err(ApiError::Internal(format!(
                                            "Remote truncate RPC error: {:?}",
                                            e
                                        )));
                                    }
                                }
                            }
                            Err(e) => {
                                return Err(ApiError::Internal(format!(
                                    "Failed to connect for truncate: {:?}",
                                    e
                                )));
                            }
                        }
                    }
                }
            }

            0 // Dummy inode in path-based KV design
        } else {
            // File doesn't exist
            if !flags.create {
                return Err(ApiError::NotFound(path.to_string()));
            }

            // Create new file (local or remote)
            let created_inode = if is_local {
                // Local file creation
                let file_meta = FileMetadata::new(path.to_string(), 0);

                self.metadata_manager
                    .store_file_metadata(file_meta)
                    .map_err(|e| ApiError::Internal(format!("Failed to create file: {:?}", e)))?;

                0 // Dummy inode
            } else {
                // Remote file creation via RPC
                if let Some(pool) = &self.connection_pool {
                    match pool.get_or_connect(&metadata_node).await {
                        Ok(client) => {
                            let request =
                                MetadataCreateFileRequest::new(path.to_string(), 0, 0o644);
                            match request.call(&*client).await {
                                Ok(response) if response.is_success() => {
                                    tracing::debug!("Remote file created: {}", path);

                                    // Cache newly created file metadata locally
                                    let file_meta = FileMetadata::new(path.to_string(), 0);
                                    if let Err(e) =
                                        self.metadata_manager.store_file_metadata(file_meta)
                                    {
                                        tracing::warn!(
                                            "Failed to cache metadata after remote create: {:?}",
                                            e
                                        );
                                    } else {
                                        tracing::debug!(
                                            "Cached metadata for newly created file {} locally",
                                            path
                                        );
                                    }

                                    0 // Dummy inode
                                }
                                Ok(response) => {
                                    return Err(ApiError::Internal(format!(
                                        "Remote create failed with status {}",
                                        response.status
                                    )));
                                }
                                Err(e) => {
                                    return Err(ApiError::Internal(format!(
                                        "Remote create RPC error: {:?}",
                                        e
                                    )));
                                }
                            }
                        }
                        Err(e) => {
                            return Err(ApiError::Internal(format!(
                                "Failed to connect for create: {:?}",
                                e
                            )));
                        }
                    }
                } else {
                    return Err(ApiError::Internal(
                        "Distributed mode not enabled but metadata is remote".to_string(),
                    ));
                }
            };

            // Update parent directory's children list
            if let (Some(parent_path), Some(filename)) =
                (Self::get_parent_path(path), Self::get_filename(path))
            {
                use std::path::Path;
                let parent_path_ref = Path::new(&parent_path);

                // Only update if parent directory is stored locally
                if let Ok(mut parent_meta) = self.metadata_manager.get_dir_metadata(parent_path_ref)
                {
                    parent_meta.add_child(filename, created_inode, InodeType::File);
                    if let Err(e) = self.metadata_manager.update_dir_metadata(parent_meta) {
                        tracing::warn!(
                            "Failed to update parent directory {} after file creation: {:?}",
                            parent_path,
                            e
                        );
                    } else {
                        tracing::debug!(
                            "Updated parent directory {} with new file {}",
                            parent_path,
                            path
                        );
                    }
                } else {
                    tracing::debug!(
                        "Parent directory {} not found locally, skipping children update",
                        parent_path
                    );
                }
            }

            created_inode
        };

        // Create file handle
        let fd = self.allocate_fd();
        let handle = FileHandle::new(fd, path.to_string(), inode, flags);

        if flags.append {
            // Set position to end of file
            // For remote files, we need to query size again
            let file_size = if is_local {
                if let Ok(meta) = self.metadata_manager.get_file_metadata(path_ref) {
                    meta.size
                } else {
                    0
                }
            } else {
                // Query remote metadata for size
                if let Some(pool) = &self.connection_pool {
                    match pool.get_or_connect(&metadata_node).await {
                        Ok(client) => {
                            let request = MetadataLookupRequest::new(path.to_string());
                            match request.call(&*client).await {
                                Ok(response) if response.is_success() && response.is_file() => {
                                    response.size
                                }
                                _ => 0,
                            }
                        }
                        _ => 0,
                    }
                } else {
                    0
                }
            };
            handle.seek(file_size);
        }

        self.open_files.borrow_mut().insert(fd, handle.clone());

        Ok(handle)
    }

    /// Read from a file
    ///
    /// # Arguments
    /// * `handle` - File handle
    /// * `buf` - Buffer to read into
    ///
    /// # Returns
    /// Number of bytes read
    #[async_backtrace::framed]
    #[instrument(level = "trace", name = "api_read", skip(self, buf), fields(path = %handle.path, len = buf.len(), pos = handle.position()))]
    pub async fn benchfs_read(&self, handle: &FileHandle, buf: &mut [u8]) -> ApiResult<usize> {
        if !handle.flags.read {
            return Err(ApiError::PermissionDenied(
                "File not opened for reading".to_string(),
            ));
        }

        // Get file metadata with caching
        tracing::trace!("Fetching file metadata");
        let file_meta = self.get_file_metadata_cached(&handle.path).await?;

        let offset = handle.position();
        let length = buf.len() as u64;

        // Check if we're at or past EOF
        if offset >= file_meta.size {
            return Ok(0);
        }

        // Calculate actual read length
        let actual_length = length.min(file_meta.size - offset);

        // Calculate which chunks to read
        let chunks = self
            .chunk_manager
            .calculate_read_chunks(offset, actual_length, file_meta.size)
            .map_err(|e| ApiError::Internal(format!("Failed to calculate chunks: {:?}", e)))?;

        // Read all chunks concurrently with zero-copy directly into user buffer
        tracing::trace!("Reading {} chunks concurrently (zero-copy)", chunks.len());
        use futures::future::join_all;

        let file_path = file_meta.path.clone();
        let fs_ptr = self as *const BenchFS;

        // Get raw pointer to buffer for zero-copy writes from multiple tasks
        // SAFETY: Each task writes to a non-overlapping region of the buffer
        let buf_ptr = buf.as_mut_ptr();
        let buf_len = buf.len();

        // Calculate buffer offsets for each chunk
        let mut buf_offset = 0usize;
        let chunk_infos: Vec<_> = chunks
            .iter()
            .map(|(chunk_index, chunk_offset, read_size)| {
                let info = (*chunk_index, *chunk_offset, *read_size, buf_offset);
                buf_offset += *read_size as usize;
                info
            })
            .collect();

        let chunk_futures: Vec<_> = chunk_infos
            .into_iter()
            .map(|(chunk_index, chunk_offset, read_size, buf_offset)| {
                let file_path = file_path.clone();

                spawn_with_name(
                    async move {
                        let fs = unsafe { &*fs_ptr };

                        // SAFETY: Each task writes to its own non-overlapping region
                        let chunk_buf = unsafe {
                            let end = (buf_offset + read_size as usize).min(buf_len);
                            std::slice::from_raw_parts_mut(buf_ptr.add(buf_offset), end - buf_offset)
                        };

                        // NOTE: Chunk cache disabled for read operations to ensure consistency
                        // in distributed environments where other nodes may modify the same data.

                        tracing::trace!(
                            "Reading chunk {} (offset={}, size={}, buf_offset={})",
                            chunk_index,
                            chunk_offset,
                            read_size,
                            buf_offset
                        );

                        // Try local storage first with actual offset and size
                        match fs
                            .chunk_store
                            .read_chunk(&file_path, chunk_index, chunk_offset, read_size)
                            .await
                        {
                            Ok(chunk_data) => {
                                // Copy to user buffer (no caching for consistency)
                                let copy_len = chunk_data.len().min(chunk_buf.len());
                                chunk_buf[..copy_len].copy_from_slice(&chunk_data[..copy_len]);
                                (chunk_index, Ok::<usize, ()>(copy_len))
                            }
                            Err(_) => {
                                if let Some(pool) = &fs.connection_pool {
                                    let chunk_key = format!("{}/{}", &file_path, chunk_index);
                                    let node_id = fs.get_chunk_node(&chunk_key);

                                    tracing::debug!(
                                        "Fetching chunk {} from remote node {} (offset={}, size={})",
                                        chunk_index,
                                        node_id,
                                        chunk_offset,
                                        read_size
                                    );

                                    match pool.get_or_connect(&node_id).await {
                                        Ok(client) => {
                                            // Use FileId-based RPC for compact headers (32 bytes vs 288 bytes)
                                            // FileId contains path_hash (lower 32 bits) and chunk_id (upper 32 bits)
                                            let file_id = FileId::new(&file_path, chunk_index);

                                            // Zero-copy: Read directly into user buffer
                                            let request = ReadChunkByIdRequest::from_file_id(
                                                file_id,
                                                chunk_offset,
                                                read_size,
                                                chunk_buf,
                                            );

                                            match request.call(&*client).await {
                                                Ok(response) if response.is_success() => {
                                                    let bytes_read = response.bytes_read as usize;
                                                    tracing::debug!(
                                                        "Successfully fetched {} bytes from remote node (zero-copy, FileId)",
                                                        bytes_read
                                                    );
                                                    // Note: For zero-copy reads, we don't cache
                                                    // because the data is already in the user buffer
                                                    (chunk_index, Ok(bytes_read))
                                                }
                                                Ok(response) => {
                                                    tracing::warn!(
                                                        "Remote read (FileId) failed with status {}",
                                                        response.status
                                                    );
                                                    // Fill with zeros on error
                                                    chunk_buf.fill(0);
                                                    (chunk_index, Ok(chunk_buf.len()))
                                                }
                                                Err(e) => {
                                                    tracing::error!("RPC error (FileId read): {:?}", e);
                                                    chunk_buf.fill(0);
                                                    (chunk_index, Ok(chunk_buf.len()))
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            tracing::error!(
                                                "Failed to connect to {}: {:?}",
                                                node_id,
                                                e
                                            );
                                            chunk_buf.fill(0);
                                            (chunk_index, Ok(chunk_buf.len()))
                                        }
                                    }
                                } else {
                                    // No connection pool, fill with zeros (sparse file)
                                    chunk_buf.fill(0);
                                    (chunk_index, Ok(chunk_buf.len()))
                                }
                            }
                        }
                    },
                    format!("benchfs_read_chunk_{}", chunk_index),
                )
            })
            .collect();

        // Execute all reads concurrently
        let chunk_results_handles = join_all(chunk_futures).await;

        // Sum up bytes read from all chunks
        let mut bytes_read = 0;
        for result in chunk_results_handles {
            let (chunk_index, read_result) = result
                .map_err(|e| ApiError::Internal(format!("Chunk read task failed: {}", e)))?;
            match read_result {
                Ok(len) => bytes_read += len,
                Err(e) => {
                    tracing::error!("Chunk {} read failed: {:?}", chunk_index, e);
                }
            }
        }

        // Advance file position
        handle.advance(bytes_read as u64);

        Ok(bytes_read)
    }

    /// Write to a file
    ///
    /// # Arguments
    /// * `handle` - File handle
    /// * `data` - Data to write
    ///
    /// # Returns
    /// Number of bytes written
    #[async_backtrace::framed]
    #[instrument(level = "trace", name = "api_write", skip(self, data), fields(path = %handle.path, len = data.len(), pos = handle.position()))]
    pub async fn benchfs_write(&self, handle: &FileHandle, data: &[u8]) -> ApiResult<usize> {
        if !handle.flags.write {
            return Err(ApiError::PermissionDenied(
                "File not opened for writing".to_string(),
            ));
        }

        // Get file metadata with caching
        tracing::trace!("Fetching file metadata");
        let mut file_meta = self.get_file_metadata_cached(&handle.path).await?;

        let offset = handle.position();
        let length = data.len() as u64;

        // Calculate which chunks to write (using read_chunks as a workaround)
        // Note: For writing, we use the same logic as reading to determine chunk boundaries
        let new_size = (offset + length).max(file_meta.size);
        let chunks = self
            .chunk_manager
            .calculate_read_chunks(offset, length, new_size)
            .map_err(|e| ApiError::Internal(format!("Failed to calculate chunks: {:?}", e)))?;

        // Invalidate cache for all chunks (write-through)
        for (chunk_index, _chunk_offset, _write_size) in &chunks {
            let chunk_id = ChunkId::from_path(&file_meta.path, *chunk_index);
            self.chunk_cache.invalidate(&chunk_id);
        }

        // Write all chunks concurrently
        tracing::trace!("Writing {} chunks concurrently", chunks.len());
        use futures::future::join_all;

        let fs_ptr = self as *const BenchFS;
        let chunk_write_futures: Vec<_> = chunks
            .iter()
            .enumerate()
            .map(|(idx, (chunk_index, chunk_offset, write_size))| {
                let chunk_index = *chunk_index;
                let chunk_offset = *chunk_offset;
                let write_size = *write_size;

                // Calculate data slice for this chunk
                let data_offset: usize = chunks.iter().take(idx).map(|(_, _, s)| *s as usize).sum();
                let data_len = write_size as usize;
                let chunk_data = data[data_offset..data_offset + data_len].to_vec();

                let file_path = file_meta.path.clone();
                let chunk_key = format!("{}/{}", &file_path, chunk_index);
                let target_node = self.get_chunk_node(&chunk_key);
                let is_local = target_node == self.metadata_manager.self_node_id();

                spawn_with_name(
                    async move {
                        let fs = unsafe { &*fs_ptr };
                        if is_local {
                            // Write to local chunk store
                            fs.chunk_store
                                .write_chunk(
                                    &file_path,
                                    chunk_index,
                                    chunk_offset,
                                    chunk_data.as_slice(),
                                )
                                .await
                                .map_err(|e| {
                                    ApiError::IoError(format!(
                                        "Failed to write chunk {}: {:?}",
                                        chunk_index, e
                                    ))
                                })?;
                            Ok::<usize, ApiError>(data_len)
                        } else if let Some(pool) = &fs.connection_pool {
                            // Write to remote node using node_id
                            tracing::debug!(
                                "Writing chunk {} to remote node {}",
                                chunk_index,
                                target_node
                            );

                            match pool.get_or_connect(&target_node).await {
                                Ok(client) => {
                                    // Use FileId-based RPC for compact headers (32 bytes vs 288 bytes)
                                    let file_id = FileId::new(&file_path, chunk_index);
                                    let request = WriteChunkByIdRequest::from_file_id(
                                        file_id,
                                        chunk_offset,
                                        chunk_data.as_slice(),
                                    );

                                    // Execute RPC
                                    match request.call(&*client).await {
                                        Ok(response) if response.is_success() => {
                                            tracing::debug!(
                                                "Successfully wrote {} bytes to remote node (FileId)",
                                                response.bytes_written
                                            );
                                            Ok(data_len)
                                        }
                                        Ok(response) => Err(ApiError::IoError(format!(
                                            "Remote write (FileId) failed with status {}",
                                            response.status
                                        ))),
                                        Err(e) => {
                                            Err(ApiError::IoError(format!("RPC error (FileId write): {:?}", e)))
                                        }
                                    }
                                }
                                Err(e) => Err(ApiError::IoError(format!(
                                    "Failed to connect to {}: {:?}",
                                    target_node, e
                                ))),
                            }
                        } else {
                            // Not in distributed mode but chunk should be on a different node
                            Err(ApiError::Internal(format!(
                                "Chunk {} should be on node {} but distributed mode is not enabled",
                                chunk_index, target_node
                            )))
                        }
                    },
                    format!("benchfs_write_chunk_{}", chunk_index),
                )
            })
            .collect();

        // Execute all writes concurrently
        let write_results_handles = join_all(chunk_write_futures).await;

        // Check results and calculate total bytes written
        tracing::trace!("Completed {} chunk writes", write_results_handles.len());
        let mut bytes_written = 0;
        for result in write_results_handles {
            let chunk_result = result
                .map_err(|e| ApiError::Internal(format!("Chunk write task failed: {}", e)))??;
            bytes_written += chunk_result;
        }

        // Update file size if we wrote past the end
        let new_size = (offset + length).max(file_meta.size);
        if new_size != file_meta.size {
            file_meta.size = new_size;
            // chunk_count is calculated on demand via calculate_chunk_count()

            // Update local metadata only
            // Remote metadata will be synchronized when file is closed
            self.metadata_manager
                .update_file_metadata(file_meta)
                .map_err(|e| ApiError::Internal(format!("Failed to update metadata: {:?}", e)))?;
        }

        // Advance file position
        handle.advance(bytes_written as u64);

        Ok(bytes_written)
    }

    /// Close a file
    ///
    /// # Arguments
    /// * `handle` - File handle
    #[async_backtrace::framed]
    pub async fn benchfs_close(&self, handle: &FileHandle) -> ApiResult<()> {
        let _span = tracing::debug_span!("api_close", path = %handle.path).entered();

        use std::path::Path;
        let path_ref = Path::new(&handle.path);

        // Sync metadata to remote server if this is a remote file
        if handle.flags.write {
            let metadata_node = self.get_metadata_node(&handle.path);
            let is_local = self.is_local_metadata(&handle.path);

            if !is_local {
                // Get final file metadata from local cache
                if let Ok(file_meta) = self.metadata_manager.get_file_metadata(path_ref) {
                    // Sync to remote metadata server
                    if let Some(pool) = &self.connection_pool {
                        // Use timeout to prevent hanging on close
                        let timeout_duration = Duration::from_secs(5);
                        let mut timeout = Delay::new(timeout_duration).fuse();

                        // Try to connect with timeout
                        let connect_future = pool.get_or_connect(&metadata_node).fuse();
                        futures::pin_mut!(connect_future);

                        let client_result = futures::select! {
                            result = connect_future => Some(result),
                            _ = timeout => {
                                tracing::warn!(
                                    "Connection timeout for metadata sync on close: {} (timeout: {:?})",
                                    handle.path,
                                    timeout_duration
                                );
                                None
                            }
                        };

                        if let Some(Ok(client)) = client_result {
                            use crate::rpc::metadata_ops::MetadataUpdateRequest;
                            let request = MetadataUpdateRequest::new(handle.path.clone())
                                .with_size(file_meta.size);

                            // Also use timeout for RPC call
                            let mut rpc_timeout = Delay::new(timeout_duration).fuse();
                            let rpc_future = request.call(&*client).fuse();
                            futures::pin_mut!(rpc_future);

                            let rpc_result = futures::select! {
                                result = rpc_future => Some(result),
                                _ = rpc_timeout => {
                                    tracing::warn!(
                                        "Metadata sync RPC timeout on close: {} (timeout: {:?})",
                                        handle.path,
                                        timeout_duration
                                    );
                                    None
                                }
                            };

                            match rpc_result {
                                Some(Ok(response)) if response.is_success() => {
                                    tracing::debug!(
                                        "Synced metadata on close: {} (size: {})",
                                        handle.path,
                                        file_meta.size
                                    );
                                }
                                Some(Ok(response)) => {
                                    tracing::warn!(
                                        "Failed to sync metadata on close: {} (status: {})",
                                        handle.path,
                                        response.status
                                    );
                                }
                                Some(Err(e)) => {
                                    tracing::warn!("Metadata sync RPC error on close: {:?}", e);
                                }
                                None => {
                                    // Already logged timeout
                                }
                            }
                        } else if let Some(Err(e)) = client_result {
                            tracing::warn!("Failed to connect for metadata sync on close: {:?}", e);
                        }
                    }
                }
            }
        }

        self.open_files.borrow_mut().remove(&handle.fd);
        Ok(())
    }

    /// Delete a file
    ///
    /// # Arguments
    /// * `path` - File path
    #[async_backtrace::framed]
    pub async fn benchfs_unlink(&self, path: &str) -> ApiResult<()> {
        use std::path::Path;
        let path_ref = Path::new(path);

        // Get file metadata to get inode
        let file_meta = self
            .metadata_manager
            .get_file_metadata(path_ref)
            .map_err(|_| ApiError::NotFound(path.to_string()))?;

        // Invalidate all cached chunks for this file
        self.chunk_cache.invalidate_path(&file_meta.path);

        // Delete all chunks
        let _ = self.chunk_store.delete_file_chunks(&file_meta.path).await;

        // Delete metadata
        self.metadata_manager
            .remove_file_metadata(path_ref)
            .map_err(|e| ApiError::Internal(format!("Failed to delete metadata: {:?}", e)))?;

        // Update parent directory's children list
        if let (Some(parent_path), Some(filename)) =
            (Self::get_parent_path(path), Self::get_filename(path))
        {
            use std::path::Path;
            let parent_path_ref = Path::new(&parent_path);

            // Only update if parent directory is stored locally
            if let Ok(mut parent_meta) = self.metadata_manager.get_dir_metadata(parent_path_ref) {
                parent_meta.remove_child(&filename);
                if let Err(e) = self.metadata_manager.update_dir_metadata(parent_meta) {
                    tracing::warn!(
                        "Failed to update parent directory {} after file deletion: {:?}",
                        parent_path,
                        e
                    );
                } else {
                    tracing::debug!(
                        "Updated parent directory {} after deleting file {}",
                        parent_path,
                        path
                    );
                }
            } else {
                tracing::debug!(
                    "Parent directory {} not found locally, skipping children update",
                    parent_path
                );
            }
        }

        Ok(())
    }

    /// Create a directory
    ///
    /// # Arguments
    /// * `path` - Directory path
    /// * `mode` - Permissions (Unix-style)
    /// Create a directory (dummy implementation)
    ///
    /// In the new KV design, directories are not needed as we use full paths as keys.
    /// This always succeeds to maintain API compatibility.
    #[async_backtrace::framed]
    pub async fn benchfs_mkdir(&self, _path: &str, _mode: u32) -> ApiResult<()> {
        // Dummy implementation: always succeed
        // Directories are meaningless in full-path KV design
        tracing::debug!("mkdir (dummy): path={}", _path);
        Ok(())
    }

    /// Remove a directory (dummy implementation)
    ///
    /// In the new KV design, directories are not needed.
    /// This always succeeds to maintain API compatibility.
    pub fn benchfs_rmdir(&self, _path: &str) -> ApiResult<()> {
        // Dummy implementation: always succeed
        tracing::debug!("rmdir (dummy): path={}", _path);
        Ok(())
    }

    /// Seek to a position in a file
    ///
    /// # Arguments
    /// * `handle` - File handle
    /// * `offset` - Offset from whence
    /// * `whence` - Seek mode (0=SET, 1=CUR, 2=END)
    ///
    /// # Returns
    /// New file position
    pub fn benchfs_seek(&self, handle: &FileHandle, offset: i64, whence: i32) -> ApiResult<u64> {
        use std::path::Path;
        let path_ref = Path::new(&handle.path);

        let new_pos = match whence {
            0 => {
                // SEEK_SET
                if offset < 0 {
                    return Err(ApiError::InvalidArgument("Negative offset".to_string()));
                }
                offset as u64
            }
            1 => {
                // SEEK_CUR
                let current = handle.position() as i64;
                let new = current + offset;
                if new < 0 {
                    return Err(ApiError::InvalidArgument(
                        "Seek before beginning".to_string(),
                    ));
                }
                new as u64
            }
            2 => {
                // SEEK_END
                let file_meta = self
                    .metadata_manager
                    .get_file_metadata(path_ref)
                    .map_err(|e| ApiError::Internal(format!("Failed to get metadata: {:?}", e)))?;
                let end = file_meta.size as i64;
                let new = end + offset;
                if new < 0 {
                    return Err(ApiError::InvalidArgument(
                        "Seek before beginning".to_string(),
                    ));
                }
                new as u64
            }
            _ => return Err(ApiError::InvalidArgument("Invalid whence".to_string())),
        };

        handle.seek(new_pos);
        Ok(new_pos)
    }

    /// Synchronize file data to storage
    ///
    /// # Arguments
    /// * `handle` - File handle
    ///
    /// # Note
    /// For InMemoryChunkStore, this is a no-op since data is already in memory.
    /// For persistent backends (IOUringChunkStore), this ensures all writes are
    /// flushed to disk before returning.
    #[async_backtrace::framed]
    pub async fn benchfs_fsync(&self, handle: &FileHandle) -> ApiResult<()> {
        use std::path::Path;
        let path_ref = Path::new(&handle.path);

        // Get file metadata to get inode and chunk information
        let file_meta = self
            .metadata_manager
            .get_file_metadata(path_ref)
            .map_err(|e| ApiError::Internal(format!("Failed to get metadata: {:?}", e)))?;

        // For each chunk in the file, ensure it's synced to disk
        // Note: InMemoryChunkStore doesn't need fsync, but IOUringChunkStore would
        // Since we can't call fsync on InMemoryChunkStore directly, we log the operation
        tracing::debug!(
            "fsync called for file {} with {} chunks",
            handle.path,
            file_meta.calculate_chunk_count()
        );

        // In the future, when using IOUringChunkStore, we would call:
        // for chunk_idx in 0..file_meta.chunk_count {
        //     self.chunk_store.fsync_chunk(file_meta.inode, chunk_idx).await?;
        // }

        Ok(())
    }

    /// Get file or directory status
    ///
    /// # Arguments
    /// * `path` - File or directory path
    ///
    /// # Returns
    /// File status information (FileStat)
    #[async_backtrace::framed]
    pub async fn benchfs_stat(&self, path: &str) -> ApiResult<crate::api::types::FileStat> {
        use crate::api::types::FileStat;
        use std::path::Path;

        let path_ref = Path::new(path);

        // Determine metadata server for this path
        let metadata_node = self.get_metadata_node(path);
        let is_local = self.is_local_metadata(path);

        if is_local {
            // Local metadata lookup
            // Try file metadata first
            if let Ok(meta) = self.metadata_manager.get_file_metadata(path_ref) {
                return Ok(FileStat::from_file_metadata(&meta));
            }

            // Try directory metadata
            if let Ok(dir_meta) = self.metadata_manager.get_dir_metadata(path_ref) {
                return Ok(FileStat::from_dir_metadata(&dir_meta));
            }

            Err(ApiError::NotFound(path.to_string()))
        } else {
            // Remote metadata lookup via RPC
            if let Some(pool) = &self.connection_pool {
                match pool.get_or_connect(&metadata_node).await {
                    Ok(client) => {
                        let request = MetadataLookupRequest::new(path.to_string());
                        match request.call(&*client).await {
                            Ok(response) if response.is_success() && response.is_file() => {
                                // File found
                                let meta = FileMetadata::new(path.to_string(), response.size);
                                Ok(FileStat::from_file_metadata(&meta))
                            }
                            Ok(response) if response.is_success() && response.is_directory() => {
                                // Directory found
                                let dir_meta =
                                    DirectoryMetadata::new(response.inode, path.to_string());
                                Ok(FileStat::from_dir_metadata(&dir_meta))
                            }
                            Ok(_) => Err(ApiError::NotFound(path.to_string())),
                            Err(e) => Err(ApiError::Internal(format!(
                                "Remote stat RPC error: {:?}",
                                e
                            ))),
                        }
                    }
                    Err(e) => Err(ApiError::Internal(format!(
                        "Failed to connect to metadata server {}: {:?}",
                        metadata_node, e
                    ))),
                }
            } else {
                Err(ApiError::Internal(
                    "Distributed mode not enabled but metadata is remote".to_string(),
                ))
            }
        }
    }

    /// Rename a file or directory
    ///
    /// # Arguments
    /// * `old_path` - Current path
    /// * `new_path` - New path
    pub fn benchfs_rename(&self, old_path: &str, new_path: &str) -> ApiResult<()> {
        use std::path::Path;
        let old_path_ref = Path::new(old_path);
        let new_path_ref = Path::new(new_path);

        // Check if new path already exists
        if self
            .metadata_manager
            .get_file_metadata(new_path_ref)
            .is_ok()
            || self.metadata_manager.get_dir_metadata(new_path_ref).is_ok()
        {
            return Err(ApiError::AlreadyExists(new_path.to_string()));
        }

        // Try to rename file
        if let Ok(mut meta) = self.metadata_manager.get_file_metadata(old_path_ref) {
            // Remove old metadata
            self.metadata_manager
                .remove_file_metadata(old_path_ref)
                .map_err(|e| {
                    ApiError::Internal(format!("Failed to remove old metadata: {:?}", e))
                })?;

            // Update path and store new metadata
            meta.path = new_path.to_string();
            self.metadata_manager
                .store_file_metadata(meta)
                .map_err(|e| {
                    ApiError::Internal(format!("Failed to store new metadata: {:?}", e))
                })?;

            return Ok(());
        }

        // Try to rename directory
        if let Ok(mut meta) = self.metadata_manager.get_dir_metadata(old_path_ref) {
            // Remove old metadata
            self.metadata_manager
                .remove_dir_metadata(old_path_ref)
                .map_err(|e| {
                    ApiError::Internal(format!("Failed to remove old metadata: {:?}", e))
                })?;

            // Update path and store new metadata
            meta.path = new_path.to_string();
            self.metadata_manager
                .store_dir_metadata(meta)
                .map_err(|e| {
                    ApiError::Internal(format!("Failed to store new metadata: {:?}", e))
                })?;

            return Ok(());
        }

        Err(ApiError::NotFound(old_path.to_string()))
    }

    /// Read directory contents
    ///
    /// # Arguments
    /// * `path` - Directory path
    ///
    /// # Returns
    /// List of entry names in the directory
    pub fn benchfs_readdir(&self, _path: &str) -> ApiResult<Vec<String>> {
        tracing::debug!("readdir (dummy): path={}", _path);
        // Dummy implementation: directories are meaningless in path-based KV design
        Ok(Vec::new())
    }

    /// Truncate a file to a specified size
    ///
    /// # Arguments
    /// * `path` - File path
    /// * `size` - New file size
    ///
    /// # Note
    /// When truncating to a smaller size, chunks beyond the new size are deleted.
    /// When truncating to a larger size, the file is extended with zeros (sparse file).
    #[async_backtrace::framed]
    pub async fn benchfs_truncate(&self, path: &str, size: u64) -> ApiResult<()> {
        use std::path::Path;
        let path_ref = Path::new(path);

        let mut file_meta = self
            .metadata_manager
            .get_file_metadata(path_ref)
            .map_err(|_| ApiError::NotFound(path.to_string()))?;

        let old_size = file_meta.size;

        // Update size
        file_meta.size = size;
        let old_chunk_count = file_meta.calculate_chunk_count();
        // chunk_count is calculated on demand

        // If truncating to smaller size, delete affected chunks
        if size < old_size {
            let chunk_size = self.chunk_manager.chunk_size() as u64;
            let new_chunk_count = (size + chunk_size - 1) / chunk_size;

            // Delete chunks beyond the new size
            for chunk_idx in new_chunk_count..old_chunk_count {
                let chunk_id = ChunkId::from_path(&file_meta.path, chunk_idx);

                // Invalidate cache
                self.chunk_cache.invalidate(&chunk_id);

                // Delete from chunk store
                if let Err(e) = self
                    .chunk_store
                    .delete_chunk(&file_meta.path, chunk_idx)
                    .await
                {
                    tracing::warn!(
                        "Failed to delete chunk {} for path {}: {:?}",
                        chunk_idx,
                        file_meta.path,
                        e
                    );
                }
            }

            // Note: chunk_locations are not tracked in path-based KV design

            // If truncating within a chunk, we need to zero out the remaining bytes
            if size % chunk_size != 0 {
                let last_chunk_idx = new_chunk_count.saturating_sub(1);
                let last_chunk_offset = size % chunk_size;
                let bytes_to_zero = chunk_size - last_chunk_offset;

                if bytes_to_zero > 0 {
                    // Write zeros to the end of the last chunk
                    let zeros = vec![0u8; bytes_to_zero as usize];
                    if let Err(e) = self
                        .chunk_store
                        .write_chunk(&file_meta.path, last_chunk_idx, last_chunk_offset, &zeros)
                        .await
                    {
                        tracing::warn!(
                            "Failed to zero out last chunk {} for path {}: {:?}",
                            last_chunk_idx,
                            file_meta.path,
                            e
                        );
                    }

                    // Invalidate cache for the last chunk since we modified it
                    let chunk_id = ChunkId::from_path(&file_meta.path, last_chunk_idx);
                    self.chunk_cache.invalidate(&chunk_id);
                }
            }
        }
        // If extending the file, it becomes a sparse file (no action needed)

        self.metadata_manager
            .update_file_metadata(file_meta)
            .map_err(|e| ApiError::Internal(format!("Failed to update metadata: {:?}", e)))?;

        Ok(())
    }

    /// Allocate a new file descriptor
    fn allocate_fd(&self) -> u64 {
        let fd = *self.next_fd.borrow();
        *self.next_fd.borrow_mut() += 1;
        fd
    }

    /// Extract parent directory path from a file/directory path
    ///
    /// # Examples
    /// * "/foo/bar/file.txt" -> Some("/foo/bar")
    /// * "/file.txt" -> Some("/")
    /// * "/" -> None
    fn get_parent_path(path: &str) -> Option<String> {
        if path == "/" {
            return None;
        }

        let path = path.trim_end_matches('/');
        if let Some(last_slash) = path.rfind('/') {
            if last_slash == 0 {
                Some("/".to_string())
            } else {
                Some(path[..last_slash].to_string())
            }
        } else {
            None
        }
    }

    /// Extract filename from a file/directory path
    ///
    /// # Examples
    /// * "/foo/bar/file.txt" -> Some("file.txt")
    /// * "/file.txt" -> Some("file.txt")
    /// * "/" -> None
    fn get_filename(path: &str) -> Option<String> {
        if path == "/" {
            return None;
        }

        let path = path.trim_end_matches('/');
        if let Some(last_slash) = path.rfind('/') {
            Some(path[last_slash + 1..].to_string())
        } else {
            Some(path.to_string())
        }
    }
}

/// Builder for BenchFS client
///
/// This builder provides a flexible way to construct a BenchFS client
/// with various configuration options. It replaces the multiple constructor
/// methods with a single, consistent interface.
///
/// # Example
///
/// ```ignore
/// use benchfs::api::file_ops::BenchFSBuilder;
///
/// let benchfs = BenchFSBuilder::new("node1".to_string(), chunk_store)
///     .with_connection_pool(pool)
///     .with_data_nodes(vec!["node1".to_string(), "node2".to_string()])
///     .with_metadata_nodes(vec!["meta1".to_string(), "meta2".to_string()])
///     .with_cache_size(200)
///     .build();
/// ```
pub struct BenchFSBuilder {
    node_id: String,
    chunk_store: Rc<IOUringChunkStore>,
    connection_pool: Option<Rc<ConnectionPool>>,
    data_nodes: Option<Vec<String>>,
    metadata_nodes: Option<Vec<String>>,
    chunk_cache_mb: usize,
}

impl BenchFSBuilder {
    /// Create a new BenchFS builder
    ///
    /// # Arguments
    /// * `node_id` - Node identifier
    /// * `chunk_store` - Chunk store for local data storage
    pub fn new(node_id: String, chunk_store: Rc<IOUringChunkStore>) -> Self {
        Self {
            node_id,
            chunk_store,
            connection_pool: None,
            data_nodes: None,
            metadata_nodes: None,
            chunk_cache_mb: crate::config::defaults::CHUNK_CACHE_MB,
        }
    }

    /// Set the connection pool for distributed mode
    pub fn with_connection_pool(mut self, pool: Rc<ConnectionPool>) -> Self {
        self.connection_pool = Some(pool);
        self
    }

    /// Set the data nodes for chunk placement
    pub fn with_data_nodes(mut self, nodes: Vec<String>) -> Self {
        self.data_nodes = Some(nodes);
        self
    }

    /// Set the metadata server nodes for distributed metadata
    pub fn with_metadata_nodes(mut self, nodes: Vec<String>) -> Self {
        self.metadata_nodes = Some(nodes);
        self
    }

    /// Set the chunk cache size in MB
    pub fn with_cache_size(mut self, mb: usize) -> Self {
        self.chunk_cache_mb = mb;
        self
    }

    /// Build the BenchFS client
    pub fn build(self) -> BenchFS {
        let metadata_manager = Rc::new(MetadataManager::new(self.node_id.clone()));
        let chunk_cache = ChunkCache::with_memory_limit(self.chunk_cache_mb);
        let chunk_manager = ChunkManager::new();

        // Determine placement nodes
        let placement_nodes = self
            .data_nodes
            .clone()
            .unwrap_or_else(|| vec![self.node_id.clone()]);
        let placement = Rc::new(RoundRobinPlacement::new(placement_nodes));

        // Create data node ring if data nodes are specified
        // This ring is used for distributing chunks across storage nodes
        let data_ring = self.data_nodes.as_ref().and_then(|nodes| {
            if nodes.len() > 1 {
                let mut ring = ConsistentHashRing::new();
                for node in nodes {
                    ring.add_node(node.clone());
                }
                Some(Rc::new(ring))
            } else {
                // Don't create ring for single node (no distribution needed)
                None
            }
        });

        // Create metadata ring if metadata nodes are specified
        let metadata_ring = self.metadata_nodes.map(|nodes| {
            let mut ring = ConsistentHashRing::new();
            for node in nodes {
                ring.add_node(node);
            }
            Rc::new(ring)
        });

        BenchFS {
            node_id: self.node_id,
            metadata_manager,
            data_ring,
            metadata_ring,
            chunk_store: self.chunk_store,
            chunk_cache,
            chunk_manager,
            placement,
            open_files: RefCell::new(HashMap::new()),
            next_fd: RefCell::new(3),
            connection_pool: self.connection_pool,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::IOUringBackend;
    use pluvio_runtime::executor::Runtime;
    use pluvio_uring::reactor::IoUringReactor;

    fn create_test_chunk_store(runtime: &Runtime) -> Rc<IOUringChunkStore> {
        use std::sync::atomic::{AtomicU64, Ordering};
        static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

        // Create io_uring reactor
        let uring_reactor = IoUringReactor::builder()
            .queue_size(256)
            .buffer_size(1 << 20)
            .submit_depth(32)
            .wait_submit_timeout(std::time::Duration::from_micros(10))
            .wait_complete_timeout(std::time::Duration::from_micros(10))
            .build();

        let allocator = uring_reactor.allocator.clone();
        let reactor_for_backend = uring_reactor.clone();
        runtime.register_reactor("io_uring", uring_reactor);

        // Create IOUringBackend and chunk store with unique directory per test
        let io_backend = Rc::new(IOUringBackend::new(allocator, reactor_for_backend));
        let test_id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let temp_dir = format!("/tmp/benchfs_test_{}_{}", std::process::id(), test_id);
        let chunk_store_dir = format!("{}/chunks", temp_dir);
        std::fs::create_dir_all(&chunk_store_dir).unwrap();

        Rc::new(IOUringChunkStore::new(&chunk_store_dir, io_backend).unwrap())
    }

    fn run_test<F>(test: F)
    where
        F: FnOnce(
                Rc<Runtime>,
                Rc<IOUringChunkStore>,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()>>>
            + 'static,
    {
        let runtime = Runtime::new(256);
        let chunk_store = create_test_chunk_store(&runtime);
        let future = test(runtime.clone(), chunk_store);
        runtime.run_with_name_and_runtime("benchfs_file_ops_test", future);
    }

    #[test]
    fn test_benbenchfs_creation() {
        let runtime = Runtime::new(256);
        let chunk_store = create_test_chunk_store(&runtime);
        let fs = BenchFS::new("node1".to_string(), chunk_store);
        assert_eq!(fs.metadata_manager.self_node_id(), "node1");
    }

    #[test]
    fn test_create_and_open_file() {
        run_test(|_runtime, chunk_store| {
            Box::pin(async move {
                let fs = BenchFS::new("node1".to_string(), chunk_store);

                // Create and open a new file
                let handle = fs
                    .benchfs_open("/test.txt", OpenFlags::create())
                    .await
                    .unwrap();
                assert_eq!(handle.path, "/test.txt");
                assert!(handle.flags.write);

                fs.benchfs_close(&handle).await.unwrap();
            })
        })
    }

    #[test]
    fn test_write_and_read_file() {
        run_test(|_runtime, chunk_store| {
            Box::pin(async move {
                let fs = BenchFS::new("node1".to_string(), chunk_store);

                // Create file
                let handle = fs
                    .benchfs_open("/test.txt", OpenFlags::create())
                    .await
                    .unwrap();

                // Write data
                let data = b"Hello, BenchFS!";
                let written = fs.benchfs_write(&handle, data).await.unwrap();
                assert_eq!(written, data.len());

                fs.benchfs_close(&handle).await.unwrap();

                // Read data
                let handle = fs
                    .benchfs_open("/test.txt", OpenFlags::read_only())
                    .await
                    .unwrap();
                let mut buf = vec![0u8; 100];
                let read = fs.benchfs_read(&handle, &mut buf).await.unwrap();
                assert_eq!(read, data.len());
                assert_eq!(&buf[..read], data);

                fs.benchfs_close(&handle).await.unwrap();
            })
        })
    }

    #[test]
    fn test_unlink_file() {
        run_test(|_runtime, chunk_store| {
            Box::pin(async move {
                let fs = BenchFS::new("node1".to_string(), chunk_store);

                // Create file
                let handle = fs
                    .benchfs_open("/test.txt", OpenFlags::create())
                    .await
                    .unwrap();
                fs.benchfs_close(&handle).await.unwrap();

                // Delete file
                fs.benchfs_unlink("/test.txt").await.unwrap();

                // Try to open deleted file
                let result = fs.benchfs_open("/test.txt", OpenFlags::read_only()).await;
                assert!(result.is_err());
            })
        })
    }

    #[test]
    fn test_mkdir_and_rmdir() {
        run_test(|_runtime, chunk_store| {
            Box::pin(async move {
                let fs = BenchFS::new("node1".to_string(), chunk_store);

                // Create directory
                fs.benchfs_mkdir("/testdir", 0o755).await.unwrap();

                // Remove directory
                fs.benchfs_rmdir("/testdir").unwrap();
            })
        })
    }

    #[test]
    fn test_seek() {
        run_test(|_runtime, chunk_store| {
            Box::pin(async move {
                let fs = BenchFS::new("node1".to_string(), chunk_store);

                // Create file with data
                let handle = fs
                    .benchfs_open("/test.txt", OpenFlags::create())
                    .await
                    .unwrap();
                fs.benchfs_write(&handle, b"0123456789").await.unwrap();
                fs.benchfs_close(&handle).await.unwrap();

                // Open and seek
                let handle = fs
                    .benchfs_open("/test.txt", OpenFlags::read_only())
                    .await
                    .unwrap();

                // SEEK_SET
                let pos = fs.benchfs_seek(&handle, 5, 0).unwrap();
                assert_eq!(pos, 5);

                // SEEK_CUR
                let pos = fs.benchfs_seek(&handle, 2, 1).unwrap();
                assert_eq!(pos, 7);

                // SEEK_END
                let pos = fs.benchfs_seek(&handle, -3, 2).unwrap();
                assert_eq!(pos, 7);

                fs.benchfs_close(&handle).await.unwrap();
            })
        })
    }
}
