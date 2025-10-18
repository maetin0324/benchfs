/// File operations for BenchFS
///
/// This module provides POSIX-like file operations that work with
/// the distributed metadata and data storage.

use std::rc::Rc;
use std::cell::RefCell;
use std::collections::HashMap;

use crate::api::types::{ApiError, ApiResult, FileHandle, OpenFlags};
use crate::metadata::{MetadataManager, FileMetadata, DirectoryMetadata};
use crate::storage::InMemoryChunkStore;
use crate::data::{ChunkManager, PlacementStrategy, RoundRobinPlacement};
use crate::rpc::connection::ConnectionPool;
use crate::rpc::data_ops::{ReadChunkRequest, WriteChunkRequest};
use crate::rpc::AmRpc;
use crate::cache::{ChunkCache, ChunkId};

/// BenchFS Filesystem Client
///
/// This is the main entry point for filesystem operations.
/// It maintains connections to metadata and data servers.
pub struct BenchFS {
    /// Metadata manager
    metadata_manager: Rc<MetadataManager>,

    /// Chunk store (for local operations)
    chunk_store: Rc<InMemoryChunkStore>,

    /// Chunk cache
    chunk_cache: ChunkCache,

    /// Chunk manager
    chunk_manager: ChunkManager,

    /// Placement strategy
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
    pub fn new(node_id: String) -> Self {
        let metadata_manager = Rc::new(MetadataManager::new(node_id.clone()));
        let chunk_store = Rc::new(InMemoryChunkStore::new());
        let chunk_cache = ChunkCache::with_memory_limit(100); // 100 MB cache
        let chunk_manager = ChunkManager::new();
        let placement = Rc::new(RoundRobinPlacement::new(vec![node_id]));

        Self {
            metadata_manager,
            chunk_store,
            chunk_cache,
            chunk_manager,
            placement,
            open_files: RefCell::new(HashMap::new()),
            next_fd: RefCell::new(3), // Start from 3 (0, 1, 2 are reserved for stdin, stdout, stderr)
            connection_pool: None,
        }
    }

    /// Create a new BenchFS client with connection pool (distributed mode)
    pub fn with_connection_pool(node_id: String, connection_pool: Rc<ConnectionPool>) -> Self {
        let metadata_manager = Rc::new(MetadataManager::new(node_id.clone()));
        let chunk_store = Rc::new(InMemoryChunkStore::new());
        let chunk_cache = ChunkCache::with_memory_limit(100);
        let chunk_manager = ChunkManager::new();
        let placement = Rc::new(RoundRobinPlacement::new(vec![node_id]));

        Self {
            metadata_manager,
            chunk_store,
            chunk_cache,
            chunk_manager,
            placement,
            open_files: RefCell::new(HashMap::new()),
            next_fd: RefCell::new(3),
            connection_pool: Some(connection_pool),
        }
    }

    /// Create a new BenchFS client with connection pool and custom target nodes (distributed mode)
    ///
    /// # Arguments
    /// * `node_id` - This client's node ID
    /// * `connection_pool` - Connection pool for RPC communication
    /// * `target_nodes` - List of target nodes for chunk placement
    pub fn with_connection_pool_and_targets(
        node_id: String,
        connection_pool: Rc<ConnectionPool>,
        target_nodes: Vec<String>
    ) -> Self {
        let metadata_manager = Rc::new(MetadataManager::new(node_id.clone()));
        let chunk_store = Rc::new(InMemoryChunkStore::new());
        let chunk_cache = ChunkCache::with_memory_limit(100);
        let chunk_manager = ChunkManager::new();
        let placement = Rc::new(RoundRobinPlacement::new(target_nodes));

        Self {
            metadata_manager,
            chunk_store,
            chunk_cache,
            chunk_manager,
            placement,
            open_files: RefCell::new(HashMap::new()),
            next_fd: RefCell::new(3),
            connection_pool: Some(connection_pool),
        }
    }

    /// Check if distributed mode is enabled
    pub fn is_distributed(&self) -> bool {
        self.connection_pool.is_some()
    }

    /// Open a file
    ///
    /// # Arguments
    /// * `path` - File path
    /// * `flags` - Open flags
    ///
    /// # Returns
    /// File handle
    pub fn benchfs_open(&self, path: &str, flags: OpenFlags) -> ApiResult<FileHandle> {
        use std::path::Path;
        let path_ref = Path::new(path);

        // Check if file exists
        let file_meta = match self.metadata_manager.get_file_metadata(path_ref) {
            Ok(meta) => Some(meta),
            Err(_) => None,
        };

        let inode = if let Some(meta) = file_meta {
            // File exists
            if flags.create && !flags.truncate {
                return Err(ApiError::AlreadyExists(path.to_string()));
            }

            if flags.truncate {
                // Truncate file
                let mut new_meta = meta.clone();
                new_meta.size = 0;
                new_meta.chunk_count = 0;
                new_meta.chunk_locations.clear();
                self.metadata_manager
                    .update_file_metadata(new_meta)
                    .map_err(|e| ApiError::Internal(format!("Failed to truncate: {:?}", e)))?;
            }

            meta.inode
        } else {
            // File doesn't exist
            if !flags.create {
                return Err(ApiError::NotFound(path.to_string()));
            }

            // Create new file
            let inode = self.metadata_manager.generate_inode();
            let file_meta = FileMetadata::new(inode, path.to_string(), 0);

            self.metadata_manager
                .store_file_metadata(file_meta)
                .map_err(|e| ApiError::Internal(format!("Failed to create file: {:?}", e)))?;

            inode
        };

        // Create file handle
        let fd = self.allocate_fd();
        let handle = FileHandle::new(fd, path.to_string(), inode, flags);

        if flags.append {
            // Set position to end of file
            if let Ok(meta) = self.metadata_manager.get_file_metadata(path_ref) {
                handle.seek(meta.size);
            }
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
    pub async fn benchfs_read(&self, handle: &FileHandle, buf: &mut [u8]) -> ApiResult<usize> {
        if !handle.flags.read {
            return Err(ApiError::PermissionDenied("File not opened for reading".to_string()));
        }

        use std::path::Path;
        let path_ref = Path::new(&handle.path);

        // Get file metadata
        let file_meta = self
            .metadata_manager
            .get_file_metadata(path_ref)
            .map_err(|e| ApiError::Internal(format!("Failed to get metadata: {:?}", e)))?;

        let offset = handle.position();
        let length = buf.len() as u64;

        // Check if we're at or past EOF
        if offset >= file_meta.size {
            return Ok(0);
        }

        // Calculate actual read length
        let actual_length = length.min(file_meta.size - offset);

        // Calculate which chunks to read
        let chunks = self.chunk_manager.calculate_read_chunks(offset, actual_length, file_meta.size)
            .map_err(|e| ApiError::Internal(format!("Failed to calculate chunks: {:?}", e)))?;

        let mut bytes_read = 0;
        for (chunk_index, chunk_offset, read_size) in chunks {
            let chunk_id = ChunkId::new(file_meta.inode, chunk_index);

            // Try to get full chunk from cache first
            let chunk_data = if let Some(cached_chunk) = self.chunk_cache.get(&chunk_id) {
                // Cache hit - extract the requested portion
                tracing::trace!("Cache hit for chunk {}", chunk_index);
                Some(cached_chunk)
            } else {
                // Cache miss - need to fetch chunk
                tracing::trace!("Cache miss for chunk {}", chunk_index);

                // Try local chunk store first
                match self.chunk_store.read_chunk(
                    file_meta.inode,
                    chunk_index,
                    0, // Read full chunk for caching
                    self.chunk_manager.chunk_size() as u64,
                ).await {
                    Ok(full_chunk) => {
                        // Cache the full chunk for future reads
                        self.chunk_cache.put(chunk_id, full_chunk.clone());
                        Some(full_chunk)
                    }
                    Err(_) => {
                        // Local read failed - try remote if distributed mode enabled
                        if let Some(pool) = &self.connection_pool {
                            // Get chunk location from metadata
                            if let Some(node_id) = crate::rpc::data_ops::get_chunk_node(chunk_index, &file_meta.chunk_locations) {
                                tracing::debug!("Fetching chunk {} from remote node {}", chunk_index, node_id);

                                // Connect to remote node using node_id
                                match pool.get_or_connect(node_id).await {
                                        Ok(client) => {
                                            // Create RPC request
                                            let request = ReadChunkRequest::new(
                                                chunk_index,
                                                0,
                                                self.chunk_manager.chunk_size() as u64,
                                                file_meta.inode,
                                            );

                                            // Execute RPC
                                            match request.call(&*client).await {
                                                Ok(response) if response.is_success() => {
                                                    let full_chunk = request.take_data();
                                                    tracing::debug!("Successfully fetched {} bytes from remote node", full_chunk.len());

                                                    // Cache for future reads
                                                    self.chunk_cache.put(chunk_id, full_chunk.clone());
                                                    Some(full_chunk)
                                                }
                                                Ok(response) => {
                                                    tracing::warn!("Remote read failed with status {}", response.status);
                                                    None
                                                }
                                                Err(e) => {
                                                    tracing::error!("RPC error: {:?}", e);
                                                    None
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            tracing::error!("Failed to connect to {}: {:?}", node_id, e);
                                            None
                                        }
                                    }
                            } else {
                                // No location info for this chunk - treat as sparse
                                None
                            }
                        } else {
                            // Not in distributed mode - treat as sparse
                            None
                        }
                    }
                }
            };

            // Extract and copy the requested portion
            if let Some(chunk) = chunk_data {
                let buf_offset = bytes_read;
                let chunk_start = chunk_offset as usize;
                let chunk_end = (chunk_start + read_size as usize).min(chunk.len());
                let copy_len = (chunk_end - chunk_start).min(buf.len() - buf_offset);

                if chunk_start < chunk.len() {
                    buf[buf_offset..buf_offset + copy_len]
                        .copy_from_slice(&chunk[chunk_start..chunk_start + copy_len]);
                    bytes_read += copy_len;
                } else {
                    // Request is beyond chunk data, fill with zeros
                    let zero_len = read_size as usize;
                    buf[buf_offset..buf_offset + zero_len].fill(0);
                    bytes_read += zero_len;
                }
            } else {
                // Chunk doesn't exist locally, return zeros (sparse file)
                let buf_offset = bytes_read;
                let zero_len = read_size as usize;
                buf[buf_offset..buf_offset + zero_len].fill(0);
                bytes_read += zero_len;
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
    pub async fn benchfs_write(&self, handle: &FileHandle, data: &[u8]) -> ApiResult<usize> {
        if !handle.flags.write {
            return Err(ApiError::PermissionDenied("File not opened for writing".to_string()));
        }

        use std::path::Path;
        let path_ref = Path::new(&handle.path);

        // Get file metadata
        let mut file_meta = self
            .metadata_manager
            .get_file_metadata(path_ref)
            .map_err(|e| ApiError::Internal(format!("Failed to get metadata: {:?}", e)))?;

        let offset = handle.position();
        let length = data.len() as u64;

        // Calculate which chunks to write (using read_chunks as a workaround)
        // Note: For writing, we use the same logic as reading to determine chunk boundaries
        let new_size = (offset + length).max(file_meta.size);
        let chunks = self.chunk_manager.calculate_read_chunks(offset, length, new_size)
            .map_err(|e| ApiError::Internal(format!("Failed to calculate chunks: {:?}", e)))?;

        let mut bytes_written = 0;
        let mut chunk_locations_updated = false;

        for (chunk_index, chunk_offset, write_size) in chunks {
            let data_offset = bytes_written;
            let data_len = write_size as usize;
            let chunk_data = &data[data_offset..data_offset + data_len];

            // Invalidate cache for this chunk (write-through)
            let chunk_id = ChunkId::new(file_meta.inode, chunk_index);
            self.chunk_cache.invalidate(&chunk_id);

            // Determine where to write this chunk
            // First, ensure chunk_locations vector is large enough
            while file_meta.chunk_locations.len() <= chunk_index as usize {
                file_meta.chunk_locations.push(String::new());
            }

            // If chunk location is not set, determine it using placement strategy
            if file_meta.chunk_locations[chunk_index as usize].is_empty() {
                use std::path::Path;
                let path_ref = Path::new(&file_meta.path);
                let node_id = self.placement.place_chunk(path_ref, chunk_index)
                    .unwrap_or_else(|| self.metadata_manager.self_node_id().to_string());
                file_meta.chunk_locations[chunk_index as usize] = node_id;
                chunk_locations_updated = true;
            }

            let target_node = &file_meta.chunk_locations[chunk_index as usize];
            let is_local = target_node == &self.metadata_manager.self_node_id();

            if is_local {
                // Write to local chunk store
                self.chunk_store
                    .write_chunk(file_meta.inode, chunk_index, chunk_offset, chunk_data)
                    .await
                    .map_err(|e| ApiError::IoError(format!("Failed to write chunk: {:?}", e)))?;
            } else if let Some(pool) = &self.connection_pool {
                // Write to remote node using node_id
                tracing::debug!("Writing chunk {} to remote node {}", chunk_index, target_node);

                match pool.get_or_connect(target_node).await {
                        Ok(client) => {
                            // Create RPC request
                            let request = WriteChunkRequest::new(
                                chunk_index,
                                chunk_offset,
                                chunk_data.to_vec(),
                                file_meta.inode,
                            );

                            // Execute RPC
                            match request.call(&*client).await {
                                Ok(response) if response.is_success() => {
                                    tracing::debug!("Successfully wrote {} bytes to remote node", response.bytes_written);
                                }
                                Ok(response) => {
                                    return Err(ApiError::IoError(format!("Remote write failed with status {}", response.status)));
                                }
                                Err(e) => {
                                    return Err(ApiError::IoError(format!("RPC error: {:?}", e)));
                                }
                            }
                        }
                        Err(e) => {
                            return Err(ApiError::IoError(format!("Failed to connect to {}: {:?}", target_node, e)));
                        }
                    }
            } else {
                // Not in distributed mode but chunk should be on a different node
                // This shouldn't happen in normal operation
                return Err(ApiError::Internal(format!("Chunk {} should be on node {} but distributed mode is not enabled", chunk_index, target_node)));
            }

            bytes_written += data_len;
        }

        // Update file size if we wrote past the end, or if chunk locations were updated
        let new_size = (offset + length).max(file_meta.size);
        if new_size != file_meta.size || chunk_locations_updated {
            file_meta.size = new_size;
            file_meta.chunk_count = file_meta.calculate_chunk_count();

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
    pub fn benchfs_close(&self, handle: &FileHandle) -> ApiResult<()> {
        self.open_files.borrow_mut().remove(&handle.fd);
        Ok(())
    }

    /// Delete a file
    ///
    /// # Arguments
    /// * `path` - File path
    pub async fn benchfs_unlink(&self, path: &str) -> ApiResult<()> {
        use std::path::Path;
        let path_ref = Path::new(path);

        // Get file metadata to get inode
        let file_meta = self
            .metadata_manager
            .get_file_metadata(path_ref)
            .map_err(|_| ApiError::NotFound(path.to_string()))?;

        // Invalidate all cached chunks for this inode
        self.chunk_cache.invalidate_inode(file_meta.inode);

        // Delete all chunks
        let _ = self.chunk_store.delete_file_chunks(file_meta.inode).await;

        // Delete metadata
        self.metadata_manager
            .remove_file_metadata(path_ref)
            .map_err(|e| ApiError::Internal(format!("Failed to delete metadata: {:?}", e)))?;

        Ok(())
    }

    /// Create a directory
    ///
    /// # Arguments
    /// * `path` - Directory path
    /// * `mode` - Permissions (Unix-style)
    pub fn benchfs_mkdir(&self, path: &str, _mode: u32) -> ApiResult<()> {
        use std::path::Path;
        let path_ref = Path::new(path);

        // Check if already exists
        if self.metadata_manager.get_dir_metadata(path_ref).is_ok() {
            return Err(ApiError::AlreadyExists(path.to_string()));
        }

        // Create directory metadata
        let inode = self.metadata_manager.generate_inode();
        let dir_meta = DirectoryMetadata::new(inode, path.to_string());

        self.metadata_manager
            .store_dir_metadata(dir_meta)
            .map_err(|e| ApiError::Internal(format!("Failed to create directory: {:?}", e)))?;

        Ok(())
    }

    /// Remove a directory
    ///
    /// # Arguments
    /// * `path` - Directory path
    pub fn benchfs_rmdir(&self, path: &str) -> ApiResult<()> {
        use std::path::Path;
        let path_ref = Path::new(path);

        // Check if directory exists
        let dir_meta = self
            .metadata_manager
            .get_dir_metadata(path_ref)
            .map_err(|_| ApiError::NotFound(path.to_string()))?;

        // Check if directory is empty
        if !dir_meta.children.is_empty() {
            return Err(ApiError::InvalidArgument("Directory not empty".to_string()));
        }

        // Delete directory
        self.metadata_manager
            .remove_dir_metadata(path_ref)
            .map_err(|e| ApiError::Internal(format!("Failed to delete directory: {:?}", e)))?;

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
                    return Err(ApiError::InvalidArgument("Seek before beginning".to_string()));
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
                    return Err(ApiError::InvalidArgument("Seek before beginning".to_string()));
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
            "fsync called for file {} (inode {}) with {} chunks",
            handle.path,
            file_meta.inode,
            file_meta.chunk_count
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
    pub fn benchfs_stat(&self, path: &str) -> ApiResult<crate::api::types::FileStat> {
        use std::path::Path;
        use crate::api::types::FileStat;

        let path_ref = Path::new(path);

        // Try file metadata first
        if let Ok(meta) = self.metadata_manager.get_file_metadata(path_ref) {
            return Ok(FileStat::from_file_metadata(&meta));
        }

        // Try directory metadata
        if let Ok(dir_meta) = self.metadata_manager.get_dir_metadata(path_ref) {
            return Ok(FileStat::from_dir_metadata(&dir_meta));
        }

        Err(ApiError::NotFound(path.to_string()))
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
        if self.metadata_manager.get_file_metadata(new_path_ref).is_ok()
            || self.metadata_manager.get_dir_metadata(new_path_ref).is_ok()
        {
            return Err(ApiError::AlreadyExists(new_path.to_string()));
        }

        // Try to rename file
        if let Ok(mut meta) = self.metadata_manager.get_file_metadata(old_path_ref) {
            // Remove old metadata
            self.metadata_manager
                .remove_file_metadata(old_path_ref)
                .map_err(|e| ApiError::Internal(format!("Failed to remove old metadata: {:?}", e)))?;

            // Update path and store new metadata
            meta.path = new_path.to_string();
            self.metadata_manager
                .store_file_metadata(meta)
                .map_err(|e| ApiError::Internal(format!("Failed to store new metadata: {:?}", e)))?;

            return Ok(());
        }

        // Try to rename directory
        if let Ok(mut meta) = self.metadata_manager.get_dir_metadata(old_path_ref) {
            // Remove old metadata
            self.metadata_manager
                .remove_dir_metadata(old_path_ref)
                .map_err(|e| ApiError::Internal(format!("Failed to remove old metadata: {:?}", e)))?;

            // Update path and store new metadata
            meta.path = new_path.to_string();
            self.metadata_manager
                .store_dir_metadata(meta)
                .map_err(|e| ApiError::Internal(format!("Failed to store new metadata: {:?}", e)))?;

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
    pub fn benchfs_readdir(&self, path: &str) -> ApiResult<Vec<String>> {
        use std::path::Path;
        let path_ref = Path::new(path);

        let dir_meta = self
            .metadata_manager
            .get_dir_metadata(path_ref)
            .map_err(|_| ApiError::NotFound(path.to_string()))?;

        // Extract names from DirectoryEntry
        let names = dir_meta.children.iter().map(|e| e.name.clone()).collect();
        Ok(names)
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
        let old_chunk_count = file_meta.chunk_count;
        file_meta.chunk_count = file_meta.calculate_chunk_count();

        // If truncating to smaller size, delete affected chunks
        if size < old_size {
            let chunk_size = self.chunk_manager.chunk_size() as u64;
            let new_chunk_count = (size + chunk_size - 1) / chunk_size;

            // Delete chunks beyond the new size
            for chunk_idx in new_chunk_count..old_chunk_count {
                let chunk_id = ChunkId::new(file_meta.inode, chunk_idx);

                // Invalidate cache
                self.chunk_cache.invalidate(&chunk_id);

                // Delete from chunk store
                if let Err(e) = self.chunk_store.delete_chunk(file_meta.inode, chunk_idx).await {
                    tracing::warn!(
                        "Failed to delete chunk {} for inode {}: {:?}",
                        chunk_idx,
                        file_meta.inode,
                        e
                    );
                }
            }

            // Truncate chunk_locations
            file_meta.chunk_locations.truncate(new_chunk_count as usize);

            // If truncating within a chunk, we need to zero out the remaining bytes
            if size % chunk_size != 0 {
                let last_chunk_idx = new_chunk_count.saturating_sub(1);
                let last_chunk_offset = size % chunk_size;
                let bytes_to_zero = chunk_size - last_chunk_offset;

                if bytes_to_zero > 0 {
                    // Write zeros to the end of the last chunk
                    let zeros = vec![0u8; bytes_to_zero as usize];
                    if let Err(e) = self.chunk_store
                        .write_chunk(file_meta.inode, last_chunk_idx, last_chunk_offset, &zeros)
                        .await
                    {
                        tracing::warn!(
                            "Failed to zero out last chunk {} for inode {}: {:?}",
                            last_chunk_idx,
                            file_meta.inode,
                            e
                        );
                    }

                    // Invalidate cache for the last chunk since we modified it
                    let chunk_id = ChunkId::new(file_meta.inode, last_chunk_idx);
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use pluvio_runtime::executor::Runtime;
    use std::rc::Rc;

    fn run_test<F>(test: F)
    where
        F: std::future::Future<Output = ()> + 'static,
    {
        let runtime = Runtime::new(256);
        runtime.clone().run(test);
    }

    #[test]
    fn test_benbenchfs_creation() {
        let fs = BenchFS::new("node1".to_string());
        assert_eq!(fs.metadata_manager.self_node_id(), "node1");
    }

    #[test]
    fn test_create_and_open_file() {
        let fs = BenchFS::new("node1".to_string());

        // Create and open a new file
        let handle = fs.benchfs_open("/test.txt", OpenFlags::create()).unwrap();
        assert_eq!(handle.path, "/test.txt");
        assert!(handle.flags.write);

        fs.benchfs_close(&handle).unwrap();
    }

    #[test]
    fn test_write_and_read_file() {
        run_test(async {
            let fs = BenchFS::new("node1".to_string());

            // Create file
            let handle = fs.benchfs_open("/test.txt", OpenFlags::create()).unwrap();

            // Write data
            let data = b"Hello, BenchFS!";
            let written = fs.benchfs_write(&handle, data).await.unwrap();
            assert_eq!(written, data.len());

            fs.benchfs_close(&handle).unwrap();

            // Read data
            let handle = fs.benchfs_open("/test.txt", OpenFlags::read_only()).unwrap();
            let mut buf = vec![0u8; 100];
            let read = fs.benchfs_read(&handle, &mut buf).await.unwrap();
            assert_eq!(read, data.len());
            assert_eq!(&buf[..read], data);

            fs.benchfs_close(&handle).unwrap();
        });
    }

    #[test]
    fn test_unlink_file() {
        run_test(async {
            let fs = BenchFS::new("node1".to_string());

            // Create file
            let handle = fs.benchfs_open("/test.txt", OpenFlags::create()).unwrap();
            fs.benchfs_close(&handle).unwrap();

            // Delete file
            fs.benchfs_unlink("/test.txt").await.unwrap();

            // Try to open deleted file
            let result = fs.benchfs_open("/test.txt", OpenFlags::read_only());
            assert!(result.is_err());
        });
    }

    #[test]
    fn test_mkdir_and_rmdir() {
        let fs = BenchFS::new("node1".to_string());

        // Create directory
        fs.benchfs_mkdir("/testdir", 0o755).unwrap();

        // Remove directory
        fs.benchfs_rmdir("/testdir").unwrap();
    }

    #[test]
    fn test_seek() {
        run_test(async {
            let fs = BenchFS::new("node1".to_string());

            // Create file with data
            let handle = fs.benchfs_open("/test.txt", OpenFlags::create()).unwrap();
            fs.benchfs_write(&handle, b"0123456789").await.unwrap();
            fs.benchfs_close(&handle).unwrap();

            // Open and seek
            let handle = fs.benchfs_open("/test.txt", OpenFlags::read_only()).unwrap();

            // SEEK_SET
            let pos = fs.benchfs_seek(&handle, 5, 0).unwrap();
            assert_eq!(pos, 5);

            // SEEK_CUR
            let pos = fs.benchfs_seek(&handle, 2, 1).unwrap();
            assert_eq!(pos, 7);

            // SEEK_END
            let pos = fs.benchfs_seek(&handle, -3, 2).unwrap();
            assert_eq!(pos, 7);

            fs.benchfs_close(&handle).unwrap();
        });
    }
}
