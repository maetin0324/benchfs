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
use crate::rpc::client::RpcClient;

/// BenchFS Filesystem Client
///
/// This is the main entry point for filesystem operations.
/// It maintains connections to metadata and data servers.
pub struct BenchFS {
    /// Metadata manager
    metadata_manager: Rc<MetadataManager>,

    /// Chunk store (for local operations)
    chunk_store: Rc<InMemoryChunkStore>,

    /// Chunk manager
    chunk_manager: ChunkManager,

    /// Placement strategy
    placement: Rc<dyn PlacementStrategy>,

    /// Open file descriptors
    open_files: RefCell<HashMap<u64, FileHandle>>,

    /// Next file descriptor
    next_fd: RefCell<u64>,

    /// RPC clients (node_id -> client)
    #[allow(dead_code)]
    rpc_clients: RefCell<HashMap<String, Rc<RpcClient>>>,
}

impl BenchFS {
    /// Create a new BenchFS client
    pub fn new(node_id: String) -> Self {
        let metadata_manager = Rc::new(MetadataManager::new(node_id.clone()));
        let chunk_store = Rc::new(InMemoryChunkStore::new());
        let chunk_manager = ChunkManager::new();
        let placement = Rc::new(RoundRobinPlacement::new(vec![node_id]));

        Self {
            metadata_manager,
            chunk_store,
            chunk_manager,
            placement,
            open_files: RefCell::new(HashMap::new()),
            next_fd: RefCell::new(3), // Start from 3 (0, 1, 2 are reserved for stdin, stdout, stderr)
            rpc_clients: RefCell::new(HashMap::new()),
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
    pub fn chfs_open(&self, path: &str, flags: OpenFlags) -> ApiResult<FileHandle> {
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
    pub fn chfs_read(&self, handle: &FileHandle, buf: &mut [u8]) -> ApiResult<usize> {
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
            // Read from local chunk store
            match self.chunk_store.read_chunk(
                file_meta.inode,
                chunk_index,
                chunk_offset,
                read_size,
            ) {
                Ok(data) => {
                    let buf_offset = bytes_read;
                    let copy_len = data.len().min(buf.len() - buf_offset);
                    buf[buf_offset..buf_offset + copy_len].copy_from_slice(&data[..copy_len]);
                    bytes_read += copy_len;
                }
                Err(_) => {
                    // Chunk doesn't exist locally, return zeros (sparse file)
                    let buf_offset = bytes_read;
                    let zero_len = read_size as usize;
                    buf[buf_offset..buf_offset + zero_len].fill(0);
                    bytes_read += zero_len;
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
    pub fn chfs_write(&self, handle: &FileHandle, data: &[u8]) -> ApiResult<usize> {
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
        for (chunk_index, chunk_offset, write_size) in chunks {
            let data_offset = bytes_written;
            let data_len = write_size as usize;
            let chunk_data = &data[data_offset..data_offset + data_len];

            // Write to local chunk store
            self.chunk_store
                .write_chunk(file_meta.inode, chunk_index, chunk_offset, chunk_data)
                .map_err(|e| ApiError::IoError(format!("Failed to write chunk: {:?}", e)))?;

            bytes_written += data_len;
        }

        // Update file size if we wrote past the end
        let new_size = (offset + length).max(file_meta.size);
        if new_size != file_meta.size {
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
    pub fn chfs_close(&self, handle: &FileHandle) -> ApiResult<()> {
        self.open_files.borrow_mut().remove(&handle.fd);
        Ok(())
    }

    /// Delete a file
    ///
    /// # Arguments
    /// * `path` - File path
    pub fn chfs_unlink(&self, path: &str) -> ApiResult<()> {
        use std::path::Path;
        let path_ref = Path::new(path);

        // Get file metadata to get inode
        let file_meta = self
            .metadata_manager
            .get_file_metadata(path_ref)
            .map_err(|_| ApiError::NotFound(path.to_string()))?;

        // Delete all chunks
        self.chunk_store.delete_file_chunks(file_meta.inode);

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
    pub fn chfs_mkdir(&self, path: &str, _mode: u32) -> ApiResult<()> {
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
    pub fn chfs_rmdir(&self, path: &str) -> ApiResult<()> {
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
    pub fn chfs_seek(&self, handle: &FileHandle, offset: i64, whence: i32) -> ApiResult<u64> {
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

    #[test]
    fn test_benchfs_creation() {
        let fs = BenchFS::new("node1".to_string());
        assert_eq!(fs.metadata_manager.self_node_id(), "node1");
    }

    #[test]
    fn test_create_and_open_file() {
        let fs = BenchFS::new("node1".to_string());

        // Create and open a new file
        let handle = fs.chfs_open("/test.txt", OpenFlags::create()).unwrap();
        assert_eq!(handle.path, "/test.txt");
        assert!(handle.flags.write);

        fs.chfs_close(&handle).unwrap();
    }

    #[test]
    fn test_write_and_read_file() {
        let fs = BenchFS::new("node1".to_string());

        // Create file
        let handle = fs.chfs_open("/test.txt", OpenFlags::create()).unwrap();

        // Write data
        let data = b"Hello, BenchFS!";
        let written = fs.chfs_write(&handle, data).unwrap();
        assert_eq!(written, data.len());

        fs.chfs_close(&handle).unwrap();

        // Read data
        let handle = fs.chfs_open("/test.txt", OpenFlags::read_only()).unwrap();
        let mut buf = vec![0u8; 100];
        let read = fs.chfs_read(&handle, &mut buf).unwrap();
        assert_eq!(read, data.len());
        assert_eq!(&buf[..read], data);

        fs.chfs_close(&handle).unwrap();
    }

    #[test]
    fn test_unlink_file() {
        let fs = BenchFS::new("node1".to_string());

        // Create file
        let handle = fs.chfs_open("/test.txt", OpenFlags::create()).unwrap();
        fs.chfs_close(&handle).unwrap();

        // Delete file
        fs.chfs_unlink("/test.txt").unwrap();

        // Try to open deleted file
        let result = fs.chfs_open("/test.txt", OpenFlags::read_only());
        assert!(result.is_err());
    }

    #[test]
    fn test_mkdir_and_rmdir() {
        let fs = BenchFS::new("node1".to_string());

        // Create directory
        fs.chfs_mkdir("/testdir", 0o755).unwrap();

        // Remove directory
        fs.chfs_rmdir("/testdir").unwrap();
    }

    #[test]
    fn test_seek() {
        let fs = BenchFS::new("node1".to_string());

        // Create file with data
        let handle = fs.chfs_open("/test.txt", OpenFlags::create()).unwrap();
        fs.chfs_write(&handle, b"0123456789").unwrap();
        fs.chfs_close(&handle).unwrap();

        // Open and seek
        let handle = fs.chfs_open("/test.txt", OpenFlags::read_only()).unwrap();

        // SEEK_SET
        let pos = fs.chfs_seek(&handle, 5, 0).unwrap();
        assert_eq!(pos, 5);

        // SEEK_CUR
        let pos = fs.chfs_seek(&handle, 2, 1).unwrap();
        assert_eq!(pos, 7);

        // SEEK_END
        let pos = fs.chfs_seek(&handle, -3, 2).unwrap();
        assert_eq!(pos, 7);

        fs.chfs_close(&handle).unwrap();
    }
}
