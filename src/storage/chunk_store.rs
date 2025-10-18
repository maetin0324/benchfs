use std::cell::RefCell;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use crate::metadata::CHUNK_SIZE;
use super::{IOUringBackend, OpenFlags, FileHandle, StorageBackend};

/// Chunk storage error types
#[derive(Debug, thiserror::Error)]
pub enum ChunkStoreError {
    #[error("Chunk not found: inode {inode}, chunk {chunk_index}")]
    ChunkNotFound { inode: u64, chunk_index: u64 },

    #[error("Invalid chunk size: expected {expected}, got {actual}")]
    InvalidChunkSize { expected: usize, actual: usize },

    #[error("Invalid offset: {0}")]
    InvalidOffset(u64),

    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Storage full: {0}")]
    StorageFull(String),

    #[error("Storage error: {0}")]
    StorageError(#[from] super::StorageError),
}

pub type ChunkStoreResult<T> = Result<T, ChunkStoreError>;

/// Chunk key for identifying chunks
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ChunkKey {
    /// File inode
    pub inode: u64,
    /// Chunk index
    pub chunk_index: u64,
}

impl ChunkKey {
    pub fn new(inode: u64, chunk_index: u64) -> Self {
        Self {
            inode,
            chunk_index,
        }
    }
}

/// In-memory chunk storage
///
/// Simple implementation that stores chunks in memory.
/// For production use, this should be backed by persistent storage with io_uring.
pub struct InMemoryChunkStore {
    /// Chunk data storage (key -> data)
    chunks: RefCell<HashMap<ChunkKey, Vec<u8>>>,

    /// Maximum number of chunks to store
    max_chunks: usize,

    /// Chunk size (default: CHUNK_SIZE)
    chunk_size: usize,
}

impl InMemoryChunkStore {
    /// Create a new in-memory chunk store
    pub fn new() -> Self {
        Self::with_capacity(1000, CHUNK_SIZE)
    }

    /// Create a new in-memory chunk store with custom capacity and chunk size
    pub fn with_capacity(max_chunks: usize, chunk_size: usize) -> Self {
        Self {
            chunks: RefCell::new(HashMap::new()),
            max_chunks,
            chunk_size,
        }
    }

    /// Write a chunk to storage
    ///
    /// # Arguments
    /// * `inode` - File inode
    /// * `chunk_index` - Chunk index
    /// * `offset` - Offset within the chunk
    /// * `data` - Data to write
    pub async fn write_chunk(
        &self,
        inode: u64,
        chunk_index: u64,
        offset: u64,
        data: &[u8],
    ) -> ChunkStoreResult<usize> {
        if offset >= self.chunk_size as u64 {
            return Err(ChunkStoreError::InvalidOffset(offset));
        }

        let key = ChunkKey::new(inode, chunk_index);
        let mut chunks = self.chunks.borrow_mut();

        // Check capacity
        if !chunks.contains_key(&key) && chunks.len() >= self.max_chunks {
            return Err(ChunkStoreError::StorageFull(format!(
                "Maximum {} chunks reached",
                self.max_chunks
            )));
        }

        // Get or create chunk buffer
        let chunk_data = chunks
            .entry(key)
            .or_insert_with(|| vec![0u8; self.chunk_size]);

        // Write data
        let offset = offset as usize;
        let end = (offset + data.len()).min(self.chunk_size);
        let bytes_to_write = end - offset;

        chunk_data[offset..end].copy_from_slice(&data[..bytes_to_write]);

        tracing::debug!(
            "Wrote {} bytes to chunk (inode={}, chunk_index={}, offset={})",
            bytes_to_write,
            inode,
            chunk_index,
            offset
        );

        Ok(bytes_to_write)
    }

    /// Read a chunk from storage
    ///
    /// # Arguments
    /// * `inode` - File inode
    /// * `chunk_index` - Chunk index
    /// * `offset` - Offset within the chunk
    /// * `length` - Number of bytes to read
    pub async fn read_chunk(
        &self,
        inode: u64,
        chunk_index: u64,
        offset: u64,
        length: u64,
    ) -> ChunkStoreResult<Vec<u8>> {
        if offset >= self.chunk_size as u64 {
            return Err(ChunkStoreError::InvalidOffset(offset));
        }

        let key = ChunkKey::new(inode, chunk_index);
        let chunks = self.chunks.borrow();

        let chunk_data = chunks.get(&key).ok_or(ChunkStoreError::ChunkNotFound {
            inode,
            chunk_index,
        })?;

        let offset = offset as usize;
        let end = (offset + length as usize).min(self.chunk_size);
        let bytes_to_read = end - offset;

        let data = chunk_data[offset..end].to_vec();

        tracing::debug!(
            "Read {} bytes from chunk (inode={}, chunk_index={}, offset={})",
            bytes_to_read,
            inode,
            chunk_index,
            offset
        );

        Ok(data)
    }

    /// Delete a chunk from storage
    pub async fn delete_chunk(&self, inode: u64, chunk_index: u64) -> ChunkStoreResult<()> {
        let key = ChunkKey::new(inode, chunk_index);
        let mut chunks = self.chunks.borrow_mut();

        chunks
            .remove(&key)
            .ok_or(ChunkStoreError::ChunkNotFound { inode, chunk_index })?;

        tracing::debug!(
            "Deleted chunk (inode={}, chunk_index={})",
            inode,
            chunk_index
        );

        Ok(())
    }

    /// Delete all chunks for a file
    pub async fn delete_file_chunks(&self, inode: u64) -> ChunkStoreResult<usize> {
        let mut chunks = self.chunks.borrow_mut();
        let mut deleted_count = 0;

        chunks.retain(|key, _| {
            if key.inode == inode {
                deleted_count += 1;
                false
            } else {
                true
            }
        });

        tracing::debug!("Deleted {} chunks for inode {}", deleted_count, inode);

        Ok(deleted_count)
    }

    /// Check if a chunk exists
    pub fn has_chunk(&self, inode: u64, chunk_index: u64) -> bool {
        let key = ChunkKey::new(inode, chunk_index);
        self.chunks.borrow().contains_key(&key)
    }

    /// Get the number of stored chunks
    pub fn chunk_count(&self) -> usize {
        self.chunks.borrow().len()
    }

    /// Get the total storage size in bytes
    pub fn storage_size(&self) -> usize {
        self.chunks.borrow().len() * self.chunk_size
    }

    /// Clear all chunks
    pub fn clear(&self) {
        self.chunks.borrow_mut().clear();
        tracing::debug!("Cleared all chunks");
    }
}

impl Default for InMemoryChunkStore {
    fn default() -> Self {
        Self::new()
    }
}

/// File-based chunk storage
///
/// Stores chunks in the local filesystem. Each chunk is stored as a separate file.
/// Directory structure: <base_dir>/<inode>/<chunk_index>
pub struct FileChunkStore {
    /// Base directory for chunk storage
    base_dir: PathBuf,

    /// Chunk size
    chunk_size: usize,
}

impl FileChunkStore {
    /// Create a new file-based chunk store
    pub fn new<P: AsRef<Path>>(base_dir: P) -> ChunkStoreResult<Self> {
        let base_dir = base_dir.as_ref().to_path_buf();

        // Create base directory if it doesn't exist
        if !base_dir.exists() {
            std::fs::create_dir_all(&base_dir)?;
        }

        Ok(Self {
            base_dir,
            chunk_size: CHUNK_SIZE,
        })
    }

    /// Get the path for a chunk file
    fn chunk_path(&self, inode: u64, chunk_index: u64) -> PathBuf {
        self.base_dir
            .join(format!("{}", inode))
            .join(format!("{}", chunk_index))
    }

    /// Ensure the directory for an inode exists
    fn ensure_inode_dir(&self, inode: u64) -> ChunkStoreResult<PathBuf> {
        let dir = self.base_dir.join(format!("{}", inode));
        if !dir.exists() {
            std::fs::create_dir_all(&dir)?;
        }
        Ok(dir)
    }

    /// Write a chunk to file
    pub async fn write_chunk(
        &self,
        inode: u64,
        chunk_index: u64,
        offset: u64,
        data: &[u8],
    ) -> ChunkStoreResult<usize> {
        if offset >= self.chunk_size as u64 {
            return Err(ChunkStoreError::InvalidOffset(offset));
        }

        self.ensure_inode_dir(inode)?;
        let path = self.chunk_path(inode, chunk_index);

        // Read existing chunk or create new one
        let mut chunk_data = if path.exists() {
            std::fs::read(&path)?
        } else {
            vec![0u8; self.chunk_size]
        };

        // Ensure chunk_data is the right size
        if chunk_data.len() < self.chunk_size {
            chunk_data.resize(self.chunk_size, 0);
        }

        // Write data
        let offset = offset as usize;
        let end = (offset + data.len()).min(self.chunk_size);
        let bytes_to_write = end - offset;

        chunk_data[offset..end].copy_from_slice(&data[..bytes_to_write]);

        // Write to file
        std::fs::write(&path, &chunk_data)?;

        tracing::debug!(
            "Wrote {} bytes to chunk file (inode={}, chunk_index={}, path={:?})",
            bytes_to_write,
            inode,
            chunk_index,
            path
        );

        Ok(bytes_to_write)
    }

    /// Read a chunk from file
    pub async fn read_chunk(
        &self,
        inode: u64,
        chunk_index: u64,
        offset: u64,
        length: u64,
    ) -> ChunkStoreResult<Vec<u8>> {
        if offset >= self.chunk_size as u64 {
            return Err(ChunkStoreError::InvalidOffset(offset));
        }

        let path = self.chunk_path(inode, chunk_index);

        if !path.exists() {
            return Err(ChunkStoreError::ChunkNotFound { inode, chunk_index });
        }

        let chunk_data = std::fs::read(&path)?;
        let offset = offset as usize;
        let end = (offset + length as usize).min(chunk_data.len());

        if offset >= chunk_data.len() {
            return Ok(Vec::new());
        }

        Ok(chunk_data[offset..end].to_vec())
    }

    /// Delete a chunk file
    pub async fn delete_chunk(&self, inode: u64, chunk_index: u64) -> ChunkStoreResult<()> {
        let path = self.chunk_path(inode, chunk_index);

        if !path.exists() {
            return Err(ChunkStoreError::ChunkNotFound { inode, chunk_index });
        }

        std::fs::remove_file(&path)?;

        tracing::debug!(
            "Deleted chunk file (inode={}, chunk_index={}, path={:?})",
            inode,
            chunk_index,
            path
        );

        Ok(())
    }

    /// Delete all chunks for a file
    pub async fn delete_file_chunks(&self, inode: u64) -> ChunkStoreResult<usize> {
        let dir = self.base_dir.join(format!("{}", inode));

        if !dir.exists() {
            return Ok(0);
        }

        let mut deleted_count = 0;

        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            if entry.path().is_file() {
                std::fs::remove_file(entry.path())?;
                deleted_count += 1;
            }
        }

        // Remove the directory
        std::fs::remove_dir(&dir)?;

        tracing::debug!("Deleted {} chunk files for inode {}", deleted_count, inode);

        Ok(deleted_count)
    }

    /// Check if a chunk exists
    pub fn has_chunk(&self, inode: u64, chunk_index: u64) -> bool {
        self.chunk_path(inode, chunk_index).exists()
    }
}

/// IO_uring-based chunk storage
///
/// Stores chunks in the local filesystem using io_uring for high-performance async I/O.
/// Directory structure: <base_dir>/<inode>/<chunk_index>
///
/// This implementation uses IOUringBackend for all file operations, providing:
/// - Non-blocking async I/O
/// - Zero-copy data transfer (with registered buffers)
/// - High IOPS and low latency
pub struct IOUringChunkStore {
    /// Base directory for chunk storage
    base_dir: PathBuf,

    /// Chunk size
    chunk_size: usize,

    /// IO_uring backend for async file operations
    backend: Rc<IOUringBackend>,

    /// Cache of open file handles (inode, chunk_index) -> FileHandle
    /// This avoids repeatedly opening the same chunk file
    open_handles: RefCell<HashMap<ChunkKey, FileHandle>>,
}

impl IOUringChunkStore {
    /// Create a new IO_uring-based chunk store
    ///
    /// # Arguments
    /// * `base_dir` - Base directory for chunk storage
    /// * `backend` - IO_uring backend for file operations
    pub fn new<P: AsRef<Path>>(base_dir: P, backend: Rc<IOUringBackend>) -> ChunkStoreResult<Self> {
        let base_dir = base_dir.as_ref().to_path_buf();

        // Create base directory if it doesn't exist
        if !base_dir.exists() {
            std::fs::create_dir_all(&base_dir)?;
        }

        Ok(Self {
            base_dir,
            chunk_size: CHUNK_SIZE,
            backend,
            open_handles: RefCell::new(HashMap::new()),
        })
    }

    /// Get the path for a chunk file
    fn chunk_path(&self, inode: u64, chunk_index: u64) -> PathBuf {
        self.base_dir
            .join(format!("{}", inode))
            .join(format!("{}", chunk_index))
    }

    /// Ensure the directory for an inode exists
    fn ensure_inode_dir(&self, inode: u64) -> ChunkStoreResult<PathBuf> {
        let dir = self.base_dir.join(format!("{}", inode));
        if !dir.exists() {
            std::fs::create_dir_all(&dir)?;
        }
        Ok(dir)
    }

    /// Open a chunk file, creating it if necessary
    ///
    /// Returns a cached handle if the file is already open.
    async fn open_chunk_file(
        &self,
        inode: u64,
        chunk_index: u64,
        write: bool,
    ) -> ChunkStoreResult<FileHandle> {
        let key = ChunkKey::new(inode, chunk_index);

        // Check if already open
        if let Some(&handle) = self.open_handles.borrow().get(&key) {
            return Ok(handle);
        }

        // Ensure directory exists
        self.ensure_inode_dir(inode)?;
        let path = self.chunk_path(inode, chunk_index);

        // Open or create the file
        let flags = if write {
            OpenFlags {
                read: true,
                write: true,
                create: true,
                truncate: false,
                append: false,
                direct: false,
            }
        } else {
            OpenFlags::read_only()
        };

        let handle = self.backend.open(&path, flags).await?;

        // Cache the handle
        self.open_handles.borrow_mut().insert(key, handle);

        Ok(handle)
    }

    /// Close a chunk file handle
    async fn close_chunk_file(&self, inode: u64, chunk_index: u64) -> ChunkStoreResult<()> {
        let key = ChunkKey::new(inode, chunk_index);

        if let Some(handle) = self.open_handles.borrow_mut().remove(&key) {
            self.backend.close(handle).await?;
        }

        Ok(())
    }

    /// Write a chunk to file using io_uring
    pub async fn write_chunk(
        &self,
        inode: u64,
        chunk_index: u64,
        offset: u64,
        data: &[u8],
    ) -> ChunkStoreResult<usize> {
        if offset >= self.chunk_size as u64 {
            return Err(ChunkStoreError::InvalidOffset(offset));
        }

        // Open the chunk file
        let handle = self.open_chunk_file(inode, chunk_index, true).await?;
        let path = self.chunk_path(inode, chunk_index);

        // For partial writes, we need to read-modify-write
        let bytes_to_write = if offset > 0 || data.len() < self.chunk_size {
            // Read existing chunk if it exists and has data
            let mut chunk_data = if path.exists() {
                let stat = self.backend.stat(&path).await?;
                if stat.size > 0 {
                    let mut buf = vec![0u8; self.chunk_size];
                    let bytes_read = self.backend.read(handle, 0, &mut buf).await?;
                    buf.truncate(bytes_read.max(self.chunk_size));
                    if buf.len() < self.chunk_size {
                        buf.resize(self.chunk_size, 0);
                    }
                    buf
                } else {
                    vec![0u8; self.chunk_size]
                }
            } else {
                vec![0u8; self.chunk_size]
            };

            // Write data to buffer
            let offset_usize = offset as usize;
            let end = (offset_usize + data.len()).min(self.chunk_size);
            let bytes_to_write = end - offset_usize;

            chunk_data[offset_usize..end].copy_from_slice(&data[..bytes_to_write]);

            // Write entire chunk back
            self.backend.write(handle, 0, &chunk_data).await?;

            bytes_to_write
        } else {
            // Full chunk write - can write directly
            let bytes_to_write = data.len().min(self.chunk_size);
            self.backend.write(handle, offset, &data[..bytes_to_write]).await?
        };

        // Sync to ensure data is persisted
        self.backend.fsync(handle).await?;

        tracing::debug!(
            "Wrote {} bytes to chunk (inode={}, chunk_index={}, offset={})",
            bytes_to_write,
            inode,
            chunk_index,
            offset
        );

        Ok(bytes_to_write)
    }

    /// Read a chunk from file using io_uring
    pub async fn read_chunk(
        &self,
        inode: u64,
        chunk_index: u64,
        offset: u64,
        length: u64,
    ) -> ChunkStoreResult<Vec<u8>> {
        if offset >= self.chunk_size as u64 {
            return Err(ChunkStoreError::InvalidOffset(offset));
        }

        let path = self.chunk_path(inode, chunk_index);

        if !path.exists() {
            return Err(ChunkStoreError::ChunkNotFound { inode, chunk_index });
        }

        // Open the chunk file
        let handle = self.open_chunk_file(inode, chunk_index, false).await?;

        // Read data
        let mut buffer = vec![0u8; length as usize];
        let bytes_read = self.backend.read(handle, offset, &mut buffer).await?;

        buffer.truncate(bytes_read);

        tracing::debug!(
            "Read {} bytes from chunk (inode={}, chunk_index={}, offset={})",
            bytes_read,
            inode,
            chunk_index,
            offset
        );

        Ok(buffer)
    }

    /// Delete a chunk file
    pub async fn delete_chunk(&self, inode: u64, chunk_index: u64) -> ChunkStoreResult<()> {
        // Close the file if it's open
        self.close_chunk_file(inode, chunk_index).await?;

        let path = self.chunk_path(inode, chunk_index);

        if !path.exists() {
            return Err(ChunkStoreError::ChunkNotFound { inode, chunk_index });
        }

        // Delete using io_uring backend
        self.backend.unlink(&path).await?;

        tracing::debug!(
            "Deleted chunk (inode={}, chunk_index={})",
            inode,
            chunk_index
        );

        Ok(())
    }

    /// Delete all chunks for a file
    pub async fn delete_file_chunks(&self, inode: u64) -> ChunkStoreResult<usize> {
        let dir = self.base_dir.join(format!("{}", inode));

        if !dir.exists() {
            return Ok(0);
        }

        let mut deleted_count = 0;

        // Close all open handles for this inode
        let keys_to_close: Vec<ChunkKey> = self
            .open_handles
            .borrow()
            .keys()
            .filter(|k| k.inode == inode)
            .copied()
            .collect();

        for key in keys_to_close {
            self.close_chunk_file(key.inode, key.chunk_index).await?;
        }

        // Delete all chunk files
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            if entry.path().is_file() {
                self.backend.unlink(&entry.path()).await?;
                deleted_count += 1;
            }
        }

        // Remove the directory using io_uring backend
        self.backend.rmdir(&dir).await?;

        tracing::debug!("Deleted {} chunks for inode {}", deleted_count, inode);

        Ok(deleted_count)
    }

    /// Check if a chunk exists
    pub fn has_chunk(&self, inode: u64, chunk_index: u64) -> bool {
        self.chunk_path(inode, chunk_index).exists()
    }

    /// Close all open file handles
    pub async fn close_all(&self) -> ChunkStoreResult<()> {
        let handles: Vec<(ChunkKey, FileHandle)> = self
            .open_handles
            .borrow()
            .iter()
            .map(|(k, v)| (*k, *v))
            .collect();

        for (key, handle) in handles {
            if let Err(e) = self.backend.close(handle).await {
                tracing::warn!(
                    "Failed to close chunk file (inode={}, chunk={}): {:?}",
                    key.inode,
                    key.chunk_index,
                    e
                );
            }
        }

        self.open_handles.borrow_mut().clear();

        Ok(())
    }

    /// Get chunk size
    pub fn chunk_size(&self) -> usize {
        self.chunk_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pluvio_runtime::executor::Runtime;

    fn run_test<F>(test: F)
    where
        F: std::future::Future<Output = ()> + 'static,
    {
        let runtime = Runtime::new(256);
        runtime.clone().run(test);
    }

    #[test]
    fn test_chunk_key() {
        let key1 = ChunkKey::new(1, 0);
        let key2 = ChunkKey::new(1, 0);
        let key3 = ChunkKey::new(1, 1);

        assert_eq!(key1, key2);
        assert_ne!(key1, key3);
    }

    #[test]
    fn test_inmemory_write_read_chunk() {
        run_test(async {
            let store = InMemoryChunkStore::new();

            let data = vec![0xAA; 1024];
            let written = store.write_chunk(1, 0, 0, &data).await.unwrap();
            assert_eq!(written, 1024);

            let read_data = store.read_chunk(1, 0, 0, 1024).await.unwrap();
            assert_eq!(read_data, data);
        });
    }

    #[test]
    fn test_inmemory_partial_write() {
        run_test(async {
            let store = InMemoryChunkStore::new();

            // Write at offset 1024
            let data = vec![0xBB; 512];
            let written = store.write_chunk(1, 0, 1024, &data).await.unwrap();
            assert_eq!(written, 512);

            // Read back
            let read_data = store.read_chunk(1, 0, 1024, 512).await.unwrap();
            assert_eq!(read_data, data);

            // Read from beginning should be zeros
            let read_data = store.read_chunk(1, 0, 0, 1024).await.unwrap();
            assert_eq!(read_data, vec![0u8; 1024]);
        });
    }

    #[test]
    fn test_inmemory_delete_chunk() {
        run_test(async {
            let store = InMemoryChunkStore::new();

            let data = vec![0xCC; 512];
            store.write_chunk(1, 0, 0, &data).await.unwrap();

            assert!(store.has_chunk(1, 0));
            store.delete_chunk(1, 0).await.unwrap();
            assert!(!store.has_chunk(1, 0));

            // Reading deleted chunk should fail
            let result = store.read_chunk(1, 0, 0, 512).await;
            assert!(result.is_err());
        });
    }

    #[test]
    fn test_inmemory_delete_file_chunks() {
        run_test(async {
            let store = InMemoryChunkStore::new();

            // Write multiple chunks for the same file
            let data = vec![0xDD; 512];
            store.write_chunk(1, 0, 0, &data).await.unwrap();
            store.write_chunk(1, 1, 0, &data).await.unwrap();
            store.write_chunk(1, 2, 0, &data).await.unwrap();

            // Write a chunk for a different file
            store.write_chunk(2, 0, 0, &data).await.unwrap();

            assert_eq!(store.chunk_count(), 4);

            // Delete all chunks for file 1
            let deleted = store.delete_file_chunks(1).await.unwrap();
            assert_eq!(deleted, 3);
            assert_eq!(store.chunk_count(), 1);

            // File 2 chunk should still exist
            assert!(store.has_chunk(2, 0));
        });
    }

    #[test]
    fn test_inmemory_storage_full() {
        run_test(async {
            let store = InMemoryChunkStore::with_capacity(2, CHUNK_SIZE);

            let data = vec![0xEE; 512];
            store.write_chunk(1, 0, 0, &data).await.unwrap();
            store.write_chunk(1, 1, 0, &data).await.unwrap();

            // Third chunk should fail
            let result = store.write_chunk(1, 2, 0, &data).await;
            assert!(result.is_err());
            assert!(matches!(result.unwrap_err(), ChunkStoreError::StorageFull(_)));
        });
    }

    #[test]
    fn test_inmemory_invalid_offset() {
        run_test(async {
            let store = InMemoryChunkStore::new();

            let data = vec![0xFF; 512];
            let result = store.write_chunk(1, 0, CHUNK_SIZE as u64 + 100, &data).await;
            assert!(result.is_err());
            assert!(matches!(result.unwrap_err(), ChunkStoreError::InvalidOffset(_)));
        });
    }

    #[test]
    fn test_inmemory_clear() {
        run_test(async {
            let store = InMemoryChunkStore::new();

            let data = vec![0x11; 512];
            store.write_chunk(1, 0, 0, &data).await.unwrap();
            store.write_chunk(2, 0, 0, &data).await.unwrap();

            assert_eq!(store.chunk_count(), 2);

            store.clear();
            assert_eq!(store.chunk_count(), 0);
        });
    }

    #[test]
    fn test_inmemory_storage_size() {
        run_test(async {
            let store = InMemoryChunkStore::new();

            assert_eq!(store.storage_size(), 0);

            let data = vec![0x22; 512];
            store.write_chunk(1, 0, 0, &data).await.unwrap();
            store.write_chunk(1, 1, 0, &data).await.unwrap();

            assert_eq!(store.storage_size(), 2 * CHUNK_SIZE);
        });
    }
}
