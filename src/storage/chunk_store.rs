use std::cell::RefCell;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::metadata::CHUNK_SIZE;

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
    pub fn write_chunk(
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
    pub fn read_chunk(
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
    pub fn delete_chunk(&self, inode: u64, chunk_index: u64) -> ChunkStoreResult<()> {
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
    pub fn delete_file_chunks(&self, inode: u64) -> ChunkStoreResult<usize> {
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
    pub fn write_chunk(
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
    pub fn read_chunk(
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
    pub fn delete_chunk(&self, inode: u64, chunk_index: u64) -> ChunkStoreResult<()> {
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
    pub fn delete_file_chunks(&self, inode: u64) -> ChunkStoreResult<usize> {
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

#[cfg(test)]
mod tests {
    use super::*;

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
        let store = InMemoryChunkStore::new();

        let data = vec![0xAA; 1024];
        let written = store.write_chunk(1, 0, 0, &data).unwrap();
        assert_eq!(written, 1024);

        let read_data = store.read_chunk(1, 0, 0, 1024).unwrap();
        assert_eq!(read_data, data);
    }

    #[test]
    fn test_inmemory_partial_write() {
        let store = InMemoryChunkStore::new();

        // Write at offset 1024
        let data = vec![0xBB; 512];
        let written = store.write_chunk(1, 0, 1024, &data).unwrap();
        assert_eq!(written, 512);

        // Read back
        let read_data = store.read_chunk(1, 0, 1024, 512).unwrap();
        assert_eq!(read_data, data);

        // Read from beginning should be zeros
        let read_data = store.read_chunk(1, 0, 0, 1024).unwrap();
        assert_eq!(read_data, vec![0u8; 1024]);
    }

    #[test]
    fn test_inmemory_delete_chunk() {
        let store = InMemoryChunkStore::new();

        let data = vec![0xCC; 512];
        store.write_chunk(1, 0, 0, &data).unwrap();

        assert!(store.has_chunk(1, 0));
        store.delete_chunk(1, 0).unwrap();
        assert!(!store.has_chunk(1, 0));

        // Reading deleted chunk should fail
        let result = store.read_chunk(1, 0, 0, 512);
        assert!(result.is_err());
    }

    #[test]
    fn test_inmemory_delete_file_chunks() {
        let store = InMemoryChunkStore::new();

        // Write multiple chunks for the same file
        let data = vec![0xDD; 512];
        store.write_chunk(1, 0, 0, &data).unwrap();
        store.write_chunk(1, 1, 0, &data).unwrap();
        store.write_chunk(1, 2, 0, &data).unwrap();

        // Write a chunk for a different file
        store.write_chunk(2, 0, 0, &data).unwrap();

        assert_eq!(store.chunk_count(), 4);

        // Delete all chunks for file 1
        let deleted = store.delete_file_chunks(1).unwrap();
        assert_eq!(deleted, 3);
        assert_eq!(store.chunk_count(), 1);

        // File 2 chunk should still exist
        assert!(store.has_chunk(2, 0));
    }

    #[test]
    fn test_inmemory_storage_full() {
        let store = InMemoryChunkStore::with_capacity(2, CHUNK_SIZE);

        let data = vec![0xEE; 512];
        store.write_chunk(1, 0, 0, &data).unwrap();
        store.write_chunk(1, 1, 0, &data).unwrap();

        // Third chunk should fail
        let result = store.write_chunk(1, 2, 0, &data);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ChunkStoreError::StorageFull(_)));
    }

    #[test]
    fn test_inmemory_invalid_offset() {
        let store = InMemoryChunkStore::new();

        let data = vec![0xFF; 512];
        let result = store.write_chunk(1, 0, CHUNK_SIZE as u64 + 100, &data);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ChunkStoreError::InvalidOffset(_)));
    }

    #[test]
    fn test_inmemory_clear() {
        let store = InMemoryChunkStore::new();

        let data = vec![0x11; 512];
        store.write_chunk(1, 0, 0, &data).unwrap();
        store.write_chunk(2, 0, 0, &data).unwrap();

        assert_eq!(store.chunk_count(), 2);

        store.clear();
        assert_eq!(store.chunk_count(), 0);
    }

    #[test]
    fn test_inmemory_storage_size() {
        let store = InMemoryChunkStore::new();

        assert_eq!(store.storage_size(), 0);

        let data = vec![0x22; 512];
        store.write_chunk(1, 0, 0, &data).unwrap();
        store.write_chunk(1, 1, 0, &data).unwrap();

        assert_eq!(store.storage_size(), 2 * CHUNK_SIZE);
    }
}
