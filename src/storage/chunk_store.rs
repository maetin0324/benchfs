use std::cell::RefCell;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use lru::LruCache;
use std::num::NonZeroUsize;

use tracing::instrument;

use super::{FileHandle, IOUringBackend, OpenFlags, StorageBackend};
use crate::metadata::CHUNK_SIZE;

/// Chunk store trait for different storage backends
///
/// In the new design, all operations use full path instead of inode.
#[async_trait::async_trait(?Send)]
pub trait ChunkStore: std::any::Any {
    async fn write_chunk(
        &self,
        path: &str,
        chunk_index: u64,
        offset: u64,
        data: &[u8],
    ) -> ChunkStoreResult<usize>;
    async fn read_chunk(
        &self,
        path: &str,
        chunk_index: u64,
        offset: u64,
        length: u64,
    ) -> ChunkStoreResult<Vec<u8>>;
    async fn delete_chunk(&self, path: &str, chunk_index: u64) -> ChunkStoreResult<()>;
    async fn delete_file_chunks(&self, path: &str) -> ChunkStoreResult<usize>;
    fn has_chunk(&self, path: &str, chunk_index: u64) -> bool;
}

/// Chunk storage error types
#[derive(Debug, thiserror::Error)]
pub enum ChunkStoreError {
    #[error("Chunk not found: path {path}, chunk {chunk_index}")]
    ChunkNotFound { path: String, chunk_index: u64 },

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
///
/// In the new design, chunks are identified by full path + chunk index
/// instead of inode + chunk index. This eliminates the need for separate
/// metadata management.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ChunkKey {
    /// Full file path
    pub path: String,
    /// Chunk index
    pub chunk_index: u64,
}

impl ChunkKey {
    pub fn new(path: String, chunk_index: u64) -> Self {
        Self { path, chunk_index }
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
    /// * `path` - File path
    /// * `chunk_index` - Chunk index
    /// * `offset` - Offset within the chunk
    /// * `data` - Data to write
    #[async_backtrace::framed]
    pub async fn write_chunk(
        &self,
        path: &str,
        chunk_index: u64,
        offset: u64,
        data: &[u8],
    ) -> ChunkStoreResult<usize> {
        if offset >= self.chunk_size as u64 {
            return Err(ChunkStoreError::InvalidOffset(offset));
        }

        let key = ChunkKey::new(path.to_string(), chunk_index);
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
            "Wrote {} bytes to chunk (path={}, chunk_index={}, offset={})",
            bytes_to_write,
            path,
            chunk_index,
            offset
        );

        Ok(bytes_to_write)
    }

    /// Read a chunk from storage
    ///
    /// # Arguments
    /// * `path` - File path
    /// * `chunk_index` - Chunk index
    /// * `offset` - Offset within the chunk
    /// * `length` - Number of bytes to read
    #[async_backtrace::framed]
    pub async fn read_chunk(
        &self,
        path: &str,
        chunk_index: u64,
        offset: u64,
        length: u64,
    ) -> ChunkStoreResult<Vec<u8>> {
        if offset >= self.chunk_size as u64 {
            return Err(ChunkStoreError::InvalidOffset(offset));
        }

        let key = ChunkKey::new(path.to_string(), chunk_index);
        let chunks = self.chunks.borrow();

        let chunk_data = chunks.get(&key).ok_or(ChunkStoreError::ChunkNotFound {
            path: path.to_string(),
            chunk_index,
        })?;

        let offset = offset as usize;
        let end = (offset + length as usize).min(self.chunk_size);
        let bytes_to_read = end - offset;

        let data = chunk_data[offset..end].to_vec();

        tracing::debug!(
            "Read {} bytes from chunk (path={}, chunk_index={}, offset={})",
            bytes_to_read,
            path,
            chunk_index,
            offset
        );

        Ok(data)
    }

    /// Delete a chunk from storage
    #[async_backtrace::framed]
    pub async fn delete_chunk(&self, path: &str, chunk_index: u64) -> ChunkStoreResult<()> {
        let key = ChunkKey::new(path.to_string(), chunk_index);
        let mut chunks = self.chunks.borrow_mut();

        chunks.remove(&key).ok_or(ChunkStoreError::ChunkNotFound {
            path: path.to_string(),
            chunk_index,
        })?;

        tracing::debug!("Deleted chunk (path={}, chunk_index={})", path, chunk_index);

        Ok(())
    }

    /// Delete all chunks for a file
    #[async_backtrace::framed]
    pub async fn delete_file_chunks(&self, path: &str) -> ChunkStoreResult<usize> {
        let mut chunks = self.chunks.borrow_mut();
        let mut deleted_count = 0;

        chunks.retain(|key, _| {
            if key.path == path {
                deleted_count += 1;
                false
            } else {
                true
            }
        });

        tracing::debug!("Deleted {} chunks for path {}", deleted_count, path);

        Ok(deleted_count)
    }

    /// Check if a chunk exists
    pub fn has_chunk(&self, path: &str, chunk_index: u64) -> bool {
        let key = ChunkKey::new(path.to_string(), chunk_index);
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

#[async_trait::async_trait(?Send)]
impl ChunkStore for InMemoryChunkStore {
    async fn write_chunk(
        &self,
        path: &str,
        chunk_index: u64,
        offset: u64,
        data: &[u8],
    ) -> ChunkStoreResult<usize> {
        self.write_chunk(path, chunk_index, offset, data).await
    }

    async fn read_chunk(
        &self,
        path: &str,
        chunk_index: u64,
        offset: u64,
        length: u64,
    ) -> ChunkStoreResult<Vec<u8>> {
        self.read_chunk(path, chunk_index, offset, length).await
    }

    async fn delete_chunk(&self, path: &str, chunk_index: u64) -> ChunkStoreResult<()> {
        self.delete_chunk(path, chunk_index).await
    }

    async fn delete_file_chunks(&self, path: &str) -> ChunkStoreResult<usize> {
        self.delete_file_chunks(path).await
    }

    fn has_chunk(&self, path: &str, chunk_index: u64) -> bool {
        self.has_chunk(path, chunk_index)
    }
}

/// File-based chunk storage
///
/// Stores chunks in the local filesystem. Each chunk is stored as a separate file.
/// Directory structure: <base_dir>/<path_hash>/<chunk_index>
/// where path_hash is a hash of the full file path to create a valid directory name.
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

    /// Hash a file path to create a valid directory name
    fn hash_path(&self, path: &str) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        path.hash(&mut hasher);
        format!("{:x}", hasher.finish())
    }

    /// Get the path for a chunk file
    fn chunk_path(&self, path: &str, chunk_index: u64) -> PathBuf {
        let path_hash = self.hash_path(path);
        self.base_dir
            .join(path_hash)
            .join(format!("{}", chunk_index))
    }

    /// Ensure the directory for a file path exists
    fn ensure_path_dir(&self, path: &str) -> ChunkStoreResult<PathBuf> {
        let path_hash = self.hash_path(path);
        let dir = self.base_dir.join(path_hash);
        if !dir.exists() {
            std::fs::create_dir_all(&dir)?;
        }
        Ok(dir)
    }

    /// Write a chunk to file
    #[async_backtrace::framed]
    pub async fn write_chunk(
        &self,
        file_path: &str,
        chunk_index: u64,
        offset: u64,
        data: &[u8],
    ) -> ChunkStoreResult<usize> {
        if offset >= self.chunk_size as u64 {
            return Err(ChunkStoreError::InvalidOffset(offset));
        }

        self.ensure_path_dir(file_path)?;
        let chunk_file_path = self.chunk_path(file_path, chunk_index);

        // Read existing chunk or create new one
        let mut chunk_data = if chunk_file_path.exists() {
            std::fs::read(&chunk_file_path)?
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
        std::fs::write(&chunk_file_path, &chunk_data)?;

        tracing::debug!(
            "Wrote {} bytes to chunk file (path={}, chunk_index={}, file={:?})",
            bytes_to_write,
            file_path,
            chunk_index,
            chunk_file_path
        );

        Ok(bytes_to_write)
    }

    /// Read a chunk from file
    #[async_backtrace::framed]
    pub async fn read_chunk(
        &self,
        file_path: &str,
        chunk_index: u64,
        offset: u64,
        length: u64,
    ) -> ChunkStoreResult<Vec<u8>> {
        if offset >= self.chunk_size as u64 {
            return Err(ChunkStoreError::InvalidOffset(offset));
        }

        let chunk_file_path = self.chunk_path(file_path, chunk_index);

        if !chunk_file_path.exists() {
            return Err(ChunkStoreError::ChunkNotFound {
                path: file_path.to_string(),
                chunk_index,
            });
        }

        let chunk_data = std::fs::read(&chunk_file_path)?;
        let offset = offset as usize;
        let end = (offset + length as usize).min(chunk_data.len());

        if offset >= chunk_data.len() {
            return Ok(Vec::new());
        }

        Ok(chunk_data[offset..end].to_vec())
    }

    /// Delete a chunk file
    #[async_backtrace::framed]
    pub async fn delete_chunk(&self, file_path: &str, chunk_index: u64) -> ChunkStoreResult<()> {
        let chunk_file_path = self.chunk_path(file_path, chunk_index);

        if !chunk_file_path.exists() {
            return Err(ChunkStoreError::ChunkNotFound {
                path: file_path.to_string(),
                chunk_index,
            });
        }

        std::fs::remove_file(&chunk_file_path)?;

        tracing::debug!(
            "Deleted chunk file (path={}, chunk_index={}, file={:?})",
            file_path,
            chunk_index,
            chunk_file_path
        );

        Ok(())
    }

    /// Delete all chunks for a file
    #[async_backtrace::framed]
    pub async fn delete_file_chunks(&self, file_path: &str) -> ChunkStoreResult<usize> {
        let path_hash = self.hash_path(file_path);
        let dir = self.base_dir.join(path_hash);

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

        tracing::debug!(
            "Deleted {} chunk files for path {}",
            deleted_count,
            file_path
        );

        Ok(deleted_count)
    }

    /// Check if a chunk exists
    pub fn has_chunk(&self, file_path: &str, chunk_index: u64) -> bool {
        self.chunk_path(file_path, chunk_index).exists()
    }
}

#[async_trait::async_trait(?Send)]
impl ChunkStore for FileChunkStore {
    async fn write_chunk(
        &self,
        path: &str,
        chunk_index: u64,
        offset: u64,
        data: &[u8],
    ) -> ChunkStoreResult<usize> {
        self.write_chunk(path, chunk_index, offset, data).await
    }

    async fn read_chunk(
        &self,
        path: &str,
        chunk_index: u64,
        offset: u64,
        length: u64,
    ) -> ChunkStoreResult<Vec<u8>> {
        self.read_chunk(path, chunk_index, offset, length).await
    }

    async fn delete_chunk(&self, path: &str, chunk_index: u64) -> ChunkStoreResult<()> {
        self.delete_chunk(path, chunk_index).await
    }

    async fn delete_file_chunks(&self, path: &str) -> ChunkStoreResult<usize> {
        self.delete_file_chunks(path).await
    }

    fn has_chunk(&self, path: &str, chunk_index: u64) -> bool {
        self.has_chunk(path, chunk_index)
    }
}

/// IO_uring-based chunk storage
///
/// Stores chunks in the local filesystem using io_uring for high-performance async I/O.
/// Directory structure: <base_dir>/<path_hash>/<chunk_index>
/// where path_hash is a hash of the full file path.
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

    /// LRU cache of open file handles (path, chunk_index) -> FileHandle
    /// This avoids repeatedly opening the same chunk file while preventing
    /// file descriptor exhaustion by automatically closing least-recently-used files
    open_handles: RefCell<LruCache<ChunkKey, FileHandle>>,

    /// Evicted file handles pending deferred cleanup
    /// These are closed in batch during close_all() to avoid async/await deadlock
    /// that occurs when calling async close() from within synchronous LRU eviction callback
    evicted_handles: RefCell<Vec<(ChunkKey, FileHandle)>>,
}

impl IOUringChunkStore {
    /// Default maximum number of open file handles to cache
    /// This prevents file descriptor exhaustion while maintaining good performance
    /// Increased to 8192 to handle 32GiB IOR tests (4MB chunks = 8192 chunks) without eviction
    const DEFAULT_MAX_OPEN_FILES: usize = 8192;

    /// Create a new IO_uring-based chunk store
    ///
    /// # Arguments
    /// * `base_dir` - Base directory for chunk storage
    /// * `backend` - IO_uring backend for file operations
    pub fn new<P: AsRef<Path>>(base_dir: P, backend: Rc<IOUringBackend>) -> ChunkStoreResult<Self> {
        Self::with_capacity(base_dir, backend, Self::DEFAULT_MAX_OPEN_FILES)
    }

    /// Create a new IO_uring-based chunk store with custom capacity
    ///
    /// # Arguments
    /// * `base_dir` - Base directory for chunk storage
    /// * `backend` - IO_uring backend for file operations
    /// * `max_open_files` - Maximum number of file handles to keep open
    pub fn with_capacity<P: AsRef<Path>>(
        base_dir: P,
        backend: Rc<IOUringBackend>,
        max_open_files: usize,
    ) -> ChunkStoreResult<Self> {
        let base_dir = base_dir.as_ref().to_path_buf();

        // Create base directory if it doesn't exist
        if !base_dir.exists() {
            std::fs::create_dir_all(&base_dir)?;
        }

        let capacity = NonZeroUsize::new(max_open_files)
            .unwrap_or_else(|| NonZeroUsize::new(Self::DEFAULT_MAX_OPEN_FILES).unwrap());

        Ok(Self {
            base_dir,
            chunk_size: CHUNK_SIZE,
            backend,
            open_handles: RefCell::new(LruCache::new(capacity)),
            evicted_handles: RefCell::new(Vec::new()),
        })
    }

    /// Hash a file path to create a valid directory name
    fn hash_path(&self, path: &str) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        path.hash(&mut hasher);
        format!("{:x}", hasher.finish())
    }

    /// Get the path for a chunk file
    fn chunk_path(&self, file_path: &str, chunk_index: u64) -> PathBuf {
        let path_hash = self.hash_path(file_path);
        self.base_dir
            .join(path_hash)
            .join(format!("{}", chunk_index))
    }

    /// Ensure the directory for a file path exists
    fn ensure_path_dir(&self, file_path: &str) -> ChunkStoreResult<PathBuf> {
        let path_hash = self.hash_path(file_path);
        let dir = self.base_dir.join(path_hash);
        if !dir.exists() {
            std::fs::create_dir_all(&dir)?;
        }
        Ok(dir)
    }

    /// Open a chunk file, creating it if necessary
    ///
    /// Returns a cached handle if the file is already open.
    /// Uses LRU eviction to automatically close least-recently-used files
    /// when the cache is full, preventing file descriptor exhaustion.
    async fn open_chunk_file(
        &self,
        file_path: &str,
        chunk_index: u64,
        write: bool,
    ) -> ChunkStoreResult<FileHandle> {
        let key = ChunkKey::new(file_path.to_string(), chunk_index);

        // Check if already open (this also updates LRU order)
        if let Some(&handle) = self.open_handles.borrow_mut().get(&key) {
            tracing::trace!(
                "Reusing cached file handle for chunk (path={}, chunk={})",
                file_path,
                chunk_index
            );
            return Ok(handle);
        }

        // Ensure directory exists
        self.ensure_path_dir(file_path)?;
        let chunk_file_path = self.chunk_path(file_path, chunk_index);

        // Open or create the file with O_DIRECT to bypass OS page cache
        let flags = if write {
            OpenFlags {
                read: true,
                write: true,
                create: true,
                truncate: false,
                append: false,
                direct: true,
            }
        } else {
            OpenFlags::read_only().with_direct()
        };

        let handle = self.backend.open(&chunk_file_path, flags).await?;

        tracing::debug!("Opened file: {:?} with fd={:?}", chunk_file_path, handle);

        // Insert into LRU cache. If cache is full, this will evict the least-recently-used entry.
        let mut cache = self.open_handles.borrow_mut();
        if let Some((evicted_key, evicted_handle)) = cache.push(key, handle) {
            // A file handle was evicted from the cache.
            // IMPORTANT: We do NOT close it here because:
            // 1. Calling async close() from within eviction creates async/await deadlock
            // 2. The await can block indefinitely if runtime is busy
            // 3. File descriptors will be closed in close_all() during cleanup
            //
            // Instead, we store evicted handles for deferred cleanup.
            // The large cache size (8192) means evictions are rare in practice.
            tracing::debug!(
                "Evicting file handle from LRU cache (path={}, chunk={}), fd={:?} - deferred close",
                evicted_key.path,
                evicted_key.chunk_index,
                evicted_handle
            );

            // Store for deferred cleanup to prevent file descriptor leaks
            self.evicted_handles
                .borrow_mut()
                .push((evicted_key, evicted_handle));
        }

        Ok(handle)
    }

    /// Close a chunk file handle
    async fn close_chunk_file(&self, file_path: &str, chunk_index: u64) -> ChunkStoreResult<()> {
        let key = ChunkKey::new(file_path.to_string(), chunk_index);

        if let Some(handle) = self.open_handles.borrow_mut().pop(&key) {
            tracing::debug!(
                "Explicitly closing file handle (path={}, chunk={})",
                file_path,
                chunk_index
            );
            self.backend.close(handle).await?;
        }

        Ok(())
    }

    /// Write a chunk to file using io_uring
    #[async_backtrace::framed]
    #[instrument(level = "trace", name = "chunk_write", skip(self, data), fields(path = file_path, chunk = chunk_index, offset, len = data.len()))]
    pub async fn write_chunk(
        &self,
        file_path: &str,
        chunk_index: u64,
        offset: u64,
        data: &[u8],
    ) -> ChunkStoreResult<usize> {
        if offset >= self.chunk_size as u64 {
            return Err(ChunkStoreError::InvalidOffset(offset));
        }

        // Open the chunk file
        let handle = self.open_chunk_file(file_path, chunk_index, true).await?;

        // Write data directly at the specified offset
        // io_uring's pwrite handles sparse files efficiently - no need for read-modify-write
        let bytes_to_write = data.len().min(self.chunk_size - offset as usize);
        self.backend
            .write(handle, offset, &data[..bytes_to_write])
            .await?;

        // NOTE: fsync is disabled for performance. Data is cached by OS and written
        // asynchronously. For durability guarantees, users should explicitly call fsync.
        // This is a common trade-off in high-performance filesystems.
        // self.backend.fsync(handle).await?;

        tracing::trace!("Wrote {} bytes", bytes_to_write);

        Ok(bytes_to_write)
    }

    /// Read a chunk from file using io_uring
    #[async_backtrace::framed]
    #[instrument(level = "trace", name = "chunk_read", skip(self), fields(path = file_path, chunk = chunk_index, offset, len = length))]
    pub async fn read_chunk(
        &self,
        file_path: &str,
        chunk_index: u64,
        offset: u64,
        length: u64,
    ) -> ChunkStoreResult<Vec<u8>> {
        if offset >= self.chunk_size as u64 {
            return Err(ChunkStoreError::InvalidOffset(offset));
        }

        let chunk_file_path = self.chunk_path(file_path, chunk_index);

        if !chunk_file_path.exists() {
            return Err(ChunkStoreError::ChunkNotFound {
                path: file_path.to_string(),
                chunk_index,
            });
        }

        // Open the chunk file
        let handle = self.open_chunk_file(file_path, chunk_index, false).await?;

        // Read data
        let mut buffer = vec![0u8; length as usize];
        let bytes_read = self.backend.read(handle, offset, &mut buffer).await?;

        buffer.truncate(bytes_read);

        tracing::trace!("Read {} bytes", bytes_read);

        Ok(buffer)
    }

    /// Delete a chunk file
    #[async_backtrace::framed]
    pub async fn delete_chunk(&self, file_path: &str, chunk_index: u64) -> ChunkStoreResult<()> {
        // Close the file if it's open
        self.close_chunk_file(file_path, chunk_index).await?;

        let chunk_file_path = self.chunk_path(file_path, chunk_index);

        if !chunk_file_path.exists() {
            return Err(ChunkStoreError::ChunkNotFound {
                path: file_path.to_string(),
                chunk_index,
            });
        }

        // Delete using io_uring backend
        self.backend.unlink(&chunk_file_path).await?;

        tracing::debug!(
            "Deleted chunk (path={}, chunk_index={})",
            file_path,
            chunk_index
        );

        Ok(())
    }

    /// Delete all chunks for a file
    #[async_backtrace::framed]
    pub async fn delete_file_chunks(&self, file_path: &str) -> ChunkStoreResult<usize> {
        let path_hash = self.hash_path(file_path);
        let dir = self.base_dir.join(path_hash);

        if !dir.exists() {
            return Ok(0);
        }

        let mut deleted_count = 0;

        // Close all open handles for this file path
        let keys_to_close: Vec<ChunkKey> = self
            .open_handles
            .borrow()
            .iter()
            .filter(|(k, _)| k.path == file_path)
            .map(|(k, _)| k.clone())
            .collect();

        for key in keys_to_close {
            self.close_chunk_file(&key.path, key.chunk_index).await?;
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

        tracing::debug!("Deleted {} chunks for path {}", deleted_count, file_path);

        Ok(deleted_count)
    }

    /// Check if a chunk exists
    pub fn has_chunk(&self, file_path: &str, chunk_index: u64) -> bool {
        self.chunk_path(file_path, chunk_index).exists()
    }

    /// Close all open file handles
    #[async_backtrace::framed]
    pub async fn close_all(&self) -> ChunkStoreResult<()> {
        // First, close all evicted handles that were deferred to prevent deadlock
        let evicted: Vec<(ChunkKey, FileHandle)> =
            self.evicted_handles.borrow_mut().drain(..).collect();
        for (key, handle) in evicted {
            if let Err(e) = self.backend.close(handle).await {
                tracing::warn!(
                    "Failed to close evicted chunk file (path={}, chunk={}): {:?}",
                    key.path,
                    key.chunk_index,
                    e
                );
            }
        }

        // Then, close all currently cached handles
        let handles: Vec<(ChunkKey, FileHandle)> = self
            .open_handles
            .borrow()
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect();

        for (key, handle) in handles {
            if let Err(e) = self.backend.close(handle).await {
                tracing::warn!(
                    "Failed to close chunk file (path={}, chunk={}): {:?}",
                    key.path,
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

    /// Write a chunk using a registered buffer (zero-copy DMA)
    ///
    /// This method writes data directly from a FixedBuffer to disk without
    /// an intermediate copy, maximizing write performance.
    ///
    /// # Arguments
    /// * `file_path` - File path
    /// * `chunk_index` - Chunk index
    /// * `offset` - Offset within the chunk
    /// * `fixed_buffer` - Pre-populated registered buffer
    /// * `data_len` - Actual data length in the buffer
    #[async_backtrace::framed]
    pub async fn write_chunk_fixed(
        &self,
        file_path: &str,
        chunk_index: u64,
        offset: u64,
        fixed_buffer: pluvio_uring::allocator::FixedBuffer,
        data_len: usize,
    ) -> ChunkStoreResult<usize> {
        if offset >= self.chunk_size as u64 {
            return Err(ChunkStoreError::InvalidOffset(offset));
        }

        // Open the chunk file
        let handle = self.open_chunk_file(file_path, chunk_index, true).await?;

        // Write data directly from registered buffer using DMA (zero-copy)
        let bytes_to_write = data_len.min(self.chunk_size - offset as usize);
        let bytes_written = self
            .backend
            .write_fixed_direct(handle, offset, fixed_buffer, bytes_to_write)
            .await?;

        tracing::debug!(
            "Wrote {} bytes (zero-copy) to chunk (path={}, chunk_index={}, offset={})",
            bytes_written,
            file_path,
            chunk_index,
            offset
        );

        Ok(bytes_written)
    }

    /// Get the buffer allocator for acquiring registered buffers
    pub fn allocator(&self) -> &std::rc::Rc<pluvio_uring::allocator::FixedBufferAllocator> {
        self.backend.allocator()
    }

    /// Read a chunk using a registered buffer (zero-copy DMA)
    ///
    /// This method reads data directly from disk into a FixedBuffer without
    /// an intermediate copy, maximizing read performance.
    ///
    /// # Arguments
    /// * `file_path` - File path
    /// * `chunk_index` - Chunk index
    /// * `offset` - Offset within the chunk
    /// * `fixed_buffer` - Pre-allocated registered buffer to read into
    ///
    /// # Returns
    /// A tuple of (bytes_read, buffer) where buffer is the FixedBuffer with data
    #[async_backtrace::framed]
    pub async fn read_chunk_fixed(
        &self,
        file_path: &str,
        chunk_index: u64,
        offset: u64,
        fixed_buffer: pluvio_uring::allocator::FixedBuffer,
    ) -> ChunkStoreResult<(usize, pluvio_uring::allocator::FixedBuffer)> {
        if offset >= self.chunk_size as u64 {
            return Err(ChunkStoreError::InvalidOffset(offset));
        }

        let chunk_file_path = self.chunk_path(file_path, chunk_index);

        if !chunk_file_path.exists() {
            return Err(ChunkStoreError::ChunkNotFound {
                path: file_path.to_string(),
                chunk_index,
            });
        }

        // Open the chunk file
        let handle = self.open_chunk_file(file_path, chunk_index, false).await?;

        // Read data directly into registered buffer using DMA (zero-copy)
        let (bytes_read, fixed_buffer) = self
            .backend
            .read_fixed_direct(handle, offset, fixed_buffer)
            .await?;

        tracing::debug!(
            "Read {} bytes (zero-copy) from chunk (path={}, chunk_index={}, offset={})",
            bytes_read,
            file_path,
            chunk_index,
            offset
        );

        Ok((bytes_read, fixed_buffer))
    }
}

#[async_trait::async_trait(?Send)]
impl ChunkStore for IOUringChunkStore {
    async fn write_chunk(
        &self,
        path: &str,
        chunk_index: u64,
        offset: u64,
        data: &[u8],
    ) -> ChunkStoreResult<usize> {
        self.write_chunk(path, chunk_index, offset, data).await
    }

    async fn read_chunk(
        &self,
        path: &str,
        chunk_index: u64,
        offset: u64,
        length: u64,
    ) -> ChunkStoreResult<Vec<u8>> {
        self.read_chunk(path, chunk_index, offset, length).await
    }

    async fn delete_chunk(&self, path: &str, chunk_index: u64) -> ChunkStoreResult<()> {
        self.delete_chunk(path, chunk_index).await
    }

    async fn delete_file_chunks(&self, path: &str) -> ChunkStoreResult<usize> {
        self.delete_file_chunks(path).await
    }

    fn has_chunk(&self, path: &str, chunk_index: u64) -> bool {
        self.has_chunk(path, chunk_index)
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
        runtime
            .clone()
            .run_with_name_and_runtime("chunk_store_test", test);
    }

    #[test]
    fn test_chunk_key() {
        let key1 = ChunkKey::new("/file1".to_string(), 0);
        let key2 = ChunkKey::new("/file1".to_string(), 0);
        let key3 = ChunkKey::new("/file1".to_string(), 1);

        assert_eq!(key1, key2);
        assert_ne!(key1, key3);
    }

    #[test]
    fn test_inmemory_write_read_chunk() {
        run_test(async {
            let store = InMemoryChunkStore::new();

            let data = vec![0xAA; 1024];
            let written = store.write_chunk("/file1", 0, 0, &data).await.unwrap();
            assert_eq!(written, 1024);

            let read_data = store.read_chunk("/file1", 0, 0, 1024).await.unwrap();
            assert_eq!(read_data, data);
        });
    }

    #[test]
    fn test_inmemory_partial_write() {
        run_test(async {
            let store = InMemoryChunkStore::new();

            // Write at offset 1024
            let data = vec![0xBB; 512];
            let written = store.write_chunk("/file1", 0, 1024, &data).await.unwrap();
            assert_eq!(written, 512);

            // Read back
            let read_data = store.read_chunk("/file1", 0, 1024, 512).await.unwrap();
            assert_eq!(read_data, data);

            // Read from beginning should be zeros
            let read_data = store.read_chunk("/file1", 0, 0, 1024).await.unwrap();
            assert_eq!(read_data, vec![0u8; 1024]);
        });
    }

    #[test]
    fn test_inmemory_delete_chunk() {
        run_test(async {
            let store = InMemoryChunkStore::new();

            let data = vec![0xCC; 512];
            store.write_chunk("/file1", 0, 0, &data).await.unwrap();

            assert!(store.has_chunk("/file1", 0));
            store.delete_chunk("/file1", 0).await.unwrap();
            assert!(!store.has_chunk("/file1", 0));

            // Reading deleted chunk should fail
            let result = store.read_chunk("/file1", 0, 0, 512).await;
            assert!(result.is_err());
        });
    }

    #[test]
    fn test_inmemory_delete_file_chunks() {
        run_test(async {
            let store = InMemoryChunkStore::new();

            // Write multiple chunks for the same file
            let data = vec![0xDD; 512];
            store.write_chunk("/file1", 0, 0, &data).await.unwrap();
            store.write_chunk("/file1", 1, 0, &data).await.unwrap();
            store.write_chunk("/file1", 2, 0, &data).await.unwrap();

            // Write a chunk for a different file
            store.write_chunk("/file2", 0, 0, &data).await.unwrap();

            assert_eq!(store.chunk_count(), 4);

            // Delete all chunks for file 1
            let deleted = store.delete_file_chunks("/file1").await.unwrap();
            assert_eq!(deleted, 3);
            assert_eq!(store.chunk_count(), 1);

            // File 2 chunk should still exist
            assert!(store.has_chunk("/file2", 0));
        });
    }

    #[test]
    fn test_inmemory_storage_full() {
        run_test(async {
            let store = InMemoryChunkStore::with_capacity(2, CHUNK_SIZE);

            let data = vec![0xEE; 512];
            store.write_chunk("/file1", 0, 0, &data).await.unwrap();
            store.write_chunk("/file1", 1, 0, &data).await.unwrap();

            // Third chunk should fail
            let result = store.write_chunk("/file1", 2, 0, &data).await;
            assert!(result.is_err());
            assert!(matches!(
                result.unwrap_err(),
                ChunkStoreError::StorageFull(_)
            ));
        });
    }

    #[test]
    fn test_inmemory_invalid_offset() {
        run_test(async {
            let store = InMemoryChunkStore::new();

            let data = vec![0xFF; 512];
            let result = store
                .write_chunk("/file1", 0, CHUNK_SIZE as u64 + 100, &data)
                .await;
            assert!(result.is_err());
            assert!(matches!(
                result.unwrap_err(),
                ChunkStoreError::InvalidOffset(_)
            ));
        });
    }

    #[test]
    fn test_inmemory_clear() {
        run_test(async {
            let store = InMemoryChunkStore::new();

            let data = vec![0x11; 512];
            store.write_chunk("/file1", 0, 0, &data).await.unwrap();
            store.write_chunk("/file2", 0, 0, &data).await.unwrap();

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
            store.write_chunk("/file1", 0, 0, &data).await.unwrap();
            store.write_chunk("/file1", 1, 0, &data).await.unwrap();

            assert_eq!(store.storage_size(), 2 * CHUNK_SIZE);
        });
    }
}
