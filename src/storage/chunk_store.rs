use std::cell::RefCell;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::rc::Rc;

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
    #[instrument(level = "trace", name = "inmemory_write_chunk", skip(self, data), fields(path, chunk = chunk_index, len = data.len()))]
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
    #[instrument(level = "trace", name = "inmemory_read_chunk", skip(self), fields(path, chunk = chunk_index, len = length))]
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
    #[instrument(level = "trace", name = "inmemory_delete_chunk", skip(self), fields(path, chunk = chunk_index))]
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
    #[instrument(level = "trace", name = "inmemory_delete_file_chunks", skip(self), fields(path))]
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

/// Page-aligned buffer for O_DIRECT I/O
///
/// O_DIRECT requires buffers to be aligned to the filesystem's block size (typically 4096).
/// This struct uses `posix_memalign` to allocate properly aligned memory.
struct AlignedBuffer {
    ptr: *mut u8,
    len: usize,
}

impl AlignedBuffer {
    /// Allocate a new aligned buffer
    ///
    /// # Arguments
    /// * `size` - Buffer size in bytes
    /// * `alignment` - Required alignment (typically 4096 for O_DIRECT)
    fn new(size: usize, alignment: usize) -> Self {
        let mut ptr: *mut libc::c_void = std::ptr::null_mut();
        let ret = unsafe { libc::posix_memalign(&mut ptr, alignment, size) };
        assert_eq!(ret, 0, "posix_memalign failed with error code {}", ret);
        assert!(!ptr.is_null(), "posix_memalign returned null pointer");
        Self {
            ptr: ptr as *mut u8,
            len: size,
        }
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }

    fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        unsafe {
            libc::free(self.ptr as *mut libc::c_void);
        }
    }
}

/// Synchronous POSIX I/O chunk storage
///
/// Stores chunks using direct POSIX syscalls (open/pwrite/pread/close) with O_DIRECT.
/// Uses the same sharded directory structure as `IOUringChunkStore` for fair comparison.
///
/// This backend is intended for benchmarking the overhead of io_uring vs synchronous syscalls
/// while keeping all other factors (O_DIRECT, sharding, per-chunk open/close) identical.
pub struct PosixChunkStore {
    /// Base directory for chunk storage
    base_dir: PathBuf,

    /// Chunk size
    chunk_size: usize,

    /// Whether to use O_DIRECT
    use_direct_io: bool,

    /// Reusable aligned buffer (single-thread, so RefCell is fine)
    aligned_buffer: RefCell<AlignedBuffer>,
}

impl PosixChunkStore {
    /// Number of shard subdirectories (same as IOUringChunkStore)
    const SHARD_COUNT: u64 = 16;

    /// O_DIRECT alignment requirement
    const ALIGNMENT: usize = 4096;

    pub fn new<P: AsRef<Path>>(base_dir: P, use_direct_io: bool) -> ChunkStoreResult<Self> {
        let base_dir = base_dir.as_ref().to_path_buf();

        if !base_dir.exists() {
            std::fs::create_dir_all(&base_dir)?;
        }

        let chunk_size = CHUNK_SIZE;
        // Allocate aligned buffer rounded up to alignment boundary
        let buf_size = (chunk_size + Self::ALIGNMENT - 1) & !(Self::ALIGNMENT - 1);
        let aligned_buffer = RefCell::new(AlignedBuffer::new(buf_size, Self::ALIGNMENT));

        tracing::info!(
            "PosixChunkStore initialized (O_DIRECT={}, chunk_size={}, buf_size={})",
            use_direct_io,
            chunk_size,
            buf_size,
        );

        Ok(Self {
            base_dir,
            chunk_size,
            use_direct_io,
            aligned_buffer,
        })
    }

    fn hash_path(&self, path: &str) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        path.hash(&mut hasher);
        format!("{:x}", hasher.finish())
    }

    fn shard_dir(&self, chunk_index: u64) -> String {
        format!("{:x}", chunk_index % Self::SHARD_COUNT)
    }

    fn chunk_path(&self, file_path: &str, chunk_index: u64) -> PathBuf {
        let path_hash = self.hash_path(file_path);
        let shard = self.shard_dir(chunk_index);
        self.base_dir
            .join(path_hash)
            .join(shard)
            .join(format!("{}", chunk_index))
    }

    fn ensure_chunk_dir(&self, file_path: &str, chunk_index: u64) -> ChunkStoreResult<PathBuf> {
        let path_hash = self.hash_path(file_path);
        let shard = self.shard_dir(chunk_index);
        let dir = self.base_dir.join(path_hash).join(shard);
        if !dir.exists() {
            std::fs::create_dir_all(&dir)?;
        }
        Ok(dir)
    }

    /// Write a chunk using synchronous POSIX I/O
    #[async_backtrace::framed]
    #[instrument(level = "trace", name = "posix_write_chunk", skip(self, data), fields(path = file_path, chunk = chunk_index, offset, len = data.len()))]
    pub async fn write_chunk(
        &self,
        file_path: &str,
        chunk_index: u64,
        offset: u64,
        data: &[u8],
    ) -> ChunkStoreResult<usize> {
        let total_start = std::time::Instant::now();

        if offset >= self.chunk_size as u64 {
            return Err(ChunkStoreError::InvalidOffset(offset));
        }

        // Ensure shard directory exists
        self.ensure_chunk_dir(file_path, chunk_index)?;
        let chunk_file_path = self.chunk_path(file_path, chunk_index);
        let c_path = std::ffi::CString::new(
            chunk_file_path
                .to_str()
                .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid path"))?,
        )
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;

        // Open
        let open_start = std::time::Instant::now();
        let mut flags = libc::O_WRONLY | libc::O_CREAT;
        if self.use_direct_io {
            flags |= libc::O_DIRECT;
        }
        let fd = unsafe { libc::open(c_path.as_ptr(), flags, 0o644 as libc::mode_t) };
        if fd < 0 {
            return Err(ChunkStoreError::IoError(std::io::Error::last_os_error()));
        }
        let open_elapsed = open_start.elapsed();

        // Prepare aligned buffer and write
        let write_start = std::time::Instant::now();
        let bytes_to_write = data.len().min(self.chunk_size - offset as usize);

        let written = if self.use_direct_io {
            // O_DIRECT: copy data into aligned buffer, round write size up to 512-byte boundary
            let mut buf = self.aligned_buffer.borrow_mut();
            let aligned_len = (bytes_to_write + 511) & !511;
            let slice = buf.as_mut_slice();
            // Zero the tail padding so we don't write garbage
            slice[..aligned_len].fill(0);
            slice[..bytes_to_write].copy_from_slice(&data[..bytes_to_write]);
            let ret = unsafe {
                libc::pwrite(fd, buf.as_slice().as_ptr() as *const libc::c_void, aligned_len, offset as libc::off_t)
            };
            if ret < 0 {
                unsafe { libc::close(fd); }
                return Err(ChunkStoreError::IoError(std::io::Error::last_os_error()));
            }
            // Truncate file to actual size if we wrote padding beyond data
            let actual_end = offset as usize + bytes_to_write;
            if aligned_len > bytes_to_write {
                unsafe { libc::ftruncate(fd, actual_end as libc::off_t); }
            }
            bytes_to_write
        } else {
            let ret = unsafe {
                libc::pwrite(fd, data.as_ptr() as *const libc::c_void, bytes_to_write, offset as libc::off_t)
            };
            if ret < 0 {
                unsafe { libc::close(fd); }
                return Err(ChunkStoreError::IoError(std::io::Error::last_os_error()));
            }
            ret as usize
        };
        let write_elapsed = write_start.elapsed();

        // Close
        let close_start = std::time::Instant::now();
        unsafe { libc::close(fd); }
        let close_elapsed = close_start.elapsed();

        let total_elapsed = total_start.elapsed();

        tracing::debug!(
            op_type = "WRITE_CHUNK_POSIX",
            chunk_index = chunk_index,
            bytes_written = written,
            open_us = open_elapsed.as_micros() as u64,
            write_us = write_elapsed.as_micros() as u64,
            close_us = close_elapsed.as_micros() as u64,
            total_us = total_elapsed.as_micros() as u64,
            "CHUNK_IO_TIMING"
        );

        Ok(written)
    }

    /// Read a chunk using synchronous POSIX I/O
    #[async_backtrace::framed]
    #[instrument(level = "trace", name = "posix_read_chunk", skip(self), fields(path = file_path, chunk = chunk_index, offset, len = length))]
    pub async fn read_chunk(
        &self,
        file_path: &str,
        chunk_index: u64,
        offset: u64,
        length: u64,
    ) -> ChunkStoreResult<Vec<u8>> {
        let total_start = std::time::Instant::now();

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

        let c_path = std::ffi::CString::new(
            chunk_file_path
                .to_str()
                .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid path"))?,
        )
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;

        // Open
        let open_start = std::time::Instant::now();
        let mut flags = libc::O_RDONLY;
        if self.use_direct_io {
            flags |= libc::O_DIRECT;
        }
        let fd = unsafe { libc::open(c_path.as_ptr(), flags) };
        if fd < 0 {
            return Err(ChunkStoreError::IoError(std::io::Error::last_os_error()));
        }
        let open_elapsed = open_start.elapsed();

        // Read
        let read_start = std::time::Instant::now();
        let read_len = length as usize;

        let data = if self.use_direct_io {
            // O_DIRECT: read into aligned buffer, round up to 512-byte boundary
            let mut buf = self.aligned_buffer.borrow_mut();
            let aligned_len = (read_len + 511) & !511;
            let ret = unsafe {
                libc::pread(fd, buf.as_mut_slice().as_ptr() as *mut libc::c_void, aligned_len, offset as libc::off_t)
            };
            if ret < 0 {
                unsafe { libc::close(fd); }
                return Err(ChunkStoreError::IoError(std::io::Error::last_os_error()));
            }
            let bytes_read = (ret as usize).min(read_len);
            buf.as_slice()[..bytes_read].to_vec()
        } else {
            let mut data = vec![0u8; read_len];
            let ret = unsafe {
                libc::pread(fd, data.as_mut_ptr() as *mut libc::c_void, read_len, offset as libc::off_t)
            };
            if ret < 0 {
                unsafe { libc::close(fd); }
                return Err(ChunkStoreError::IoError(std::io::Error::last_os_error()));
            }
            data.truncate(ret as usize);
            data
        };
        let read_elapsed = read_start.elapsed();

        // Close
        let close_start = std::time::Instant::now();
        unsafe { libc::close(fd); }
        let close_elapsed = close_start.elapsed();

        let total_elapsed = total_start.elapsed();

        tracing::debug!(
            op_type = "READ_CHUNK_POSIX",
            chunk_index = chunk_index,
            bytes_read = data.len(),
            open_us = open_elapsed.as_micros() as u64,
            read_us = read_elapsed.as_micros() as u64,
            close_us = close_elapsed.as_micros() as u64,
            total_us = total_elapsed.as_micros() as u64,
            "CHUNK_IO_TIMING"
        );

        Ok(data)
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
        Ok(())
    }

    /// Delete all chunks for a file (handles sharded directory structure)
    #[async_backtrace::framed]
    pub async fn delete_file_chunks(&self, file_path: &str) -> ChunkStoreResult<usize> {
        let path_hash = self.hash_path(file_path);
        let dir = self.base_dir.join(path_hash);

        if !dir.exists() {
            return Ok(0);
        }

        let mut deleted_count = 0;

        for shard_entry in std::fs::read_dir(&dir)? {
            let shard_entry = shard_entry?;
            let shard_path = shard_entry.path();

            if shard_path.is_dir() {
                for chunk_entry in std::fs::read_dir(&shard_path)? {
                    let chunk_entry = chunk_entry?;
                    if chunk_entry.path().is_file() {
                        std::fs::remove_file(chunk_entry.path())?;
                        deleted_count += 1;
                    }
                }
                std::fs::remove_dir(&shard_path)?;
            } else if shard_path.is_file() {
                std::fs::remove_file(&shard_path)?;
                deleted_count += 1;
            }
        }

        std::fs::remove_dir(&dir)?;

        tracing::debug!("Deleted {} chunks for path {}", deleted_count, file_path);
        Ok(deleted_count)
    }

    /// Check if a chunk exists
    pub fn has_chunk(&self, file_path: &str, chunk_index: u64) -> bool {
        self.chunk_path(file_path, chunk_index).exists()
    }
}

#[async_trait::async_trait(?Send)]
impl ChunkStore for PosixChunkStore {
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
/// Directory structure: <base_dir>/<path_hash>/<shard>/<chunk_index>
/// where path_hash is a hash of the full file path, and shard = chunk_index % SHARD_COUNT.
///
/// This implementation uses IOUringBackend for all file operations, providing:
/// - Non-blocking async I/O
/// - Zero-copy data transfer (with registered buffers)
/// - High IOPS and low latency
///
/// ## O_DIRECT Strategy
/// - WRITE operations use O_DIRECT to bypass page cache, maximizing NVMe write buffer utilization
/// - READ operations do NOT use O_DIRECT, allowing the OS page cache to be utilized
/// - This hybrid approach provides high WRITE throughput while enabling cache hits on READ
pub struct IOUringChunkStore {
    /// Base directory for chunk storage
    base_dir: PathBuf,

    /// Chunk size
    chunk_size: usize,

    /// IO_uring backend for async file operations
    backend: Rc<IOUringBackend>,
}

impl IOUringChunkStore {
    /// Number of shard subdirectories for chunk storage
    /// Chunks are distributed across shards based on chunk_index % SHARD_COUNT
    /// Reduced from 256 to 16 to minimize directory overhead while still avoiding
    /// excessive files per directory
    const SHARD_COUNT: u64 = 16;

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

        tracing::info!(
            "IOUringChunkStore initialized (no file handle caching, O_DIRECT enabled for both READ and WRITE)"
        );

        Ok(Self {
            base_dir,
            chunk_size: CHUNK_SIZE,
            backend,
        })
    }

    /// Create a new IO_uring-based chunk store with custom capacity
    ///
    /// # Arguments
    /// * `base_dir` - Base directory for chunk storage
    /// * `backend` - IO_uring backend for file operations
    /// * `_max_open_files` - Deprecated: no longer used (kept for API compatibility)
    #[deprecated(note = "max_open_files is no longer used, use new() instead")]
    pub fn with_capacity<P: AsRef<Path>>(
        base_dir: P,
        backend: Rc<IOUringBackend>,
        _max_open_files: usize,
    ) -> ChunkStoreResult<Self> {
        Self::new(base_dir, backend)
    }

    /// Hash a file path to create a valid directory name
    fn hash_path(&self, path: &str) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        path.hash(&mut hasher);
        format!("{:x}", hasher.finish())
    }

    /// Get the shard directory for a chunk
    /// Chunks are distributed across SHARD_COUNT subdirectories to improve
    /// filesystem performance when handling large numbers of chunks
    fn shard_dir(&self, chunk_index: u64) -> String {
        format!("{:x}", chunk_index % Self::SHARD_COUNT)
    }

    /// Get the path for a chunk file
    /// Directory structure: base_dir/path_hash/shard/chunk_index
    /// where shard = chunk_index % 16 (hex formatted)
    fn chunk_path(&self, file_path: &str, chunk_index: u64) -> PathBuf {
        let path_hash = self.hash_path(file_path);
        let shard = self.shard_dir(chunk_index);
        self.base_dir
            .join(path_hash)
            .join(shard)
            .join(format!("{}", chunk_index))
    }

    /// Ensure the directory for a chunk exists (including shard subdirectory)
    fn ensure_chunk_dir(&self, file_path: &str, chunk_index: u64) -> ChunkStoreResult<PathBuf> {
        let path_hash = self.hash_path(file_path);
        let shard = self.shard_dir(chunk_index);
        let dir = self.base_dir.join(path_hash).join(shard);
        if !dir.exists() {
            std::fs::create_dir_all(&dir)?;
        }
        Ok(dir)
    }

    /// Open a chunk file for reading or writing
    ///
    /// This method opens files without caching - each operation gets a fresh file handle.
    /// This allows different O_DIRECT settings for READ vs WRITE:
    /// - WRITE: Uses O_DIRECT to bypass page cache, maximizing NVMe write buffer utilization
    /// - READ: Does NOT use O_DIRECT, allowing OS page cache to be utilized for repeated reads
    ///
    /// File handles are closed immediately after each operation.
    async fn open_chunk_file(
        &self,
        file_path: &str,
        chunk_index: u64,
        write: bool,
    ) -> ChunkStoreResult<FileHandle> {
        let open_start = std::time::Instant::now();

        // Ensure shard directory exists (includes path_hash and shard subdirectory)
        if write {
            self.ensure_chunk_dir(file_path, chunk_index)?;
        }
        let ensure_dir_elapsed = open_start.elapsed();

        let chunk_file_path = self.chunk_path(file_path, chunk_index);

        // O_DIRECT is used for both READ and WRITE operations.
        // Past benchmarks showed no performance difference between buffered and direct I/O,
        // so O_DIRECT is used consistently to bypass page cache and reduce memory pressure.
        let flags = OpenFlags {
            read: !write,  // READ-only for read operations
            write: write,  // WRITE-only for write operations
            create: write, // Only create on write operations
            truncate: false,
            append: false,
            direct: true,  // O_DIRECT for both READ and WRITE
        };

        let open_syscall_start = std::time::Instant::now();
        let handle = self.backend.open(&chunk_file_path, flags).await?;
        let open_syscall_elapsed = open_syscall_start.elapsed();
        let total_elapsed = open_start.elapsed();

        // Log file open timing for performance analysis
        tracing::debug!(
            op_type = if write { "WRITE" } else { "READ" },
            chunk_index = chunk_index,
            ensure_dir_us = ensure_dir_elapsed.as_micros() as u64,
            open_syscall_us = open_syscall_elapsed.as_micros() as u64,
            total_open_us = total_elapsed.as_micros() as u64,
            "CHUNK_FILE_OPEN"
        );

        tracing::trace!(
            "Opened file: {:?} with fd={:?} (write={}, direct={})",
            chunk_file_path,
            handle,
            write,
            write
        );

        Ok(handle)
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

        // Open the chunk file with O_DIRECT for write
        let handle = self.open_chunk_file(file_path, chunk_index, true).await?;

        // Write data directly at the specified offset
        // io_uring's pwrite handles sparse files efficiently - no need for read-modify-write
        let bytes_to_write = data.len().min(self.chunk_size - offset as usize);
        let result = self
            .backend
            .write(handle, offset, &data[..bytes_to_write])
            .await;

        // Close the file handle immediately (no caching)
        if let Err(e) = self.backend.close(handle).await {
            tracing::warn!("Failed to close chunk file after write: {:?}", e);
        }

        result?;

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

        // Open the chunk file without O_DIRECT (uses page cache for better read performance)
        // Note: Will fail with StorageError if file doesn't exist
        let handle = self.open_chunk_file(file_path, chunk_index, false).await?;

        // Read data
        let mut buffer = vec![0u8; length as usize];
        let result = self.backend.read(handle, offset, &mut buffer).await;

        // Close the file handle immediately (no caching)
        if let Err(e) = self.backend.close(handle).await {
            tracing::warn!("Failed to close chunk file after read: {:?}", e);
        }

        let bytes_read = result?;
        buffer.truncate(bytes_read);

        tracing::trace!("Read {} bytes", bytes_read);

        Ok(buffer)
    }

    /// Fsync a chunk file to ensure data is persisted to disk
    ///
    /// This opens the chunk file, calls fsync, and closes it.
    /// Used to ensure write durability before returning from fsync operations.
    #[async_backtrace::framed]
    #[instrument(level = "trace", name = "chunk_fsync", skip(self), fields(path = file_path, chunk = chunk_index))]
    pub async fn fsync_chunk(&self, file_path: &str, chunk_index: u64) -> ChunkStoreResult<()> {
        let chunk_file_path = self.chunk_path(file_path, chunk_index);

        if !chunk_file_path.exists() {
            // File doesn't exist, nothing to sync
            return Ok(());
        }

        // Open the chunk file for reading (just need to get a handle for fsync)
        let flags = OpenFlags {
            read: true,
            write: false,
            create: false,
            truncate: false,
            append: false,
            direct: false,
        };

        let handle = self.backend.open(&chunk_file_path, flags).await?;

        // Call fsync on the file
        let result = self.backend.fsync(handle).await;

        // Close the file handle
        if let Err(e) = self.backend.close(handle).await {
            tracing::warn!("Failed to close chunk file after fsync: {:?}", e);
        }

        result?;

        tracing::trace!(
            "Fsynced chunk (path={}, chunk_index={})",
            file_path,
            chunk_index
        );

        Ok(())
    }

    /// Delete a chunk file
    #[async_backtrace::framed]
    #[instrument(level = "trace", name = "iouring_delete_chunk", skip(self), fields(path = file_path, chunk = chunk_index))]
    pub async fn delete_chunk(&self, file_path: &str, chunk_index: u64) -> ChunkStoreResult<()> {
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
    /// Handles the sharded directory structure: base_dir/path_hash/shard/chunk_index
    #[async_backtrace::framed]
    #[instrument(level = "trace", name = "iouring_delete_file_chunks", skip(self), fields(path = file_path))]
    pub async fn delete_file_chunks(&self, file_path: &str) -> ChunkStoreResult<usize> {
        let path_hash = self.hash_path(file_path);
        let dir = self.base_dir.join(path_hash);

        if !dir.exists() {
            return Ok(0);
        }

        let mut deleted_count = 0;

        // Delete all chunk files in all shard directories
        for shard_entry in std::fs::read_dir(&dir)? {
            let shard_entry = shard_entry?;
            let shard_path = shard_entry.path();

            if shard_path.is_dir() {
                // Delete all chunk files in this shard directory
                for chunk_entry in std::fs::read_dir(&shard_path)? {
                    let chunk_entry = chunk_entry?;
                    if chunk_entry.path().is_file() {
                        self.backend.unlink(&chunk_entry.path()).await?;
                        deleted_count += 1;
                    }
                }
                // Remove the shard directory
                self.backend.rmdir(&shard_path).await?;
            } else if shard_path.is_file() {
                // Handle legacy non-sharded files (backward compatibility)
                self.backend.unlink(&shard_path).await?;
                deleted_count += 1;
            }
        }

        // Remove the path_hash directory
        self.backend.rmdir(&dir).await?;

        tracing::debug!("Deleted {} chunks for path {}", deleted_count, file_path);

        Ok(deleted_count)
    }

    /// Check if a chunk exists
    pub fn has_chunk(&self, file_path: &str, chunk_index: u64) -> bool {
        self.chunk_path(file_path, chunk_index).exists()
    }

    /// Close all open file handles
    ///
    /// With the removal of file handle caching, this method is now a no-op.
    /// File handles are closed immediately after each read/write operation.
    /// Kept for API compatibility.
    #[async_backtrace::framed]
    #[instrument(level = "trace", name = "iouring_close_all", skip(self))]
    pub async fn close_all(&self) -> ChunkStoreResult<()> {
        // No-op: file handles are closed immediately after each operation
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
    #[instrument(level = "trace", name = "iouring_write_chunk_fixed", skip(self, fixed_buffer), fields(path = file_path, chunk = chunk_index, offset, len = data_len))]
    pub async fn write_chunk_fixed(
        &self,
        file_path: &str,
        chunk_index: u64,
        offset: u64,
        fixed_buffer: pluvio_uring::allocator::FixedBuffer,
        data_len: usize,
    ) -> ChunkStoreResult<usize> {
        let total_start = std::time::Instant::now();

        if offset >= self.chunk_size as u64 {
            return Err(ChunkStoreError::InvalidOffset(offset));
        }

        // Open the chunk file with O_DIRECT for write
        let open_start = std::time::Instant::now();
        let handle = self.open_chunk_file(file_path, chunk_index, true).await?;
        let open_elapsed = open_start.elapsed();

        // Write data directly from registered buffer using DMA (zero-copy)
        let bytes_to_write = data_len.min(self.chunk_size - offset as usize);
        let write_start = std::time::Instant::now();
        let result = self
            .backend
            .write_fixed_direct(handle, offset, fixed_buffer, bytes_to_write)
            .await;
        let write_elapsed = write_start.elapsed();

        // Close the file handle immediately (no caching)
        let close_start = std::time::Instant::now();
        if let Err(e) = self.backend.close(handle).await {
            tracing::warn!("Failed to close chunk file after write_fixed: {:?}", e);
        }
        let close_elapsed = close_start.elapsed();

        let bytes_written = result?;
        let total_elapsed = total_start.elapsed();

        // Log detailed timing breakdown for WRITE performance analysis
        tracing::debug!(
            op_type = "WRITE_CHUNK_FIXED",
            chunk_index = chunk_index,
            bytes_written = bytes_written,
            open_us = open_elapsed.as_micros() as u64,
            write_us = write_elapsed.as_micros() as u64,
            close_us = close_elapsed.as_micros() as u64,
            total_us = total_elapsed.as_micros() as u64,
            "CHUNK_IO_TIMING"
        );

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
    #[instrument(level = "trace", name = "iouring_read_chunk_fixed", skip(self, fixed_buffer), fields(path = file_path, chunk = chunk_index, offset))]
    pub async fn read_chunk_fixed(
        &self,
        file_path: &str,
        chunk_index: u64,
        offset: u64,
        fixed_buffer: pluvio_uring::allocator::FixedBuffer,
    ) -> ChunkStoreResult<(usize, pluvio_uring::allocator::FixedBuffer)> {
        let total_start = std::time::Instant::now();

        if offset >= self.chunk_size as u64 {
            return Err(ChunkStoreError::InvalidOffset(offset));
        }

        // Open the chunk file without O_DIRECT (uses page cache for better read performance)
        // Note: Will fail with StorageError if file doesn't exist
        let open_start = std::time::Instant::now();
        let handle = self.open_chunk_file(file_path, chunk_index, false).await?;
        let open_elapsed = open_start.elapsed();

        // Read data directly into registered buffer using DMA (zero-copy)
        let read_start = std::time::Instant::now();
        let result = self
            .backend
            .read_fixed_direct(handle, offset, fixed_buffer)
            .await;
        let read_elapsed = read_start.elapsed();

        // Close the file handle immediately (no caching)
        let close_start = std::time::Instant::now();
        if let Err(e) = self.backend.close(handle).await {
            tracing::warn!("Failed to close chunk file after read_fixed: {:?}", e);
        }
        let close_elapsed = close_start.elapsed();

        let (bytes_read, fixed_buffer) = result?;
        let total_elapsed = total_start.elapsed();

        // Log detailed timing breakdown for READ performance analysis
        tracing::debug!(
            op_type = "READ_CHUNK_FIXED",
            chunk_index = chunk_index,
            bytes_read = bytes_read,
            open_us = open_elapsed.as_micros() as u64,
            read_us = read_elapsed.as_micros() as u64,
            close_us = close_elapsed.as_micros() as u64,
            total_us = total_elapsed.as_micros() as u64,
            "CHUNK_IO_TIMING"
        );

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
