use std::cell::RefCell;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use tracing::instrument;

use super::inode::{BenchfsChunk0Extension, EXT_OFFSET, MSIZE, PosixMetadataHeader};
use super::{FileHandle, IOUringBackend, OpenFlags, StorageBackend};
use crate::metadata::CHUNK_SIZE;
use zerocopy::IntoBytes;

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

    /// Reflection escape hatch for callers that need backend-specific
    /// methods (e.g. CHFS POSIX-style chunk-0 metadata helpers on
    /// `IOUringChunkStore`). Default implementations are not provided.
    fn as_any(&self) -> &dyn std::any::Any;
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
    #[instrument(
        level = "trace",
        name = "inmemory_delete_file_chunks",
        skip(self),
        fields(path)
    )]
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

    fn as_any(&self) -> &dyn std::any::Any {
        self
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

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Dummy (no-op) chunk storage for RPC overhead measurement
///
/// Discards all writes and returns zero-filled buffers on read.
/// No disk I/O is performed, allowing pure RPC layer throughput measurement.
pub struct DummyChunkStore {
    _chunk_size: usize,
}

impl DummyChunkStore {
    pub fn new() -> Self {
        let chunk_size = CHUNK_SIZE;
        tracing::info!(
            "DummyChunkStore initialized (no-op, chunk_size={})",
            chunk_size,
        );
        Self {
            _chunk_size: chunk_size,
        }
    }

    #[async_backtrace::framed]
    pub async fn write_chunk(
        &self,
        _file_path: &str,
        chunk_index: u64,
        _offset: u64,
        data: &[u8],
    ) -> ChunkStoreResult<usize> {
        let written = data.len();

        tracing::debug!(
            op_type = "WRITE_CHUNK_DUMMY",
            chunk_index = chunk_index,
            bytes_written = written,
            total_us = 0u64,
            "CHUNK_IO_TIMING"
        );

        Ok(written)
    }

    #[async_backtrace::framed]
    pub async fn read_chunk(
        &self,
        _file_path: &str,
        chunk_index: u64,
        _offset: u64,
        length: u64,
    ) -> ChunkStoreResult<Vec<u8>> {
        let data = vec![0u8; length as usize];

        tracing::debug!(
            op_type = "READ_CHUNK_DUMMY",
            chunk_index = chunk_index,
            bytes_read = data.len(),
            total_us = 0u64,
            "CHUNK_IO_TIMING"
        );

        Ok(data)
    }

    pub async fn delete_chunk(&self, _file_path: &str, _chunk_index: u64) -> ChunkStoreResult<()> {
        Ok(())
    }

    pub async fn delete_file_chunks(&self, _file_path: &str) -> ChunkStoreResult<usize> {
        Ok(0)
    }

    pub fn has_chunk(&self, _file_path: &str, _chunk_index: u64) -> bool {
        true
    }
}

#[async_trait::async_trait(?Send)]
impl ChunkStore for DummyChunkStore {
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

    fn as_any(&self) -> &dyn std::any::Any {
        self
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
        let c_path = std::ffi::CString::new(chunk_file_path.to_str().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid path")
        })?)
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
                libc::pwrite(
                    fd,
                    buf.as_slice().as_ptr() as *const libc::c_void,
                    aligned_len,
                    offset as libc::off_t,
                )
            };
            if ret < 0 {
                unsafe {
                    libc::close(fd);
                }
                return Err(ChunkStoreError::IoError(std::io::Error::last_os_error()));
            }
            // Truncate file to actual size if we wrote padding beyond data
            let actual_end = offset as usize + bytes_to_write;
            if aligned_len > bytes_to_write {
                unsafe {
                    libc::ftruncate(fd, actual_end as libc::off_t);
                }
            }
            bytes_to_write
        } else {
            let ret = unsafe {
                libc::pwrite(
                    fd,
                    data.as_ptr() as *const libc::c_void,
                    bytes_to_write,
                    offset as libc::off_t,
                )
            };
            if ret < 0 {
                unsafe {
                    libc::close(fd);
                }
                return Err(ChunkStoreError::IoError(std::io::Error::last_os_error()));
            }
            ret as usize
        };
        let write_elapsed = write_start.elapsed();

        // Close
        let close_start = std::time::Instant::now();
        unsafe {
            libc::close(fd);
        }
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

        let c_path = std::ffi::CString::new(chunk_file_path.to_str().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid path")
        })?)
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
                libc::pread(
                    fd,
                    buf.as_mut_slice().as_ptr() as *mut libc::c_void,
                    aligned_len,
                    offset as libc::off_t,
                )
            };
            if ret < 0 {
                unsafe {
                    libc::close(fd);
                }
                return Err(ChunkStoreError::IoError(std::io::Error::last_os_error()));
            }
            let bytes_read = (ret as usize).min(read_len);
            buf.as_slice()[..bytes_read].to_vec()
        } else {
            let mut data = vec![0u8; read_len];
            let ret = unsafe {
                libc::pread(
                    fd,
                    data.as_mut_ptr() as *mut libc::c_void,
                    read_len,
                    offset as libc::off_t,
                )
            };
            if ret < 0 {
                unsafe {
                    libc::close(fd);
                }
                return Err(ChunkStoreError::IoError(std::io::Error::last_os_error()));
            }
            data.truncate(ret as usize);
            data
        };
        let read_elapsed = read_start.elapsed();

        // Close
        let close_start = std::time::Instant::now();
        unsafe {
            libc::close(fd);
        }
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

    /// Read a chunk directly into a provided buffer, avoiding intermediate Vec allocation.
    ///
    /// Returns the number of bytes read.
    #[async_backtrace::framed]
    #[instrument(level = "trace", name = "posix_read_chunk_into", skip(self, buf), fields(path = file_path, chunk = chunk_index, offset, len = length))]
    pub async fn read_chunk_into(
        &self,
        file_path: &str,
        chunk_index: u64,
        offset: u64,
        length: u64,
        buf: &mut [u8],
    ) -> ChunkStoreResult<usize> {
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

        let c_path = std::ffi::CString::new(chunk_file_path.to_str().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid path")
        })?)
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
        let read_len = (length as usize).min(buf.len());

        let bytes_read = if self.use_direct_io {
            // O_DIRECT: read into internal aligned buffer, then copy to destination
            let mut aligned_buf = self.aligned_buffer.borrow_mut();
            let aligned_len = (read_len + 511) & !511;
            let ret = unsafe {
                libc::pread(
                    fd,
                    aligned_buf.as_mut_slice().as_ptr() as *mut libc::c_void,
                    aligned_len,
                    offset as libc::off_t,
                )
            };
            if ret < 0 {
                unsafe {
                    libc::close(fd);
                }
                return Err(ChunkStoreError::IoError(std::io::Error::last_os_error()));
            }
            let actual = (ret as usize).min(read_len);
            buf[..actual].copy_from_slice(&aligned_buf.as_slice()[..actual]);
            actual
        } else {
            // Non-O_DIRECT: read directly into destination buffer
            let ret = unsafe {
                libc::pread(
                    fd,
                    buf.as_mut_ptr() as *mut libc::c_void,
                    read_len,
                    offset as libc::off_t,
                )
            };
            if ret < 0 {
                unsafe {
                    libc::close(fd);
                }
                return Err(ChunkStoreError::IoError(std::io::Error::last_os_error()));
            }
            ret as usize
        };
        let read_elapsed = read_start.elapsed();

        // Close
        let close_start = std::time::Instant::now();
        unsafe {
            libc::close(fd);
        }
        let close_elapsed = close_start.elapsed();

        let total_elapsed = total_start.elapsed();

        tracing::debug!(
            op_type = "READ_CHUNK_POSIX_INTO",
            chunk_index = chunk_index,
            bytes_read = bytes_read,
            open_us = open_elapsed.as_micros() as u64,
            read_us = read_elapsed.as_micros() as u64,
            close_us = close_elapsed.as_micros() as u64,
            total_us = total_elapsed.as_micros() as u64,
            "CHUNK_IO_TIMING"
        );

        Ok(bytes_read)
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

    fn as_any(&self) -> &dyn std::any::Any {
        self
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

    /// Set of chunks whose CHFS-style header has been written in this
    /// process lifetime. Used to skip the existence stat on subsequent
    /// writes to the same chunk. Best-effort: a missing entry just
    /// triggers a redundant stat, never a missed header write.
    header_initialized: RefCell<std::collections::HashSet<ChunkKey>>,

    /// Cache of (path_hash, shard) pairs whose directory we've already
    /// confirmed exists. Lets `ensure_chunk_dir` skip the sync
    /// `dir.exists()` stat on every subsequent chunk in the same
    /// shard. With ior-easy FPP and 2048 chunks per rank file, this
    /// turns ~2048 sync syscalls per file into 16 (shard count).
    known_shard_dirs: RefCell<std::collections::HashSet<(String, u64)>>,

    /// FIFO fd cache: each (file_path, chunk_index, use_direct) → opened
    /// FileHandle. Holding fds open across phase transitions lets later
    /// writes/reads skip the per-chunk `OpenAt` SQE. 17070 profile showed
    /// open_us mean 49µs is non-trivial; 20183 showed 38% of ior-hard
    /// per-RPC server time is open(). The `use_direct` axis in the key
    /// keeps O_DIRECT-opened entries (4 KiB-aligned IO) and non-direct
    /// entries (unaligned ior-hard 47 KiB) in separate slots, so a
    /// follow-up unaligned op can't EINVAL on an O_DIRECT fd cached from
    /// the prior aligned phase (job 17075 hazard).
    ///
    /// Eviction is FIFO via `fd_cache_order` — on full cache, the oldest
    /// inserted entry is closed. We skip MRU promotion on hit (O(N) on
    /// VecDeque) since ior-hard reuses chunks heavily within its phase
    /// (90 writes per chunk), so insertion order ≈ LRU order in practice.
    /// Capped via `BENCHFS_CHUNK_FD_CACHE_SIZE` (default 0 = disabled).
    fd_cache: RefCell<std::collections::HashMap<(String, u64, bool), FileHandle>>,
    fd_cache_order: RefCell<std::collections::VecDeque<(String, u64, bool)>>,
    fd_cache_cap: usize,

    /// mmap fast-path cache for non-O_DIRECT chunks (i.e. ior-hard's
    /// unaligned 47 KiB writes). Each entry is `(path, chunk_index) →
    /// (raw_fd, mmap_ptr, mmap_len)`. Writes that hit this cache skip
    /// io_uring entirely and `memcpy` directly into the MAP_SHARED
    /// region — page cache absorbs the write at ~2µs vs io_uring's
    /// ~100µs random-write floor. The fd is owned by this cache (NOT
    /// shared with `fd_cache`) so eviction can safely munmap+close.
    /// Enabled only when `BENCHFS_CHUNK_MMAP_WRITE=1`; disabled otherwise.
    mmap_cache: RefCell<std::collections::HashMap<(String, u64), MmapEntry>>,
    mmap_cache_order: RefCell<std::collections::VecDeque<(String, u64)>>,
    mmap_enabled: bool,

    /// Tracks `(file_path, chunk_index)` pairs that have had their extent
    /// pre-allocated via `fallocate(KEEP_SIZE)` in the unified-layout file.
    /// Per-chunk layout already fallocates inside `get_or_open_chunk_file`,
    /// but unified collapses N chunks into one file and we must do the
    /// equivalent lazily — once per (file, chunk_id) on its first write —
    /// or ext4 extent-tree growth dominates every first-write-to-offset.
    /// A blanket fallocate at file-open time over-allocates by ~80x in
    /// FPP mode (40-server unified holds N rank files, each only
    /// receiving 1/40 of that rank's data — `data_per_rank / 40`).
    unified_chunk_prealloc: RefCell<std::collections::HashSet<(String, u64)>>,
}

#[derive(Copy, Clone)]
struct MmapEntry {
    fd: i32,
    ptr: *mut u8,
    len: usize,
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

        // Default off: caching `O_DIRECT`-opened fds across phases produced
        // EINVAL on follow-up non-aligned writes in mdtest (job 17075),
        // and EMFILE without an in-wrapper ulimit bump (job 17074). Opt
        // in via `[iouring] chunk_fd_cache_size = N` in benchfs.toml.
        let fd_cache_cap = crate::runtime_config::RuntimeConfig::global()
            .iouring
            .chunk_fd_cache_size;

        tracing::info!(
            "IOUringChunkStore initialized (fd_cache_cap={}, O_DIRECT enabled for both READ and WRITE)",
            fd_cache_cap
        );

        Ok(Self {
            base_dir,
            chunk_size: CHUNK_SIZE,
            backend,
            header_initialized: RefCell::new(std::collections::HashSet::new()),
            known_shard_dirs: RefCell::new(std::collections::HashSet::new()),
            fd_cache: RefCell::new(std::collections::HashMap::with_capacity(
                fd_cache_cap.min(1024),
            )),
            fd_cache_order: RefCell::new(std::collections::VecDeque::with_capacity(
                fd_cache_cap.min(1024),
            )),
            fd_cache_cap,
            mmap_cache: RefCell::new(std::collections::HashMap::with_capacity(
                fd_cache_cap.min(1024),
            )),
            mmap_cache_order: RefCell::new(std::collections::VecDeque::with_capacity(
                fd_cache_cap.min(1024),
            )),
            unified_chunk_prealloc: RefCell::new(std::collections::HashSet::new()),
            mmap_enabled: crate::runtime_config::RuntimeConfig::global()
                .storage
                .chunk_mmap_write,
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

    /// Number of sharded sub-files per logical file when unified layout is
    /// enabled. Splitting one big sparse file into N inodes lets the kernel's
    /// per-inode page-cache lock be acquired in parallel for buffered writes
    /// — the lock contention is the real ceiling for ior-hard's 47 KiB
    /// unaligned writes (job 20250 profile showed each io_uring write
    /// pays ~128 µs regardless of in-flight depth, consistent with serialized
    /// i_lock acquisition).
    fn unified_shard_count(&self) -> u64 {
        crate::runtime_config::RuntimeConfig::global()
            .storage
            .unified_shards
            .max(1) as u64
    }

    /// Unified path-level data file. With `BENCHFS_UNIFIED_SHARDS=1` (default)
    /// all chunks for a logical file land in one sparse file; with shards > 1
    /// chunks are spread across N sub-files keyed by `chunk_index % N`. The
    /// shard index becomes part of the filename so chunks within a shard sit
    /// at offsets `(chunk_index / N) * chunk_size`.
    fn unified_data_path(&self, file_path: &str, chunk_index: u64) -> PathBuf {
        let path_hash = self.hash_path(file_path);
        let shards = self.unified_shard_count();
        if shards <= 1 {
            self.base_dir.join(path_hash).join("data")
        } else {
            let shard = chunk_index % shards;
            self.base_dir
                .join(path_hash)
                .join(format!("data.{:02x}", shard))
        }
    }

    /// File-relative byte offset for a `(chunk_index, offset_within_chunk)`
    /// pair under the current unified-shard scheme.
    fn unified_file_offset(&self, chunk_index: u64, offset_within_chunk: u64) -> u64 {
        let shards = self.unified_shard_count();
        let within_shard_idx = if shards <= 1 {
            chunk_index
        } else {
            chunk_index / shards
        };
        within_shard_idx * self.chunk_size as u64 + offset_within_chunk
    }

    fn unified_layout_enabled(&self) -> bool {
        crate::runtime_config::RuntimeConfig::global()
            .storage
            .chunk_layout
            .eq_ignore_ascii_case("unified")
    }

    /// Ensure the directory for a chunk exists (including shard subdirectory)
    fn ensure_chunk_dir(&self, file_path: &str, chunk_index: u64) -> ChunkStoreResult<PathBuf> {
        let path_hash = self.hash_path(file_path);
        let shard = chunk_index % Self::SHARD_COUNT;
        // Fast path: we've already created/observed this shard dir.
        // Job 17063 timing showed each chunk's open_chunk_file → ensure_chunk_dir
        // doing a sync `dir.exists()` stat per chunk; with 2048 chunks per rank
        // file and only 16 shards, ~99% of those stats are redundant.
        let cache_key = (path_hash.clone(), shard);
        if self.known_shard_dirs.borrow().contains(&cache_key) {
            return Ok(self
                .base_dir
                .join(&cache_key.0)
                .join(self.shard_dir(chunk_index)));
        }
        let shard_str = self.shard_dir(chunk_index);
        let dir = self.base_dir.join(&path_hash).join(&shard_str);
        if !dir.exists() {
            std::fs::create_dir_all(&dir)?;
        }
        self.known_shard_dirs.borrow_mut().insert(cache_key);
        Ok(dir)
    }

    /// Block size that O_DIRECT requires the request length / offset to be a
    /// multiple of. 4096 covers every NVMe device used on Sirius (512-byte and
    /// 4K logical block sizes alike). When a request's len/offset is not a
    /// multiple of this value, the file is opened buffered to avoid `EINVAL`.
    ///
    /// Note: chunk data starts at file offset `MSIZE` (also 4096), so a
    /// chunk-relative aligned offset/len stays aligned at the file level.
    const DIRECT_IO_ALIGN: u64 = 4096;

    /// Ensure the CHFS POSIX-style metadata header (chunk_size / msize / flags)
    /// has been written at offset 0 of this chunk's file. This is a no-op if
    /// the header was already written in this process or persisted to disk
    /// (size >= MSIZE). Mirrors CHFS's `set_metadata` / `fs_open` create path
    /// (chfs/chfsd/fs_posix.c).
    async fn ensure_chunk_header(&self, file_path: &str, chunk_index: u64) -> ChunkStoreResult<()> {
        let key = ChunkKey::new(file_path.to_string(), chunk_index);
        if self.header_initialized.borrow().contains(&key) {
            return Ok(());
        }

        let chunk_file_path = self.chunk_path(file_path, chunk_index);

        // Persisted-on-disk fast path: a previous process (or run) may have
        // already initialized the header.
        let already_persisted = matches!(
            std::fs::metadata(&chunk_file_path),
            Ok(m) if m.len() >= MSIZE,
        );
        if already_persisted {
            self.header_initialized.borrow_mut().insert(key);
            return Ok(());
        }

        self.ensure_chunk_dir(file_path, chunk_index)?;

        let header = PosixMetadataHeader::new(self.chunk_size as u64, 0);
        let header_buf = header.as_msize_buffer();

        // Buffered open + a single MSIZE-byte write at offset 0. We don't
        // use O_DIRECT here because the buffer (Vec<u8>) is not guaranteed
        // page-aligned; the data path (which uses registered buffers from
        // FixedBufferAllocator) keeps O_DIRECT.
        let flags = OpenFlags {
            read: false,
            write: true,
            create: true,
            truncate: false,
            append: false,
            direct: false,
        };
        let handle = self.backend.open(&chunk_file_path, flags).await?;
        let result = self.backend.write(handle, 0, &header_buf).await;
        if let Err(e) = self.backend.close(handle).await {
            tracing::warn!("Failed to close chunk file after header init: {:?}", e);
        }
        result?;

        self.header_initialized.borrow_mut().insert(key);

        tracing::trace!(
            "Wrote POSIX metadata header for chunk (path={}, chunk_index={})",
            file_path,
            chunk_index
        );

        Ok(())
    }

    /// Stat a chunk: returns (data_size_in_bytes, decoded header) where
    /// `data_size_in_bytes = sb.st_size - MSIZE` (CHFS semantics).
    /// Returns `ChunkNotFound` if the chunk file does not exist or is
    /// shorter than MSIZE (i.e., header not yet written).
    pub async fn stat_chunk(
        &self,
        file_path: &str,
        chunk_index: u64,
    ) -> ChunkStoreResult<(u64, PosixMetadataHeader)> {
        let chunk_file_path = self.chunk_path(file_path, chunk_index);

        let st = match self.backend.stat(&chunk_file_path).await {
            Ok(s) => s,
            Err(super::StorageError::NotFound(_)) => {
                return Err(ChunkStoreError::ChunkNotFound {
                    path: file_path.to_string(),
                    chunk_index,
                });
            }
            Err(e) => return Err(ChunkStoreError::StorageError(e)),
        };

        if st.size < MSIZE {
            return Err(ChunkStoreError::ChunkNotFound {
                path: file_path.to_string(),
                chunk_index,
            });
        }

        let flags = OpenFlags {
            read: true,
            write: false,
            create: false,
            truncate: false,
            append: false,
            direct: false,
        };
        let handle = self.backend.open(&chunk_file_path, flags).await?;
        let mut buf = vec![0u8; std::mem::size_of::<PosixMetadataHeader>()];
        let read_res = self.backend.read(handle, 0, &mut buf).await;
        if let Err(e) = self.backend.close(handle).await {
            tracing::warn!("Failed to close chunk file after stat: {:?}", e);
        }
        let n = read_res?;
        if n != buf.len() {
            return Err(ChunkStoreError::IoError(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "short read of chunk header: got {} of {} bytes",
                    n,
                    buf.len()
                ),
            )));
        }

        let header = PosixMetadataHeader::from_bytes_validated(&buf).ok_or_else(|| {
            ChunkStoreError::IoError(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "chunk header msize mismatch (expected MSIZE)",
            ))
        })?;

        let data_size = st.size - MSIZE;
        Ok((data_size, header))
    }

    /// Read the BenchFS-specific chunk-0 extension (logical file size etc.).
    /// Only meaningful for `chunk_index == 0`; callers must not use this for
    /// non-zero chunks. Returns `None` if the chunk file does not exist or
    /// is shorter than the header + extension region.
    pub async fn read_chunk0_extension(
        &self,
        file_path: &str,
    ) -> ChunkStoreResult<Option<BenchfsChunk0Extension>> {
        let chunk_file_path = self.chunk_path(file_path, 0);
        if !chunk_file_path.exists() {
            return Ok(None);
        }

        let flags = OpenFlags {
            read: true,
            write: false,
            create: false,
            truncate: false,
            append: false,
            direct: false,
        };
        let handle = self.backend.open(&chunk_file_path, flags).await?;
        let mut buf = vec![0u8; std::mem::size_of::<BenchfsChunk0Extension>()];
        let read_res = self.backend.read(handle, EXT_OFFSET, &mut buf).await;
        if let Err(e) = self.backend.close(handle).await {
            tracing::warn!("Failed to close chunk file after ext read: {:?}", e);
        }
        let n = read_res?;
        if n != buf.len() {
            return Ok(None);
        }
        Ok(BenchfsChunk0Extension::from_bytes_validated(&buf))
    }

    /// Write the BenchFS-specific chunk-0 extension. Ensures the chunk-0
    /// header exists first so the file is at least MSIZE bytes long. Used
    /// by metadata RPC handlers to persist `logical_size` updates.
    pub async fn write_chunk0_extension(
        &self,
        file_path: &str,
        ext: BenchfsChunk0Extension,
    ) -> ChunkStoreResult<()> {
        // Make sure the CHFS struct metadata header exists (allocates the
        // MSIZE-byte reserved region) before we touch its bytes 16..32.
        self.ensure_chunk_header(file_path, 0).await?;

        let chunk_file_path = self.chunk_path(file_path, 0);
        let flags = OpenFlags {
            read: false,
            write: true,
            create: false,
            truncate: false,
            append: false,
            direct: false,
        };
        let handle = self.backend.open(&chunk_file_path, flags).await?;
        let result = self.backend.write(handle, EXT_OFFSET, ext.as_bytes()).await;
        if let Err(e) = self.backend.close(handle).await {
            tracing::warn!("Failed to close chunk file after ext write: {:?}", e);
        }
        result?;
        Ok(())
    }

    /// Convenience: persist a logical file size in chunk-0's BenchFS extension
    /// using max-extend semantics (matches `MetadataUpdate` behavior). Pass
    /// `new_size = 0` to force a truncate to zero.
    pub async fn update_logical_size(
        &self,
        file_path: &str,
        new_size: u64,
    ) -> ChunkStoreResult<u64> {
        let current = self
            .read_chunk0_extension(file_path)
            .await?
            .unwrap_or_else(BenchfsChunk0Extension::zero);
        let updated = if new_size == 0 {
            0
        } else {
            current.logical_size.max(new_size)
        };
        self.write_chunk0_extension(file_path, BenchfsChunk0Extension::with_size(updated))
            .await?;
        Ok(updated)
    }

    /// Read the persisted logical file size for `file_path`. Returns 0 if
    /// chunk-0 doesn't exist (file not yet created) or has no extension yet.
    pub async fn read_logical_size(&self, file_path: &str) -> ChunkStoreResult<u64> {
        Ok(self
            .read_chunk0_extension(file_path)
            .await?
            .map(|e| e.logical_size)
            .unwrap_or(0))
    }

    /// Whether `(offset, len)` satisfies O_DIRECT's alignment requirements.
    ///
    /// Note: buffer alignment is already guaranteed by `FixedBufferAllocator`
    /// (page-aligned allocations). The remaining unknowns are the file offset
    /// and request length, both of which the caller controls per request.
    pub(crate) fn direct_io_aligned(offset: u64, len: u64) -> bool {
        offset.is_multiple_of(Self::DIRECT_IO_ALIGN) && len.is_multiple_of(Self::DIRECT_IO_ALIGN)
    }

    /// Unified-layout open: one fd per (path, write_or_read, use_direct).
    /// Always cached. Same `O_RDWR` semantics as the per-chunk cache —
    /// once opened, the handle survives both write and read phases.
    async fn get_or_open_unified_file(
        &self,
        file_path: &str,
        chunk_index: u64,
        write: bool,
        use_direct: bool,
    ) -> ChunkStoreResult<(FileHandle, bool)> {
        // Key by (path, shard, use_direct). With shards=1 this collapses to
        // a single per-path entry like before; with shards > 1 each shard
        // gets its own cached fd so concurrent writes to different shards
        // don't contend on the same inode lock in the kernel page cache.
        let shards = self.unified_shard_count();
        let shard_idx = if shards <= 1 { 0 } else { chunk_index % shards };
        let key = (file_path.to_string(), shard_idx, use_direct);
        {
            let cache = self.fd_cache.borrow();
            if let Some(&h) = cache.get(&key) {
                return Ok((h, true));
            }
        }
        // Ensure parent dir exists (path_hash dir).
        if write {
            let path_hash = self.hash_path(file_path);
            let dir = self.base_dir.join(&path_hash);
            if !dir.exists() {
                std::fs::create_dir_all(&dir)?;
            }
        }
        let data_path = self.unified_data_path(file_path, chunk_index);
        let flags = OpenFlags {
            read: true,
            write: true,
            create: write,
            truncate: false,
            append: false,
            direct: use_direct,
        };
        let handle = self.backend.open(&data_path, flags).await?;
        let mut evicted: Vec<FileHandle> = Vec::new();
        {
            let mut cache = self.fd_cache.borrow_mut();
            let mut order = self.fd_cache_order.borrow_mut();
            // Unified path caches even when cap=0 (≤1 fd per path), but
            // when cap>0 we still respect the FIFO limit so unified +
            // per-chunk entries don't exhaust fds together.
            if self.fd_cache_cap > 0 {
                while cache.len() >= self.fd_cache_cap {
                    match order.pop_front() {
                        Some(old) => {
                            if let Some(h) = cache.remove(&old) {
                                evicted.push(h);
                            }
                        }
                        None => break,
                    }
                }
            }
            cache.insert(key.clone(), handle);
            order.push_back(key);
        }
        for h in evicted {
            if let Err(e) = self.backend.close(h).await {
                tracing::warn!("Failed to close evicted unified fd: {:?}", e);
            }
        }
        Ok((handle, true))
    }

    /// Try cache → open path. Returns `(handle, cached)` where `cached`
    /// signals the caller must NOT close the handle (it's owned by the
    /// cache). When `cached=false` the caller owns the handle and should
    /// close it as usual.
    ///
    /// Cached fds are opened `O_RDWR` so the same handle works for both
    /// the write phase (chunk creation) and the read-back phase. Both
    /// paths still respect `use_direct` from the request — if the cached
    /// fd's direct flag mismatches what the current op needs, we fall
    /// back to a fresh non-cached open.
    async fn get_or_open_chunk_file(
        &self,
        file_path: &str,
        chunk_index: u64,
        write: bool,
        use_direct: bool,
    ) -> ChunkStoreResult<(FileHandle, bool)> {
        if self.fd_cache_cap == 0 {
            // `open_chunk_file` itself returns (handle, cached). With
            // fd_cache_cap=0 the cached flag is always false, but we
            // still need to unpack the tuple correctly.
            return self
                .open_chunk_file(file_path, chunk_index, write, use_direct)
                .await;
        }
        // Cache key includes use_direct so an O_DIRECT entry cached from
        // an aligned ior-easy phase doesn't get returned to an unaligned
        // ior-hard 47 KiB request (which would EINVAL on submit).
        let key = (file_path.to_string(), chunk_index, use_direct);
        {
            let cache = self.fd_cache.borrow();
            if let Some(&h) = cache.get(&key) {
                return Ok((h, true));
            }
        }
        // Cache miss → open `O_RDWR` so the cache entry serves both
        // write and read paths.
        if write {
            self.ensure_chunk_dir(file_path, chunk_index)?;
        }
        let chunk_file_path = self.chunk_path(file_path, chunk_index);
        let flags = OpenFlags {
            read: true,
            write: true,
            create: write,
            truncate: false,
            append: false,
            direct: use_direct,
        };
        let handle = self.backend.open(&chunk_file_path, flags).await?;
        // Pre-allocate the full chunk size on first open-for-write so
        // strided 47 KiB ior-hard writes don't pay ext4 extent allocation
        // on every first-write-to-offset. Best-effort (FALLOC_FL_KEEP_SIZE
        // so the file's logical size stays where we'd otherwise set it).
        // We only do this once per (path, chunk_index) — subsequent cache
        // hits skip this path entirely.
        if write {
            let chunk_size = self.chunk_size as libc::off_t;
            // SAFETY: handle.0 is a valid open fd from backend.open above.
            let rc = unsafe { libc::fallocate(handle.0, libc::FALLOC_FL_KEEP_SIZE, 0, chunk_size) };
            if rc != 0 {
                let err = std::io::Error::last_os_error();
                // Not fatal — write path will trigger allocation on demand.
                tracing::trace!(
                    "fallocate on chunk {} failed (non-fatal): {:?}",
                    chunk_index,
                    err
                );
            }
        }
        let mut evicted: Vec<FileHandle> = Vec::new();
        {
            let mut cache = self.fd_cache.borrow_mut();
            let mut order = self.fd_cache_order.borrow_mut();
            while cache.len() >= self.fd_cache_cap {
                match order.pop_front() {
                    Some(old) => {
                        if let Some(h) = cache.remove(&old) {
                            evicted.push(h);
                        }
                    }
                    None => break,
                }
            }
            cache.insert(key.clone(), handle);
            order.push_back(key);
        }
        for h in evicted {
            if let Err(e) = self.backend.close(h).await {
                tracing::warn!("Failed to close evicted fd: {:?}", e);
            }
        }
        Ok((handle, true))
    }

    /// Open a chunk file for reading or writing.
    ///
    /// `use_direct` controls whether the file is opened with `O_DIRECT`.
    /// Pass `false` when the request's offset/len is not aligned to
    /// [`DIRECT_IO_ALIGN`] — io_uring's `read_fixed` / `write_fixed` work fine
    /// without `O_DIRECT`, but `O_DIRECT` itself rejects unaligned transfers.
    ///
    /// File handles are closed immediately after each operation; no caching.
    async fn open_chunk_file(
        &self,
        file_path: &str,
        chunk_index: u64,
        write: bool,
        use_direct: bool,
    ) -> ChunkStoreResult<(FileHandle, bool)> {
        let open_start = std::time::Instant::now();

        // NOTE: fd cache lookup was attempted here but caused EIO in
        // mdtest-hard (iter56) — eviction closes a handle that an in-
        // flight RPC task still holds. mdtest-hard touches each file
        // exactly once during the write phase, so caching wouldn't help
        // anyway: the dominant cost is the first-touch O_CREAT, not
        // repeated opens. Read phase could benefit, but reusing the
        // existing get_or_open_chunk_file path (which is unified-layout
        // only) is the cleaner refactor.
        // Ensure shard directory exists (includes path_hash and shard subdirectory)
        if write {
            self.ensure_chunk_dir(file_path, chunk_index)?;
        }
        let ensure_dir_elapsed = open_start.elapsed();

        let chunk_file_path = self.chunk_path(file_path, chunk_index);

        let flags = OpenFlags {
            read: !write,
            write,
            create: write,
            truncate: false,
            append: false,
            direct: use_direct,
        };

        let open_syscall_start = std::time::Instant::now();
        let handle = self.backend.open(&chunk_file_path, flags).await?;
        let open_syscall_elapsed = open_syscall_start.elapsed();
        let total_elapsed = open_start.elapsed();

        // Log file open timing for performance analysis
        tracing::debug!(
            op_type = if write { "WRITE" } else { "READ" },
            chunk_index = chunk_index,
            direct = use_direct,
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
            use_direct
        );

        Ok((handle, false))
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

        let bytes_to_write = data.len().min(self.chunk_size - offset as usize);
        // O_DIRECT only when both offset and request length are 4K-aligned;
        // ior-hard's 47008-byte transfers fall back to buffered I/O.
        let use_direct = Self::direct_io_aligned(offset, bytes_to_write as u64);

        // RESTORE 5/9 layout: data lives at file offset 0 (no MSIZE shift) and
        // no per-write CHFS POSIX header. This removes the per-chunk
        // `ensure_chunk_header` + sync `std::fs::metadata` that was added
        // by commit 38a82e9 and showed up as the WriteChunk regression at
        // ppn=16 / b=16g shared. The CHFS POSIX metadata helpers
        // (`write_chunk0_extension`, `stat_chunk`) are unaffected — they
        // still write at offset 0 (the metadata server's own writes), but
        // the bulk-data path no longer pays the header overhead and stays
        // raw-data-at-offset-0 like the 5/9 baseline (106 GiB/s).
        let host = gethostname::gethostname()
            .to_string_lossy()
            .into_owned();
        // Transient io_uring ENOMEM retry. iter77-84 showed
        // `IORING_OP_WRITE_FIXED` CQE.res = -ENOMEM from kernel
        // `io_rw_alloc_async` under high in-flight pressure (~1 in
        // millions). locusta has no client-side retry so even one
        // ENOMEM aborts IOR. Sleep+retry up to 3 times absorbs the
        // transient.
        let max_attempts: u32 = 3;
        let mut attempt: u32 = 0;
        let written = loop {
            attempt += 1;
            let (handle, cached) = match self
                .open_chunk_file(file_path, chunk_index, true, use_direct)
                .await
            {
                Ok(pair) => pair,
                Err(e) => {
                    eprintln!(
                        "[{host}][chunk_store_fail] stage=OPEN attempt={attempt} path={} chunk={} offset={} len={} use_direct={} err={:?}",
                        file_path, chunk_index, offset, bytes_to_write, use_direct, e
                    );
                    return Err(e);
                }
            };

            let result = self
                .backend
                .write(handle, offset, &data[..bytes_to_write])
                .await;

            if !cached {
                if let Err(e) = self.backend.close(handle).await {
                    eprintln!(
                        "[{host}][chunk_store_fail] stage=CLOSE path={} chunk={} err={:?}",
                        file_path, chunk_index, e
                    );
                }
            }

            match result {
                Ok(n) => break n,
                Err(e) => {
                    let is_enomem = matches!(
                        &e,
                        crate::storage::error::StorageError::IoError(io_err) if io_err.raw_os_error() == Some(libc::ENOMEM)
                    );
                    if is_enomem && attempt < max_attempts {
                        eprintln!(
                            "[{host}][chunk_store_fail] stage=WRITE ENOMEM attempt={attempt}/{max_attempts} path={} chunk={} offset={} len={} — retrying",
                            file_path, chunk_index, offset, bytes_to_write
                        );
                        pluvio_timer::sleep(std::time::Duration::from_millis(
                            1u64 << (attempt - 1),
                        ))
                        .await;
                        continue;
                    }
                    eprintln!(
                        "[{host}][chunk_store_fail] stage=WRITE attempt={attempt}/{max_attempts} path={} chunk={} offset={} len={} use_direct={} cached={} err={:?}",
                        file_path, chunk_index, offset, bytes_to_write, use_direct, cached, e
                    );
                    return Err(ChunkStoreError::IoError(std::io::Error::other(format!(
                        "{e:?}"
                    ))));
                }
            }
        };

        tracing::trace!("Wrote {} bytes (attempts={})", written, attempt);

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

        // O_DIRECT for aligned chunks — historical default that the prior
        // 144 GiB/s read benchmark relied on. Drop O_DIRECT only when the
        // request can't satisfy O_DIRECT alignment (ior-hard with 47008 B).
        let use_direct = Self::direct_io_aligned(offset, length);
        let (handle, cached) = self
            .open_chunk_file(file_path, chunk_index, false, use_direct)
            .await?;

        // 5/9 raw layout: data at file offset 0.
        let mut buffer = vec![0u8; length as usize];
        let result = self.backend.read(handle, offset, &mut buffer).await;

        if !cached {
            if let Err(e) = self.backend.close(handle).await {
                tracing::warn!("Failed to close chunk file after read: {:?}", e);
            }
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

        // Drop header-initialized memo so a subsequent re-create writes the header.
        self.header_initialized
            .borrow_mut()
            .remove(&ChunkKey::new(file_path.to_string(), chunk_index));

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

        // Purge any header-initialized entries for this path.
        self.header_initialized
            .borrow_mut()
            .retain(|k| k.path != file_path);

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
    /// Zero-copy write where the buffer is owned elsewhere (e.g. the
    /// locusta RoundtripPut grant). The caller must guarantee the
    /// pointed-to memory was acquired from this store's allocator and stays
    /// valid until the returned future resolves.
    ///
    /// # Safety
    /// See [`IOUringBackend::write_via_registered`].
    // async_backtrace removed from hot path: 1-2µs per call thread-local
    // push/pop on the ior-hard 47 KiB write path adds up at 320k RPC/s
    // aggregate (job 20166). Keep on lower-frequency methods.
    pub async fn write_chunk_via_registered(
        &self,
        file_path: &str,
        chunk_index: u64,
        offset: u64,
        ptr: *const u8,
        len: u32,
        buf_index: u16,
    ) -> ChunkStoreResult<usize> {
        if offset >= self.chunk_size as u64 {
            return Err(ChunkStoreError::InvalidOffset(offset));
        }
        let use_direct = Self::direct_io_aligned(offset, len as u64);
        let unified = self.unified_layout_enabled();

        // mmap fast path for non-O_DIRECT chunks (ior-hard 47 KiB unaligned
        // writes). Bypasses io_uring entirely; memcpy into MAP_SHARED region
        // commits to page cache at memcpy speed (~2µs vs ~100µs).
        if self.mmap_enabled && !use_direct && !unified {
            return self
                .write_chunk_via_mmap(file_path, chunk_index, offset, ptr, len)
                .await;
        }
        let open_start = std::time::Instant::now();
        let (handle, cached, file_offset) = if unified {
            let (h, c) = self
                .get_or_open_unified_file(file_path, chunk_index, true, use_direct)
                .await?;
            (h, c, self.unified_file_offset(chunk_index, offset))
        } else {
            let (h, c) = self
                .get_or_open_chunk_file(file_path, chunk_index, true, use_direct)
                .await?;
            (h, c, offset)
        };
        let open_us = open_start.elapsed().as_micros() as u64;
        // Transient io_uring ENOMEM retry (see write_chunk).
        let write_start = std::time::Instant::now();
        let max_attempts: u32 = 3;
        let mut attempt: u32 = 0;
        let result = loop {
            attempt += 1;
            let r = self
                .backend
                .write_via_registered(handle, file_offset, ptr, len, buf_index)
                .await;
            let is_enomem = matches!(
                &r,
                Err(crate::storage::error::StorageError::IoError(io_err))
                    if io_err.raw_os_error() == Some(libc::ENOMEM)
            );
            if is_enomem && attempt < max_attempts {
                eprintln!(
                    "[write_chunk_via_registered] WRITE ENOMEM attempt={attempt}/{max_attempts} path={} chunk={} offset={} len={} — retrying",
                    file_path, chunk_index, offset, len
                );
                pluvio_timer::sleep(std::time::Duration::from_millis(1u64 << (attempt - 1)))
                    .await;
                continue;
            }
            break r;
        };
        let write_us = write_start.elapsed().as_micros() as u64;
        let close_start = std::time::Instant::now();
        if !cached {
            if let Err(e) = self.backend.close(handle).await {
                tracing::warn!(
                    "Failed to close chunk file after write_via_registered: {:?}",
                    e
                );
            }
        }
        let close_us = close_start.elapsed().as_micros() as u64;
        if crate::stats::is_stats_enabled() {
            use std::sync::atomic::{AtomicU64, Ordering};
            static N: AtomicU64 = AtomicU64::new(0);
            let n = N.fetch_add(1, Ordering::Relaxed);
            if n.is_multiple_of(1000) {
                tracing::info!(
                    target: "rpc_handler_timing",
                    kind = "write_chunk_via_registered",
                    n = n,
                    open_us = open_us,
                    write_us = write_us,
                    close_us = close_us,
                    "WRITE_CHUNK_TIMING"
                );
            }
        }
        Ok(
            result
                .map_err(|e| ChunkStoreError::IoError(std::io::Error::other(format!("{e:?}"))))?,
        )
    }

    /// mmap fast-path write. Skips io_uring entirely — memcpy from the
    /// caller's registered buffer directly into a MAP_SHARED page-cache
    /// region. For ior-hard's random 47 KiB writes this is ~50× faster
    /// than `write_via_registered` (memcpy 47 KiB ≈ 2µs vs io_uring's
    /// ~100µs NVMe-bound floor). Data is durable at fsync / munmap time;
    /// io500 ior-hard tolerates that since it fsyncs at close.
    async fn write_chunk_via_mmap(
        &self,
        file_path: &str,
        chunk_index: u64,
        offset: u64,
        ptr: *const u8,
        len: u32,
    ) -> ChunkStoreResult<usize> {
        let key = (file_path.to_string(), chunk_index);
        // Cache lookup
        let entry_opt = {
            let cache = self.mmap_cache.borrow();
            cache.get(&key).copied()
        };
        let entry = match entry_opt {
            Some(e) => e,
            None => self.get_or_mmap_chunk(file_path, chunk_index, &key).await?,
        };
        if (offset as usize)
            .checked_add(len as usize)
            .map(|e| e > entry.len)
            .unwrap_or(true)
        {
            return Err(ChunkStoreError::InvalidOffset(offset));
        }
        // SAFETY: entry.ptr came from a successful mmap of `entry.len` bytes
        // and is alive until munmap on eviction. The cache holds it, so it's
        // alive across this synchronous memcpy. `ptr` is the caller's
        // registered DMA buffer, valid for the duration of this future.
        unsafe {
            std::ptr::copy_nonoverlapping(ptr, entry.ptr.add(offset as usize), len as usize);
        }
        Ok(len as usize)
    }

    /// Open + mmap a chunk file. Caller-owned (no fd_cache sharing) so
    /// eviction can safely munmap+close without affecting the io_uring
    /// fd_cache. Inserts into `mmap_cache` with FIFO eviction.
    async fn get_or_mmap_chunk(
        &self,
        file_path: &str,
        chunk_index: u64,
        key: &(String, u64),
    ) -> ChunkStoreResult<MmapEntry> {
        self.ensure_chunk_dir(file_path, chunk_index)?;
        let chunk_file_path = self.chunk_path(file_path, chunk_index);
        let c_path = std::ffi::CString::new(chunk_file_path.as_os_str().as_encoded_bytes())
            .map_err(|_| {
                ChunkStoreError::IoError(std::io::Error::other("path contains nul byte"))
            })?;
        // O_RDWR | O_CREAT, no O_DIRECT (mmap requires page cache).
        let fd = unsafe {
            libc::open(
                c_path.as_ptr(),
                libc::O_RDWR | libc::O_CREAT,
                0o644 as libc::mode_t,
            )
        };
        if fd < 0 {
            return Err(ChunkStoreError::IoError(std::io::Error::last_os_error()));
        }
        let chunk_size = self.chunk_size;
        // Pre-allocate so mmap covers a fully-backed range.
        let rc = unsafe { libc::fallocate(fd, 0, 0, chunk_size as libc::off_t) };
        if rc != 0 {
            let err = std::io::Error::last_os_error();
            unsafe { libc::close(fd) };
            return Err(ChunkStoreError::IoError(err));
        }
        let mmap_ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                chunk_size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd,
                0,
            )
        };
        if mmap_ptr == libc::MAP_FAILED {
            let err = std::io::Error::last_os_error();
            unsafe { libc::close(fd) };
            return Err(ChunkStoreError::IoError(err));
        }
        let entry = MmapEntry {
            fd,
            ptr: mmap_ptr as *mut u8,
            len: chunk_size,
        };
        // FIFO insert + evict.
        let mut to_evict: Vec<MmapEntry> = Vec::new();
        {
            let mut cache = self.mmap_cache.borrow_mut();
            let mut order = self.mmap_cache_order.borrow_mut();
            while cache.len() >= self.fd_cache_cap.max(1) {
                match order.pop_front() {
                    Some(k) => {
                        if let Some(e) = cache.remove(&k) {
                            to_evict.push(e);
                        }
                    }
                    None => break,
                }
            }
            cache.insert(key.clone(), entry);
            order.push_back(key.clone());
        }
        for e in to_evict {
            unsafe {
                // Async msync would be ideal but blocks single-thread executor.
                // Skip explicit msync; kernel flushes dirty pages on close
                // and on memory pressure. fsync at file-close time handles
                // durability.
                libc::munmap(e.ptr as *mut libc::c_void, e.len);
                libc::close(e.fd);
            }
        }
        Ok(entry)
    }

    /// Zero-copy read into an externally-owned registered buffer.
    /// See [`write_chunk_via_registered`] for safety constraints.
    pub async fn read_chunk_via_registered(
        &self,
        file_path: &str,
        chunk_index: u64,
        offset: u64,
        ptr: *mut u8,
        len: u32,
        buf_index: u16,
    ) -> ChunkStoreResult<usize> {
        if offset >= self.chunk_size as u64 {
            return Err(ChunkStoreError::InvalidOffset(offset));
        }
        let use_direct = Self::direct_io_aligned(offset, len as u64);
        let unified = self.unified_layout_enabled();
        let (handle, cached, file_offset) = if unified {
            let (h, c) = self
                .get_or_open_unified_file(file_path, chunk_index, false, use_direct)
                .await?;
            (h, c, self.unified_file_offset(chunk_index, offset))
        } else {
            let (h, c) = self
                .get_or_open_chunk_file(file_path, chunk_index, false, use_direct)
                .await?;
            (h, c, offset)
        };
        let result = self
            .backend
            .read_via_registered(handle, file_offset, ptr, len, buf_index)
            .await;
        if !cached {
            if let Err(e) = self.backend.close(handle).await {
                tracing::warn!(
                    "Failed to close chunk file after read_via_registered: {:?}",
                    e
                );
            }
        }
        Ok(
            result
                .map_err(|e| ChunkStoreError::IoError(std::io::Error::other(format!("{e:?}"))))?,
        )
    }

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

        let bytes_to_write = data_len.min(self.chunk_size - offset as usize);
        // O_DIRECT only when both offset and request length are 4K-aligned;
        // ior-hard's 47008-byte transfers fall back to buffered I/O.
        let use_direct = Self::direct_io_aligned(offset, bytes_to_write as u64);

        // Restored 5/9 layout (see write_chunk above).
        let open_start = std::time::Instant::now();
        let (handle, cached) = self
            .open_chunk_file(file_path, chunk_index, true, use_direct)
            .await?;
        let open_elapsed = open_start.elapsed();

        // Write data from registered buffer (zero-copy DMA when O_DIRECT).
        // 5/9 raw layout: data at file offset 0.
        let write_start = std::time::Instant::now();
        let result = self
            .backend
            .write_fixed_direct(handle, offset, fixed_buffer, bytes_to_write)
            .await;
        let write_elapsed = write_start.elapsed();

        let close_start = std::time::Instant::now();
        if !cached {
            if let Err(e) = self.backend.close(handle).await {
                tracing::warn!("Failed to close chunk file after write_fixed: {:?}", e);
            }
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

    /// Read a chunk using a registered buffer (zero-copy DMA when aligned).
    ///
    /// Reads up to `length` bytes from `offset` into `fixed_buffer`. The actual
    /// transfer length passed to io_uring is `min(length, fixed_buffer.len())`
    /// so the kernel only fills (and only reports) the requested range.
    ///
    /// # Returns
    /// A tuple of (bytes_read, buffer) where buffer is the FixedBuffer with data
    #[async_backtrace::framed]
    #[instrument(level = "trace", name = "iouring_read_chunk_fixed", skip(self, fixed_buffer), fields(path = file_path, chunk = chunk_index, offset, len = length))]
    pub async fn read_chunk_fixed(
        &self,
        file_path: &str,
        chunk_index: u64,
        offset: u64,
        length: u64,
        fixed_buffer: pluvio_uring::allocator::FixedBuffer,
    ) -> ChunkStoreResult<(usize, pluvio_uring::allocator::FixedBuffer)> {
        let total_start = std::time::Instant::now();

        if offset >= self.chunk_size as u64 {
            return Err(ChunkStoreError::InvalidOffset(offset));
        }

        let read_len = (length as usize).min(fixed_buffer.len());
        // O_DIRECT for aligned chunks — historical default the prior 144 GiB/s
        // read benchmark relied on. Drop O_DIRECT only for ior-hard's
        // non-4K-aligned 47008 B transfers (offset/len misaligned).
        let use_direct = Self::direct_io_aligned(offset, read_len as u64);

        let unified = self.unified_layout_enabled();
        let open_start = std::time::Instant::now();
        let (handle, cached, file_offset) = if unified {
            let (h, c) = self
                .get_or_open_unified_file(file_path, chunk_index, false, use_direct)
                .await?;
            (h, c, self.unified_file_offset(chunk_index, offset))
        } else {
            let (h, c) = self
                .open_chunk_file(file_path, chunk_index, false, use_direct)
                .await?;
            (h, c, offset)
        };
        let open_elapsed = open_start.elapsed();

        // 5/9 raw layout: data at file offset 0.
        let read_start = std::time::Instant::now();
        let result = self
            .backend
            .read_fixed_direct(handle, file_offset, fixed_buffer, read_len)
            .await;
        let read_elapsed = read_start.elapsed();

        // Close the file handle (skip if cached in unified layout).
        let close_start = std::time::Instant::now();
        if !cached {
            if let Err(e) = self.backend.close(handle).await {
                tracing::warn!("Failed to close chunk file after read_fixed: {:?}", e);
            }
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

    fn as_any(&self) -> &dyn std::any::Any {
        self
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
