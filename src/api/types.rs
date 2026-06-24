/// API types for BenchFS filesystem operations
use std::rc::Rc;

/// Per-file deferred-write bookkeeping. `benchfs_write` pipelines RPCs by
/// spawning each chunk write as a task and stashing its `JoinHandle` here
/// instead of awaiting inline. `benchfs_close` / `benchfs_fsync` drains
/// this list so the byte-count visible to callers stays consistent with
/// what hit the wire by phase-end.
///
/// At ior-hard's 47 KiB pattern the previous code's RTT-serialized RPCs
/// capped per-client throughput at `1/RTT ≈ 5k writes/s`. Pipelining many
/// in-flight RPCs per client is the only way to break the per-client
/// ceiling without an API change.
pub type PendingWriteHandle = pluvio_runtime::task::JoinHandle<crate::api::types::ApiResult<usize>>;

/// File handle for open files
#[derive(Clone)]
pub struct FileHandle {
    /// File descriptor (unique ID)
    pub fd: u64,

    /// File path
    pub path: String,

    /// File inode
    pub inode: u64,

    /// Current file position
    pub(crate) position: Rc<std::cell::RefCell<u64>>,

    /// Open flags
    pub flags: OpenFlags,

    /// In-flight chunk-write task handles spawned by `benchfs_write` but
    /// not yet awaited. Drained on fsync / close. `Rc<RefCell<…>>` is
    /// sound because the pluvio runtime is single-threaded.
    pub(crate) pending_writes: Rc<std::cell::RefCell<Vec<PendingWriteHandle>>>,
}

/// Open flags (similar to POSIX)
#[derive(Debug, Clone, Copy)]
pub struct OpenFlags {
    /// Read access
    pub read: bool,

    /// Write access
    pub write: bool,

    /// Create if doesn't exist
    pub create: bool,

    /// Truncate on open
    pub truncate: bool,

    /// Append mode
    pub append: bool,

    /// `O_EXCL`: fail with EEXIST if file already exists. Only meaningful
    /// together with `create`. The new client-side-open design SKIPS the
    /// MetadataCreate RPC entirely on `create && !exclusive`; the
    /// metadata is established lazily by the close-time MetadataUpdate
    /// upsert. With `exclusive`, an atomic test-and-set MetadataCreate is
    /// still required (single RPC to hash(path) owner).
    pub exclusive: bool,
}

impl OpenFlags {
    pub fn read_only() -> Self {
        Self {
            read: true,
            write: false,
            create: false,
            truncate: false,
            append: false,
            exclusive: false,
        }
    }

    pub fn write_only() -> Self {
        Self {
            read: false,
            write: true,
            create: false,
            truncate: false,
            append: false,
            exclusive: false,
        }
    }

    pub fn read_write() -> Self {
        Self {
            read: true,
            write: true,
            create: false,
            truncate: false,
            append: false,
            exclusive: false,
        }
    }

    pub fn create() -> Self {
        Self {
            read: false,
            write: true,
            create: true,
            truncate: false,
            append: false,
            exclusive: false,
        }
    }
}

/// API errors
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("File not found: {0}")]
    NotFound(String),

    #[error("Permission denied: {0}")]
    PermissionDenied(String),

    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    #[error("I/O error: {0}")]
    IoError(String),

    #[error("File already exists: {0}")]
    AlreadyExists(String),

    #[error("Not a directory: {0}")]
    NotADirectory(String),

    #[error("Is a directory: {0}")]
    IsADirectory(String),

    #[error("RPC error: {0}")]
    RpcError(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

pub type ApiResult<T> = Result<T, ApiError>;

/// File type enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileType {
    RegularFile,
    Directory,
    Symlink,
    Other,
}

/// File status (similar to POSIX stat)
#[derive(Debug, Clone)]
pub struct FileStat {
    /// File inode
    pub inode: u64,

    /// File type
    pub file_type: FileType,

    /// File size in bytes
    pub size: u64,

    /// Number of chunks (for files)
    pub chunk_count: u64,

    /// File mode/permissions (Unix-style)
    pub mode: u32,

    /// Access time (Unix timestamp)
    pub atime: i64,

    /// Modification time (Unix timestamp)
    pub mtime: i64,

    /// Change time (Unix timestamp)
    pub ctime: i64,
}

impl FileStat {
    /// Create FileStat from FileMetadata
    pub fn from_file_metadata(meta: &crate::metadata::FileMetadata) -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        Self {
            inode: 0, // Dummy inode in path-based KV design
            file_type: FileType::RegularFile,
            size: meta.size,
            chunk_count: meta.calculate_chunk_count(),
            mode: 0o644, // Default file permissions
            atime: now,
            mtime: now,
            ctime: now,
        }
    }

    /// Create FileStat from DirectoryMetadata
    pub fn from_dir_metadata(meta: &crate::metadata::DirectoryMetadata) -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        Self {
            inode: meta.inode,
            file_type: FileType::Directory,
            size: 4096, // Directory size (conventional value)
            chunk_count: 0,
            mode: 0o755, // Default directory permissions
            atime: now,
            mtime: now,
            ctime: now,
        }
    }

    /// Check if this is a regular file
    pub fn is_file(&self) -> bool {
        self.file_type == FileType::RegularFile
    }

    /// Check if this is a directory
    pub fn is_dir(&self) -> bool {
        self.file_type == FileType::Directory
    }
}

impl std::fmt::Debug for FileHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileHandle")
            .field("fd", &self.fd)
            .field("path", &self.path)
            .field("inode", &self.inode)
            .field("position", &*self.position.borrow())
            .field("flags", &self.flags)
            .field("pending_writes", &self.pending_writes.borrow().len())
            .finish()
    }
}

impl FileHandle {
    /// Create a new file handle
    pub fn new(fd: u64, path: String, inode: u64, flags: OpenFlags) -> Self {
        Self {
            fd,
            path,
            inode,
            position: Rc::new(std::cell::RefCell::new(0)),
            flags,
            pending_writes: Rc::new(std::cell::RefCell::new(Vec::new())),
        }
    }

    /// Get current file position
    pub fn position(&self) -> u64 {
        *self.position.borrow()
    }

    /// Set file position
    pub fn seek(&self, pos: u64) {
        *self.position.borrow_mut() = pos;
    }

    /// Advance position
    pub fn advance(&self, bytes: u64) {
        *self.position.borrow_mut() += bytes;
    }
}
