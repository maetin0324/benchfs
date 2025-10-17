/// API types for BenchFS filesystem operations

use std::rc::Rc;

/// File handle for open files
#[derive(Debug, Clone)]
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
}

impl OpenFlags {
    pub fn read_only() -> Self {
        Self {
            read: true,
            write: false,
            create: false,
            truncate: false,
            append: false,
        }
    }

    pub fn write_only() -> Self {
        Self {
            read: false,
            write: true,
            create: false,
            truncate: false,
            append: false,
        }
    }

    pub fn read_write() -> Self {
        Self {
            read: true,
            write: true,
            create: false,
            truncate: false,
            append: false,
        }
    }

    pub fn create() -> Self {
        Self {
            read: false,
            write: true,
            create: true,
            truncate: false,
            append: false,
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

impl FileHandle {
    /// Create a new file handle
    pub fn new(fd: u64, path: String, inode: u64, flags: OpenFlags) -> Self {
        Self {
            fd,
            path,
            inode,
            position: Rc::new(std::cell::RefCell::new(0)),
            flags,
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
