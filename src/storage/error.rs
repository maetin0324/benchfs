use thiserror::Error;

/// ストレージエラー
#[derive(Error, Debug)]
pub enum StorageError {
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("File not found: {0}")]
    NotFound(String),

    #[error("Permission denied: {0}")]
    PermissionDenied(String),

    #[error("File already exists: {0}")]
    AlreadyExists(String),

    #[error("Invalid file handle: {0:?}")]
    InvalidHandle(crate::storage::FileHandle),

    #[error("Invalid offset: {offset} (file size: {file_size})")]
    InvalidOffset { offset: u64, file_size: u64 },

    #[error("Buffer too small: required {required}, got {actual}")]
    BufferTooSmall { required: usize, actual: usize },

    #[error("Operation not supported: {0}")]
    Unsupported(String),

    #[error("IO-uring error: {0}")]
    IOUringError(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

pub type StorageResult<T> = Result<T, StorageError>;
