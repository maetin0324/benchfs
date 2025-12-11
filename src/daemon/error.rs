//! Error types for the daemon module.

use std::io;

use super::shm::ShmError;

/// Error type for daemon operations
#[derive(Debug)]
pub enum DaemonError {
    /// Shared memory error
    Shm(ShmError),
    /// IO error
    Io(io::Error),
    /// Daemon not running
    DaemonNotRunning,
    /// Daemon startup failed
    DaemonStartupFailed(String),
    /// Daemon startup timeout
    DaemonStartupTimeout,
    /// Connection failed
    ConnectionFailed(String),
    /// Invalid operation
    InvalidOperation(String),
    /// Request failed
    RequestFailed { status: i32, message: String },
    /// Timeout
    Timeout,
    /// Buffer too small
    BufferTooSmall { required: usize, available: usize },
    /// Path too long
    PathTooLong { len: usize, max: usize },
}

impl std::fmt::Display for DaemonError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DaemonError::Shm(e) => write!(f, "Shared memory error: {}", e),
            DaemonError::Io(e) => write!(f, "IO error: {}", e),
            DaemonError::DaemonNotRunning => write!(f, "Daemon is not running"),
            DaemonError::DaemonStartupFailed(msg) => write!(f, "Daemon startup failed: {}", msg),
            DaemonError::DaemonStartupTimeout => write!(f, "Daemon startup timeout"),
            DaemonError::ConnectionFailed(msg) => write!(f, "Connection failed: {}", msg),
            DaemonError::InvalidOperation(msg) => write!(f, "Invalid operation: {}", msg),
            DaemonError::RequestFailed { status, message } => {
                write!(f, "Request failed with status {}: {}", status, message)
            }
            DaemonError::Timeout => write!(f, "Operation timeout"),
            DaemonError::BufferTooSmall {
                required,
                available,
            } => {
                write!(
                    f,
                    "Buffer too small: required {} bytes, available {} bytes",
                    required, available
                )
            }
            DaemonError::PathTooLong { len, max } => {
                write!(f, "Path too long: {} bytes, max {} bytes", len, max)
            }
        }
    }
}

impl std::error::Error for DaemonError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            DaemonError::Shm(e) => Some(e),
            DaemonError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<ShmError> for DaemonError {
    fn from(e: ShmError) -> Self {
        DaemonError::Shm(e)
    }
}

impl From<io::Error> for DaemonError {
    fn from(e: io::Error) -> Self {
        DaemonError::Io(e)
    }
}

/// Convert errno to error status code
pub fn errno_to_status(errno: i32) -> i32 {
    -errno.abs()
}

/// Convert status code to errno
pub fn status_to_errno(status: i32) -> i32 {
    (-status).abs()
}

/// Common POSIX error codes
pub mod errno {
    pub const SUCCESS: i32 = 0;
    pub const EPERM: i32 = -1;
    pub const ENOENT: i32 = -2;
    pub const EIO: i32 = -5;
    pub const ENXIO: i32 = -6;
    pub const EBADF: i32 = -9;
    pub const EAGAIN: i32 = -11;
    pub const ENOMEM: i32 = -12;
    pub const EACCES: i32 = -13;
    pub const EBUSY: i32 = -16;
    pub const EEXIST: i32 = -17;
    pub const ENODEV: i32 = -19;
    pub const ENOTDIR: i32 = -20;
    pub const EISDIR: i32 = -21;
    pub const EINVAL: i32 = -22;
    pub const EMFILE: i32 = -24;
    pub const ENFILE: i32 = -23;
    pub const EFBIG: i32 = -27;
    pub const ENOSPC: i32 = -28;
    pub const EROFS: i32 = -30;
    pub const ETIMEDOUT: i32 = -110;
}
