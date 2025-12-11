//! Protocol definitions for client-daemon communication via shared memory.
//!
//! This module defines the data structures used for IPC between IOR client processes
//! and the BenchFS client daemon through shared memory ring buffers.

use std::sync::atomic::{AtomicU32, AtomicU64};

/// Magic number for shared memory validation ("BENCHFSD")
pub const SHM_MAGIC: u64 = 0x42454E4346534844;

/// Protocol version
pub const SHM_VERSION: u32 = 1;

/// Default number of client slots
pub const DEFAULT_NUM_SLOTS: u32 = 256;

/// Default data buffer size per slot (4MB, matches chunk size)
pub const DEFAULT_DATA_BUFFER_SIZE: usize = 4 * 1024 * 1024;

/// Default request ring size (number of entries)
pub const DEFAULT_REQUEST_RING_SIZE: u32 = 64;

/// Default response ring size (number of entries)
pub const DEFAULT_RESPONSE_RING_SIZE: u32 = 64;

/// Maximum path length that can be inlined in request
pub const INLINE_PATH_LEN: usize = 64;

/// Maximum number of slots (1024 = 16 words * 64 bits)
pub const MAX_SLOTS: u32 = 1024;

/// Number of u64 words in slot allocation bitmap
pub const SLOT_BITMAP_WORDS: usize = 16;

/// Page size for alignment
pub const PAGE_SIZE: usize = 4096;

/// Startup timeout in milliseconds
pub const DEFAULT_STARTUP_TIMEOUT_MS: u64 = 30000;

/// Operation types for requests
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpType {
    /// No operation (invalid)
    Nop = 0,
    /// Open/create file
    Open = 1,
    /// Close file
    Close = 2,
    /// Read data
    Read = 3,
    /// Write data
    Write = 4,
    /// Delete file
    Unlink = 5,
    /// Get file statistics
    Stat = 6,
    /// Create directory
    Mkdir = 7,
    /// Sync file
    Fsync = 8,
    /// Truncate file
    Truncate = 9,
    /// Seek (update position)
    Seek = 10,
    /// Client disconnect
    Disconnect = 255,
}

impl From<u32> for OpType {
    fn from(v: u32) -> Self {
        match v {
            1 => OpType::Open,
            2 => OpType::Close,
            3 => OpType::Read,
            4 => OpType::Write,
            5 => OpType::Unlink,
            6 => OpType::Stat,
            7 => OpType::Mkdir,
            8 => OpType::Fsync,
            9 => OpType::Truncate,
            10 => OpType::Seek,
            255 => OpType::Disconnect,
            _ => OpType::Nop,
        }
    }
}

/// Open flags (matches POSIX flags)
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenFlags {
    /// Read only
    ReadOnly = 0,
    /// Write only
    WriteOnly = 1,
    /// Read and write
    ReadWrite = 2,
    /// Create if not exists
    Create = 0x40,
    /// Truncate existing file
    Truncate = 0x200,
    /// Append mode
    Append = 0x400,
}

/// Seek whence values
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeekWhence {
    /// Seek from beginning
    Set = 0,
    /// Seek from current position
    Cur = 1,
    /// Seek from end
    End = 2,
}

/// Global control block at the start of shared memory.
/// This structure is placed at offset 0 and is page-aligned.
#[repr(C, align(4096))]
pub struct GlobalControlBlock {
    /// Magic number for validation
    pub magic: u64,
    /// Protocol version
    pub version: u32,
    /// Daemon process ID (0 if not started)
    pub daemon_pid: AtomicU32,
    /// Ready flag (0 = not ready, 1 = ready)
    pub ready_flag: AtomicU32,
    /// Shutdown flag (0 = running, 1 = shutdown requested)
    pub shutdown_flag: AtomicU32,
    /// Number of slots
    pub num_slots: u32,
    /// Size of each slot in bytes
    pub slot_size: u64,
    /// Data buffer size per slot
    pub data_buffer_size: u64,
    /// Request ring size (number of entries)
    pub request_ring_size: u32,
    /// Response ring size (number of entries)
    pub response_ring_size: u32,
    /// Slot allocation bitmap (1 = allocated, 0 = free)
    pub slot_bitmap: [AtomicU64; SLOT_BITMAP_WORDS],
    /// Reserved for future use
    _reserved: [u8; 3904], // Pad to 4096 bytes
}

impl GlobalControlBlock {
    /// Initialize a new control block
    pub fn init(
        num_slots: u32,
        slot_size: u64,
        data_buffer_size: u64,
        request_ring_size: u32,
        response_ring_size: u32,
    ) -> Self {
        const ZERO_ATOMIC: AtomicU64 = AtomicU64::new(0);
        Self {
            magic: SHM_MAGIC,
            version: SHM_VERSION,
            daemon_pid: AtomicU32::new(0),
            ready_flag: AtomicU32::new(0),
            shutdown_flag: AtomicU32::new(0),
            num_slots,
            slot_size,
            data_buffer_size,
            request_ring_size,
            response_ring_size,
            slot_bitmap: [ZERO_ATOMIC; SLOT_BITMAP_WORDS],
            _reserved: [0; 3904],
        }
    }

    /// Validate the control block
    pub fn validate(&self) -> bool {
        self.magic == SHM_MAGIC && self.version == SHM_VERSION
    }
}

/// Per-slot control block.
/// Each client gets one slot with its own request/response rings and data buffer.
#[repr(C, align(64))]
pub struct SlotControlBlock {
    /// Client process ID (0 if slot is free)
    pub client_pid: AtomicU32,
    /// Request ring head index (updated by client)
    pub request_head: AtomicU32,
    /// Request ring tail index (updated by daemon)
    pub request_tail: AtomicU32,
    /// Response ring head index (updated by daemon)
    pub response_head: AtomicU32,
    /// Response ring tail index (updated by client)
    pub response_tail: AtomicU32,
    /// Reserved/padding
    _padding: [u8; 44],
}

impl SlotControlBlock {
    /// Initialize a new slot control block
    pub fn init() -> Self {
        Self {
            client_pid: AtomicU32::new(0),
            request_head: AtomicU32::new(0),
            request_tail: AtomicU32::new(0),
            response_head: AtomicU32::new(0),
            response_tail: AtomicU32::new(0),
            _padding: [0; 44],
        }
    }
}

/// Request ring entry (128 bytes, cache-line aligned).
#[derive(Clone, Copy)]
#[repr(C, align(64))]
pub struct RequestEntry {
    /// Unique request ID
    pub request_id: u64,
    /// Operation type
    pub op_type: u32,
    /// Operation flags (OpenFlags for Open, SeekWhence for Seek, etc.)
    pub flags: u32,
    /// File descriptor (for operations on open files)
    pub fd: u64,
    /// Offset for read/write/seek operations
    pub offset: u64,
    /// Length for read/write operations, or new size for truncate
    pub length: u64,
    /// Offset within data buffer where request data starts
    pub data_offset: u64,
    /// Path length (0 if using fd instead)
    pub path_len: u32,
    /// Inline path (for short paths)
    pub path: [u8; INLINE_PATH_LEN],
    /// Padding to 128 bytes
    _padding: [u8; 4],
}

impl RequestEntry {
    /// Create a new zeroed request entry
    pub fn new() -> Self {
        Self {
            request_id: 0,
            op_type: OpType::Nop as u32,
            flags: 0,
            fd: 0,
            offset: 0,
            length: 0,
            data_offset: 0,
            path_len: 0,
            path: [0; INLINE_PATH_LEN],
            _padding: [0; 4],
        }
    }

    /// Get the operation type
    pub fn op(&self) -> OpType {
        OpType::from(self.op_type)
    }

    /// Get the path as a string slice (if path_len > 0 and fits inline)
    pub fn get_inline_path(&self) -> Option<&str> {
        if self.path_len == 0 || self.path_len as usize > INLINE_PATH_LEN {
            return None;
        }
        std::str::from_utf8(&self.path[..self.path_len as usize]).ok()
    }

    /// Set the inline path
    pub fn set_inline_path(&mut self, path: &str) -> bool {
        let bytes = path.as_bytes();
        if bytes.len() > INLINE_PATH_LEN {
            return false;
        }
        self.path[..bytes.len()].copy_from_slice(bytes);
        self.path_len = bytes.len() as u32;
        true
    }
}

impl Default for RequestEntry {
    fn default() -> Self {
        Self::new()
    }
}

/// Response ring entry (64 bytes, cache-line aligned).
#[derive(Clone, Copy)]
#[repr(C, align(64))]
pub struct ResponseEntry {
    /// Request ID this response corresponds to
    pub request_id: u64,
    /// Status code (0 = success, negative = error code)
    pub status: i32,
    /// Padding
    _padding: u32,
    /// Result value (bytes read/written, fd for open, file size for stat, etc.)
    pub result: u64,
    /// Offset within data buffer where response data starts (for read operations)
    pub data_offset: u64,
    /// Length of response data in data buffer
    pub data_len: u64,
    /// Reserved for future use
    _reserved: [u8; 24],
}

impl ResponseEntry {
    /// Create a new zeroed response entry
    pub fn new() -> Self {
        Self {
            request_id: 0,
            status: 0,
            _padding: 0,
            result: 0,
            data_offset: 0,
            data_len: 0,
            _reserved: [0; 24],
        }
    }

    /// Create a success response
    pub fn success(request_id: u64, result: u64) -> Self {
        Self {
            request_id,
            status: 0,
            _padding: 0,
            result,
            data_offset: 0,
            data_len: 0,
            _reserved: [0; 24],
        }
    }

    /// Create a success response with data
    pub fn success_with_data(
        request_id: u64,
        result: u64,
        data_offset: u64,
        data_len: u64,
    ) -> Self {
        Self {
            request_id,
            status: 0,
            _padding: 0,
            result,
            data_offset,
            data_len,
            _reserved: [0; 24],
        }
    }

    /// Create an error response
    pub fn error(request_id: u64, status: i32) -> Self {
        Self {
            request_id,
            status,
            _padding: 0,
            result: 0,
            data_offset: 0,
            data_len: 0,
            _reserved: [0; 24],
        }
    }

    /// Check if the response indicates success
    pub fn is_success(&self) -> bool {
        self.status == 0
    }
}

impl Default for ResponseEntry {
    fn default() -> Self {
        Self::new()
    }
}

// Compile-time size assertions
const _: () = assert!(std::mem::size_of::<GlobalControlBlock>() == PAGE_SIZE);
const _: () = assert!(std::mem::size_of::<SlotControlBlock>() == 64);
const _: () = assert!(std::mem::size_of::<RequestEntry>() == 128);
const _: () = assert!(std::mem::size_of::<ResponseEntry>() == 64);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_global_control_block_size() {
        assert_eq!(std::mem::size_of::<GlobalControlBlock>(), PAGE_SIZE);
    }

    #[test]
    fn test_slot_control_block_size() {
        assert_eq!(std::mem::size_of::<SlotControlBlock>(), 64);
    }

    #[test]
    fn test_request_entry_size() {
        assert_eq!(std::mem::size_of::<RequestEntry>(), 128);
    }

    #[test]
    fn test_response_entry_size() {
        assert_eq!(std::mem::size_of::<ResponseEntry>(), 64);
    }

    #[test]
    fn test_inline_path() {
        let mut req = RequestEntry::new();
        assert!(req.set_inline_path("/test/path"));
        assert_eq!(req.get_inline_path(), Some("/test/path"));

        // Path too long
        let long_path = "a".repeat(INLINE_PATH_LEN + 1);
        assert!(!req.set_inline_path(&long_path));
    }

    #[test]
    fn test_op_type_conversion() {
        assert_eq!(OpType::from(1), OpType::Open);
        assert_eq!(OpType::from(3), OpType::Read);
        assert_eq!(OpType::from(4), OpType::Write);
        assert_eq!(OpType::from(999), OpType::Nop);
    }
}
