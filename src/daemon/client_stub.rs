//! Client stub for daemon communication.
//!
//! This module provides the client-side interface for communicating with the
//! BenchFS daemon via shared memory ring buffers.

use std::sync::atomic::{AtomicU64, Ordering};

use tracing::{debug, trace};

use super::error::DaemonError;
use super::launcher::{connect_to_daemon, LauncherConfig};
use super::protocol::{OpType, RequestEntry, ResponseEntry, INLINE_PATH_LEN};
use super::ring::{RequestRing, ResponseRing};
use super::shm::SharedMemoryRegion;

/// Client stub for daemon communication.
///
/// Each IOR process creates one DaemonClientStub to communicate with the daemon.
pub struct DaemonClientStub {
    /// Shared memory region
    shm: SharedMemoryRegion,
    /// Allocated slot ID
    slot_id: u32,
    /// Next request ID
    next_request_id: AtomicU64,
}

impl DaemonClientStub {
    /// Connect to the daemon, spawning it if necessary.
    ///
    /// # Arguments
    /// * `config` - Launcher configuration
    pub fn connect(config: &LauncherConfig) -> Result<Self, DaemonError> {
        let shm = connect_to_daemon(config)?;

        // Allocate a slot
        let slot_id = shm.allocate_slot()?;

        // Set our PID
        let slot = shm.slot_control(slot_id)?;
        slot.client_pid
            .store(std::process::id(), Ordering::Release);

        debug!("Connected to daemon, allocated slot {}", slot_id);

        Ok(Self {
            shm,
            slot_id,
            next_request_id: AtomicU64::new(1),
        })
    }

    /// Attach to existing shared memory (for testing).
    pub fn attach(shm_name: &str) -> Result<Self, DaemonError> {
        let shm = SharedMemoryRegion::attach(shm_name)?;

        if !shm.is_daemon_ready() {
            return Err(DaemonError::DaemonNotRunning);
        }

        let slot_id = shm.allocate_slot()?;

        let slot = shm.slot_control(slot_id)?;
        slot.client_pid
            .store(std::process::id(), Ordering::Release);

        Ok(Self {
            shm,
            slot_id,
            next_request_id: AtomicU64::new(1),
        })
    }

    /// Get the slot ID.
    pub fn slot_id(&self) -> u32 {
        self.slot_id
    }

    /// Get the data buffer size.
    pub fn data_buffer_size(&self) -> usize {
        self.shm.config().data_buffer_size
    }

    /// Allocate a new request ID.
    fn alloc_request_id(&self) -> u64 {
        self.next_request_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Send a request and wait for response (busy polling).
    fn send_request(&self, mut request: RequestEntry) -> Result<ResponseEntry, DaemonError> {
        let request_id = self.alloc_request_id();
        request.request_id = request_id;

        let req_ring = RequestRing::new(&self.shm, self.slot_id)?;
        let resp_ring = ResponseRing::new(&self.shm, self.slot_id)?;

        // Push request (spin if full)
        while !req_ring.push(&request) {
            std::hint::spin_loop();
        }

        trace!("Sent request {}", request_id);

        // Wait for response (busy polling)
        loop {
            if let Some(response) = resp_ring.try_pop_by_id(request_id) {
                trace!("Received response for {}", request_id);
                return Ok(response);
            }
            std::hint::spin_loop();
        }
    }

    /// Set path in request (inline if short, otherwise in data buffer).
    fn set_path(
        &self,
        request: &mut RequestEntry,
        path: &str,
        data_offset: &mut usize,
    ) -> Result<(), DaemonError> {
        let path_bytes = path.as_bytes();

        if path_bytes.len() <= INLINE_PATH_LEN {
            // Inline path
            request.path[..path_bytes.len()].copy_from_slice(path_bytes);
            request.path_len = path_bytes.len() as u32;
        } else {
            // Path in data buffer
            let buffer = self.shm.data_buffer_mut(self.slot_id)?;
            if *data_offset + path_bytes.len() > buffer.len() {
                return Err(DaemonError::PathTooLong {
                    len: path_bytes.len(),
                    max: buffer.len() - *data_offset,
                });
            }

            buffer[*data_offset..*data_offset + path_bytes.len()].copy_from_slice(path_bytes);
            request.data_offset = *data_offset as u64;
            request.path_len = path_bytes.len() as u32;
            *data_offset += path_bytes.len();
        }

        Ok(())
    }

    /// Open a file.
    ///
    /// # Arguments
    /// * `path` - File path
    /// * `flags` - Open flags
    ///
    /// # Returns
    /// File descriptor on success.
    pub fn open(&self, path: &str, flags: u32) -> Result<u64, DaemonError> {
        let mut request = RequestEntry::new();
        request.op_type = OpType::Open as u32;
        request.flags = flags;

        let mut data_offset = 0;
        self.set_path(&mut request, path, &mut data_offset)?;

        let response = self.send_request(request)?;

        if response.is_success() {
            Ok(response.result)
        } else {
            Err(DaemonError::RequestFailed {
                status: response.status,
                message: format!("open({}) failed", path),
            })
        }
    }

    /// Close a file.
    ///
    /// # Arguments
    /// * `fd` - File descriptor
    pub fn close(&self, fd: u64) -> Result<(), DaemonError> {
        let mut request = RequestEntry::new();
        request.op_type = OpType::Close as u32;
        request.fd = fd;

        let response = self.send_request(request)?;

        if response.is_success() {
            Ok(())
        } else {
            Err(DaemonError::RequestFailed {
                status: response.status,
                message: format!("close({}) failed", fd),
            })
        }
    }

    /// Read from a file.
    ///
    /// # Arguments
    /// * `fd` - File descriptor
    /// * `offset` - File offset
    /// * `buffer` - Buffer to read into
    ///
    /// # Returns
    /// Number of bytes read.
    pub fn read(&self, fd: u64, offset: u64, buffer: &mut [u8]) -> Result<usize, DaemonError> {
        let data_buffer_size = self.data_buffer_size();
        if buffer.len() > data_buffer_size {
            return Err(DaemonError::BufferTooSmall {
                required: buffer.len(),
                available: data_buffer_size,
            });
        }

        let mut request = RequestEntry::new();
        request.op_type = OpType::Read as u32;
        request.fd = fd;
        request.offset = offset;
        request.length = buffer.len() as u64;

        let response = self.send_request(request)?;

        if response.is_success() {
            let bytes_read = response.result as usize;
            let data_offset = response.data_offset as usize;

            // Copy data from shared memory to user buffer
            let shm_buffer = self.shm.data_buffer(self.slot_id)?;
            buffer[..bytes_read].copy_from_slice(&shm_buffer[data_offset..data_offset + bytes_read]);

            Ok(bytes_read)
        } else {
            Err(DaemonError::RequestFailed {
                status: response.status,
                message: format!("read({}, {}, {}) failed", fd, offset, buffer.len()),
            })
        }
    }

    /// Write to a file.
    ///
    /// # Arguments
    /// * `fd` - File descriptor
    /// * `offset` - File offset
    /// * `data` - Data to write
    ///
    /// # Returns
    /// Number of bytes written.
    pub fn write(&self, fd: u64, offset: u64, data: &[u8]) -> Result<usize, DaemonError> {
        let data_buffer_size = self.data_buffer_size();
        if data.len() > data_buffer_size {
            return Err(DaemonError::BufferTooSmall {
                required: data.len(),
                available: data_buffer_size,
            });
        }

        // Copy data to shared memory buffer
        {
            let buffer = self.shm.data_buffer_mut(self.slot_id)?;
            buffer[..data.len()].copy_from_slice(data);
        }

        let mut request = RequestEntry::new();
        request.op_type = OpType::Write as u32;
        request.fd = fd;
        request.offset = offset;
        request.data_offset = 0;
        request.length = data.len() as u64;

        let response = self.send_request(request)?;

        if response.is_success() {
            Ok(response.result as usize)
        } else {
            Err(DaemonError::RequestFailed {
                status: response.status,
                message: format!("write({}, {}, {}) failed", fd, offset, data.len()),
            })
        }
    }

    /// Delete a file.
    ///
    /// # Arguments
    /// * `path` - File path
    pub fn unlink(&self, path: &str) -> Result<(), DaemonError> {
        let mut request = RequestEntry::new();
        request.op_type = OpType::Unlink as u32;

        let mut data_offset = 0;
        self.set_path(&mut request, path, &mut data_offset)?;

        let response = self.send_request(request)?;

        if response.is_success() {
            Ok(())
        } else {
            Err(DaemonError::RequestFailed {
                status: response.status,
                message: format!("unlink({}) failed", path),
            })
        }
    }

    /// Get file size.
    ///
    /// # Arguments
    /// * `path` - File path
    ///
    /// # Returns
    /// File size in bytes.
    pub fn stat(&self, path: &str) -> Result<u64, DaemonError> {
        let mut request = RequestEntry::new();
        request.op_type = OpType::Stat as u32;

        let mut data_offset = 0;
        self.set_path(&mut request, path, &mut data_offset)?;

        let response = self.send_request(request)?;

        if response.is_success() {
            Ok(response.result)
        } else {
            Err(DaemonError::RequestFailed {
                status: response.status,
                message: format!("stat({}) failed", path),
            })
        }
    }

    /// Create a directory.
    ///
    /// # Arguments
    /// * `path` - Directory path
    pub fn mkdir(&self, path: &str) -> Result<(), DaemonError> {
        let mut request = RequestEntry::new();
        request.op_type = OpType::Mkdir as u32;

        let mut data_offset = 0;
        self.set_path(&mut request, path, &mut data_offset)?;

        let response = self.send_request(request)?;

        if response.is_success() {
            Ok(())
        } else {
            Err(DaemonError::RequestFailed {
                status: response.status,
                message: format!("mkdir({}) failed", path),
            })
        }
    }

    /// Sync file to disk.
    ///
    /// # Arguments
    /// * `fd` - File descriptor
    pub fn fsync(&self, fd: u64) -> Result<(), DaemonError> {
        let mut request = RequestEntry::new();
        request.op_type = OpType::Fsync as u32;
        request.fd = fd;

        let response = self.send_request(request)?;

        if response.is_success() {
            Ok(())
        } else {
            Err(DaemonError::RequestFailed {
                status: response.status,
                message: format!("fsync({}) failed", fd),
            })
        }
    }

    /// Truncate a file.
    ///
    /// # Arguments
    /// * `path` - File path
    /// * `size` - New size
    pub fn truncate(&self, path: &str, size: u64) -> Result<(), DaemonError> {
        let mut request = RequestEntry::new();
        request.op_type = OpType::Truncate as u32;
        request.length = size;

        let mut data_offset = 0;
        self.set_path(&mut request, path, &mut data_offset)?;

        let response = self.send_request(request)?;

        if response.is_success() {
            Ok(())
        } else {
            Err(DaemonError::RequestFailed {
                status: response.status,
                message: format!("truncate({}, {}) failed", path, size),
            })
        }
    }

    /// Seek to position.
    ///
    /// # Arguments
    /// * `fd` - File descriptor
    /// * `offset` - New position
    ///
    /// # Returns
    /// New position.
    pub fn seek(&self, fd: u64, offset: i64) -> Result<u64, DaemonError> {
        let mut request = RequestEntry::new();
        request.op_type = OpType::Seek as u32;
        request.fd = fd;
        request.offset = offset as u64;

        let response = self.send_request(request)?;

        if response.is_success() {
            Ok(response.result)
        } else {
            Err(DaemonError::RequestFailed {
                status: response.status,
                message: format!("seek({}, {}) failed", fd, offset),
            })
        }
    }

    /// Disconnect from daemon.
    pub fn disconnect(&self) -> Result<(), DaemonError> {
        let mut request = RequestEntry::new();
        request.op_type = OpType::Disconnect as u32;

        let _ = self.send_request(request);
        Ok(())
    }
}

impl Drop for DaemonClientStub {
    fn drop(&mut self) {
        // Send disconnect request
        let _ = self.disconnect();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_id_allocation() {
        // This test just verifies the AtomicU64 behavior
        let counter = AtomicU64::new(1);
        assert_eq!(counter.fetch_add(1, Ordering::Relaxed), 1);
        assert_eq!(counter.fetch_add(1, Ordering::Relaxed), 2);
        assert_eq!(counter.fetch_add(1, Ordering::Relaxed), 3);
    }
}
