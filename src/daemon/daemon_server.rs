//! Client daemon server implementation.
//!
//! The daemon runs as a separate process and handles requests from multiple
//! IOR client processes via shared memory ring buffers.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::atomic::Ordering;

use tracing::{debug, error, info, trace};

use super::error::{errno, DaemonError};
use super::protocol::{OpType, RequestEntry, ResponseEntry, INLINE_PATH_LEN};
use super::ring::{RequestRing, ResponseRing};
use super::shm::{SharedMemoryRegion, ShmConfig};

use crate::api::file_ops::BenchFS;
use crate::api::types::{ApiError, FileHandle, OpenFlags};
use crate::rpc::connection::ConnectionPool;

// POSIX open flags
const O_RDONLY: u32 = 0x0000;
const O_WRONLY: u32 = 0x0001;
const O_RDWR: u32 = 0x0002;
const O_CREAT: u32 = 0x0040;
const O_TRUNC: u32 = 0x0200;
const O_APPEND: u32 = 0x0400;

/// Convert raw flags to OpenFlags
fn flags_to_open_flags(flags: u32) -> OpenFlags {
    let access_mode = flags & 0x3;
    OpenFlags {
        read: access_mode == O_RDONLY || access_mode == O_RDWR,
        write: access_mode == O_WRONLY || access_mode == O_RDWR,
        create: (flags & O_CREAT) != 0,
        truncate: (flags & O_TRUNC) != 0,
        append: (flags & O_APPEND) != 0,
    }
}

/// File descriptor info stored by the daemon
struct FdInfo {
    /// File handle from BenchFS
    handle: FileHandle,
    /// Slot ID that owns this fd
    slot_id: u32,
}

/// Client daemon server.
///
/// The daemon polls shared memory slots for requests from client processes
/// and forwards them to BenchFS servers via UCX.
pub struct ClientDaemon {
    /// Shared memory region
    shm: SharedMemoryRegion,
    /// BenchFS client for actual I/O operations
    benchfs: Rc<BenchFS>,
    /// Connection pool for RPC
    connection_pool: Rc<ConnectionPool>,
    /// File descriptor mapping (global fd -> FdInfo)
    fd_map: RefCell<HashMap<u64, FdInfo>>,
    /// Next file descriptor to allocate
    next_fd: RefCell<u64>,
    /// Shutdown flag
    shutdown: RefCell<bool>,
}

impl ClientDaemon {
    /// Create a new client daemon.
    ///
    /// # Arguments
    /// * `shm_name` - Shared memory name
    /// * `config` - Shared memory configuration
    /// * `benchfs` - BenchFS client instance
    /// * `connection_pool` - Connection pool for RPC
    pub fn new(
        shm_name: &str,
        config: ShmConfig,
        benchfs: Rc<BenchFS>,
        connection_pool: Rc<ConnectionPool>,
    ) -> Result<Self, DaemonError> {
        let shm = SharedMemoryRegion::create(shm_name, config)?;

        Ok(Self {
            shm,
            benchfs,
            connection_pool,
            fd_map: RefCell::new(HashMap::new()),
            next_fd: RefCell::new(1), // Start from 1, 0 is invalid
            shutdown: RefCell::new(false),
        })
    }

    /// Attach to existing shared memory (for testing or recovery).
    pub fn attach(
        shm_name: &str,
        benchfs: Rc<BenchFS>,
        connection_pool: Rc<ConnectionPool>,
    ) -> Result<Self, DaemonError> {
        let shm = SharedMemoryRegion::attach(shm_name)?;

        Ok(Self {
            shm,
            benchfs,
            connection_pool,
            fd_map: RefCell::new(HashMap::new()),
            next_fd: RefCell::new(1),
            shutdown: RefCell::new(false),
        })
    }

    /// Get the shared memory name.
    pub fn shm_name(&self) -> &str {
        self.shm.name()
    }

    /// Mark the daemon as ready.
    pub fn set_ready(&self) {
        let pid = std::process::id();
        self.shm.set_daemon_ready(pid);
        info!("Daemon ready (pid={})", pid);
    }

    /// Request daemon shutdown.
    pub fn request_shutdown(&self) {
        *self.shutdown.borrow_mut() = true;
        self.shm.request_shutdown();
    }

    /// Check if shutdown is requested.
    pub fn is_shutdown_requested(&self) -> bool {
        *self.shutdown.borrow() || self.shm.is_shutdown_requested()
    }

    /// Run the daemon main loop (blocking).
    ///
    /// This function polls all slots for requests and processes them.
    /// It uses busy-polling for minimum latency.
    pub async fn run(&self) -> Result<(), DaemonError> {
        info!("Starting daemon main loop");

        let num_slots = self.shm.config().num_slots;

        while !self.is_shutdown_requested() {
            let mut processed_any = false;

            // Poll all slots
            for slot_id in 0..num_slots {
                // Check if slot is allocated
                let slot_control = self.shm.slot_control(slot_id)?;
                let client_pid = slot_control.client_pid.load(Ordering::Acquire);
                if client_pid == 0 {
                    continue;
                }

                // Process requests from this slot
                let req_ring = RequestRing::new(&self.shm, slot_id)?;
                let resp_ring = ResponseRing::new(&self.shm, slot_id)?;

                while let Some(request) = req_ring.try_pop() {
                    processed_any = true;
                    let response = self.handle_request(slot_id, &request).await;

                    // Push response (spin if full)
                    while !resp_ring.push(&response) {
                        std::hint::spin_loop();

                        // Check for shutdown while waiting
                        if self.is_shutdown_requested() {
                            break;
                        }
                    }
                }
            }

            // If no requests were processed, yield to avoid 100% CPU
            if !processed_any {
                std::hint::spin_loop();
            }
        }

        info!("Daemon main loop exited");
        Ok(())
    }

    /// Handle a single request.
    async fn handle_request(&self, slot_id: u32, request: &RequestEntry) -> ResponseEntry {
        let request_id = request.request_id;
        let op = request.op();

        trace!(
            "Handling request: id={}, op={:?}, slot={}",
            request_id,
            op,
            slot_id
        );

        let result = match op {
            OpType::Open => self.handle_open(slot_id, request).await,
            OpType::Close => self.handle_close(slot_id, request).await,
            OpType::Read => self.handle_read(slot_id, request).await,
            OpType::Write => self.handle_write(slot_id, request).await,
            OpType::Unlink => self.handle_unlink(slot_id, request).await,
            OpType::Stat => self.handle_stat(slot_id, request).await,
            OpType::Mkdir => self.handle_mkdir(slot_id, request).await,
            OpType::Fsync => self.handle_fsync(slot_id, request).await,
            OpType::Truncate => self.handle_truncate(slot_id, request).await,
            OpType::Seek => self.handle_seek(slot_id, request).await,
            OpType::Disconnect => {
                self.handle_disconnect(slot_id).await;
                ResponseEntry::success(request_id, 0)
            }
            OpType::Nop => ResponseEntry::error(request_id, errno::EINVAL),
        };

        result
    }

    /// Get path from request (inline or from data buffer).
    fn get_path(&self, slot_id: u32, request: &RequestEntry) -> Result<String, DaemonError> {
        if request.path_len == 0 {
            return Err(DaemonError::InvalidOperation(
                "Path is required".to_string(),
            ));
        }

        if request.path_len as usize <= INLINE_PATH_LEN {
            // Path is inline
            let path = std::str::from_utf8(&request.path[..request.path_len as usize])
                .map_err(|_| DaemonError::InvalidOperation("Invalid UTF-8 in path".to_string()))?;
            Ok(path.to_string())
        } else {
            // Path is in data buffer
            let buffer = self.shm.data_buffer(slot_id)?;
            let path_offset = request.data_offset as usize;
            let path_len = request.path_len as usize;

            if path_offset + path_len > buffer.len() {
                return Err(DaemonError::BufferTooSmall {
                    required: path_offset + path_len,
                    available: buffer.len(),
                });
            }

            let path = std::str::from_utf8(&buffer[path_offset..path_offset + path_len])
                .map_err(|_| DaemonError::InvalidOperation("Invalid UTF-8 in path".to_string()))?;
            Ok(path.to_string())
        }
    }

    /// Allocate a new file descriptor.
    fn alloc_fd(&self, handle: FileHandle, slot_id: u32) -> u64 {
        let fd = *self.next_fd.borrow();
        *self.next_fd.borrow_mut() = fd.wrapping_add(1);
        if *self.next_fd.borrow() == 0 {
            *self.next_fd.borrow_mut() = 1;
        }

        self.fd_map.borrow_mut().insert(
            fd,
            FdInfo {
                handle,
                slot_id,
            },
        );

        fd
    }

    /// Get file handle by fd.
    fn get_handle(&self, fd: u64, slot_id: u32) -> Result<FileHandle, i32> {
        let fd_map = self.fd_map.borrow();
        match fd_map.get(&fd) {
            Some(info) if info.slot_id == slot_id => Ok(info.handle.clone()),
            Some(_) => Err(errno::EBADF), // Wrong slot
            None => Err(errno::EBADF),
        }
    }

    /// Close a file descriptor.
    fn close_fd(&self, fd: u64, slot_id: u32) -> Result<FileHandle, i32> {
        let mut fd_map = self.fd_map.borrow_mut();
        match fd_map.remove(&fd) {
            Some(info) if info.slot_id == slot_id => Ok(info.handle),
            Some(info) => {
                // Put it back, wrong slot
                fd_map.insert(fd, info);
                Err(errno::EBADF)
            }
            None => Err(errno::EBADF),
        }
    }

    /// Handle Open request.
    async fn handle_open(&self, slot_id: u32, request: &RequestEntry) -> ResponseEntry {
        let request_id = request.request_id;

        let path = match self.get_path(slot_id, request) {
            Ok(p) => p,
            Err(e) => {
                error!("Failed to get path: {}", e);
                return ResponseEntry::error(request_id, errno::EINVAL);
            }
        };

        let flags = flags_to_open_flags(request.flags);

        debug!("Open: path={}, flags={:?}", path, flags);

        match self.benchfs.benchfs_open(&path, flags).await {
            Ok(handle) => {
                let fd = self.alloc_fd(handle, slot_id);
                ResponseEntry::success(request_id, fd)
            }
            Err(e) => {
                let status = api_error_to_errno(&e);
                debug!("Open failed: {} (errno={})", e, status);
                ResponseEntry::error(request_id, status)
            }
        }
    }

    /// Handle Close request.
    async fn handle_close(&self, slot_id: u32, request: &RequestEntry) -> ResponseEntry {
        let request_id = request.request_id;
        let fd = request.fd;

        debug!("Close: fd={}", fd);

        match self.close_fd(fd, slot_id) {
            Ok(handle) => {
                match self.benchfs.benchfs_close(&handle).await {
                    Ok(()) => ResponseEntry::success(request_id, 0),
                    Err(e) => {
                        let status = api_error_to_errno(&e);
                        ResponseEntry::error(request_id, status)
                    }
                }
            }
            Err(status) => ResponseEntry::error(request_id, status),
        }
    }

    /// Handle Read request.
    async fn handle_read(&self, slot_id: u32, request: &RequestEntry) -> ResponseEntry {
        let request_id = request.request_id;
        let fd = request.fd;
        let offset = request.offset;
        let length = request.length as usize;

        debug!("Read: fd={}, offset={}, length={}", fd, offset, length);

        // Get handle
        let handle = match self.get_handle(fd, slot_id) {
            Ok(h) => h,
            Err(status) => return ResponseEntry::error(request_id, status),
        };

        // Get data buffer for response
        let buffer = match self.shm.data_buffer_mut(slot_id) {
            Ok(b) => b,
            Err(_) => return ResponseEntry::error(request_id, errno::EIO),
        };

        if length > buffer.len() {
            return ResponseEntry::error(request_id, errno::EINVAL);
        }

        // Seek to offset (SEEK_SET = 0)
        if let Err(e) = self.benchfs.benchfs_seek(&handle, offset as i64, 0) {
            let status = api_error_to_errno(&e);
            return ResponseEntry::error(request_id, status);
        }

        // Read data
        match self.benchfs.benchfs_read(&handle, &mut buffer[..length]).await {
            Ok(bytes_read) => {
                ResponseEntry::success_with_data(request_id, bytes_read as u64, 0, bytes_read as u64)
            }
            Err(e) => {
                let status = api_error_to_errno(&e);
                ResponseEntry::error(request_id, status)
            }
        }
    }

    /// Handle Write request.
    async fn handle_write(&self, slot_id: u32, request: &RequestEntry) -> ResponseEntry {
        let request_id = request.request_id;
        let fd = request.fd;
        let offset = request.offset;
        let data_offset = request.data_offset as usize;
        let length = request.length as usize;

        debug!(
            "Write: fd={}, offset={}, data_offset={}, length={}",
            fd, offset, data_offset, length
        );

        // Get handle
        let handle = match self.get_handle(fd, slot_id) {
            Ok(h) => h,
            Err(status) => return ResponseEntry::error(request_id, status),
        };

        // Get data from shared memory buffer (zero-copy reference)
        let buffer = match self.shm.data_buffer(slot_id) {
            Ok(b) => b,
            Err(_) => return ResponseEntry::error(request_id, errno::EIO),
        };

        if data_offset + length > buffer.len() {
            return ResponseEntry::error(request_id, errno::EINVAL);
        }

        let data = &buffer[data_offset..data_offset + length];

        // Seek to offset (SEEK_SET = 0)
        if let Err(e) = self.benchfs.benchfs_seek(&handle, offset as i64, 0) {
            let status = api_error_to_errno(&e);
            return ResponseEntry::error(request_id, status);
        }

        // Write data
        match self.benchfs.benchfs_write(&handle, data).await {
            Ok(bytes_written) => ResponseEntry::success(request_id, bytes_written as u64),
            Err(e) => {
                let status = api_error_to_errno(&e);
                ResponseEntry::error(request_id, status)
            }
        }
    }

    /// Handle Unlink request.
    async fn handle_unlink(&self, slot_id: u32, request: &RequestEntry) -> ResponseEntry {
        let request_id = request.request_id;

        let path = match self.get_path(slot_id, request) {
            Ok(p) => p,
            Err(_) => return ResponseEntry::error(request_id, errno::EINVAL),
        };

        debug!("Unlink: path={}", path);

        match self.benchfs.benchfs_unlink(&path).await {
            Ok(()) => ResponseEntry::success(request_id, 0),
            Err(e) => {
                let status = api_error_to_errno(&e);
                ResponseEntry::error(request_id, status)
            }
        }
    }

    /// Handle Stat request.
    async fn handle_stat(&self, slot_id: u32, request: &RequestEntry) -> ResponseEntry {
        let request_id = request.request_id;

        let path = match self.get_path(slot_id, request) {
            Ok(p) => p,
            Err(_) => return ResponseEntry::error(request_id, errno::EINVAL),
        };

        debug!("Stat: path={}", path);

        match self.benchfs.benchfs_stat(&path).await {
            Ok(stat) => {
                // Return file size as result
                ResponseEntry::success(request_id, stat.size)
            }
            Err(e) => {
                let status = api_error_to_errno(&e);
                ResponseEntry::error(request_id, status)
            }
        }
    }

    /// Handle Mkdir request.
    async fn handle_mkdir(&self, slot_id: u32, request: &RequestEntry) -> ResponseEntry {
        let request_id = request.request_id;

        let path = match self.get_path(slot_id, request) {
            Ok(p) => p,
            Err(_) => return ResponseEntry::error(request_id, errno::EINVAL),
        };

        debug!("Mkdir: path={}", path);

        match self.benchfs.benchfs_mkdir(&path, 0o755).await {
            Ok(()) => ResponseEntry::success(request_id, 0),
            Err(e) => {
                let status = api_error_to_errno(&e);
                ResponseEntry::error(request_id, status)
            }
        }
    }

    /// Handle Fsync request.
    async fn handle_fsync(&self, slot_id: u32, request: &RequestEntry) -> ResponseEntry {
        let request_id = request.request_id;
        let fd = request.fd;

        debug!("Fsync: fd={}", fd);

        let handle = match self.get_handle(fd, slot_id) {
            Ok(h) => h,
            Err(status) => return ResponseEntry::error(request_id, status),
        };

        match self.benchfs.benchfs_fsync(&handle).await {
            Ok(()) => ResponseEntry::success(request_id, 0),
            Err(e) => {
                let status = api_error_to_errno(&e);
                ResponseEntry::error(request_id, status)
            }
        }
    }

    /// Handle Truncate request.
    async fn handle_truncate(&self, slot_id: u32, request: &RequestEntry) -> ResponseEntry {
        let request_id = request.request_id;

        let path = match self.get_path(slot_id, request) {
            Ok(p) => p,
            Err(_) => return ResponseEntry::error(request_id, errno::EINVAL),
        };

        let size = request.length;

        debug!("Truncate: path={}, size={}", path, size);

        match self.benchfs.benchfs_truncate(&path, size).await {
            Ok(()) => ResponseEntry::success(request_id, 0),
            Err(e) => {
                let status = api_error_to_errno(&e);
                ResponseEntry::error(request_id, status)
            }
        }
    }

    /// Handle Seek request.
    async fn handle_seek(&self, slot_id: u32, request: &RequestEntry) -> ResponseEntry {
        let request_id = request.request_id;
        let fd = request.fd;
        let offset = request.offset as i64;

        debug!("Seek: fd={}, offset={}", fd, offset);

        let handle = match self.get_handle(fd, slot_id) {
            Ok(h) => h,
            Err(status) => return ResponseEntry::error(request_id, status),
        };

        // SEEK_SET = 0
        match self.benchfs.benchfs_seek(&handle, offset, 0) {
            Ok(new_pos) => ResponseEntry::success(request_id, new_pos),
            Err(e) => {
                let status = api_error_to_errno(&e);
                ResponseEntry::error(request_id, status)
            }
        }
    }

    /// Handle client disconnect.
    async fn handle_disconnect(&self, slot_id: u32) {
        debug!("Client disconnect: slot={}", slot_id);

        // Close all file descriptors owned by this slot
        let fds_to_close: Vec<u64> = self
            .fd_map
            .borrow()
            .iter()
            .filter(|(_, info)| info.slot_id == slot_id)
            .map(|(fd, _)| *fd)
            .collect();

        for fd in fds_to_close {
            if let Some(info) = self.fd_map.borrow_mut().remove(&fd) {
                let _ = self.benchfs.benchfs_close(&info.handle).await;
            }
        }

        // Release the slot
        let _ = self.shm.release_slot(slot_id);
    }
}

/// Convert ApiError to errno status code.
fn api_error_to_errno(error: &ApiError) -> i32 {
    let msg = error.to_string().to_lowercase();

    if msg.contains("not found") || msg.contains("no such") {
        errno::ENOENT
    } else if msg.contains("exists") || msg.contains("already") {
        errno::EEXIST
    } else if msg.contains("permission") || msg.contains("access") {
        errno::EACCES
    } else if msg.contains("invalid") {
        errno::EINVAL
    } else if msg.contains("no space") || msg.contains("full") {
        errno::ENOSPC
    } else if msg.contains("timeout") {
        errno::ETIMEDOUT
    } else if msg.contains("directory") {
        errno::EISDIR
    } else if msg.contains("busy") {
        errno::EBUSY
    } else {
        errno::EIO
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_api_error_to_errno() {
        // These are basic string matching tests
        let cases = vec![
            ("File not found", errno::ENOENT),
            ("No such file", errno::ENOENT),
            ("File already exists", errno::EEXIST),
            ("Permission denied", errno::EACCES),
            ("Invalid argument", errno::EINVAL),
            ("No space left", errno::ENOSPC),
            ("Operation timeout", errno::ETIMEDOUT),
            ("Is a directory", errno::EISDIR),
            ("Device busy", errno::EBUSY),
            ("Unknown error", errno::EIO),
        ];

        for (msg, expected) in cases {
            let error = ApiError::IoError(msg.to_string());
            assert_eq!(
                api_error_to_errno(&error),
                expected,
                "Failed for message: {}",
                msg
            );
        }
    }
}
