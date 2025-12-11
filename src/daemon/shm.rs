//! Shared memory management for client-daemon communication.
//!
//! This module handles creation, attachment, and management of POSIX shared memory
//! regions used for IPC between IOR client processes and the BenchFS daemon.

use std::ffi::CString;
use std::io;
use std::ptr::NonNull;
use std::sync::atomic::Ordering;

use super::protocol::{
    GlobalControlBlock, RequestEntry, ResponseEntry, SlotControlBlock, DEFAULT_DATA_BUFFER_SIZE,
    DEFAULT_NUM_SLOTS, DEFAULT_REQUEST_RING_SIZE, DEFAULT_RESPONSE_RING_SIZE, MAX_SLOTS,
    SHM_MAGIC, SLOT_BITMAP_WORDS,
};

/// Error type for shared memory operations
#[derive(Debug)]
pub enum ShmError {
    /// Failed to create shared memory
    CreateFailed(io::Error),
    /// Failed to open shared memory
    OpenFailed(io::Error),
    /// Failed to set shared memory size
    TruncateFailed(io::Error),
    /// Failed to map shared memory
    MmapFailed(io::Error),
    /// Invalid magic number
    InvalidMagic,
    /// Version mismatch
    VersionMismatch,
    /// No free slots available
    NoFreeSlots,
    /// Invalid slot ID
    InvalidSlot,
    /// Failed to acquire lock
    LockFailed(io::Error),
    /// Invalid configuration
    InvalidConfig(String),
}

impl std::fmt::Display for ShmError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ShmError::CreateFailed(e) => write!(f, "Failed to create shared memory: {}", e),
            ShmError::OpenFailed(e) => write!(f, "Failed to open shared memory: {}", e),
            ShmError::TruncateFailed(e) => write!(f, "Failed to set shared memory size: {}", e),
            ShmError::MmapFailed(e) => write!(f, "Failed to map shared memory: {}", e),
            ShmError::InvalidMagic => write!(f, "Invalid shared memory magic number"),
            ShmError::VersionMismatch => write!(f, "Shared memory version mismatch"),
            ShmError::NoFreeSlots => write!(f, "No free slots available"),
            ShmError::InvalidSlot => write!(f, "Invalid slot ID"),
            ShmError::LockFailed(e) => write!(f, "Failed to acquire lock: {}", e),
            ShmError::InvalidConfig(msg) => write!(f, "Invalid configuration: {}", msg),
        }
    }
}

impl std::error::Error for ShmError {}

/// Configuration for shared memory region
#[derive(Debug, Clone)]
pub struct ShmConfig {
    /// Number of client slots
    pub num_slots: u32,
    /// Data buffer size per slot
    pub data_buffer_size: usize,
    /// Request ring size (number of entries)
    pub request_ring_size: u32,
    /// Response ring size (number of entries)
    pub response_ring_size: u32,
}

impl Default for ShmConfig {
    fn default() -> Self {
        Self {
            num_slots: DEFAULT_NUM_SLOTS,
            data_buffer_size: DEFAULT_DATA_BUFFER_SIZE,
            request_ring_size: DEFAULT_REQUEST_RING_SIZE,
            response_ring_size: DEFAULT_RESPONSE_RING_SIZE,
        }
    }
}

impl ShmConfig {
    /// Validate the configuration
    pub fn validate(&self) -> Result<(), ShmError> {
        if self.num_slots == 0 || self.num_slots > MAX_SLOTS {
            return Err(ShmError::InvalidConfig(format!(
                "num_slots must be between 1 and {}",
                MAX_SLOTS
            )));
        }
        if self.data_buffer_size == 0 {
            return Err(ShmError::InvalidConfig(
                "data_buffer_size must be > 0".to_string(),
            ));
        }
        if self.request_ring_size == 0 || !self.request_ring_size.is_power_of_two() {
            return Err(ShmError::InvalidConfig(
                "request_ring_size must be a power of 2".to_string(),
            ));
        }
        if self.response_ring_size == 0 || !self.response_ring_size.is_power_of_two() {
            return Err(ShmError::InvalidConfig(
                "response_ring_size must be a power of 2".to_string(),
            ));
        }
        Ok(())
    }

    /// Calculate the size of a single slot
    pub fn slot_size(&self) -> usize {
        let control_size = std::mem::size_of::<SlotControlBlock>();
        let request_ring_size =
            std::mem::size_of::<RequestEntry>() * self.request_ring_size as usize;
        let response_ring_size =
            std::mem::size_of::<ResponseEntry>() * self.response_ring_size as usize;
        control_size + request_ring_size + response_ring_size + self.data_buffer_size
    }

    /// Calculate total shared memory size
    pub fn total_size(&self) -> usize {
        std::mem::size_of::<GlobalControlBlock>() + self.slot_size() * self.num_slots as usize
    }
}

/// Shared memory region handle.
/// Can be used by both daemon (creator) and clients (attachers).
pub struct SharedMemoryRegion {
    /// Shared memory name
    name: String,
    /// Base pointer to mapped region
    base_ptr: NonNull<u8>,
    /// Total size of the region
    total_size: usize,
    /// Whether this instance owns (created) the shared memory
    is_owner: bool,
    /// Configuration
    config: ShmConfig,
}

// SAFETY: SharedMemoryRegion can be sent between threads because:
// - The shared memory region is process-shared, not thread-specific
// - All access to shared data uses atomic operations
unsafe impl Send for SharedMemoryRegion {}

// SAFETY: SharedMemoryRegion can be shared between threads because:
// - All access to shared data uses atomic operations
// - The underlying shared memory is designed for concurrent access
unsafe impl Sync for SharedMemoryRegion {}

impl SharedMemoryRegion {
    /// Create a new shared memory region (daemon side).
    ///
    /// # Arguments
    /// * `name` - Shared memory name (will be prefixed with '/' if needed)
    /// * `config` - Configuration for the shared memory region
    pub fn create(name: &str, config: ShmConfig) -> Result<Self, ShmError> {
        config.validate()?;

        let shm_name = Self::normalize_name(name);
        let c_name =
            CString::new(shm_name.as_str()).map_err(|_| ShmError::InvalidConfig("Invalid shared memory name".to_string()))?;

        // Create shared memory
        let fd = unsafe {
            libc::shm_open(
                c_name.as_ptr(),
                libc::O_CREAT | libc::O_RDWR | libc::O_EXCL,
                0o600,
            )
        };

        if fd < 0 {
            return Err(ShmError::CreateFailed(io::Error::last_os_error()));
        }

        let total_size = config.total_size();

        // Set size
        let ret = unsafe { libc::ftruncate(fd, total_size as libc::off_t) };
        if ret < 0 {
            let err = io::Error::last_os_error();
            unsafe {
                libc::close(fd);
                libc::shm_unlink(c_name.as_ptr());
            }
            return Err(ShmError::TruncateFailed(err));
        }

        // Map the region
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                total_size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd,
                0,
            )
        };

        // Close fd (mmap keeps reference)
        unsafe { libc::close(fd) };

        if ptr == libc::MAP_FAILED {
            unsafe { libc::shm_unlink(c_name.as_ptr()) };
            return Err(ShmError::MmapFailed(io::Error::last_os_error()));
        }

        let base_ptr =
            NonNull::new(ptr as *mut u8).expect("mmap returned null pointer but not MAP_FAILED");

        // Initialize the control block
        let control = base_ptr.as_ptr() as *mut GlobalControlBlock;
        unsafe {
            std::ptr::write(
                control,
                GlobalControlBlock::init(
                    config.num_slots,
                    config.slot_size() as u64,
                    config.data_buffer_size as u64,
                    config.request_ring_size,
                    config.response_ring_size,
                ),
            );
        }

        // Initialize all slot control blocks
        let slot_size = config.slot_size();
        for i in 0..config.num_slots {
            let slot_offset = std::mem::size_of::<GlobalControlBlock>() + slot_size * i as usize;
            let slot_ptr = unsafe { base_ptr.as_ptr().add(slot_offset) } as *mut SlotControlBlock;
            unsafe {
                std::ptr::write(slot_ptr, SlotControlBlock::init());
            }
        }

        Ok(Self {
            name: shm_name,
            base_ptr,
            total_size,
            is_owner: true,
            config,
        })
    }

    /// Attach to an existing shared memory region (client side).
    ///
    /// # Arguments
    /// * `name` - Shared memory name
    pub fn attach(name: &str) -> Result<Self, ShmError> {
        let shm_name = Self::normalize_name(name);
        let c_name =
            CString::new(shm_name.as_str()).map_err(|_| ShmError::InvalidConfig("Invalid shared memory name".to_string()))?;

        // Open shared memory
        let fd = unsafe { libc::shm_open(c_name.as_ptr(), libc::O_RDWR, 0) };

        if fd < 0 {
            return Err(ShmError::OpenFailed(io::Error::last_os_error()));
        }

        // First, map just the control block to read configuration
        let control_size = std::mem::size_of::<GlobalControlBlock>();
        let control_ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                control_size,
                libc::PROT_READ,
                libc::MAP_SHARED,
                fd,
                0,
            )
        };

        if control_ptr == libc::MAP_FAILED {
            unsafe { libc::close(fd) };
            return Err(ShmError::MmapFailed(io::Error::last_os_error()));
        }

        let control = control_ptr as *const GlobalControlBlock;

        // Validate and read configuration
        let (num_slots, slot_size, data_buffer_size, request_ring_size, response_ring_size) =
            unsafe {
                if (*control).magic != SHM_MAGIC {
                    libc::munmap(control_ptr, control_size);
                    libc::close(fd);
                    return Err(ShmError::InvalidMagic);
                }

                (
                    (*control).num_slots,
                    (*control).slot_size,
                    (*control).data_buffer_size,
                    (*control).request_ring_size,
                    (*control).response_ring_size,
                )
            };

        // Unmap control block
        unsafe { libc::munmap(control_ptr, control_size) };

        // Calculate total size and remap everything
        let total_size = control_size + slot_size as usize * num_slots as usize;

        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                total_size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd,
                0,
            )
        };

        unsafe { libc::close(fd) };

        if ptr == libc::MAP_FAILED {
            return Err(ShmError::MmapFailed(io::Error::last_os_error()));
        }

        let base_ptr =
            NonNull::new(ptr as *mut u8).expect("mmap returned null pointer but not MAP_FAILED");

        let config = ShmConfig {
            num_slots,
            data_buffer_size: data_buffer_size as usize,
            request_ring_size,
            response_ring_size,
        };

        Ok(Self {
            name: shm_name,
            base_ptr,
            total_size,
            is_owner: false,
            config,
        })
    }

    /// Try to open an existing shared memory region.
    /// Returns Ok(Some(region)) if exists, Ok(None) if doesn't exist.
    pub fn try_attach(name: &str) -> Result<Option<Self>, ShmError> {
        match Self::attach(name) {
            Ok(region) => Ok(Some(region)),
            Err(ShmError::OpenFailed(e)) if e.raw_os_error() == Some(libc::ENOENT) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Normalize shared memory name (ensure it starts with '/')
    fn normalize_name(name: &str) -> String {
        if name.starts_with('/') {
            name.to_string()
        } else {
            format!("/{}", name)
        }
    }

    /// Get the shared memory name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the total size
    pub fn total_size(&self) -> usize {
        self.total_size
    }

    /// Get the configuration
    pub fn config(&self) -> &ShmConfig {
        &self.config
    }

    /// Get reference to the global control block
    pub fn control(&self) -> &GlobalControlBlock {
        unsafe { &*(self.base_ptr.as_ptr() as *const GlobalControlBlock) }
    }

    /// Get mutable reference to the global control block
    pub fn control_mut(&self) -> &mut GlobalControlBlock {
        unsafe { &mut *(self.base_ptr.as_ptr() as *mut GlobalControlBlock) }
    }

    /// Check if the daemon is ready
    pub fn is_daemon_ready(&self) -> bool {
        self.control().ready_flag.load(Ordering::Acquire) == 1
    }

    /// Set the daemon ready flag (daemon side)
    pub fn set_daemon_ready(&self, pid: u32) {
        self.control().daemon_pid.store(pid, Ordering::Release);
        self.control().ready_flag.store(1, Ordering::Release);
    }

    /// Request daemon shutdown
    pub fn request_shutdown(&self) {
        self.control().shutdown_flag.store(1, Ordering::Release);
    }

    /// Check if shutdown is requested
    pub fn is_shutdown_requested(&self) -> bool {
        self.control().shutdown_flag.load(Ordering::Acquire) == 1
    }

    /// Allocate a slot for a client (lock-free).
    /// Returns the slot ID on success.
    pub fn allocate_slot(&self) -> Result<u32, ShmError> {
        let control = self.control();

        for word_idx in 0..SLOT_BITMAP_WORDS {
            let bitmap = &control.slot_bitmap[word_idx];

            loop {
                let current = bitmap.load(Ordering::Acquire);
                if current == u64::MAX {
                    // All 64 bits in this word are allocated
                    break;
                }

                // Find first free bit
                let free_bit = (!current).trailing_zeros();
                let slot_id = word_idx as u32 * 64 + free_bit;

                if slot_id >= self.config.num_slots {
                    // Beyond configured slots
                    break;
                }

                let new_value = current | (1u64 << free_bit);

                if bitmap
                    .compare_exchange(current, new_value, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    return Ok(slot_id);
                }
                // CAS failed, retry
            }
        }

        Err(ShmError::NoFreeSlots)
    }

    /// Release a slot (lock-free).
    pub fn release_slot(&self, slot_id: u32) -> Result<(), ShmError> {
        if slot_id >= self.config.num_slots {
            return Err(ShmError::InvalidSlot);
        }

        let word_idx = (slot_id / 64) as usize;
        let bit_idx = slot_id % 64;
        let bitmap = &self.control().slot_bitmap[word_idx];

        loop {
            let current = bitmap.load(Ordering::Acquire);
            let new_value = current & !(1u64 << bit_idx);

            if bitmap
                .compare_exchange(current, new_value, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                // Clear the slot control block
                let slot = self.slot_control_mut(slot_id)?;
                slot.client_pid.store(0, Ordering::Release);
                slot.request_head.store(0, Ordering::Release);
                slot.request_tail.store(0, Ordering::Release);
                slot.response_head.store(0, Ordering::Release);
                slot.response_tail.store(0, Ordering::Release);
                return Ok(());
            }
        }
    }

    /// Get pointer to slot base
    fn slot_base(&self, slot_id: u32) -> Result<*mut u8, ShmError> {
        if slot_id >= self.config.num_slots {
            return Err(ShmError::InvalidSlot);
        }

        let slot_offset = std::mem::size_of::<GlobalControlBlock>()
            + self.config.slot_size() * slot_id as usize;

        Ok(unsafe { self.base_ptr.as_ptr().add(slot_offset) })
    }

    /// Get reference to slot control block
    pub fn slot_control(&self, slot_id: u32) -> Result<&SlotControlBlock, ShmError> {
        let ptr = self.slot_base(slot_id)? as *const SlotControlBlock;
        Ok(unsafe { &*ptr })
    }

    /// Get mutable reference to slot control block
    pub fn slot_control_mut(&self, slot_id: u32) -> Result<&mut SlotControlBlock, ShmError> {
        let ptr = self.slot_base(slot_id)? as *mut SlotControlBlock;
        Ok(unsafe { &mut *ptr })
    }

    /// Get pointer to request ring for a slot
    pub fn request_ring_ptr(&self, slot_id: u32) -> Result<*mut RequestEntry, ShmError> {
        let slot_base = self.slot_base(slot_id)?;
        let offset = std::mem::size_of::<SlotControlBlock>();
        Ok(unsafe { slot_base.add(offset) as *mut RequestEntry })
    }

    /// Get pointer to response ring for a slot
    pub fn response_ring_ptr(&self, slot_id: u32) -> Result<*mut ResponseEntry, ShmError> {
        let slot_base = self.slot_base(slot_id)?;
        let offset = std::mem::size_of::<SlotControlBlock>()
            + std::mem::size_of::<RequestEntry>() * self.config.request_ring_size as usize;
        Ok(unsafe { slot_base.add(offset) as *mut ResponseEntry })
    }

    /// Get pointer to data buffer for a slot
    pub fn data_buffer_ptr(&self, slot_id: u32) -> Result<*mut u8, ShmError> {
        let slot_base = self.slot_base(slot_id)?;
        let offset = std::mem::size_of::<SlotControlBlock>()
            + std::mem::size_of::<RequestEntry>() * self.config.request_ring_size as usize
            + std::mem::size_of::<ResponseEntry>() * self.config.response_ring_size as usize;
        Ok(unsafe { slot_base.add(offset) })
    }

    /// Get data buffer as a slice
    pub fn data_buffer(&self, slot_id: u32) -> Result<&[u8], ShmError> {
        let ptr = self.data_buffer_ptr(slot_id)?;
        Ok(unsafe { std::slice::from_raw_parts(ptr, self.config.data_buffer_size) })
    }

    /// Get data buffer as a mutable slice
    pub fn data_buffer_mut(&self, slot_id: u32) -> Result<&mut [u8], ShmError> {
        let ptr = self.data_buffer_ptr(slot_id)?;
        Ok(unsafe { std::slice::from_raw_parts_mut(ptr, self.config.data_buffer_size) })
    }
}

impl Drop for SharedMemoryRegion {
    fn drop(&mut self) {
        // Unmap the region
        unsafe {
            libc::munmap(self.base_ptr.as_ptr() as *mut libc::c_void, self.total_size);
        }

        // If we created the shared memory, unlink it
        if self.is_owner {
            if let Ok(c_name) = CString::new(self.name.as_str()) {
                unsafe {
                    libc::shm_unlink(c_name.as_ptr());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shm_config_validation() {
        let mut config = ShmConfig::default();
        assert!(config.validate().is_ok());

        config.num_slots = 0;
        assert!(config.validate().is_err());

        config.num_slots = MAX_SLOTS + 1;
        assert!(config.validate().is_err());

        config = ShmConfig::default();
        config.request_ring_size = 3; // Not power of 2
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_slot_size_calculation() {
        let config = ShmConfig {
            num_slots: 1,
            data_buffer_size: 4 * 1024 * 1024,
            request_ring_size: 64,
            response_ring_size: 64,
        };

        let expected_slot_size = 64 // SlotControlBlock
            + 128 * 64 // RequestRing
            + 64 * 64  // ResponseRing
            + 4 * 1024 * 1024; // DataBuffer

        assert_eq!(config.slot_size(), expected_slot_size);
    }

    #[test]
    fn test_shm_name_normalization() {
        assert_eq!(
            SharedMemoryRegion::normalize_name("test"),
            "/test".to_string()
        );
        assert_eq!(
            SharedMemoryRegion::normalize_name("/test"),
            "/test".to_string()
        );
    }
}
