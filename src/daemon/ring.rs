//! Lock-free SPSC (Single Producer Single Consumer) ring buffer implementation.
//!
//! This module provides ring buffers for communication between clients and the daemon:
//! - RequestRing: Client (producer) -> Daemon (consumer)
//! - ResponseRing: Daemon (producer) -> Client (consumer)

use std::sync::atomic::{AtomicU32, Ordering};

use super::protocol::{RequestEntry, ResponseEntry};
use super::shm::{SharedMemoryRegion, ShmError};

/// Request ring buffer (client produces, daemon consumes).
pub struct RequestRing<'a> {
    /// Pointer to ring entries
    entries: *mut RequestEntry,
    /// Number of entries (must be power of 2)
    capacity: u32,
    /// Mask for index wrapping
    mask: u32,
    /// Head index (producer/client updates)
    head: &'a AtomicU32,
    /// Tail index (consumer/daemon updates)
    tail: &'a AtomicU32,
}

impl<'a> RequestRing<'a> {
    /// Create a new request ring from shared memory region.
    pub fn new(shm: &'a SharedMemoryRegion, slot_id: u32) -> Result<Self, ShmError> {
        let slot_control = shm.slot_control(slot_id)?;
        let entries = shm.request_ring_ptr(slot_id)?;
        let capacity = shm.config().request_ring_size;

        debug_assert!(capacity.is_power_of_two());

        Ok(Self {
            entries,
            capacity,
            mask: capacity - 1,
            head: &slot_control.request_head,
            tail: &slot_control.request_tail,
        })
    }

    /// Check if the ring is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);
        head == tail
    }

    /// Check if the ring is full.
    #[inline]
    pub fn is_full(&self) -> bool {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);
        head.wrapping_sub(tail) >= self.capacity
    }

    /// Get the number of entries in the ring.
    #[inline]
    pub fn len(&self) -> u32 {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);
        head.wrapping_sub(tail)
    }

    /// Get the capacity of the ring.
    #[inline]
    pub fn capacity(&self) -> u32 {
        self.capacity
    }

    /// Push a request entry (producer/client side).
    /// Returns false if the ring is full.
    pub fn push(&self, entry: &RequestEntry) -> bool {
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Acquire);

        // Check if full
        if head.wrapping_sub(tail) >= self.capacity {
            return false;
        }

        // Write entry
        let idx = (head & self.mask) as usize;
        unsafe {
            std::ptr::write_volatile(self.entries.add(idx), *entry);
        }

        // Update head (release ensures entry is visible before head update)
        self.head.store(head.wrapping_add(1), Ordering::Release);

        true
    }

    /// Try to pop a request entry (consumer/daemon side).
    /// Returns None if the ring is empty.
    pub fn try_pop(&self) -> Option<RequestEntry> {
        let tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Acquire);

        // Check if empty
        if tail == head {
            return None;
        }

        // Read entry
        let idx = (tail & self.mask) as usize;
        let entry = unsafe { std::ptr::read_volatile(self.entries.add(idx)) };

        // Update tail (release ensures we've finished reading before updating tail)
        self.tail.store(tail.wrapping_add(1), Ordering::Release);

        Some(entry)
    }

    /// Peek at the next entry without removing it (consumer side).
    pub fn peek(&self) -> Option<&RequestEntry> {
        let tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Acquire);

        if tail == head {
            return None;
        }

        let idx = (tail & self.mask) as usize;
        Some(unsafe { &*self.entries.add(idx) })
    }
}

// SAFETY: RequestRing uses atomic operations for synchronization
unsafe impl Send for RequestRing<'_> {}
unsafe impl Sync for RequestRing<'_> {}

/// Response ring buffer (daemon produces, client consumes).
pub struct ResponseRing<'a> {
    /// Pointer to ring entries
    entries: *mut ResponseEntry,
    /// Number of entries (must be power of 2)
    capacity: u32,
    /// Mask for index wrapping
    mask: u32,
    /// Head index (producer/daemon updates)
    head: &'a AtomicU32,
    /// Tail index (consumer/client updates)
    tail: &'a AtomicU32,
}

impl<'a> ResponseRing<'a> {
    /// Create a new response ring from shared memory region.
    pub fn new(shm: &'a SharedMemoryRegion, slot_id: u32) -> Result<Self, ShmError> {
        let slot_control = shm.slot_control(slot_id)?;
        let entries = shm.response_ring_ptr(slot_id)?;
        let capacity = shm.config().response_ring_size;

        debug_assert!(capacity.is_power_of_two());

        Ok(Self {
            entries,
            capacity,
            mask: capacity - 1,
            head: &slot_control.response_head,
            tail: &slot_control.response_tail,
        })
    }

    /// Check if the ring is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);
        head == tail
    }

    /// Check if the ring is full.
    #[inline]
    pub fn is_full(&self) -> bool {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);
        head.wrapping_sub(tail) >= self.capacity
    }

    /// Get the number of entries in the ring.
    #[inline]
    pub fn len(&self) -> u32 {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);
        head.wrapping_sub(tail)
    }

    /// Get the capacity of the ring.
    #[inline]
    pub fn capacity(&self) -> u32 {
        self.capacity
    }

    /// Push a response entry (producer/daemon side).
    /// Returns false if the ring is full.
    pub fn push(&self, entry: &ResponseEntry) -> bool {
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Acquire);

        // Check if full
        if head.wrapping_sub(tail) >= self.capacity {
            return false;
        }

        // Write entry
        let idx = (head & self.mask) as usize;
        unsafe {
            std::ptr::write_volatile(self.entries.add(idx), *entry);
        }

        // Update head
        self.head.store(head.wrapping_add(1), Ordering::Release);

        true
    }

    /// Try to pop a response entry (consumer/client side).
    /// Returns None if the ring is empty.
    pub fn try_pop(&self) -> Option<ResponseEntry> {
        let tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Acquire);

        // Check if empty
        if tail == head {
            return None;
        }

        // Read entry
        let idx = (tail & self.mask) as usize;
        let entry = unsafe { std::ptr::read_volatile(self.entries.add(idx)) };

        // Update tail
        self.tail.store(tail.wrapping_add(1), Ordering::Release);

        Some(entry)
    }

    /// Peek at the next entry without removing it (consumer side).
    pub fn peek(&self) -> Option<&ResponseEntry> {
        let tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Acquire);

        if tail == head {
            return None;
        }

        let idx = (tail & self.mask) as usize;
        Some(unsafe { &*self.entries.add(idx) })
    }

    /// Try to pop a response with a specific request ID (client side).
    /// This is useful when waiting for a specific response.
    pub fn try_pop_by_id(&self, request_id: u64) -> Option<ResponseEntry> {
        let tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Acquire);

        // Check if empty
        if tail == head {
            return None;
        }

        // Check if the next entry matches
        let idx = (tail & self.mask) as usize;
        let entry_ptr = unsafe { self.entries.add(idx) };
        let entry_id = unsafe { (*entry_ptr).request_id };

        if entry_id == request_id {
            let entry = unsafe { std::ptr::read_volatile(entry_ptr) };
            self.tail.store(tail.wrapping_add(1), Ordering::Release);
            Some(entry)
        } else {
            None
        }
    }
}

// SAFETY: ResponseRing uses atomic operations for synchronization
unsafe impl Send for ResponseRing<'_> {}
unsafe impl Sync for ResponseRing<'_> {}

/// A slot view that provides access to request ring, response ring, and data buffer.
pub struct SlotView<'a> {
    /// Shared memory region
    shm: &'a SharedMemoryRegion,
    /// Slot ID
    slot_id: u32,
}

impl<'a> SlotView<'a> {
    /// Create a new slot view.
    pub fn new(shm: &'a SharedMemoryRegion, slot_id: u32) -> Result<Self, ShmError> {
        if slot_id >= shm.config().num_slots {
            return Err(ShmError::InvalidSlot);
        }
        Ok(Self { shm, slot_id })
    }

    /// Get the slot ID.
    pub fn slot_id(&self) -> u32 {
        self.slot_id
    }

    /// Get the request ring.
    pub fn request_ring(&self) -> Result<RequestRing<'a>, ShmError> {
        RequestRing::new(self.shm, self.slot_id)
    }

    /// Get the response ring.
    pub fn response_ring(&self) -> Result<ResponseRing<'a>, ShmError> {
        ResponseRing::new(self.shm, self.slot_id)
    }

    /// Get the data buffer.
    pub fn data_buffer(&self) -> Result<&[u8], ShmError> {
        self.shm.data_buffer(self.slot_id)
    }

    /// Get the data buffer mutably.
    pub fn data_buffer_mut(&self) -> Result<&mut [u8], ShmError> {
        self.shm.data_buffer_mut(self.slot_id)
    }

    /// Get the data buffer size.
    pub fn data_buffer_size(&self) -> usize {
        self.shm.config().data_buffer_size
    }

    /// Set client PID for this slot.
    pub fn set_client_pid(&self, pid: u32) -> Result<(), ShmError> {
        let slot = self.shm.slot_control(self.slot_id)?;
        slot.client_pid.store(pid, Ordering::Release);
        Ok(())
    }

    /// Get client PID for this slot.
    pub fn client_pid(&self) -> Result<u32, ShmError> {
        let slot = self.shm.slot_control(self.slot_id)?;
        Ok(slot.client_pid.load(Ordering::Acquire))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::daemon::protocol::OpType;
    use crate::daemon::shm::ShmConfig;
    use std::sync::atomic::{AtomicU64, Ordering};

    // Unique counter to ensure each test gets a unique shared memory name
    static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn create_test_shm(test_name: &str) -> SharedMemoryRegion {
        let unique_id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let name = format!(
            "/benchfs_ring_{}_{}_{}",
            std::process::id(),
            test_name,
            unique_id
        );
        let config = ShmConfig {
            num_slots: 2,
            data_buffer_size: 4096,
            request_ring_size: 4,
            response_ring_size: 4,
        };

        // Clean up any existing shm
        let c_name = std::ffi::CString::new(name.as_str()).unwrap();
        unsafe { libc::shm_unlink(c_name.as_ptr()) };

        SharedMemoryRegion::create(&name, config).unwrap()
    }

    #[test]
    fn test_request_ring_push_pop() {
        let shm = create_test_shm("req_push_pop");
        let ring = RequestRing::new(&shm, 0).unwrap();

        assert!(ring.is_empty());
        assert!(!ring.is_full());
        assert_eq!(ring.len(), 0);

        // Push entries
        for i in 0..4 {
            let mut entry = RequestEntry::new();
            entry.request_id = i;
            entry.op_type = OpType::Read as u32;
            assert!(ring.push(&entry));
        }

        assert!(!ring.is_empty());
        assert!(ring.is_full());
        assert_eq!(ring.len(), 4);

        // Should fail when full
        let entry = RequestEntry::new();
        assert!(!ring.push(&entry));

        // Pop entries
        for i in 0..4 {
            let entry = ring.try_pop().unwrap();
            assert_eq!(entry.request_id, i);
        }

        assert!(ring.is_empty());
        assert!(ring.try_pop().is_none());
    }

    #[test]
    fn test_response_ring_push_pop() {
        let shm = create_test_shm("resp_push_pop");
        let ring = ResponseRing::new(&shm, 0).unwrap();

        assert!(ring.is_empty());

        // Push entries
        for i in 0..4 {
            let entry = ResponseEntry::success(i, i * 100);
            assert!(ring.push(&entry));
        }

        assert!(ring.is_full());

        // Pop entries
        for i in 0..4 {
            let entry = ring.try_pop().unwrap();
            assert_eq!(entry.request_id, i);
            assert_eq!(entry.result, i * 100);
        }

        assert!(ring.is_empty());
    }

    #[test]
    fn test_response_ring_pop_by_id() {
        let shm = create_test_shm("resp_pop_by_id");
        let ring = ResponseRing::new(&shm, 0).unwrap();

        let entry = ResponseEntry::success(42, 100);
        ring.push(&entry);

        // Wrong ID
        assert!(ring.try_pop_by_id(99).is_none());

        // Correct ID
        let result = ring.try_pop_by_id(42).unwrap();
        assert_eq!(result.result, 100);
    }

    #[test]
    fn test_slot_view() {
        let shm = create_test_shm("slot_view");
        let view = SlotView::new(&shm, 0).unwrap();

        assert_eq!(view.slot_id(), 0);
        assert_eq!(view.data_buffer_size(), 4096);

        // Test data buffer
        let buffer = view.data_buffer_mut().unwrap();
        buffer[0] = 0xAB;
        buffer[1] = 0xCD;

        let buffer = view.data_buffer().unwrap();
        assert_eq!(buffer[0], 0xAB);
        assert_eq!(buffer[1], 0xCD);

        // Test client PID
        view.set_client_pid(1234).unwrap();
        assert_eq!(view.client_pid().unwrap(), 1234);
    }

    #[test]
    fn test_ring_wrap_around() {
        let shm = create_test_shm("wrap_around");
        let req_ring = RequestRing::new(&shm, 0).unwrap();
        let resp_ring = ResponseRing::new(&shm, 0).unwrap();

        // Push and pop multiple times to test wrap-around
        for round in 0..10 {
            // Fill ring
            for i in 0..4 {
                let mut entry = RequestEntry::new();
                entry.request_id = round * 4 + i;
                assert!(req_ring.push(&entry));

                let resp = ResponseEntry::success(round * 4 + i, 0);
                assert!(resp_ring.push(&resp));
            }

            // Empty ring
            for i in 0..4 {
                let entry = req_ring.try_pop().unwrap();
                assert_eq!(entry.request_id, round * 4 + i);

                let resp = resp_ring.try_pop().unwrap();
                assert_eq!(resp.request_id, round * 4 + i);
            }
        }
    }
}
