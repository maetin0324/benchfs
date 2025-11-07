use crate::constants::MAX_PATH_LENGTH;
use std::cell::RefCell;
use std::rc::{Rc, Weak};

/// Simple reusable buffer allocator for small RPC payloads (e.g., path bytes).
///
/// Each buffer is a Vec<u8> with capacity `MAX_PATH_LENGTH`. Buffers are reused
/// to avoid repeated allocations when receiving paths over UCX rendezvous.
pub struct PathBufferPool {
    buffers: RefCell<Vec<Vec<u8>>>,
}

impl PathBufferPool {
    pub fn new(preallocate: usize) -> Self {
        let mut buffers = Vec::with_capacity(preallocate);
        for _ in 0..preallocate {
            buffers.push(vec![0u8; MAX_PATH_LENGTH]);
        }
        Self {
            buffers: RefCell::new(buffers),
        }
    }

    /// Acquire a reusable buffer. If the pool is empty a new buffer is allocated.
    pub fn acquire(self: &Rc<Self>) -> PathBufferLease {
        let buffer = self
            .buffers
            .borrow_mut()
            .pop()
            .unwrap_or_else(|| vec![0u8; MAX_PATH_LENGTH]);
        PathBufferLease {
            data: buffer,
            pool: Rc::downgrade(self),
        }
    }

    fn release(&self, mut buffer: Vec<u8>) {
        if buffer.len() != MAX_PATH_LENGTH {
            buffer.resize(MAX_PATH_LENGTH, 0);
        }
        self.buffers.borrow_mut().push(buffer);
    }
}

/// RAII guard that returns the buffer to the pool on drop.
pub struct PathBufferLease {
    data: Vec<u8>,
    pool: Weak<PathBufferPool>,
}

impl PathBufferLease {
    pub fn capacity(&self) -> usize {
        self.data.len()
    }

    /// Get a mutable slice limited to `len` bytes.
    pub fn as_mut_slice(&mut self, len: usize) -> &mut [u8] {
        assert!(
            len <= self.data.len(),
            "Requested slice length {} exceeds buffer capacity {}",
            len,
            self.data.len()
        );
        &mut self.data[..len]
    }

    /// Get an immutable slice limited to `len` bytes.
    pub fn as_slice(&self, len: usize) -> &[u8] {
        assert!(
            len <= self.data.len(),
            "Requested slice length {} exceeds buffer capacity {}",
            len,
            self.data.len()
        );
        &self.data[..len]
    }

    pub fn as_str(&self, len: usize) -> Result<&str, std::str::Utf8Error> {
        std::str::from_utf8(self.as_slice(len))
    }
}

impl Drop for PathBufferLease {
    fn drop(&mut self) {
        if let Some(pool) = self.pool.upgrade() {
            let buffer = std::mem::take(&mut self.data);
            pool.release(buffer);
        }
    }
}
