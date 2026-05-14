//! Zero-copy IO buffer registration.
//!
//! Allocates RDMA-registered buffers from the locusta arena and exposes their
//! raw pointers to C callers (IOR aiori). When `benchfs_read`/`benchfs_write`
//! is later invoked with one of these pointers, the data_ops fast path uses
//! the underlying [`DmaBuffer`] directly as the RDMA target/source, skipping
//! the otherwise-mandatory client-side memcpy in
//! `transport_locusta::send_get`/`send_put`.

use std::cell::RefCell;
use std::collections::HashMap;

use rrrpc::relay::client::DmaBuffer;

use super::runtime::with_benchfs_ctx;

thread_local! {
    /// Registry of arena-backed buffers handed out to C code, keyed by the
    /// raw pointer (so `benchfs_read` can recover the [`DmaBuffer`] from
    /// just the `*const u8` it was handed).
    static IO_BUFFERS: RefCell<HashMap<usize, DmaBuffer>> = RefCell::new(HashMap::new());
}

/// Allocate a registered IO buffer from the locusta arena.
///
/// On success returns 0 and writes the buffer pointer to `*out_ptr`. The
/// caller owns the pointer until [`benchfs_free_io_buffer`] is invoked.
///
/// Returns -1 if BenchFS is not initialized for this thread, -2 if the
/// locusta transport is missing, -3 if the arena cannot satisfy the
/// requested size.
///
/// # Safety
/// `out_ptr` must be a valid pointer to a single `*mut u8` location.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn benchfs_alloc_io_buffer(
    size: usize,
    out_ptr: *mut *mut u8,
) -> i32 {
    if out_ptr.is_null() || size == 0 {
        return -1;
    }
    let result = with_benchfs_ctx(|fs| {
        let pool = match fs.connection_pool_ref() {
            Some(p) => p,
            None => return -2i32,
        };
        let transport = match pool.locusta_transport_ref() {
            Some(t) => t,
            None => return -2i32,
        };
        let mut dma_buf: rrrpc::relay::client::DmaBuffer = match transport.arena_alloc(size as u32) {
            Some(b) => b,
            None => return -3i32,
        };
        let ptr = dma_buf.as_mut_slice().as_mut_ptr();
        unsafe { *out_ptr = ptr; }
        IO_BUFFERS.with(|m| {
            m.borrow_mut().insert(ptr as usize, dma_buf);
        });
        0i32
    });
    match result {
        Ok(code) => code,
        Err(_) => -1,
    }
}

/// Free a buffer previously returned by [`benchfs_alloc_io_buffer`].
///
/// Returns 0 on success, -1 if the pointer was not allocated by us.
///
/// # Safety
/// `ptr` must be a pointer returned by [`benchfs_alloc_io_buffer`] that has
/// not yet been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn benchfs_free_io_buffer(ptr: *mut u8) -> i32 {
    if ptr.is_null() {
        return -1;
    }
    let removed = IO_BUFFERS.with(|m| m.borrow_mut().remove(&(ptr as usize)).is_some());
    if removed { 0 } else { -1 }
}

/// Look up a registered DmaBuffer by pointer. Returns the offset of the
/// matching buffer's start within the registry so callers know if/where the
/// pointer falls. Callers should use [`with_io_buffer`] when they need
/// borrowed access to invoke locusta APIs.
pub fn is_io_buffer_ptr(ptr: *const u8) -> bool {
    IO_BUFFERS.with(|m| m.borrow().contains_key(&(ptr as usize)))
}

/// Borrow the registered [`DmaBuffer`] for `ptr` and invoke `f` with a
/// reference to it. Returns `None` if `ptr` is not registered.
///
/// The closure receives `&DmaBuffer` so it can be passed directly to
/// `LocustaTransport::send_get_with_buffer` etc.
pub fn with_io_buffer<F, R>(ptr: *const u8, f: F) -> Option<R>
where
    F: FnOnce(&DmaBuffer) -> R,
{
    IO_BUFFERS.with(|m| {
        let map = m.borrow();
        map.get(&(ptr as usize)).map(f)
    })
}

/// Look up the registered [`DmaBuffer`] for `ptr` and return a raw pointer
/// to it. Callers must guarantee the buffer is not freed via
/// [`benchfs_free_io_buffer`] for the lifetime of any reference they
/// construct from this pointer.
///
/// Used by the async zero-copy read path where the await would otherwise
/// require holding a `RefCell` borrow across suspension points (which
/// breaks single-threaded reborrows from other futures). The map only
/// removes entries at C-level free, which IOR only calls at the very end
/// of a test phase, so the buffer is stable during the await.
pub fn io_buffer_ptr_raw(ptr: *const u8) -> Option<*const DmaBuffer> {
    IO_BUFFERS.with(|m| {
        m.borrow()
            .get(&(ptr as usize))
            .map(|b| b as *const DmaBuffer)
    })
}
