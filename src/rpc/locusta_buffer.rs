//! Server-side RDMA buffers for the locusta backend, backed by pluvio's
//! [`FixedBufferAllocator`].
//!
//! The same pinned, page-aligned buffers that BenchFS uses for io_uring
//! fixed I/O are also registered with the locusta server's HCA Protection
//! Domain — so a chunk write/read can flow `network ↔ MR ↔ io_uring`
//! without an intervening copy.
//!
//! [`RegisteredFixedBuffer`] is a cheap, `Clone`-able value that exposes
//! the [`rrrpc::server::RdmaBuffer`] trait. It holds an `Arc<FixedBuffer>`
//! so the underlying pluvio handle returns to its pool automatically when
//! the last clone is dropped (i.e. after the locusta server is done with
//! the RDMA transfer).
//!
//! [`register_with_pd`] performs the one-time MR registration of every
//! buffer in a [`FixedBufferAllocator`] against a given `mlx5::pd::Pd`.
//! It returns the owning `Vec<MemoryRegion>` — the caller must keep those
//! alive for the lifetime of the allocator (typically by storing them in
//! the same struct as the allocator).

#![cfg(feature = "transport-locusta")]

use std::sync::Arc;

use mlx5::pd::{AccessFlags, MemoryRegion, Pd};
use pluvio_uring::allocator::{FixedBuffer, FixedBufferAllocator};
use rrrpc::server::RdmaBuffer;

use crate::rpc::RpcError;

/// A cheap, Clone-able view of a pluvio [`FixedBuffer`] that satisfies
/// [`rrrpc::server::RdmaBuffer`].
///
/// All clones share the underlying `Arc<FixedBuffer>`; when the last clone
/// is dropped, the inner [`FixedBuffer`]'s `Drop` returns it to the pluvio
/// allocator. The (addr, len, lkey, rkey) tuple is cached at construction
/// because the locusta RdmaBuffer trait wants accessors callable from a
/// shared reference.
#[derive(Clone)]
pub struct RegisteredFixedBuffer {
    /// Kept so the FixedBuffer's RAII Drop fires when all clones are gone.
    _inner: Arc<FixedBuffer>,
    addr: *mut u8,
    len: usize,
    lkey: u32,
    rkey: u32,
    /// io_uring fixed buffer index — lets the io_uring backend submit a
    /// `WriteFixed` / `ReadFixed` SQE against this buffer without
    /// allocating + memcpy'ing into a separate `FixedBuffer`. Saves a
    /// 4 MiB memcpy per chunk RPC (job 17061 timing showed ~0.4ms/chunk).
    buf_index: u16,
}

impl RegisteredFixedBuffer {
    /// io_uring fixed-buffer index for direct `WriteFixed`/`ReadFixed` SQEs.
    pub fn buf_index(&self) -> u16 {
        self.buf_index
    }

    /// Shrink the buffer's reported length to `n` so the locusta RDMA WRITE
    /// only transfers `n` bytes (not the full FixedBuffer capacity).
    ///
    /// **Critical correctness invariant**: when the server replies to a
    /// `RoundtripGet`, `try_issue_dma_read_write` posts `mlx5::write_imm`
    /// with `len = segment.len()`. If `segment.len()` exceeds the size the
    /// client allocated for its receive buffer, the WRITE will trample
    /// neighbouring DMA-arena allocations that belong to *other* concurrent
    /// RPC requests — those requests then complete with garbage in their
    /// recv slot. ior-hard's 47008-byte reads exposed this as ~0.3% data
    /// verification errors (job 20115/20118).
    pub fn shrink_to(&mut self, n: usize) {
        debug_assert!(n <= self.len, "shrink_to({n}) > underlying {}", self.len);
        if n < self.len {
            self.len = n;
        }
    }
}

// The locusta `RdmaBuffer` trait demands `Send + 'static`. Pluvio's
// `FixedBuffer` is `!Send` because it carries an `Rc<FixedBufferAllocator>`,
// but the locusta backend runs entirely on a single pluvio runtime thread,
// so the buffer is never sent across threads. Asserting `Send` here is
// sound under that operational invariant.
unsafe impl Send for RegisteredFixedBuffer {}
unsafe impl Sync for RegisteredFixedBuffer {}

impl RegisteredFixedBuffer {
    /// Wrap an already-acquired pluvio [`FixedBuffer`] whose backing memory
    /// has been RDMA-registered (via [`register_with_pd`]).
    ///
    /// Panics if the buffer has zero `rdma_lkey()` — that indicates the
    /// allocator never went through `register_with_pd`.
    pub fn from_fixed_buffer(fb: FixedBuffer) -> Self {
        let addr = fb
            .buffer
            .as_ref()
            .map(|b| b.as_slice().as_ptr() as *mut u8)
            .unwrap_or(std::ptr::null_mut());
        let len = fb.len();
        let lkey = fb.rdma_lkey();
        let rkey = fb.rdma_rkey();
        let buf_index = fb.index() as u16;
        assert!(
            lkey != 0,
            "FixedBuffer used for RDMA but allocator was never registered \
             (call register_with_pd first)"
        );
        Self {
            _inner: Arc::new(fb),
            addr,
            len,
            lkey,
            rkey,
            buf_index,
        }
    }
}

impl RdmaBuffer for RegisteredFixedBuffer {
    fn as_ptr(&self) -> *const u8 {
        self.addr
    }
    fn as_mut_ptr(&self) -> *mut u8 {
        self.addr
    }
    fn len(&self) -> usize {
        self.len
    }
    fn rkey(&self) -> u32 {
        self.rkey
    }
    fn lkey(&self) -> u32 {
        self.lkey
    }
    fn addr(&self) -> u64 {
        self.addr as u64
    }
}

/// Register every buffer in `allocator` with `pd` for RDMA access.
///
/// Returns the owning `Vec<MemoryRegion>` so the caller can keep the MRs
/// alive (typically alongside the allocator itself). Each buffer is
/// registered with `LOCAL_WRITE | REMOTE_WRITE | REMOTE_READ` access so it
/// can be used as both a `RoundtripPut` landing site and a `RoundtripGet`
/// source.
pub fn register_with_pd(
    allocator: &FixedBufferAllocator,
    pd: &Pd,
) -> Result<Vec<MemoryRegion>, RpcError> {
    let mut mrs: Vec<MemoryRegion> = Vec::new();
    allocator.register_rdma_keys::<RpcError, _>(|ptr, len| {
        let mr = unsafe {
            pd.register(
                ptr,
                len,
                AccessFlags::LOCAL_WRITE | AccessFlags::REMOTE_WRITE | AccessFlags::REMOTE_READ,
            )
        }
        .map_err(|e| RpcError::ConnectionError(format!("MR register: {e:?}")))?;
        let keys = (mr.lkey(), mr.rkey());
        mrs.push(mr);
        Ok(keys)
    })?;
    Ok(mrs)
}
