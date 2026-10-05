//! ODP registration cache for ordinary application buffers.
//!
//! A conventional pinned MR cannot safely be reused after an unobserved free
//! or munmap. Explicit ODP MRs use kernel MMU invalidation; unsupported devices
//! take the copy path. No malloc interposition or application allocator change.
use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::time::{Duration, Instant};

use mlx5::pd::{MemoryRegion, Pd};
use rrrpc::relay::client::RegisteredBuffer;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Range {
    start: usize,
    len: usize,
    writable: bool,
}

impl Range {
    fn pages(ptr: usize, len: usize, writable: bool, page: usize) -> Option<Self> {
        if ptr == 0 || len == 0 || !page.is_power_of_two() {
            return None;
        }
        let end = ptr.checked_add(len)?.checked_add(page - 1)? & !(page - 1);
        let start = ptr & !(page - 1);
        Some(Self {
            start,
            len: end.checked_sub(start)?,
            writable,
        })
    }
    fn contains(&self, ptr: usize, len: usize, writable: bool) -> bool {
        (!writable || self.writable)
            && ptr >= self.start
            && ptr
                .checked_add(len)
                .is_some_and(|end| end <= self.start + self.len)
    }
}

struct Entry<T> {
    range: Range,
    value: Rc<T>,
    used: Instant,
}

/// Generic policy allows deterministic tests without an HCA. Busy entries
/// remain charged to both limits and cannot be evicted underneath a request.
struct Cache<T> {
    entries: Vec<Entry<T>>,
    max_entries: usize,
    max_bytes: usize,
    idle: Duration,
}

impl<T> Cache<T> {
    fn acquire<E>(
        &mut self,
        range: Range,
        now: Instant,
        register: impl FnOnce() -> Result<T, E>,
    ) -> Result<Option<Rc<T>>, E> {
        self.entries
            .retain(|e| Rc::strong_count(&e.value) > 1 || now.duration_since(e.used) < self.idle);
        if let Some(e) = self.entries.iter_mut().find(|e| e.range == range) {
            e.used = now;
            return Ok(Some(e.value.clone()));
        }
        if self.max_entries == 0 || range.len > self.max_bytes {
            return Ok(None);
        }
        while self.entries.len() >= self.max_entries
            || self.entries.iter().map(|e| e.range.len).sum::<usize>() > self.max_bytes - range.len
        {
            let victim = self
                .entries
                .iter()
                .enumerate()
                .filter(|(_, e)| Rc::strong_count(&e.value) == 1)
                .min_by_key(|(_, e)| e.used)
                .map(|(i, _)| i);
            let Some(i) = victim else {
                return Ok(None);
            };
            self.entries.swap_remove(i);
        }
        let value = Rc::new(register()?);
        self.entries.push(Entry {
            range,
            value: value.clone(),
            used: now,
        });
        Ok(Some(value))
    }
}

struct Active {
    id: u64,
    range: Range,
    mr: Rc<MemoryRegion>,
    in_use: Rc<Cell<usize>>,
}

pub struct UserBufferCache {
    cache: Cache<MemoryRegion>,
    active: Rc<RefCell<Vec<Active>>>,
    next_id: u64,
    min_bytes: usize,
    page_size: usize,
    enabled: bool,
    registrations: u64,
    hits: u64,
    fallbacks: u64,
    prefetch_failures: u64,
    prefetch_on_reuse: bool,
    reuse_prefetches: u64,
    /// Time spent on MR management (not per-DMA): registration, ODP
    /// prefetch, and deregistration by eviction/expiry/clear.
    register_time: Duration,
    prefetch_time: Duration,
    release_time: Duration,
    puts: Cell<u64>,
    gets: Cell<u64>,
}

impl UserBufferCache {
    pub fn new(cfg: &crate::runtime_config::LocustaTuning) -> Self {
        Self {
            cache: Cache {
                entries: Vec::new(),
                max_entries: cfg.user_mr_cache_entries,
                max_bytes: cfg.user_mr_cache_bytes,
                idle: Duration::from_millis(cfg.user_mr_cache_idle_ms),
            },
            active: Rc::new(RefCell::new(Vec::new())),
            next_id: 0,
            min_bytes: cfg.user_mr_min_bytes,
            page_size: unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize,
            enabled: cfg.user_mr_cache,
            registrations: 0,
            hits: 0,
            fallbacks: 0,
            prefetch_failures: 0,
            prefetch_on_reuse: cfg.user_mr_prefetch_on_reuse,
            reuse_prefetches: 0,
            register_time: Duration::ZERO,
            prefetch_time: Duration::ZERO,
            release_time: Duration::ZERO,
            puts: Cell::new(0),
            gets: Cell::new(0),
        }
    }

    /// # Safety
    /// The caller owns this range until the scope is dropped, after ALL its
    /// RPCs complete. Writable ranges must have exclusive application access.
    pub unsafe fn begin(
        &mut self,
        pd: &Pd,
        ptr: usize,
        len: usize,
        writable: bool,
    ) -> Option<UserBufferScope> {
        if !self.enabled || len < self.min_bytes {
            return None;
        }
        let range = Range::pages(ptr, len, writable, self.page_size)?;
        let mut created = false;
        let mut register_time = Duration::ZERO;
        let start = Instant::now();
        let result = self.cache.acquire(range, start, || {
            created = true;
            let t = Instant::now();
            // ODP is mandatory: ordinary pinned registration is NOT a safe fallback.
            let mr = unsafe { pd.register_odp(range.start as *mut u8, range.len, writable) };
            register_time = t.elapsed();
            mr
        });
        if created {
            // The rest of a creating acquire is expiry/eviction deregistration.
            self.register_time += register_time;
            self.release_time += start.elapsed().saturating_sub(register_time);
        }
        let mr = match result {
            Ok(Some(mr)) => mr,
            other => {
                self.fallbacks += 1;
                if self.fallbacks == 1 {
                    tracing::warn!(error = ?other.err(), "USER_MR copy fallback (ODP unavailable or cache limit)");
                }
                return None;
            }
        };
        if !created {
            self.hits += 1;
        } else {
            self.registrations += 1;
        }
        // Warm the translations: an on-demand fault during a Get is answered
        // with an RNR NAK that stalls the server's RDMA WRITE. A reused read
        // destination may have been remapped since its last use. A Put source
        // faults locally without that wait, so it is warmed at creation only.
        if created || (writable && self.prefetch_on_reuse) {
            if !created {
                self.reuse_prefetches += 1;
            }
            let t = Instant::now();
            let prefetched = mr.prefetch_odp(writable);
            self.prefetch_time += t.elapsed();
            if let Err(error) = prefetched {
                self.prefetch_failures += 1;
                if self.prefetch_failures == 1 {
                    tracing::warn!(?error, "USER_MR ODP prefetch failed; pages fault on demand");
                }
            }
        }
        self.next_id = self
            .next_id
            .checked_add(1)
            .expect("user buffer scope id overflow");
        let id = self.next_id;
        // Active bounds are the user's exact range, not the rounded MR pages.
        let in_use = Rc::new(Cell::new(0));
        self.active.borrow_mut().push(Active {
            id,
            range: Range {
                start: ptr,
                len,
                writable,
            },
            mr,
            in_use: in_use.clone(),
        });
        Some(UserBufferScope {
            active: self.active.clone(),
            id,
            in_use,
        })
    }

    pub fn lookup(&self, ptr: usize, len: usize, writable: bool) -> Option<UserBufferSlice> {
        let len32 = u32::try_from(len).ok()?;
        let entries = self.active.borrow();
        let e = entries
            .iter()
            .find(|e| e.range.contains(ptr, len, writable))?;
        let count = if writable { &self.gets } else { &self.puts };
        count.set(count.get() + 1);
        e.in_use.set(e.in_use.get() + 1);
        Some(UserBufferSlice {
            in_use: e.in_use.clone(),
            // The active FFI scope owns the user memory; this Rc owns the MR.
            buffer: unsafe { RegisteredBuffer::new(ptr as u64, len32, e.mr.lkey(), e.mr.rkey()) },
            _mr: e.mr.clone(),
        })
    }

    pub fn clear_idle(&mut self) {
        let t = Instant::now();
        self.cache
            .entries
            .retain(|e| Rc::strong_count(&e.value) > 1);
        self.release_time += t.elapsed();
        tracing::info!(
            registrations = self.registrations,
            hits = self.hits,
            fallbacks = self.fallbacks,
            prefetch_failures = self.prefetch_failures,
            reuse_prefetches = self.reuse_prefetches,
            register_us = self.register_time.as_micros() as u64,
            prefetch_us = self.prefetch_time.as_micros() as u64,
            release_us = self.release_time.as_micros() as u64,
            retained = self.cache.entries.len(),
            put_with_buffer = self.puts.get(),
            get_with_buffer = self.gets.get(),
            "USER_MR_CACHE"
        );
    }
}

pub struct UserBufferScope {
    active: Rc<RefCell<Vec<Active>>>,
    id: u64,
    in_use: Rc<Cell<usize>>,
}
impl Drop for UserBufferScope {
    fn drop(&mut self) {
        if self.in_use.get() != 0 {
            eprintln!(
                "BenchFS: user buffer released with DMA in flight; aborting to prevent memory corruption"
            );
            std::process::abort();
        }
        self.active.borrow_mut().retain(|e| e.id != self.id);
    }
}

pub struct UserBufferSlice {
    pub buffer: RegisteredBuffer,
    _mr: Rc<MemoryRegion>,
    in_use: Rc<Cell<usize>>,
}
impl Drop for UserBufferSlice {
    fn drop(&mut self) {
        self.in_use.set(self.in_use.get() - 1);
    }
}

/// Until the transport can flush a failed QP, never return application memory
/// to its caller on cancellation or timeout while the NIC can still access it.
#[derive(Default)]
pub(super) struct InFlightDma(pub bool);
impl Drop for InFlightDma {
    fn drop(&mut self) {
        if self.0 {
            eprintln!("BenchFS: external DMA cancelled/timed out before completion; aborting");
            std::process::abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn cache(entries: usize, bytes: usize) -> Cache<usize> {
        Cache {
            entries: vec![],
            max_entries: entries,
            max_bytes: bytes,
            idle: Duration::from_secs(1),
        }
    }
    fn range(n: usize) -> Range {
        Range {
            start: n * 4096,
            len: 4096,
            writable: true,
        }
    }
    #[test]
    fn bounds_and_access() {
        let r = Range::pages(4100, 4096, false, 4096).unwrap();
        assert_eq!(r.len, 8192);
        assert!(r.contains(8192, 4096, false));
        assert!(!r.contains(8192, 4097, false));
        assert!(!r.contains(4100, 1, true));
        assert!(Range::pages(usize::MAX - 1, 4, true, 4096).is_none());
        assert!(!r.contains(usize::MAX, 1, false));
    }
    #[test]
    fn reuse_expiry_and_busy_limit() {
        let now = Instant::now();
        let mut c = cache(1, 4096);
        let a = c
            .acquire(range(1), now, || Ok::<_, ()>(11))
            .unwrap()
            .unwrap();
        let b = c
            .acquire(range(1), now, || -> Result<usize, ()> {
                panic!("must reuse")
            })
            .unwrap()
            .unwrap();
        assert!(Rc::ptr_eq(&a, &b));
        assert!(
            c.acquire(range(2), now, || Ok::<_, ()>(22))
                .unwrap()
                .is_none()
        );
        // Expired but in-flight registration remains charged and alive.
        assert!(
            c.acquire(range(2), now + Duration::from_secs(2), || Ok::<_, ()>(22))
                .unwrap()
                .is_none()
        );
        drop(a);
        drop(b);
        let fresh = c
            .acquire(range(1), now + Duration::from_secs(2), || Ok::<_, ()>(33))
            .unwrap()
            .unwrap();
        assert_eq!(*fresh, 33);
    }
    #[test]
    fn failed_registration_and_byte_limit() {
        let mut c = cache(4, 4096);
        let now = Instant::now();
        assert_eq!(
            c.acquire(range(1), now, || Err::<usize, _>(7)).unwrap_err(),
            7
        );
        assert!(c.entries.is_empty());
        drop(c.acquire(range(1), now, || Ok::<_, ()>(1)).unwrap());
        let x = c
            .acquire(range(2), now, || Ok::<_, ()>(2))
            .unwrap()
            .unwrap();
        assert_eq!(c.entries.len(), 1);
        assert_eq!(*x, 2);
    }
}
