//! BenchFS client-side API.
//!
//! Historically `BenchFS` (file_ops.rs) was used as a single struct for both
//! libbenchfs.so (client used by IOR) and benchfsd_mpi (server). The client
//! pulled in `MetadataManager`, which is the **server's authoritative
//! metadata store**, and treated it as a local cache. iter27's per-RPC
//! integrity log (2026-05-19) localised a verification-error bug to exactly
//! this confusion: each client rank's local "cache" held only that rank's
//! own write-side max file size, not the global max, so ranks whose
//! per-rank max ended earlier in a shared file under-read on the read
//! phase.
//!
//! `BenchFSClient` is the client-only entrypoint. It does NOT own a
//! `MetadataManager`. The client side resolves authoritative metadata via
//! [`ConnectionPool`] RPC to the metadata-owning node, optionally serving
//! repeat lookups from a small read-only cache.
//!
//! For the transition we keep `BenchFS` (file_ops.rs) as the legacy
//! shared struct; new FFI code goes through `BenchFSClient` so the
//! client/server boundary is enforced at the type level.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use crate::api::file_ops::BenchFS;
use crate::api::types::{ApiError, ApiResult, FileHandle, FileStat, OpenFlags};
use crate::metadata::{ConsistentHashRing, FileMetadata};
use crate::rpc::connection::ConnectionPool;

/// Client-side handle used by libbenchfs (the FFI surface for IOR and the
/// pfind binary). Contains only client-relevant state:
///
/// - Routing rings to locate metadata and chunk owners
/// - A [`ConnectionPool`] for outbound RPCs
/// - An open-file handle table local to this process
/// - A small, explicitly-named read-side metadata cache
///
/// It deliberately omits the server-only [`MetadataManager`] /
/// [`InodeStore`] / central parent index, which were the source of the
/// iter27 ior-hard-read regression.
///
/// The current implementation delegates to the legacy `BenchFS` for the
/// async heavy-lift methods so the migration is incremental. The wrapper
/// guarantees the FFI cannot touch server-only fields directly.
pub struct BenchFSClient {
    /// Underlying legacy struct (will be split further once every callsite
    /// uses the typed wrappers).
    inner: Rc<BenchFS>,
    /// This client's node identifier. Mirrors `BenchFS::self_node_id()`
    /// but is held directly so the routing checks no longer have to go
    /// through the server-internal `MetadataManager`.
    node_id: String,
    /// Optional cache of file metadata seen by this client. Stored
    /// explicitly here (separate from any server-side store) so its
    /// non-authoritative nature is obvious in code review.
    read_cache: RefCell<HashMap<String, FileMetadata>>,
}

impl BenchFSClient {
    /// Wrap a `BenchFS` instance built by `BenchFSBuilder::new_client` (or
    /// equivalent) as the client surface.
    ///
    /// The caller is responsible for ensuring `inner` was constructed in
    /// distributed-client mode (chunk_store=None or co-located fast path
    /// only, connection_pool=Some).
    pub fn from_inner(inner: Rc<BenchFS>) -> Self {
        let node_id = inner.self_node_id();
        Self {
            inner,
            node_id,
            read_cache: RefCell::new(HashMap::new()),
        }
    }

    /// Borrow the underlying legacy struct. Used while migrating; new code
    /// should reach for the typed methods directly.
    pub fn inner(&self) -> &Rc<BenchFS> {
        &self.inner
    }

    /// This client's node identifier (= `BenchFSBuilder` `node_id`).
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Borrow the connection pool used for outbound RPCs.
    pub fn connection_pool(&self) -> Option<&Rc<ConnectionPool>> {
        self.inner.connection_pool_ref()
    }

    /// Borrow the metadata routing ring.
    pub fn metadata_ring(&self) -> Option<&Rc<ConsistentHashRing>> {
        self.inner.metadata_ring_ref()
    }

    /// Borrow the data routing ring.
    pub fn data_ring(&self) -> Option<&Rc<ConsistentHashRing>> {
        self.inner.data_ring_ref()
    }

    /// Cache lookup. Returns `None` if no cached entry — the caller is
    /// expected to fetch authoritative metadata via RPC, then optionally
    /// `cache_set`.
    pub fn cache_get(&self, path: &str) -> Option<FileMetadata> {
        self.read_cache.borrow().get(path).cloned()
    }

    /// Insert / overwrite a cached metadata entry.
    pub fn cache_set(&self, path: &str, meta: FileMetadata) {
        self.read_cache.borrow_mut().insert(path.to_string(), meta);
    }

    /// Drop a cached entry. Called from `close()` / `unlink()` /
    /// `truncate()` to keep the cache from going stale for paths whose
    /// authoritative state just changed.
    pub fn cache_invalidate(&self, path: &str) {
        self.read_cache.borrow_mut().remove(path);
    }

    /// Drop all cached entries. Used by FFI finalize and explicit phase
    /// transitions in benchmarks.
    pub fn cache_clear(&self) {
        self.read_cache.borrow_mut().clear();
    }

    pub async fn open(&self, path: &str, flags: OpenFlags) -> ApiResult<FileHandle> {
        self.inner.benchfs_open(path, flags).await
    }

    pub async fn read(&self, handle: &FileHandle, buf: &mut [u8]) -> ApiResult<usize> {
        self.inner.benchfs_read(handle, buf).await
    }

    pub async fn write(&self, handle: &FileHandle, data: &[u8]) -> ApiResult<usize> {
        self.inner.benchfs_write(handle, data).await
    }

    pub async fn close(&self, handle: &FileHandle) -> ApiResult<()> {
        // Invalidate cache: after close the size may have changed.
        let path = handle.path.clone();
        let r = self.inner.benchfs_close(handle).await;
        self.cache_invalidate(&path);
        r
    }

    pub async fn unlink(&self, path: &str) -> ApiResult<()> {
        let r = self.inner.benchfs_unlink(path).await;
        self.cache_invalidate(path);
        r
    }

    pub async fn mkdir(&self, path: &str, mode: u32) -> ApiResult<()> {
        self.inner.benchfs_mkdir(path, mode).await
    }

    pub fn rmdir(&self, path: &str) -> ApiResult<()> {
        let r = self.inner.benchfs_rmdir(path);
        self.cache_invalidate(path);
        r
    }

    pub fn seek(&self, handle: &FileHandle, offset: i64, whence: i32) -> ApiResult<u64> {
        self.inner.benchfs_seek(handle, offset, whence)
    }

    pub async fn fsync(&self, handle: &FileHandle) -> ApiResult<()> {
        self.inner.benchfs_fsync(handle).await
    }

    pub async fn stat(&self, path: &str) -> ApiResult<FileStat> {
        self.inner.benchfs_stat(path).await
    }

    pub fn rename(&self, old_path: &str, new_path: &str) -> ApiResult<()> {
        let r = self.inner.benchfs_rename(old_path, new_path);
        self.cache_invalidate(old_path);
        self.cache_invalidate(new_path);
        r
    }

    pub async fn readdir(
        &self,
        path: &str,
    ) -> ApiResult<Vec<(String, crate::metadata::InodeType, u64)>> {
        self.inner.benchfs_readdir(path).await
    }

    pub async fn truncate(&self, path: &str, size: u64) -> ApiResult<()> {
        let r = self.inner.benchfs_truncate(path, size).await;
        self.cache_invalidate(path);
        r
    }
}

// `BenchFSClient` is single-thread-only via the `RefCell`/`Rc` fields it
// holds — `Send`/`Sync` are not auto-derived. No explicit negative impl
// is needed (Rust's negative impls behind `#![feature(negative_impls)]`
// are unstable).

/// Server-side handle. Owns the authoritative `MetadataManager`,
/// `InodeStore`, and central parent-index propagation machinery.
///
/// Built by `benchfsd_mpi`; never owned by FFI / IOR clients. Currently
/// wraps `BenchFS` to keep the migration small.
pub struct BenchFSServer {
    inner: Rc<BenchFS>,
}

impl BenchFSServer {
    pub fn from_inner(inner: Rc<BenchFS>) -> Self {
        Self { inner }
    }

    pub fn inner(&self) -> &Rc<BenchFS> {
        &self.inner
    }
}

// `ApiError` re-export pulled in to keep the doc-cross-references compiling.
#[allow(unused_imports)]
use ApiError as _ApiErrorRef;
