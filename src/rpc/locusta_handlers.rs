//! Server-side [`LocustaServerHandler`] impls for the BenchFS AmRpc types.
//!
//! Each impl mirrors its UCX counterpart in `metadata_ops.rs` /
//! `data_ops.rs`, but:
//!
//! - reads the request header from a single `body: &[u8]` (after the
//!   2B rpc_id prefix is stripped by `LocustaServerDispatch`),
//! - skips the UCX-style "receive bulk path" step because the path is
//!   already inline in `small_req`,
//! - calls the handle's consumer (`req.reply()` / `req.grant()`) once
//!   the response is ready.
//!
//! Phase 2.4 first slice: metadata RPCs only. The chunk RPCs
//! (Read/WriteChunkById) need an async dispatch because the chunk
//! store is io_uring-backed; that lands in slice 2.

#![cfg(feature = "transport-locusta")]

use std::path::Path;
use std::rc::Rc;

use rrrpc::server::Request;
use zerocopy::{FromBytes, IntoBytes};

use crate::metadata::{DirectoryMetadata, FileMetadata};
use crate::rpc::data_ops::{
    ReadChunkByIdRequest, ReadChunkByIdRequestHeader, ReadChunkResponseHeader,
    WriteChunkByIdRequest, WriteChunkByIdRequestHeader, WriteChunkResponseHeader,
};
use crate::rpc::handlers::RpcHandlerContext;
use crate::rpc::locusta_buffer::RegisteredFixedBuffer;
use crate::rpc::locusta_server::LocustaServerHandler;
use crate::rpc::metadata_ops::{
    MetadataCreateDirRequest, MetadataCreateDirRequestHeader, MetadataCreateDirResponseHeader,
    MetadataCreateFileRequest, MetadataCreateFileRequestHeader, MetadataCreateFileResponseHeader,
    MetadataDeleteRequest, MetadataDeleteRequestHeader, MetadataDeleteResponseHeader,
    MetadataLookupRequest, MetadataLookupRequestHeader, MetadataLookupResponseHeader,
    MetadataUpdateRequest, MetadataUpdateRequestHeader, MetadataUpdateResponseHeader,
};
use crate::rpc::readdir_ops::{
    READDIR_MAX_ENTRIES_PER_CALL, ReaddirRequest, ReaddirRequestHeader, ReaddirResponseHeader,
    encode_entries,
};
use rrrpc::server::RdmaBuffer;

thread_local! {
    /// `(path_hash, chunk_id) → PutGrant arrival timestamp`. Populated in
    /// the PutGrant branch and consumed in `RoundtripPutReady` so we can
    /// emit `grant_to_ready_us` in `WRITE_READY_TIMING`. The map is only
    /// touched when stats are enabled, so the cost is gated.
    static PUT_GRANT_TS: std::cell::RefCell<
        std::collections::HashMap<(u32, u64), std::time::Instant>
    > = std::cell::RefCell::new(std::collections::HashMap::new());
}

/// Helper: pull the rpc-specific request header out of the body,
/// then return the trailing bytes (path or other variable-length data).
fn split_header<'b, H>(body: &'b [u8]) -> Option<(H, &'b [u8])>
where
    H: FromBytes + Copy,
{
    let hdr_size = std::mem::size_of::<H>();
    if body.len() < hdr_size {
        return None;
    }
    let hdr = H::read_from_bytes(&body[..hdr_size]).ok()?;
    Some((hdr, &body[hdr_size..]))
}

/// `RoundtripEager` reply helper — sends `bytes` as the small response.
/// Panics on the rare non-eager variants since metadata RPCs are always
/// eager.
pub(crate) fn reply_eager(req: Request<RegisteredFixedBuffer>, bytes: Vec<u8>) {
    match req {
        Request::RoundtripEager(h) => h.reply(bytes),
        other => {
            eprintln!(
                "[locusta_handlers] non-eager variant {:?} dropped — metadata expects RoundtripEager",
                std::mem::discriminant(&other)
            );
            // Drop sends an error response automatically.
            drop(other);
        }
    }
}

/// Split `path` into (parent, child) like
/// `MetadataManager::split_parent_child`. Returns `None` for "/" and
/// other paths without a parent.
fn parent_of(path: &str) -> Option<String> {
    let p = path.strip_suffix('/').unwrap_or(path);
    let idx = p.rfind('/')?;
    if idx == 0 {
        Some("/".to_string())
    } else {
        Some(p[..idx].to_string())
    }
}

/// CHFS-style central parent index: if `parent_owner != self_node_id`,
/// send a `DirIndexUpdate` RPC so the parent's owner mirrors the
/// (parent, child) entry. Fire-and-forget — we spawn the task and let
/// the create RPC reply immediately. If the parent is local, no-op
/// (the file owner already did `dir_index_insert` via
/// `store_file_metadata`).
///
/// Gated on `BENCHFS_CENTRAL_PARENT_INDEX=1` so the new path can be
/// A/B tested against the old fan-out behavior without recompiling.
pub(crate) fn propagate_dir_index_insert(
    ctx: &Rc<RpcHandlerContext>,
    path: &str,
    ty: crate::metadata::InodeType,
) {
    if !central_parent_index_enabled() {
        return;
    }
    let Some(parent) = parent_of(path) else {
        return;
    };
    let owner = match ctx
        .metadata_manager
        .get_owner_node(std::path::Path::new(&parent))
    {
        Ok(o) => o,
        Err(_) => return,
    };
    if owner == ctx.self_node_id {
        return; // already inserted locally via store_*_metadata
    }
    let pool = match &ctx.connection_pool {
        Some(p) => p.clone(),
        None => return,
    };
    let path_owned = path.to_string();
    let owner_owned = owner;
    // `spawn` (no name) instead of `spawn_with_name` because mdtest-easy
    // fires this 320M times per phase — a per-RPC `String::from(name)`
    // alloc would dominate. `tracing::warn!` below retains visibility on
    // the failure paths, which is what we actually care to debug.
    pluvio_runtime::spawn(async move {
        use crate::rpc::dir_index_ops::DirIndexUpdateRequest;
        match pool.get_or_connect(&owner_owned).await {
            Ok(client) => {
                let req = DirIndexUpdateRequest::insert(path_owned, ty);
                if let Err(e) =
                    <DirIndexUpdateRequest as crate::rpc::AmRpc>::call(&req, &client).await
                {
                    tracing::warn!("DirIndexUpdate insert -> {} failed: {:?}", owner_owned, e);
                }
            }
            Err(e) => {
                tracing::warn!(
                    "DirIndexUpdate: failed to connect to {}: {:?}",
                    owner_owned,
                    e
                );
            }
        }
    });
}

pub(crate) fn propagate_dir_index_remove(ctx: &Rc<RpcHandlerContext>, path: &str) {
    if !central_parent_index_enabled() {
        return;
    }
    let Some(parent) = parent_of(path) else {
        return;
    };
    let owner = match ctx
        .metadata_manager
        .get_owner_node(std::path::Path::new(&parent))
    {
        Ok(o) => o,
        Err(_) => return,
    };
    if owner == ctx.self_node_id {
        return;
    }
    let pool = match &ctx.connection_pool {
        Some(p) => p.clone(),
        None => return,
    };
    let path_owned = path.to_string();
    let owner_owned = owner;
    // See note in propagate_dir_index_insert about avoiding spawn_with_name
    // in this per-RPC fire-and-forget path.
    pluvio_runtime::spawn(async move {
        use crate::rpc::dir_index_ops::DirIndexUpdateRequest;
        match pool.get_or_connect(&owner_owned).await {
            Ok(client) => {
                let req = DirIndexUpdateRequest::remove(path_owned);
                if let Err(e) =
                    <DirIndexUpdateRequest as crate::rpc::AmRpc>::call(&req, &client).await
                {
                    tracing::warn!("DirIndexUpdate remove -> {} failed: {:?}", owner_owned, e);
                }
            }
            Err(e) => {
                tracing::warn!(
                    "DirIndexUpdate: failed to connect to {}: {:?}",
                    owner_owned,
                    e
                );
            }
        }
    });
}

fn central_parent_index_enabled() -> bool {
    crate::runtime_config::RuntimeConfig::global()
        .metadata
        .central_parent_index
}

/// Pulls `path_len` UTF-8 bytes from `rest` (the trailing portion of the
/// small_req body after the per-RPC header). Returns `None` on
/// malformed/short input.
fn take_path<'b>(rest: &'b [u8], path_len: usize) -> Option<&'b str> {
    if rest.len() < path_len {
        return None;
    }
    std::str::from_utf8(&rest[..path_len]).ok()
}

impl LocustaServerHandler for MetadataLookupRequest {
    async fn handle_locusta(
        ctx: Rc<RpcHandlerContext>,
        body: Vec<u8>,
        req: Request<RegisteredFixedBuffer>,
    ) {
        let (header, rest) = match split_header::<MetadataLookupRequestHeader>(&body) {
            Some(pair) => pair,
            None => {
                reply_eager(
                    req,
                    MetadataLookupResponseHeader::error(-22) // EINVAL
                        .as_bytes()
                        .to_vec(),
                );
                return;
            }
        };
        let path_str = match take_path(rest, header.path_len as usize) {
            Some(s) => s,
            None => {
                reply_eager(
                    req,
                    MetadataLookupResponseHeader::error(-22).as_bytes().to_vec(),
                );
                return;
            }
        };
        let path = Path::new(path_str);

        let resp = if let Ok(file_meta) = ctx.metadata_manager.get_file_metadata(path) {
            MetadataLookupResponseHeader::file(file_meta.size)
        } else if ctx.metadata_manager.get_dir_metadata(path).is_ok() {
            MetadataLookupResponseHeader::directory()
        } else {
            // Lazy recovery: in-memory miss; consult on-disk inode store.
            // Cheap fast path (Option check) when persistence is off.
            if let Some(store) = ctx.inode_store.as_ref() {
                if store.policy().flushes_per_op() {
                    match store.read_inode(path_str).await {
                        Ok(Some(on_disk)) => {
                            use crate::storage::OnDiskInodeType;
                            match on_disk.inode_type {
                                OnDiskInodeType::File => {
                                    // Populate the in-memory cache so future
                                    // lookups stay fast.
                                    let fm = FileMetadata::new(
                                        path_str.to_string(),
                                        on_disk.logical_size,
                                    );
                                    let _ = ctx.metadata_manager.store_file_metadata(fm);
                                    let resp_hdr =
                                        MetadataLookupResponseHeader::file(on_disk.logical_size);
                                    reply_eager(req, resp_hdr.as_bytes().to_vec());
                                    return;
                                }
                                OnDiskInodeType::Directory => {
                                    let inode = ctx.metadata_manager.generate_inode();
                                    let dm = DirectoryMetadata::new(inode, path_str.to_string());
                                    let _ = ctx.metadata_manager.store_dir_metadata(dm);
                                    let resp_hdr = MetadataLookupResponseHeader::directory();
                                    reply_eager(req, resp_hdr.as_bytes().to_vec());
                                    return;
                                }
                                OnDiskInodeType::Symlink => {} // not yet supported
                            }
                        }
                        Ok(None) => {}
                        Err(e) => {
                            eprintln!(
                                "[locusta_handlers] inode_store.read_inode({}) failed: {}",
                                path_str, e
                            );
                        }
                    }
                }
            }
            MetadataLookupResponseHeader::not_found()
        };
        reply_eager(req, resp.as_bytes().to_vec());
    }
}

impl LocustaServerHandler for MetadataCreateFileRequest {
    async fn handle_locusta(
        ctx: Rc<RpcHandlerContext>,
        body: Vec<u8>,
        req: Request<RegisteredFixedBuffer>,
    ) {
        let (header, rest) = match split_header::<MetadataCreateFileRequestHeader>(&body) {
            Some(pair) => pair,
            None => {
                reply_eager(
                    req,
                    MetadataCreateFileResponseHeader::error(-22)
                        .as_bytes()
                        .to_vec(),
                );
                return;
            }
        };
        let path_str = match take_path(rest, header.path_len as usize) {
            Some(s) => s,
            None => {
                reply_eager(
                    req,
                    MetadataCreateFileResponseHeader::error(-22)
                        .as_bytes()
                        .to_vec(),
                );
                return;
            }
        };
        let file_meta = FileMetadata::new(path_str.to_string(), header.size);
        let resp = match ctx.metadata_manager.store_file_metadata(file_meta) {
            Ok(()) => {
                // CHFS-style central parent index — also mirror to the
                // parent's owner so readdir is a single RPC.
                propagate_dir_index_insert(&ctx, path_str, crate::metadata::InodeType::File);
                // Persist on disk best-effort. RAM is the cache; disk is
                // the source of truth for restart. We await the write
                // inline — each RPC handler is already its own task
                // dispatched by locusta_server, so multiple in-flight
                // inode writes happen naturally in parallel. Errors are
                // logged and do not fail the RPC.
                if let Some(store) = ctx.inode_store.as_ref() {
                    if store.policy().flushes_per_op() {
                        if let Err(e) = store.create_file(path_str, header.mode, header.size).await
                        {
                            eprintln!(
                                "[locusta_handlers] inode_store.create_file({}) failed: {}",
                                path_str, e
                            );
                        }
                    }
                }
                MetadataCreateFileResponseHeader::success(0)
            }
            Err(crate::metadata::manager::MetadataError::AlreadyExists(_)) => {
                MetadataCreateFileResponseHeader::error(-17) // EEXIST
            }
            Err(_) => MetadataCreateFileResponseHeader::error(-5), // EIO
        };
        reply_eager(req, resp.as_bytes().to_vec());
    }
}

impl LocustaServerHandler for MetadataCreateDirRequest {
    async fn handle_locusta(
        ctx: Rc<RpcHandlerContext>,
        body: Vec<u8>,
        req: Request<RegisteredFixedBuffer>,
    ) {
        let (header, rest) = match split_header::<MetadataCreateDirRequestHeader>(&body) {
            Some(pair) => pair,
            None => {
                reply_eager(
                    req,
                    MetadataCreateDirResponseHeader::error(-22)
                        .as_bytes()
                        .to_vec(),
                );
                return;
            }
        };
        let path_str = match take_path(rest, header.path_len as usize) {
            Some(s) => s,
            None => {
                reply_eager(
                    req,
                    MetadataCreateDirResponseHeader::error(-22)
                        .as_bytes()
                        .to_vec(),
                );
                return;
            }
        };
        let inode = ctx.metadata_manager.generate_inode();
        let dir_meta = DirectoryMetadata::new(inode, path_str.to_string());
        let resp = match ctx.metadata_manager.store_dir_metadata(dir_meta) {
            Ok(()) => {
                propagate_dir_index_insert(&ctx, path_str, crate::metadata::InodeType::Directory);
                if let Some(store) = ctx.inode_store.as_ref() {
                    if store.policy().flushes_per_op() {
                        if let Err(e) = store.create_dir(path_str, header.mode).await {
                            eprintln!(
                                "[locusta_handlers] inode_store.create_dir({}) failed: {}",
                                path_str, e
                            );
                        }
                    }
                }
                MetadataCreateDirResponseHeader::success(inode)
            }
            Err(crate::metadata::manager::MetadataError::AlreadyExists(_)) => {
                MetadataCreateDirResponseHeader::error(-17) // EEXIST
            }
            Err(_) => MetadataCreateDirResponseHeader::error(-5), // EIO
        };
        reply_eager(req, resp.as_bytes().to_vec());
    }
}

impl LocustaServerHandler for MetadataDeleteRequest {
    async fn handle_locusta(
        ctx: Rc<RpcHandlerContext>,
        body: Vec<u8>,
        req: Request<RegisteredFixedBuffer>,
    ) {
        let (header, rest) = match split_header::<MetadataDeleteRequestHeader>(&body) {
            Some(pair) => pair,
            None => {
                reply_eager(
                    req,
                    MetadataDeleteResponseHeader::error(-22).as_bytes().to_vec(),
                );
                return;
            }
        };
        let path_str = match take_path(rest, header.path_len as usize) {
            Some(s) => s,
            None => {
                reply_eager(
                    req,
                    MetadataDeleteResponseHeader::error(-22).as_bytes().to_vec(),
                );
                return;
            }
        };
        let path = Path::new(path_str);
        let resp = match header.entry_type {
            1 => match ctx.metadata_manager.remove_file_metadata(path) {
                Ok(()) => {
                    propagate_dir_index_remove(&ctx, path_str);
                    if let Some(store) = ctx.inode_store.as_ref() {
                        if store.policy().flushes_per_op() {
                            if let Err(e) = store.remove(path_str, false).await {
                                eprintln!(
                                    "[locusta_handlers] inode_store.remove({}) failed: {}",
                                    path_str, e
                                );
                            }
                        }
                    }
                    MetadataDeleteResponseHeader::success()
                }
                Err(_) => MetadataDeleteResponseHeader::error(-2), // ENOENT
            },
            2 => match ctx.metadata_manager.remove_dir_metadata(path) {
                Ok(()) => {
                    propagate_dir_index_remove(&ctx, path_str);
                    if let Some(store) = ctx.inode_store.as_ref() {
                        if store.policy().flushes_per_op() {
                            if let Err(e) = store.remove(path_str, true).await {
                                eprintln!(
                                    "[locusta_handlers] inode_store.remove_dir({}) failed: {}",
                                    path_str, e
                                );
                            }
                        }
                    }
                    MetadataDeleteResponseHeader::success()
                }
                Err(_) => MetadataDeleteResponseHeader::error(-2),
            },
            _ => MetadataDeleteResponseHeader::error(-22), // EINVAL
        };
        reply_eager(req, resp.as_bytes().to_vec());
    }
}

impl LocustaServerHandler for MetadataUpdateRequest {
    async fn handle_locusta(
        ctx: Rc<RpcHandlerContext>,
        body: Vec<u8>,
        req: Request<RegisteredFixedBuffer>,
    ) {
        let (header, rest) = match split_header::<MetadataUpdateRequestHeader>(&body) {
            Some(pair) => pair,
            None => {
                reply_eager(
                    req,
                    MetadataUpdateResponseHeader::error(-22).as_bytes().to_vec(),
                );
                return;
            }
        };
        let path_str = match take_path(rest, header.path_len as usize) {
            Some(s) => s,
            None => {
                reply_eager(
                    req,
                    MetadataUpdateResponseHeader::error(-22).as_bytes().to_vec(),
                );
                return;
            }
        };
        let path = Path::new(path_str);
        let mut file_meta = match ctx.metadata_manager.get_file_metadata(path) {
            Ok(m) => m,
            Err(_) => {
                reply_eager(
                    req,
                    MetadataUpdateResponseHeader::error(-2).as_bytes().to_vec(), // ENOENT
                );
                return;
            }
        };
        if header.should_update_size() {
            file_meta.size = if header.new_size == 0 {
                0
            } else {
                file_meta.size.max(header.new_size)
            };
        }
        let new_size_after = file_meta.size;
        let resp = match ctx.metadata_manager.update_file_metadata(file_meta) {
            Ok(()) => {
                if let Some(store) = ctx.inode_store.as_ref() {
                    if store.policy().flushes_per_op() {
                        if let Err(e) = store.update_size(path_str, new_size_after).await {
                            eprintln!(
                                "[locusta_handlers] inode_store.update_size({}) failed: {}",
                                path_str, e
                            );
                        }
                    }
                }
                MetadataUpdateResponseHeader::success()
            }
            Err(_) => MetadataUpdateResponseHeader::error(-5), // EIO
        };
        reply_eager(req, resp.as_bytes().to_vec());
    }
}

// ============================================================================
// Chunk RPCs (data_ops.rs)
// ============================================================================

/// Resolve a path from `path_hash`. Mirrors UCX `WriteChunkById::server_handler`
/// behaviour: fall back to a synthetic `/benchfs/<hash>` when the FileId
/// registry hasn't seen this hash yet (e.g. clients using FileId-only
/// access). All chunk RPCs share this helper so paths line up.
fn resolve_chunk_path(ctx: &Rc<RpcHandlerContext>, path_hash: u32) -> String {
    ctx.file_id_registry()
        .lookup(path_hash)
        .unwrap_or_else(|| format!("/benchfs/{:08x}", path_hash))
}

/// FNV-1a 64-bit hash. Used only when `BENCHFS_INTEGRITY_LOG=1` to
/// diagnose ior-hard-read verification errors: server logs a hash of
/// the chunk buffer at write-completion and at read-start, and the
/// pair is compared offline to localise corruption (disk vs network).
#[inline]
fn quick_hash(buf: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    let chunks = buf.chunks_exact(8);
    let rem = chunks.remainder();
    for c in chunks {
        let v = u64::from_ne_bytes(c.try_into().unwrap());
        h ^= v;
        h = h.wrapping_mul(0x100000001b3);
    }
    for &b in rem {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

fn integrity_log_enabled() -> bool {
    crate::runtime_config::RuntimeConfig::global()
        .observability
        .integrity_log
}

/// Per-PID integrity log writer. Returns a `&'static Mutex<File>` opened
/// in append mode at `${observability.integrity_dir}/benchfs-integrity-${pid}.log`.
/// Eliminates the cross-process stderr interleaving that made iter25's
/// log unparseable for chunk_offsets near other ranks' offsets.
fn integrity_log_writer() -> &'static std::sync::Mutex<std::fs::File> {
    use std::sync::OnceLock;
    static W: OnceLock<std::sync::Mutex<std::fs::File>> = OnceLock::new();
    W.get_or_init(|| {
        let dir = crate::runtime_config::RuntimeConfig::global()
            .observability
            .integrity_dir
            .clone();
        let pid = std::process::id();
        let path = format!("{}/benchfs-integrity-{}.log", dir, pid);
        let f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .unwrap_or_else(|e| panic!("integrity log open {} failed: {e}", path));
        std::sync::Mutex::new(f)
    })
}

#[inline]
fn integrity_emit(line: &str) {
    use std::io::Write;
    if let Ok(mut w) = integrity_log_writer().lock() {
        let _ = writeln!(*w, "{line}");
    }
}

/// Counters for the WriteChunkById Put pipeline. Used to localise hangs:
/// if `granted` keeps rising while `replied` is flat, the bottleneck is in
/// `write_chunk_via_registered`; if `received` keeps rising while
/// `granted` is flat, allocator exhaustion is throttling PutGrant; if all
/// three rise together but client still sees hang, the issue is upstream
/// (relay daemon, RDMA write_imm, send ring) of the server entirely.
pub static WCB_PUT_RECEIVED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static WCB_PUT_GRANTED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static WCB_PUT_GRANT_REJECTED: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub static WCB_PUT_READY: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static WCB_PUT_REPLIED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Sync fast-path for `WriteChunkById::PutGrant`. The body of this handler
/// has no `.await` (try_acquire + grant), so spawning it as a future onto
/// the executor is wasted overhead at ior-hard's ~327k RPCs/s aggregate.
/// `LocustaServerDispatch::dispatch` calls this inline instead of going
/// through the async `handle_locusta` + executor::spawn path.
pub fn handle_write_chunk_put_grant_sync(
    ctx: &Rc<RpcHandlerContext>,
    body: &[u8],
    h: rrrpc::server::PutGrantHandle<RegisteredFixedBuffer>,
) {
    WCB_PUT_RECEIVED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let header = match split_header::<WriteChunkByIdRequestHeader>(body) {
        Some((hdr, _)) => hdr,
        None => {
            WCB_PUT_GRANT_REJECTED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            drop(h);
            return;
        }
    };
    let grant_start = std::time::Instant::now();
    if crate::stats::is_stats_enabled() {
        let key = (header.path_hash(), header.chunk_id() as u64);
        PUT_GRANT_TS.with(|m| {
            m.borrow_mut().insert(key, grant_start);
        });
    }
    let alloc = Rc::clone(&ctx.allocator);
    let fb = match pluvio_uring::allocator::FixedBufferAllocator::try_acquire(&alloc) {
        Some(fb) => fb,
        None => {
            WCB_PUT_GRANT_REJECTED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            eprintln!("[locusta_handlers] WriteChunkById PutGrant: allocator empty");
            drop(h);
            return;
        }
    };
    let alloc_us = grant_start.elapsed().as_micros() as u64;
    if (fb.len() as u32) < h.dma_len() {
        WCB_PUT_GRANT_REJECTED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        eprintln!(
            "[locusta_handlers] WriteChunkById buffer too small ({} < {})",
            fb.len(),
            h.dma_len()
        );
        drop(fb);
        drop(h);
        return;
    }
    h.grant(RegisteredFixedBuffer::from_fixed_buffer(fb));
    WCB_PUT_GRANTED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    if crate::stats::is_stats_enabled() {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        if n.is_multiple_of(1000) {
            tracing::info!(
                target: "rpc_handler_timing",
                kind = "write_grant_sync",
                n = n,
                alloc_us = alloc_us,
                "WRITE_GRANT_TIMING"
            );
        }
    }
}

impl LocustaServerHandler for WriteChunkByIdRequest<'_> {
    async fn handle_locusta(
        ctx: Rc<RpcHandlerContext>,
        body: Vec<u8>,
        req: Request<RegisteredFixedBuffer>,
    ) {
        let (header, _) = match split_header::<WriteChunkByIdRequestHeader>(&body) {
            Some(pair) => pair,
            None => {
                // Drop the handle on malformed input — sends an error reply.
                drop(req);
                return;
            }
        };

        match req {
            Request::PutGrant(h) => {
                // Fallback async path; `LocustaServerDispatch::dispatch`
                // now bypasses this via `handle_write_chunk_put_grant_sync`
                // for ior-hard's hot path. Kept so the trait stays complete
                // and the async path still works if dispatch ever changes.
                handle_write_chunk_put_grant_sync(&ctx, &body, h);
                // header is unused on this branch when the sync helper
                // owns parsing; silence the dead-code path-hash extraction.
                let _ = header;
            }
            Request::RoundtripPutReady(h) => {
                WCB_PUT_READY.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let ready_start = std::time::Instant::now();
                // Compute grant→ready gap: time between PutGrant arrival
                // and PutReady arrival = RDMA write transit + relay
                // daemon scheduling. If this is large but io_us is small,
                // the bottleneck is in the transit, not the disk.
                let grant_to_ready_us = if crate::stats::is_stats_enabled() {
                    let key = (header.path_hash(), header.chunk_id() as u64);
                    PUT_GRANT_TS.with(|m| {
                        m.borrow_mut()
                            .remove(&key)
                            .map(|t| ready_start.duration_since(t).as_micros() as u64)
                    })
                } else {
                    None
                };
                // Phase 2: client's RDMA write has landed in the granted
                // buffer. The buffer is RDMA-registered AND was acquired
                // from the same io_uring allocator the chunk store uses,
                // so we can submit `WriteFixed` against it in place and
                // avoid the 4 MiB memcpy in `IOUringBackend::write`
                // (job 17061 timing).
                let path = resolve_chunk_path(&ctx, header.path_hash());
                let chunk_index = header.chunk_id() as u64;
                let offset = header.offset;
                let data_len = header.length as usize;
                let buf = h.buffer();
                let ptr = buf.as_ptr();
                let len = data_len.min(buf.len()) as u32;
                let buf_index = buf.buf_index();
                if integrity_log_enabled() {
                    let slice = unsafe { std::slice::from_raw_parts(ptr, len as usize) };
                    let hash = quick_hash(slice);
                    integrity_emit(&format!(
                        "WRITE path_hash={:08x} chunk={} offset={} len={} hash={:016x}",
                        header.path_hash(),
                        chunk_index,
                        offset,
                        len,
                        hash
                    ));
                }
                let io_start = std::time::Instant::now();
                let io_store = ctx
                    .chunk_store
                    .as_any()
                    .downcast_ref::<crate::storage::IOUringChunkStore>();
                let resp = match io_store {
                    Some(store) => {
                        match store
                            .write_chunk_via_registered(
                                &path,
                                chunk_index,
                                offset,
                                ptr,
                                len,
                                buf_index,
                            )
                            .await
                        {
                            Ok(bytes) => WriteChunkResponseHeader::success(bytes as u64),
                            Err(e) => {
                                eprintln!(
                                    "[locusta_handlers] WriteChunkById via_registered failed: {e:?}"
                                );
                                WriteChunkResponseHeader::error(-5)
                            }
                        }
                    }
                    None => {
                        let slice: &[u8] = unsafe { std::slice::from_raw_parts(ptr, len as usize) };
                        match ctx
                            .chunk_store
                            .write_chunk(&path, chunk_index, offset, slice)
                            .await
                        {
                            Ok(bytes) => WriteChunkResponseHeader::success(bytes as u64),
                            Err(e) => {
                                eprintln!("[locusta_handlers] WriteChunkById store failed: {e:?}");
                                WriteChunkResponseHeader::error(-5)
                            }
                        }
                    }
                };
                let io_us = io_start.elapsed().as_micros() as u64;
                h.reply(resp.as_bytes().to_vec());
                WCB_PUT_REPLIED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let total_us = ready_start.elapsed().as_micros() as u64;
                if crate::stats::is_stats_enabled() {
                    use std::sync::atomic::{AtomicU64, Ordering};
                    static N: AtomicU64 = AtomicU64::new(0);
                    let n = N.fetch_add(1, Ordering::Relaxed);
                    if n.is_multiple_of(1000) {
                        tracing::info!(
                            target: "rpc_handler_timing",
                            kind = "write_ready",
                            n = n,
                            io_us = io_us,
                            total_us = total_us,
                            grant_to_ready_us = grant_to_ready_us.unwrap_or(0),
                            data_len = data_len,
                            "WRITE_READY_TIMING"
                        );
                    }
                }
            }
            Request::RoundtripEager(h) => {
                // Eager fast-path for small writes (mdtest-hard's 3901-byte
                // payload). Client packed header + payload into the eager
                // body and we write straight through to the chunk store.
                let profile = crate::rpc::perf_breakdown::is_enabled();
                let t_total = if profile {
                    Some(std::time::Instant::now())
                } else {
                    None
                };
                let header_size = std::mem::size_of::<WriteChunkByIdRequestHeader>();
                if body.len() < header_size {
                    h.reply(WriteChunkResponseHeader::error(-22).as_bytes().to_vec());
                    return;
                }
                let payload = &body[header_size..];
                let parse_us = t_total.map(|t| t.elapsed().as_micros() as u64).unwrap_or(0);
                let t_path = if profile {
                    Some(std::time::Instant::now())
                } else {
                    None
                };
                let path = resolve_chunk_path(&ctx, header.path_hash());
                let path_us = t_path.map(|t| t.elapsed().as_micros() as u64).unwrap_or(0);
                let chunk_index = header.chunk_id() as u64;
                let offset = header.offset;

                let t_disk = if profile {
                    Some(std::time::Instant::now())
                } else {
                    None
                };
                let resp = match ctx
                    .chunk_store
                    .write_chunk(&path, chunk_index, offset, payload)
                    .await
                {
                    Ok(bytes) => WriteChunkResponseHeader::success(bytes as u64),
                    Err(e) => {
                        eprintln!("[locusta_handlers] WriteChunkById eager store failed: {e:?}");
                        WriteChunkResponseHeader::error(-5)
                    }
                };
                let disk_us = t_disk.map(|t| t.elapsed().as_micros() as u64).unwrap_or(0);
                let t_reply = if profile {
                    Some(std::time::Instant::now())
                } else {
                    None
                };
                h.reply(resp.as_bytes().to_vec());
                if profile {
                    let reply_us = t_reply.map(|t| t.elapsed().as_micros() as u64).unwrap_or(0);
                    let total_us = t_total.map(|t| t.elapsed().as_micros() as u64).unwrap_or(0);
                    crate::rpc::perf_breakdown::srv_record(
                        parse_us, path_us, disk_us, reply_us, total_us,
                    );
                }
            }
            other => {
                eprintln!(
                    "[locusta_handlers] WriteChunkById unexpected variant {:?}",
                    std::mem::discriminant(&other)
                );
                drop(other);
            }
        }
    }
}

impl LocustaServerHandler for ReadChunkByIdRequest<'_> {
    async fn handle_locusta(
        ctx: Rc<RpcHandlerContext>,
        body: Vec<u8>,
        req: Request<RegisteredFixedBuffer>,
    ) {
        // Static debug counter to confirm reads are reaching the handler.
        use std::sync::atomic::{AtomicU64, Ordering};
        static READ_HANDLER_INVOCATIONS: AtomicU64 = AtomicU64::new(0);
        let nth = READ_HANDLER_INVOCATIONS.fetch_add(1, Ordering::Relaxed);
        if nth < 5 || nth.is_multiple_of(10000) {
            tracing::info!("[locusta_handlers] ReadChunkById handler invocation #{nth}");
        }
        let (header, _) = match split_header::<ReadChunkByIdRequestHeader>(&body) {
            Some(pair) => pair,
            None => {
                drop(req);
                return;
            }
        };

        match req {
            Request::RoundtripGet(h) => {
                let handler_start = std::time::Instant::now();
                let path = resolve_chunk_path(&ctx, header.path_hash());
                let chunk_index = header.chunk_id() as u64;
                let offset = header.offset;
                let length = header.length;

                // Zero-copy path: read directly into an RDMA-registered
                // FixedBuffer via io_uring fixed buffer read, then hand
                // that same buffer to the locusta handle for RDMA send.
                // The old path read into a Vec<u8>, allocated a separate
                // FixedBuffer, and memcpy'd 4 MiB between them — that
                // memcpy was the dominant read-path overhead (job 17048
                // showed 5.78 GiB/s read vs 11.04 GiB/s write at the
                // same chunk size, with dispatch latency ruled out).
                let alloc = Rc::clone(&ctx.allocator);
                let alloc_start = std::time::Instant::now();
                let fb = match pluvio_uring::allocator::FixedBufferAllocator::try_acquire(&alloc) {
                    Some(fb) => fb,
                    None => {
                        eprintln!("[locusta_handlers] ReadChunkById: server allocator empty");
                        drop(h);
                        return;
                    }
                };
                let alloc_us = alloc_start.elapsed().as_micros() as u64;

                let io_store = ctx
                    .chunk_store
                    .as_any()
                    .downcast_ref::<crate::storage::IOUringChunkStore>();
                let io_start = std::time::Instant::now();
                let (n, fb) = match io_store {
                    Some(store) => {
                        match store
                            .read_chunk_fixed(&path, chunk_index, offset, length, fb)
                            .await
                        {
                            Ok((n, fb)) => (n, fb),
                            Err(e) => {
                                eprintln!(
                                    "[locusta_handlers] ReadChunkById store_fixed failed: {e:?}"
                                );
                                drop(h);
                                return;
                            }
                        }
                    }
                    None => {
                        // Fallback for non-io_uring chunk stores (tests):
                        // do the old read-into-Vec + copy path.
                        let data = match ctx
                            .chunk_store
                            .read_chunk(&path, chunk_index, offset, length)
                            .await
                        {
                            Ok(d) => d,
                            Err(e) => {
                                eprintln!("[locusta_handlers] ReadChunkById store failed: {e:?}");
                                drop(h);
                                return;
                            }
                        };
                        let mut fb = fb;
                        let n = data.len().min(fb.len()).min(h.dma_len() as usize);
                        fb.as_mut_slice()[..n].copy_from_slice(&data[..n]);
                        (n, fb)
                    }
                };

                let io_us = io_start.elapsed().as_micros() as u64;
                let n = n.min(h.dma_len() as usize);
                if nth < 5 {
                    tracing::info!(
                        "[locusta_handlers] ReadChunkById #{nth}: read_chunk_fixed returned {n} bytes (dma_len={})",
                        h.dma_len()
                    );
                }
                // SHRINK to `n` so the locusta RDMA WRITE only transfers `n`
                // bytes, not the full 4 MiB FixedBuffer. Without this, ior-hard's
                // 47008-byte reads cause the server to RDMA-write 4 MiB into a
                // 47008-byte client buffer, trampling adjacent DMA-arena slots
                // (job 20115/20118 ~0.3% verification errors).
                if integrity_log_enabled() {
                    let slice = unsafe { std::slice::from_raw_parts(fb.as_ptr(), n) };
                    let hash = quick_hash(slice);
                    integrity_emit(&format!(
                        "READ path_hash={:08x} chunk={} offset={} len={} hash={:016x}",
                        header.path_hash(),
                        chunk_index,
                        offset,
                        n,
                        hash
                    ));
                }
                let mut buf = RegisteredFixedBuffer::from_fixed_buffer(fb);
                buf.shrink_to(n);
                let resp = ReadChunkResponseHeader::success(n as u64);
                let reply_start = std::time::Instant::now();
                h.reply(buf, resp.as_bytes().to_vec());
                let reply_us = reply_start.elapsed().as_micros() as u64;
                let total_us = handler_start.elapsed().as_micros() as u64;
                if crate::stats::is_stats_enabled() {
                    use std::sync::atomic::{AtomicU64, Ordering};
                    static N: AtomicU64 = AtomicU64::new(0);
                    let n_log = N.fetch_add(1, Ordering::Relaxed);
                    if n_log.is_multiple_of(1000) {
                        tracing::info!(
                            target: "rpc_handler_timing",
                            kind = "read_handler",
                            n = n_log,
                            alloc_us = alloc_us,
                            io_us = io_us,
                            reply_us = reply_us,
                            total_us = total_us,
                            data_len = n,
                            "READ_HANDLER_TIMING"
                        );
                    }
                }
                if nth < 5 {
                    tracing::info!("[locusta_handlers] ReadChunkById #{nth}: h.reply returned");
                }
            }
            other => {
                eprintln!(
                    "[locusta_handlers] ReadChunkById unexpected variant {:?}",
                    std::mem::discriminant(&other)
                );
                drop(other);
            }
        }
    }
}

// ============================================================================
// Readdir
// ============================================================================

impl LocustaServerHandler for ReaddirRequest {
    async fn handle_locusta(
        ctx: Rc<RpcHandlerContext>,
        body: Vec<u8>,
        req: Request<RegisteredFixedBuffer>,
    ) {
        let t_start = std::time::Instant::now();
        let (header, rest) = match split_header::<ReaddirRequestHeader>(&body) {
            Some(pair) => pair,
            None => {
                reply_eager(req, ReaddirResponseHeader::error(-22).as_bytes().to_vec());
                return;
            }
        };
        let path_str = match take_path(rest, header.path_len as usize) {
            Some(s) => s,
            None => {
                reply_eager(req, ReaddirResponseHeader::error(-22).as_bytes().to_vec());
                return;
            }
        };

        let max = header.max_entries.clamp(1, READDIR_MAX_ENTRIES_PER_CALL) as usize;
        let offset = header.offset as usize;

        let t_list = std::time::Instant::now();
        let (entries, truncated) = ctx
            .metadata_manager
            .list_local_entries_with_size(path_str, offset, max);
        let list_us = t_list.elapsed().as_micros();

        let t_encode = std::time::Instant::now();
        let mut entry_data: Vec<u8> = Vec::with_capacity(entries.len() * 56);
        let packed = encode_entries(&mut entry_data, &entries);
        let eod = !truncated && packed == entries.len();
        let resp_header =
            ReaddirResponseHeader::success(packed as u32, entry_data.len() as u32, eod);

        let mut out =
            Vec::with_capacity(std::mem::size_of::<ReaddirResponseHeader>() + entry_data.len());
        out.extend_from_slice(resp_header.as_bytes());
        out.extend_from_slice(&entry_data);
        let encode_us = t_encode.elapsed().as_micros();
        let resp_bytes = out.len();
        let total_us = t_start.elapsed().as_micros();

        // Only log slow calls (>5 ms) to keep stderr light at scale.
        if total_us > 5000 {
            eprintln!(
                "[svr_readdir_slow] path={} offset={} entries={} truncated={} resp_bytes={} list_us={} encode_us={} total_us={}",
                path_str,
                offset,
                entries.len(),
                truncated,
                resp_bytes,
                list_us,
                encode_us,
                total_us
            );
        }
        reply_eager(req, out);
    }
}
