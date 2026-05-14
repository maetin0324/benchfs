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
fn reply_eager(req: Request<RegisteredFixedBuffer>, bytes: Vec<u8>) {
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
            Ok(()) => MetadataCreateFileResponseHeader::success(0),
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
            Ok(()) => MetadataCreateDirResponseHeader::success(inode),
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
                Ok(()) => MetadataDeleteResponseHeader::success(),
                Err(_) => MetadataDeleteResponseHeader::error(-2), // ENOENT
            },
            2 => match ctx.metadata_manager.remove_dir_metadata(path) {
                Ok(()) => MetadataDeleteResponseHeader::success(),
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
        let resp = match ctx.metadata_manager.update_file_metadata(file_meta) {
            Ok(()) => MetadataUpdateResponseHeader::success(),
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
                let grant_start = std::time::Instant::now();
                // Record PutGrant arrival timestamp keyed by chunk id so
                // we can compute the grant→ready gap when PutReady fires
                // for the same chunk. Lets us see where the missing time
                // between server WRITE_GRANT and WRITE_READY actually sits
                // (RDMA transit, daemon scheduling, etc).
                if crate::stats::is_stats_enabled() {
                    let key = (header.path_hash(), header.chunk_id() as u64);
                    PUT_GRANT_TS.with(|m| {
                        m.borrow_mut().insert(key, grant_start);
                    });
                }
                // Phase 1: allocate a server-side RDMA-registered buffer
                // big enough for the client's incoming DMA write.
                // `try_acquire` has receiver `&Rc<Self>` so we use UFCS
                // to dodge `Rc`-deref method resolution.
                let alloc = Rc::clone(&ctx.allocator);
                let fb = match pluvio_uring::allocator::FixedBufferAllocator::try_acquire(&alloc) {
                    Some(fb) => fb,
                    None => {
                        eprintln!("[locusta_handlers] WriteChunkById PutGrant: allocator empty");
                        drop(h);
                        return;
                    }
                };
                let alloc_us = grant_start.elapsed().as_micros() as u64;
                if (fb.len() as u32) < h.dma_len() {
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
                if crate::stats::is_stats_enabled() {
                    use std::sync::atomic::{AtomicU64, Ordering};
                    static N: AtomicU64 = AtomicU64::new(0);
                    let n = N.fetch_add(1, Ordering::Relaxed);
                    if n.is_multiple_of(1000) {
                        tracing::info!(
                            target: "rpc_handler_timing",
                            kind = "write_grant",
                            n = n,
                            alloc_us = alloc_us,
                            "WRITE_GRANT_TIMING"
                        );
                    }
                }
            }
            Request::RoundtripPutReady(h) => {
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
                let buf = RegisteredFixedBuffer::from_fixed_buffer(fb);
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
