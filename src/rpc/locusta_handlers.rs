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
