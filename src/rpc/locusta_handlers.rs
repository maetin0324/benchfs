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

use crate::rpc::handlers::RpcHandlerContext;
use crate::rpc::locusta_buffer::RegisteredFixedBuffer;
use crate::rpc::locusta_server::LocustaServerHandler;
use crate::rpc::metadata_ops::{
    MetadataLookupRequest, MetadataLookupRequestHeader, MetadataLookupResponseHeader,
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

impl LocustaServerHandler for MetadataLookupRequest {
    fn handle_locusta(
        ctx: &Rc<RpcHandlerContext>,
        body: &[u8],
        req: Request<RegisteredFixedBuffer>,
    ) {
        let (header, rest) = match split_header::<MetadataLookupRequestHeader>(body) {
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
        let path_len = header.path_len as usize;
        if rest.len() < path_len {
            reply_eager(
                req,
                MetadataLookupResponseHeader::error(-22).as_bytes().to_vec(),
            );
            return;
        }
        let path_str = match std::str::from_utf8(&rest[..path_len]) {
            Ok(s) => s,
            Err(_) => {
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
