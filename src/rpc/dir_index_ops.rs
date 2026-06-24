//! DirIndexUpdate RPC — CHFS-style central parent index.
//!
//! When a file/directory is created on its content owner (computed by
//! `hash(path)`), this RPC propagates the (parent, child) entry to the
//! parent's owner (computed by `hash(parent_path)`). The parent's owner
//! then maintains the *full* children list for that parent, so a later
//! `readdir(parent)` can be served by a single RPC instead of a 40-server
//! fan-out.
//!
//! Design:
//! - Eager pattern, very small payload (header + path bytes).
//! - The handler simply forwards the path to
//!   `MetadataManager::dir_index_insert` / `dir_index_remove`, which
//!   already does ancestor propagation in the local index.
//! - Locusta-only path. The UCX `server_handler` returns an error.

use std::io::IoSlice;
use std::rc::Rc;

use pluvio_ucx::async_ucx::ucp::AmMsg;

#[cfg(feature = "transport-locusta")]
use rrrpc::server::Request;

use crate::metadata::InodeType;
use crate::rpc::helpers::{
    RpcIoSliceHelper, parse_header, receive_path, send_rpc_response_via_reply,
};
#[cfg(feature = "transport-locusta")]
use crate::rpc::locusta_buffer::RegisteredFixedBuffer;
#[cfg(feature = "transport-locusta")]
use crate::rpc::locusta_server::LocustaServerHandler;
use crate::rpc::{AmRpc, AmRpcCallType, RpcClient, RpcError, RpcId, ServerResponse};

pub const RPC_DIR_INDEX_UPDATE: RpcId = 27;

pub const DIR_INDEX_OP_INSERT: u8 = 0;
pub const DIR_INDEX_OP_REMOVE: u8 = 1;

pub const DIR_INDEX_TYPE_FILE: u8 = 1;
pub const DIR_INDEX_TYPE_DIR: u8 = 2;

#[repr(C)]
#[derive(
    Debug,
    Clone,
    Copy,
    zerocopy::FromBytes,
    zerocopy::IntoBytes,
    zerocopy::KnownLayout,
    zerocopy::Immutable,
)]
pub struct DirIndexUpdateRequestHeader {
    pub path_len: u32,
    pub op: u8,
    pub inode_type: u8,
    _padding: [u8; 2],
}

impl DirIndexUpdateRequestHeader {
    pub fn insert(path_len: usize, ty: InodeType) -> Self {
        Self {
            path_len: path_len as u32,
            op: DIR_INDEX_OP_INSERT,
            inode_type: match ty {
                InodeType::File => DIR_INDEX_TYPE_FILE,
                InodeType::Directory => DIR_INDEX_TYPE_DIR,
                InodeType::Symlink => DIR_INDEX_TYPE_FILE,
            },
            _padding: [0; 2],
        }
    }

    pub fn remove(path_len: usize) -> Self {
        Self {
            path_len: path_len as u32,
            op: DIR_INDEX_OP_REMOVE,
            inode_type: 0,
            _padding: [0; 2],
        }
    }

    pub fn inode_type_enum(&self) -> InodeType {
        match self.inode_type {
            DIR_INDEX_TYPE_DIR => InodeType::Directory,
            _ => InodeType::File,
        }
    }
}

#[repr(C)]
#[derive(
    Debug,
    Clone,
    Copy,
    zerocopy::FromBytes,
    zerocopy::IntoBytes,
    zerocopy::KnownLayout,
    zerocopy::Immutable,
)]
pub struct DirIndexUpdateResponseHeader {
    pub status: i32,
    _padding: [u8; 4],
}

impl DirIndexUpdateResponseHeader {
    pub fn success() -> Self {
        Self {
            status: 0,
            _padding: [0; 4],
        }
    }

    pub fn error(status: i32) -> Self {
        Self {
            status,
            _padding: [0; 4],
        }
    }

    pub fn is_success(&self) -> bool {
        self.status == 0
    }
}

pub struct DirIndexUpdateRequest {
    header: DirIndexUpdateRequestHeader,
    path: String,
    io_helper: RpcIoSliceHelper<1>,
}

impl DirIndexUpdateRequest {
    pub fn insert(path: String, ty: InodeType) -> Self {
        let header = DirIndexUpdateRequestHeader::insert(path.len(), ty);
        let io_helper = RpcIoSliceHelper::new(vec![path.as_bytes().to_vec()]);
        Self {
            header,
            path,
            io_helper,
        }
    }

    pub fn remove(path: String) -> Self {
        let header = DirIndexUpdateRequestHeader::remove(path.len());
        let io_helper = RpcIoSliceHelper::new(vec![path.as_bytes().to_vec()]);
        Self {
            header,
            path,
            io_helper,
        }
    }

    pub fn path(&self) -> &str {
        &self.path
    }
}

impl AmRpc for DirIndexUpdateRequest {
    type RequestHeader = DirIndexUpdateRequestHeader;
    type ResponseHeader = DirIndexUpdateResponseHeader;

    fn rpc_id() -> RpcId {
        RPC_DIR_INDEX_UPDATE
    }

    fn call_type(&self) -> AmRpcCallType {
        AmRpcCallType::None
    }

    fn request_header(&self) -> &Self::RequestHeader {
        &self.header
    }

    fn request_data(&self) -> &[IoSlice<'_>] {
        self.io_helper.get()
    }

    async fn call(&self, client: &RpcClient) -> Result<Self::ResponseHeader, RpcError> {
        client.execute(self).await
    }

    async fn call_no_reply(&self, _client: &RpcClient) -> Result<(), RpcError> {
        Err(RpcError::HandlerError(
            "DirIndexUpdate requires a reply".to_string(),
        ))
    }

    async fn server_handler(
        ctx: Rc<crate::rpc::handlers::RpcHandlerContext>,
        mut am_msg: AmMsg,
    ) -> Result<(ServerResponse<Self::ResponseHeader>, AmMsg), (RpcError, AmMsg)> {
        // UCX-side mirror of the locusta `handle_locusta` impl below. Kept
        // in lockstep so a transport A/B compares only UCX vs locusta, not
        // a logic gap.
        let (corr_id, header): (u32, DirIndexUpdateRequestHeader) = match parse_header(&am_msg) {
            Ok(x) => x,
            Err(e) => return Err((e, am_msg)),
        };

        let path_str = match receive_path(&ctx, &mut am_msg, header.path_len).await {
            Ok(p) => p,
            Err(e) => return Err((e, am_msg)),
        };

        let response_header = match header.op {
            DIR_INDEX_OP_INSERT => {
                ctx.metadata_manager
                    .dir_index_insert(&path_str, header.inode_type_enum());
                DirIndexUpdateResponseHeader::success()
            }
            DIR_INDEX_OP_REMOVE => {
                ctx.metadata_manager.dir_index_remove(&path_str);
                DirIndexUpdateResponseHeader::success()
            }
            _ => DirIndexUpdateResponseHeader::error(-22),
        };

        send_rpc_response_via_reply(
            Self::reply_stream_id(),
            corr_id,
            &response_header,
            None,
            am_msg,
        )
        .await
    }

    fn error_response(_error: &RpcError) -> Self::ResponseHeader {
        DirIndexUpdateResponseHeader::error(-5)
    }
}

#[cfg(feature = "transport-locusta")]
impl LocustaServerHandler for DirIndexUpdateRequest {
    async fn handle_locusta(
        ctx: Rc<crate::rpc::handlers::RpcHandlerContext>,
        body: Vec<u8>,
        req: Request<RegisteredFixedBuffer>,
    ) {
        use zerocopy::FromBytes;

        let header_size = std::mem::size_of::<DirIndexUpdateRequestHeader>();
        if body.len() < header_size {
            reply_status(req, -22);
            return;
        }
        let header: DirIndexUpdateRequestHeader =
            match DirIndexUpdateRequestHeader::read_from_bytes(&body[..header_size]) {
                Ok(h) => h,
                Err(_) => {
                    reply_status(req, -22);
                    return;
                }
            };
        let rest = &body[header_size..];
        let path_len = header.path_len as usize;
        if rest.len() < path_len {
            reply_status(req, -22);
            return;
        }
        let path_str = match std::str::from_utf8(&rest[..path_len]) {
            Ok(s) => s,
            Err(_) => {
                reply_status(req, -22);
                return;
            }
        };

        match header.op {
            DIR_INDEX_OP_INSERT => {
                ctx.metadata_manager
                    .dir_index_insert(path_str, header.inode_type_enum());
            }
            DIR_INDEX_OP_REMOVE => {
                ctx.metadata_manager.dir_index_remove(path_str);
            }
            _ => {
                reply_status(req, -22);
                return;
            }
        }
        reply_status(req, 0);
    }
}

#[cfg(feature = "transport-locusta")]
fn reply_status(req: Request<RegisteredFixedBuffer>, status: i32) {
    use zerocopy::IntoBytes;
    let resp = if status == 0 {
        DirIndexUpdateResponseHeader::success()
    } else {
        DirIndexUpdateResponseHeader::error(status)
    };
    let bytes = resp.as_bytes().to_vec();
    crate::rpc::locusta_handlers::reply_eager(req, bytes);
}
