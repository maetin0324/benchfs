//! Readdir RPC: CHFS-style scatter readdir.
//!
//! Wire format
//! -----------
//!
//! Request payload (after the 2-byte rpc_id prefix that the transport
//! prepends):
//!
//! ```text
//! ReaddirRequestHeader (16 B):
//!   path_len    : u32   — bytes following the header
//!   offset      : u32   — entries to skip on the server side
//!   max_entries : u32   — cap on returned entries
//!   _padding    : u32
//! Then `path_len` UTF-8 bytes (the parent path).
//! ```
//!
//! Response payload — `Vec<u8>` consisting of a fixed header followed
//! by packed entries:
//!
//! ```text
//! ReaddirResponseHeader (16 B):
//!   status      : i32   — 0 on success, negative errno otherwise
//!   entry_count : u32   — number of entries packed below
//!   data_len    : u32   — bytes of trailing entry data
//!   eod_flag    : u8    — 1 if no more entries past `offset+entry_count`
//!   _padding    : [u8;3]
//! Then `data_len` bytes packed as a sequence of:
//!   entry_type  : u8   — 0=file, 1=directory, 2=symlink
//!   name_len    : u16  (LE)
//!   name        : `name_len` UTF-8 bytes
//! ```
//!
//! Why pack everything into the AmRpc-header bytes (and not the
//! response_buffer half) — the locusta backend delivers an Eager small
//! response as a single `Vec<u8>` in `RpcResponse::header_bytes`. Putting
//! everything inline keeps the same wire format for both backends and
//! lets us reuse `LocustaCallable::call_locusta` with a custom decoder
//! (see [`call_readdir`]).

use std::io::IoSlice;
use std::rc::Rc;

use pluvio_ucx::async_ucx::ucp::AmMsg;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::metadata::InodeType;
use crate::rpc::helpers::{
    RpcIoSliceHelper, parse_header, receive_path, send_rpc_response_via_reply,
};
use crate::rpc::{AmRpc, AmRpcCallType, RpcClient, RpcError, RpcId};

pub const RPC_READDIR: RpcId = 26;

/// Maximum trailing entry-data size we will encode in one response.
///
/// **重要**: locusta の SEND は 1 message が `send_buf_size`
/// (BENCHFS_LOCUSTA_SEND_BUF_SIZE、デフォルト 32 KiB) を超えると
/// `enqueue_peer_message_parts` の outgoing queue で stuck し、client
/// 側が timeout する (job 20658)。送信バッファを超えない上限にする
/// 必要があり、ResponseHeader (16B) + wire overhead を考慮して
/// 16 KiB に設定。これを超えると server が eod_flag=0 で返し、
/// client は次の offset で paginate する。
pub const READDIR_MAX_DATA_BYTES: usize = 16 * 1024;

/// Per-RPC cap on entries; mostly a sanity ceiling so a buggy server
/// can't blow up the response buffer.
pub const READDIR_MAX_ENTRIES_PER_CALL: u32 = 4096;

// ---------------------------------------------------------------------
// Wire structs
// ---------------------------------------------------------------------

#[repr(C)]
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable)]
pub struct ReaddirRequestHeader {
    pub path_len: u32,
    pub offset: u32,
    pub max_entries: u32,
    _padding: u32,
}

impl ReaddirRequestHeader {
    pub fn new(path_len: usize, offset: u32, max_entries: u32) -> Self {
        Self {
            path_len: path_len as u32,
            offset,
            max_entries,
            _padding: 0,
        }
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable)]
pub struct ReaddirResponseHeader {
    pub status: i32,
    pub entry_count: u32,
    pub data_len: u32,
    pub eod_flag: u8,
    _padding: [u8; 3],
}

impl ReaddirResponseHeader {
    pub fn success(entry_count: u32, data_len: u32, eod: bool) -> Self {
        Self {
            status: 0,
            entry_count,
            data_len,
            eod_flag: u8::from(eod),
            _padding: [0; 3],
        }
    }

    pub fn error(errno: i32) -> Self {
        Self {
            status: errno,
            entry_count: 0,
            data_len: 0,
            eod_flag: 1,
            _padding: [0; 3],
        }
    }

    pub fn is_success(&self) -> bool {
        self.status == 0
    }

    pub fn is_eod(&self) -> bool {
        self.eod_flag != 0
    }
}

// ---------------------------------------------------------------------
// Entry encode / decode
// ---------------------------------------------------------------------

const ENTRY_TYPE_FILE: u8 = 0;
const ENTRY_TYPE_DIR: u8 = 1;
const ENTRY_TYPE_SYMLINK: u8 = 2;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReaddirEntryWire {
    pub name: String,
    pub entry_type: InodeType,
    /// File size in bytes for `entry_type == File`. 0 for directories
    /// and symlinks. Server fills this from local FileMetadata so the
    /// client can skip a separate `MetadataLookup` (readdirplus
    /// semantics). io500 find phase uses this directly for the `-size`
    /// predicate, cutting 400 k per-file stat RPCs.
    pub size: u64,
}

fn encode_entry_type(t: InodeType) -> u8 {
    match t {
        InodeType::File => ENTRY_TYPE_FILE,
        InodeType::Directory => ENTRY_TYPE_DIR,
        InodeType::Symlink => ENTRY_TYPE_SYMLINK,
    }
}

fn decode_entry_type(t: u8) -> Option<InodeType> {
    match t {
        ENTRY_TYPE_FILE => Some(InodeType::File),
        ENTRY_TYPE_DIR => Some(InodeType::Directory),
        ENTRY_TYPE_SYMLINK => Some(InodeType::Symlink),
        _ => None,
    }
}

/// Per-entry wire layout (readdirplus):
///   `[type:1][size:8][name_len:2][name:N]` = 11 + N bytes.
/// `size` is little-endian u64; 0 for directories and symlinks.
pub fn encode_entries(out: &mut Vec<u8>, entries: &[(String, InodeType, u64)]) -> usize {
    let mut packed = 0usize;
    for (name, ty, size) in entries {
        let name_bytes = name.as_bytes();
        if name_bytes.len() > u16::MAX as usize {
            tracing::warn!("readdir: skipping name longer than u16 max: {}", name);
            continue;
        }
        let needed = 1 + 8 + 2 + name_bytes.len();
        if out.len() + needed > READDIR_MAX_DATA_BYTES {
            break;
        }
        out.push(encode_entry_type(*ty));
        out.extend_from_slice(&size.to_le_bytes());
        out.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
        out.extend_from_slice(name_bytes);
        packed += 1;
    }
    packed
}

pub fn decode_entries(buf: &[u8]) -> Result<Vec<ReaddirEntryWire>, RpcError> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < buf.len() {
        // 1 byte type + 8 bytes size + 2 bytes name_len = 11 bytes header
        if i + 11 > buf.len() {
            return Err(RpcError::TransportError(
                "readdir entry truncated (header)".to_string(),
            ));
        }
        let ty_byte = buf[i];
        let size = u64::from_le_bytes([
            buf[i + 1],
            buf[i + 2],
            buf[i + 3],
            buf[i + 4],
            buf[i + 5],
            buf[i + 6],
            buf[i + 7],
            buf[i + 8],
        ]);
        let name_len = u16::from_le_bytes([buf[i + 9], buf[i + 10]]) as usize;
        i += 11;
        if i + name_len > buf.len() {
            return Err(RpcError::TransportError(
                "readdir entry truncated (name)".to_string(),
            ));
        }
        let name = std::str::from_utf8(&buf[i..i + name_len])
            .map_err(|_| RpcError::TransportError("readdir entry: invalid UTF-8".to_string()))?
            .to_string();
        i += name_len;
        let entry_type = decode_entry_type(ty_byte).ok_or_else(|| {
            RpcError::TransportError(format!("readdir entry: bad type byte {ty_byte}"))
        })?;
        out.push(ReaddirEntryWire {
            name,
            entry_type,
            size,
        });
    }
    Ok(out)
}

// ---------------------------------------------------------------------
// AmRpc impl — used by UCX path AND as a basis for the locusta path.
// ---------------------------------------------------------------------

pub struct ReaddirRequest {
    header: ReaddirRequestHeader,
    path: String,
    io_helper: RpcIoSliceHelper<1>,
    /// Pre-allocated receive buffer for UCX backend (response_buffer()).
    /// Sized at READDIR_MAX_DATA_BYTES + header so we never truncate.
    pub recv_buf: std::cell::UnsafeCell<Vec<u8>>,
    /// Lazily-built IoSliceMut around `recv_buf`.
    pub cached_recv_slice: std::cell::UnsafeCell<Option<std::io::IoSliceMut<'static>>>,
}

impl ReaddirRequest {
    pub fn new(path: String, offset: u32, max_entries: u32) -> Self {
        let io_helper = RpcIoSliceHelper::new(vec![path.as_bytes().to_vec()]);
        let max_entries = max_entries.min(READDIR_MAX_ENTRIES_PER_CALL);
        Self {
            header: ReaddirRequestHeader::new(path.len(), offset, max_entries),
            path,
            io_helper,
            recv_buf: std::cell::UnsafeCell::new(vec![0u8; READDIR_MAX_DATA_BYTES]),
            cached_recv_slice: std::cell::UnsafeCell::new(None),
        }
    }

    pub fn path(&self) -> &str {
        &self.path
    }
}

impl AmRpc for ReaddirRequest {
    type RequestHeader = ReaddirRequestHeader;
    type ResponseHeader = ReaddirResponseHeader;

    fn rpc_id() -> RpcId {
        RPC_READDIR
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

    fn response_buffer(&self) -> &[std::io::IoSliceMut<'_>] {
        // UCX backend uses this to recv the trailing entry bytes via
        // `msg.recv_data_vectored`. Lazily build the IoSliceMut on first
        // call (same pattern as ReadChunkByIdRequest).
        unsafe {
            let cache = &mut *self.cached_recv_slice.get();
            if cache.is_none() {
                let buf: &mut Vec<u8> = &mut *self.recv_buf.get();
                let ptr = buf.as_mut_ptr();
                let len = buf.len();
                let slice = std::slice::from_raw_parts_mut(ptr, len);
                let ioslice: std::io::IoSliceMut<'static> =
                    std::mem::transmute::<std::io::IoSliceMut<'_>, std::io::IoSliceMut<'static>>(
                        std::io::IoSliceMut::new(slice),
                    );
                *cache = Some(ioslice);
            }
            std::slice::from_ref(cache.as_ref().unwrap())
        }
    }

    async fn call(&self, client: &RpcClient) -> Result<Self::ResponseHeader, RpcError> {
        client.execute(self).await
    }

    async fn call_no_reply(&self, _client: &RpcClient) -> Result<(), RpcError> {
        Err(RpcError::HandlerError(
            "Readdir requires a reply".to_string(),
        ))
    }

    async fn server_handler(
        ctx: Rc<crate::rpc::handlers::RpcHandlerContext>,
        mut am_msg: AmMsg,
    ) -> Result<(crate::rpc::ServerResponse<Self::ResponseHeader>, AmMsg), (RpcError, AmMsg)> {
        let header: ReaddirRequestHeader = match parse_header(&am_msg) {
            Ok(h) => h,
            Err(e) => return Err((e, am_msg)),
        };
        let path_str = match receive_path(&ctx, &mut am_msg, header.path_len).await {
            Ok(s) => s,
            Err(e) => return Err((e, am_msg)),
        };

        let max = header.max_entries.clamp(1, READDIR_MAX_ENTRIES_PER_CALL) as usize;
        let offset = header.offset as usize;

        let (entries, truncated) = ctx
            .metadata_manager
            .list_local_entries_with_size(&path_str, offset, max);

        let mut entry_data: Vec<u8> = Vec::with_capacity(entries.len() * 56);
        let packed = encode_entries(&mut entry_data, &entries);
        let eod = !truncated && packed == entries.len();
        let resp_header =
            ReaddirResponseHeader::success(packed as u32, entry_data.len() as u32, eod);

        send_rpc_response_via_reply(
            Self::reply_stream_id(),
            &resp_header,
            Some(&entry_data),
            am_msg,
        )
        .await
    }

    fn error_response(error: &RpcError) -> Self::ResponseHeader {
        ReaddirResponseHeader::error(crate::rpc::helpers::rpc_error_to_errno(error))
    }
}

// ---------------------------------------------------------------------
// High-level client API: fan-out across peers
// ---------------------------------------------------------------------

/// Send a single readdir RPC to one peer and decode the response.
///
/// Returns `(entries, header)` so callers can detect truncation via
/// `header.eod_flag` and paginate via repeated calls with increasing
/// offsets.
pub async fn call_readdir_single(
    client: &RpcClient,
    path: &str,
    offset: u32,
    max_entries: u32,
) -> Result<(Vec<ReaddirEntryWire>, ReaddirResponseHeader), RpcError> {
    let req = ReaddirRequest::new(path.to_string(), offset, max_entries);

    #[cfg(feature = "transport-locusta")]
    if client.is_locusta() {
        return call_readdir_locusta(client, &req).await;
    }

    // UCX path: standard AmRpc::call() puts entries into recv_buf.
    let resp = req.call(client).await?;
    if !resp.is_success() {
        return Ok((Vec::new(), resp));
    }
    let len = resp.data_len as usize;
    let recv = unsafe { &*req.recv_buf.get() };
    let entries = decode_entries(&recv[..len.min(recv.len())])?;
    Ok((entries, resp))
}

#[cfg(feature = "transport-locusta")]
async fn call_readdir_locusta(
    client: &RpcClient,
    req: &ReaddirRequest,
) -> Result<(Vec<ReaddirEntryWire>, ReaddirResponseHeader), RpcError> {
    use crate::rpc::transport::RpcTransport;
    use zerocopy::IntoBytes;

    // Same vectored small_req layout as LocustaCallable's blanket impl.
    let backend = client.locusta_backend().ok_or_else(|| {
        RpcError::HandlerError("expected locusta backend in call_readdir_locusta".to_string())
    })?;
    let header_bytes = req.request_header().as_bytes();
    let data = req.request_data();
    let parts_storage: [IoSlice<'_>; 2] = [
        IoSlice::new(header_bytes),
        IoSlice::new(if data.is_empty() { &[] } else { &data[0] }),
    ];
    let parts = if data.is_empty() {
        &parts_storage[..1]
    } else {
        &parts_storage[..2]
    };

    let resp = backend
        .transport
        .send_eager(&backend.peer_node_id, ReaddirRequest::rpc_id(), parts)
        .await?;

    let hdr_size = std::mem::size_of::<ReaddirResponseHeader>();
    if resp.header_bytes.len() < hdr_size {
        return Err(RpcError::InvalidHeader);
    }
    let header = ReaddirResponseHeader::read_from_bytes(&resp.header_bytes[..hdr_size])
        .map_err(|_| RpcError::InvalidHeader)?;
    if !header.is_success() {
        return Ok((Vec::new(), header));
    }
    let data_len = header.data_len as usize;
    let entries_bytes = &resp.header_bytes
        [hdr_size..hdr_size + data_len.min(resp.header_bytes.len().saturating_sub(hdr_size))];
    let entries = decode_entries(entries_bytes)?;
    Ok((entries, header))
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_roundtrip() {
        let entries = vec![
            ("a.txt".to_string(), InodeType::File),
            ("b".to_string(), InodeType::Directory),
            ("c.lnk".to_string(), InodeType::Symlink),
        ];
        let mut buf = Vec::new();
        let n = encode_entries(&mut buf, &entries);
        assert_eq!(n, 3);
        let decoded = decode_entries(&buf).unwrap();
        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded[0].name, "a.txt");
        assert!(matches!(decoded[0].entry_type, InodeType::File));
        assert!(matches!(decoded[1].entry_type, InodeType::Directory));
        assert!(matches!(decoded[2].entry_type, InodeType::Symlink));
    }

    #[test]
    fn encode_stops_at_budget() {
        // 256 KiB budget with 100-byte names: roughly 2500 entries fit.
        let mut entries = Vec::new();
        for i in 0..10_000 {
            entries.push((format!("name_{i:050}_padding"), InodeType::File));
        }
        let mut buf = Vec::new();
        let packed = encode_entries(&mut buf, &entries);
        assert!(packed < entries.len(), "expected truncation due to budget");
        assert!(buf.len() <= READDIR_MAX_DATA_BYTES);
    }

    #[test]
    fn response_header_roundtrip() {
        let h = ReaddirResponseHeader::success(42, 1024, true);
        let bytes = h.as_bytes();
        let parsed = ReaddirResponseHeader::read_from_bytes(bytes).unwrap();
        assert_eq!(parsed.entry_count, 42);
        assert_eq!(parsed.data_len, 1024);
        assert!(parsed.is_success());
        assert!(parsed.is_eod());

        let err = ReaddirResponseHeader::error(-2);
        assert!(!err.is_success());
        assert_eq!(err.status, -2);
    }
}
