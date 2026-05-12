//! Transport abstraction layer over the RPC backend.
//!
//! Phase 1 scaffolding for migrating away from the UCX-specific
//! `pluvio_ucx::Endpoint::am_send_vectorized` path to a pluggable backend
//! (UCX or locusta/rrrpc). The `RpcTransport` trait captures the four
//! conceptual send operations that map to locusta's 5-pattern model:
//!
//! - `send_eager`  → RoundtripEager  (small request, small response)
//! - `send_put`    → RoundtripPut    (small request + DMA write, small response)
//! - `send_get`    → RoundtripGet    (small request, DMA read + small response)
//! - `send_oneway` → OnewayEager      (fire-and-forget small request)
//!
//! The existing `AmRpc` trait is intentionally preserved; its `call`/`call_no_reply`
//! implementations will be rewritten in Phase 1.2 to dispatch through this trait
//! instead of calling UCX APIs directly.

use crate::rpc::RpcError;

/// Logical RPC operation type, used to pick the underlying transport pattern.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RpcCallType {
    /// Small request, small response (no DMA).
    Eager,
    /// Small request + bulk payload written to server via DMA, small response.
    Put,
    /// Small request, response includes bulk payload read from server via DMA.
    Get,
    /// Fire-and-forget small request, no response.
    OnewayEager,
}

/// Identifier for the destination node.
///
/// Today this is a human-readable node id resolved via the address registry
/// (`/registry_dir/{node_id}`). The locusta backend will map this to a
/// `rrrpc::DestId` (u16) internally; UCX backend uses the node id to look up
/// a cached `WorkerAddress`.
pub type NodeId = String;

/// Response from a single RPC call.
#[derive(Debug)]
pub struct RpcResponse {
    /// Serialized response header (zerocopy-decodable on the caller side).
    pub header_bytes: Vec<u8>,
    /// Length of bulk data delivered to the caller's recv buffer (Get only).
    pub data_len: usize,
}

impl RpcResponse {
    pub fn header_only(header_bytes: Vec<u8>) -> Self {
        Self {
            header_bytes,
            data_len: 0,
        }
    }
}

/// Generic RPC transport abstraction.
///
/// All async methods are `!Send` because BenchFS uses `pluvio_runtime`,
/// a single-threaded async runtime that runs futures on the local thread.
/// Implementations may hold `Rc<RefCell<...>>` state without `Send` bounds.
pub trait RpcTransport {
    /// Send a small request (vectored) and wait for a small response.
    ///
    /// `parts` are concatenated into the wire `small_req`. Splitting the
    /// header and the trailing path/data into separate slices lets
    /// callers like [`crate::rpc::locusta_call::LocustaCallable`] skip
    /// the per-call `Vec` allocation that a `&[u8]` API would force.
    fn send_eager<'a>(
        &'a self,
        dest: &'a NodeId,
        rpc_id: u16,
        parts: &'a [std::io::IoSlice<'a>],
    ) -> impl std::future::Future<Output = Result<RpcResponse, RpcError>> + 'a;

    /// Send a small vectored request followed by a DMA-written bulk
    /// payload. Returns when the server has acked the small response.
    fn send_put<'a>(
        &'a self,
        dest: &'a NodeId,
        rpc_id: u16,
        parts: &'a [std::io::IoSlice<'a>],
        payload: &'a [u8],
    ) -> impl std::future::Future<Output = Result<RpcResponse, RpcError>> + 'a;

    /// Send a small vectored request and receive a DMA-read bulk
    /// response into `recv`. `RpcResponse::data_len` reports the number
    /// of bytes written to `recv`.
    fn send_get<'a>(
        &'a self,
        dest: &'a NodeId,
        rpc_id: u16,
        parts: &'a [std::io::IoSlice<'a>],
        recv: &'a mut [u8],
    ) -> impl std::future::Future<Output = Result<RpcResponse, RpcError>> + 'a;

    /// Fire-and-forget: send a small vectored request without waiting
    /// for a reply.
    fn send_oneway<'a>(
        &'a self,
        dest: &'a NodeId,
        rpc_id: u16,
        parts: &'a [std::io::IoSlice<'a>],
    ) -> impl std::future::Future<Output = Result<(), RpcError>> + 'a;
}
