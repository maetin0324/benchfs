//! Locusta-side client dispatch for [`AmRpc`] requests.
//!
//! Every type that implements [`AmRpc`] automatically gets
//! [`LocustaCallable::call_locusta`] via a blanket impl. The default
//! implementation marshals the request through the wire-format
//! accessors (`request_header()` bytes + `request_data()` IoSlices) and
//! dispatches to [`LocustaTransport`] using `T::call_type()`.
//!
//! This lets BenchFS code switch a single RPC site to Locusta without
//! changing the `AmRpc` impl:
//!
//! ```ignore
//! let req = MetadataLookupRequest::new(path);
//! let resp = req.call_locusta(&peer_id, &transport).await?;
//! ```
//!
//! Currently only `AmRpcCallType::None` (eager) is wired up. Phase 2.2
//! Put/Get will extend the same trait.

#![cfg(feature = "transport-locusta")]

use std::io::IoSlice;

use zerocopy::{FromBytes, IntoBytes};

use crate::rpc::transport::RpcTransport;
use crate::rpc::transport_locusta::LocustaTransport;
use crate::rpc::{AmRpc, AmRpcCallType, RpcError};

#[allow(async_fn_in_trait)]
pub trait LocustaCallable: AmRpc {
    /// Send this RPC via locusta, awaiting the deserialized response
    /// header.
    ///
    /// `peer` is the locusta `NodeId` of the target server, as registered
    /// via `LocustaConfig::peer_node_ids` at transport-init time.
    ///
    /// Hot path is zero-allocation: the request_header bytes and the
    /// `request_data()` IoSlices are passed through to the transport as
    /// a small `IoSlice` stack array, which the transport flattens into
    /// its reusable scratch buffer.
    async fn call_locusta(
        &self,
        peer: &str,
        transport: &LocustaTransport,
    ) -> Result<Self::ResponseHeader, RpcError> {
        let header_bytes = self.request_header().as_bytes();
        let data = self.request_data();

        // Build a stack-allocated IoSlice array large enough for the
        // common case (header + ≤4 data slices). Most BenchFS RPCs have
        // 0 or 1 data slice, so this is overprovisioned but cheap.
        let parts_storage: [IoSlice<'_>; 8] = std::array::from_fn(|i| {
            if i == 0 {
                IoSlice::new(header_bytes)
            } else if i - 1 < data.len() {
                IoSlice::new(&data[i - 1])
            } else {
                IoSlice::new(&[])
            }
        });
        let parts = &parts_storage[..1 + data.len().min(7)];

        let peer_id = peer.to_string();
        let decode =
            |resp: crate::rpc::transport::RpcResponse| -> Result<Self::ResponseHeader, RpcError> {
                Self::ResponseHeader::read_from_bytes(
                    &resp.header_bytes[..std::mem::size_of::<Self::ResponseHeader>()],
                )
                .map_err(|_| RpcError::InvalidHeader)
            };

        match self.call_type() {
            AmRpcCallType::None => {
                let resp = transport
                    .send_eager(&peer_id, Self::rpc_id(), parts)
                    .await?;
                decode(resp)
            }
            AmRpcCallType::Put => {
                // small_req = header only (the DMA write delivers the
                // payload separately). `data` holds the bulk payload —
                // BenchFS chunk RPCs always use a single contiguous
                // IoSlice, which we can pass through as-is.
                if data.len() != 1 {
                    return Err(RpcError::HandlerError(format!(
                        "LocustaCallable: Put expects 1 IoSlice, got {}",
                        data.len()
                    )));
                }
                let header_only = [IoSlice::new(header_bytes)];
                let payload: &[u8] = &data[0];
                let resp = transport
                    .send_put(&peer_id, Self::rpc_id(), &header_only, payload)
                    .await?;
                decode(resp)
            }
            AmRpcCallType::Get => {
                // small_req = header only. The bulk response lands in
                // the caller-provided `response_buffer()` (an
                // `IoSliceMut`). BenchFS chunk RPCs use a single
                // contiguous recv slice.
                let resp_buf = self.response_buffer();
                if resp_buf.len() != 1 {
                    return Err(RpcError::HandlerError(format!(
                        "LocustaCallable: Get expects 1 IoSliceMut, got {}",
                        resp_buf.len()
                    )));
                }
                let header_only = [IoSlice::new(header_bytes)];
                // SAFETY: The caller owns the buffer behind
                // `response_buffer()[0]` and guarantees exclusive access
                // for the duration of this future (BenchFS's runtime is
                // single-threaded). We cast through `*mut u8` here
                // because the AmRpc trait method returns `&[IoSliceMut]`
                // — i.e. we have shared access to a writable region
                // descriptor — and the existing UCX path relies on the
                // same convention (see `data_ops.rs:209`).
                let recv: &mut [u8] = unsafe {
                    let ptr = resp_buf[0].as_ptr() as *mut u8;
                    let len = resp_buf[0].len();
                    std::slice::from_raw_parts_mut(ptr, len)
                };
                let resp = transport
                    .send_get(&peer_id, Self::rpc_id(), &header_only, recv)
                    .await?;
                decode(resp)
            }
            AmRpcCallType::PutGet => Err(RpcError::HandlerError(
                "LocustaCallable: PutGet has no direct locusta analogue".to_string(),
            )),
        }
    }
}

impl<T: AmRpc> LocustaCallable for T {}
