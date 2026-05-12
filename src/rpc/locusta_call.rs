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
    async fn call_locusta(
        &self,
        peer: &str,
        transport: &LocustaTransport,
    ) -> Result<Self::ResponseHeader, RpcError> {
        let header_bytes = self.request_header().as_bytes().to_vec();

        // For eager paths the wire format BenchFS uses is `header bytes ||
        // request_data IoSlices` — same convention as the demo binary.
        let mut small_req = header_bytes;
        for slice in self.request_data() {
            small_req.extend_from_slice(slice);
        }

        match self.call_type() {
            AmRpcCallType::None => {
                let peer_id = peer.to_string();
                let resp = transport
                    .send_eager(&peer_id, Self::rpc_id(), &small_req)
                    .await?;
                Self::ResponseHeader::read_from_bytes(
                    &resp.header_bytes[..std::mem::size_of::<Self::ResponseHeader>()],
                )
                .map_err(|_| RpcError::InvalidHeader)
            }
            AmRpcCallType::Put | AmRpcCallType::PutGet | AmRpcCallType::Get => {
                Err(RpcError::HandlerError(format!(
                    "LocustaCallable: call_type {:?} not yet implemented (Phase 2.2)",
                    std::mem::discriminant(&self.call_type())
                )))
            }
        }
    }
}

impl<T: AmRpc> LocustaCallable for T {}
