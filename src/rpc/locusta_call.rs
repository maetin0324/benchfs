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
use std::time::Duration;

use futures::FutureExt;
use zerocopy::{FromBytes, IntoBytes};

use crate::rpc::transport::RpcTransport;
use crate::rpc::transport_locusta::LocustaTransport;
use crate::rpc::{AmRpc, AmRpcCallType, RpcError};

/// Hang watchdog: when a single `call_locusta` future has been pending
/// for this long, emit a `tracing::warn!` describing rpc_id/peer/phase
/// so we can root-cause client-side hangs from the logs without having
/// to attach a debugger to a 280-rank job. The wait does NOT abort —
/// we keep awaiting so a slow but live RPC eventually completes.
const HANG_WARN_INTERVAL: Duration = Duration::from_secs(30);

async fn watchdog<F, T>(
    label: &'static str,
    peer: &str,
    rpc_id: u16,
    payload_bytes: usize,
    fut: F,
) -> T
where
    F: std::future::Future<Output = T>,
{
    let mut fut = Box::pin(fut.fuse());
    let mut elapsed_secs: u64 = 0;
    loop {
        let mut tick = pluvio_timer::Delay::new(HANG_WARN_INTERVAL).fuse();
        futures::select! {
            r = fut => return r,
            _ = tick => {
                elapsed_secs += HANG_WARN_INTERVAL.as_secs();
                tracing::warn!(
                    target: "rpc_hang",
                    phase = label,
                    peer = %peer,
                    rpc_id = rpc_id,
                    payload_bytes = payload_bytes,
                    elapsed_secs = elapsed_secs,
                    "locusta RPC pending — possible hang"
                );
            }
        }
    }
}

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
                let hdr_size = std::mem::size_of::<Self::ResponseHeader>();
                if resp.header_bytes.len() < hdr_size {
                    return Err(RpcError::InvalidHeader);
                }
                Self::ResponseHeader::read_from_bytes(&resp.header_bytes[..hdr_size])
                    .map_err(|_| RpcError::InvalidHeader)
            };

        match self.call_type() {
            AmRpcCallType::None => {
                // Per-stage profiling for WriteChunkById eager path
                // (BENCHFS_RPC_PROFILE=1). The blanket impl runs for every
                // AmRpc, so we filter on rpc_id == RPC_WRITE_CHUNK_BY_ID
                // to keep the cost off other RPCs.
                let profile = crate::rpc::perf_breakdown::is_enabled()
                    && Self::rpc_id() == crate::rpc::data_ops::RPC_WRITE_CHUNK_BY_ID;
                let t_total = if profile {
                    Some(std::time::Instant::now())
                } else {
                    None
                };
                let t_send = t_total;
                let total_bytes: usize = parts.iter().map(|s| s.len()).sum();
                // Eager-path retry on stuck RPCs (iter90-93 hang pattern):
                // at ppn>=10 the locusta RC QP occasionally drops one
                // RPC's completion and the client awaits forever. Wrap
                // the send in a 30 s timeout and resubmit up to 3
                // attempts. Resubmits go through `transport.send_eager`
                // which assigns a fresh req_id, so the lost inflight
                // entry becomes garbage but doesn't deadlock.
                let attempt_timeout = std::time::Duration::from_secs(30);
                let max_attempts: u32 = 3;
                let mut last_err: Option<RpcError> = None;
                let mut resp_opt: Option<crate::rpc::transport::RpcResponse> = None;
                for attempt in 1..=max_attempts {
                    let fut = Box::pin(transport.send_eager(&peer_id, Self::rpc_id(), parts));
                    match pluvio_timer::timeout(attempt_timeout, fut).await {
                        Ok(Ok(r)) => {
                            if attempt > 1 {
                                tracing::warn!(
                                    target: "rpc_eager_retry",
                                    peer = %peer_id,
                                    rpc_id = Self::rpc_id(),
                                    attempt = attempt,
                                    "eager RPC succeeded after retry"
                                );
                            }
                            resp_opt = Some(r);
                            break;
                        }
                        Ok(Err(RpcError::ConnectionError(ref msg)))
                            if msg.contains("unknown peer node") =>
                        {
                            // Peer not in node_to_dest (perhaps we just
                            // reset it). Re-handshake and try again.
                            tracing::warn!(
                                target: "rpc_eager_retry",
                                peer = %peer_id,
                                rpc_id = Self::rpc_id(),
                                attempt = attempt,
                                "peer absent, running add_peer"
                            );
                            if let Err(e2) = transport
                                .add_peer(&peer_id, std::time::Duration::from_secs(60))
                            {
                                last_err = Some(e2);
                            }
                        }
                        Ok(Err(e)) => {
                            // Other transport error — don't retry.
                            return Err(e);
                        }
                        Err(_) => {
                            tracing::warn!(
                                target: "rpc_eager_retry",
                                peer = %peer_id,
                                rpc_id = Self::rpc_id(),
                                attempt = attempt,
                                "eager RPC timed out — resetting peer"
                            );
                            // Drop client-side state for this peer so
                            // the next send_eager fails fast with
                            // ConnectionError("unknown peer"), which
                            // the branch above will trigger
                            // add_peer to re-handshake.
                            transport.reset_peer(&peer_id);
                            last_err = Some(RpcError::TransportError(format!(
                                "eager timeout after 30s (attempt {attempt})"
                            )));
                        }
                    }
                }
                let resp = match resp_opt {
                    Some(r) => r,
                    None => return Err(last_err.unwrap_or_else(|| {
                        RpcError::TransportError("eager retry exhausted".to_string())
                    })),
                };
                let send_wait_us = t_send.map(|t| t.elapsed().as_micros() as u64).unwrap_or(0);
                let t_decode = if profile {
                    Some(std::time::Instant::now())
                } else {
                    None
                };
                let r = decode(resp);
                if profile {
                    let decode_us = t_decode
                        .map(|t| t.elapsed().as_micros() as u64)
                        .unwrap_or(0);
                    let total_us = t_total
                        .map(|t| t.elapsed().as_micros() as u64)
                        .unwrap_or(0);
                    // We can't easily separate send vs wait without
                    // changing the transport; record the combined
                    // send+wait window as `send_wait_us` for now and
                    // leave wait=0 — at this layer that combined value
                    // already dominates and is the headline number.
                    crate::rpc::perf_breakdown::cli_record(
                        0,
                        send_wait_us,
                        0,
                        decode_us,
                        total_us,
                    );
                }
                r
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
                // Zero-copy fast path: if the caller's payload buffer was
                // allocated from the locusta arena via
                // `benchfs_alloc_io_buffer`, hand the DmaBuffer straight to
                // `call_put` so the client's RDMA WRITE issues from the
                // application buffer directly — no alloc + memcpy in
                // `transport_locusta::send_put` (jobs 20146 timing).
                let src_ptr = payload.as_ptr();
                if let Some(dma_buf_raw) = crate::ffi::io_buffer::io_buffer_ptr_raw(src_ptr) {
                    // SAFETY: same single-thread invariant as the Get path
                    // — the IO_BUFFERS map only mutates from synchronous C
                    // calls and IOR holds the buffer for the whole phase.
                    let dma_buf: &rrrpc::relay::client::DmaBuffer = unsafe { &*dma_buf_raw };
                    let resp = watchdog(
                        "put_with_buffer",
                        &peer_id,
                        Self::rpc_id(),
                        payload.len(),
                        transport.send_put_with_buffer(
                            &peer_id,
                            Self::rpc_id(),
                            &header_only,
                            dma_buf,
                        ),
                    )
                    .await?;
                    return decode(resp);
                }
                let resp = watchdog(
                    "put",
                    &peer_id,
                    Self::rpc_id(),
                    payload.len(),
                    transport.send_put(&peer_id, Self::rpc_id(), &header_only, payload),
                )
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
                let recv_ptr = resp_buf[0].as_ptr();
                let recv_len = resp_buf[0].len();
                // Zero-copy fast path: if the caller's recv buffer was
                // allocated from the locusta arena via
                // `benchfs_alloc_io_buffer`, hand the underlying DmaBuffer
                // straight to call_get so the server's RDMA WRITE lands in
                // the application buffer directly — no intermediate
                // `recv.copy_from_slice(dma_buf)` 4 MiB memcpy
                // (transport_locusta.rs:1411, p50 ≈ 274 µs measured 18249).
                if let Some(dma_buf_raw) = crate::ffi::io_buffer::io_buffer_ptr_raw(recv_ptr) {
                    // SAFETY: `dma_buf_raw` points into the IO_BUFFERS map,
                    // which is single-thread TLS and only mutated by
                    // `benchfs_alloc_io_buffer` / `benchfs_free_io_buffer`
                    // from synchronous C calls. IOR holds the buffer for
                    // the whole phase, so the DmaBuffer is stable through
                    // this RPC's await.
                    let dma_buf: &rrrpc::relay::client::DmaBuffer = unsafe { &*dma_buf_raw };
                    let resp = watchdog(
                        "get_with_buffer",
                        &peer_id,
                        Self::rpc_id(),
                        recv_len,
                        transport.send_get_with_buffer(
                            &peer_id,
                            Self::rpc_id(),
                            &header_only,
                            dma_buf,
                        ),
                    )
                    .await?;
                    return decode(resp);
                }
                let recv: &mut [u8] =
                    unsafe { std::slice::from_raw_parts_mut(recv_ptr as *mut u8, recv_len) };
                let resp = watchdog(
                    "get",
                    &peer_id,
                    Self::rpc_id(),
                    recv_len,
                    transport.send_get(&peer_id, Self::rpc_id(), &header_only, recv),
                )
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
