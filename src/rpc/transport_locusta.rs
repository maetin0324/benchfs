//! locusta (rrrpc) RPC transport backend.
//!
//! Behind the `transport-locusta` Cargo feature. Wraps a single
//! `Client + RelayDaemon + ServerContext` triple (all running on the
//! same thread) and exposes the [`RpcTransport`] async API. The async
//! bridge polls the locusta state machines from inside each call's
//! future (busy-poll yield) until the response slot becomes ready.
//!
//! This is the Phase 1b skeleton — single-client per-process, Eager-only
//! cross-node ping is functional; Put/Get and per-peer dispatch land in
//! Phase 1c/2.

#![cfg(feature = "transport-locusta")]

use std::alloc::{Layout, alloc};
use std::cell::RefCell;
use std::collections::HashMap;
use std::fs;
use std::net::{SocketAddrV4, UdpSocket};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use mlx5::pd::AccessFlags;
use mlx5::qp::RcQpConfig;
use rrrpc::arena::DmaArena;
use rrrpc::rdma_util::RecvBufferPool;
use rrrpc::relay::client::{Client, Response, ResponseFuture};
use rrrpc::relay::daemon::{Channel, RdmaContext, RelayConfig, RelayDaemon};
use rrrpc::relay::jiffy::JiffyQueue;
use rrrpc::relay::shm::{ChannelHeader, ChannelState, ShmLayout, init_channel_header};
use rrrpc::relay::spsc::FastForwardRing;
use rrrpc::server::{Request, ServerConfig, ServerContext, ServerRdmaContext};
use rrrpc::wire::{CompletionSlot, QpExchangeInfo, RequestSlot};

use mlx5::pd::MemoryRegion;
use pluvio_uring::allocator::FixedBufferAllocator;

use crate::rpc::RpcError;
use crate::rpc::locusta_buffer::{RegisteredFixedBuffer, register_with_pd};
use crate::rpc::transport::{NodeId, RpcResponse, RpcTransport};
use crate::rpc::udp_handshake::{
    self, ExchangePacket, MSG_ACK, MSG_REQUEST, MSG_RESPONSE, PACKET_SIZE,
};

const QP_INFO_SIZE: usize = std::mem::size_of::<QpExchangeInfo>();
const _: () = assert!(QP_INFO_SIZE == 64);

/// Whether the pluvio Reactor is the sole driver of `LocustaInner::tick()`.
///
/// Sourced from `RuntimeConfig::global().locusta.reactor_mode`. When enabled:
///   * `LocustaTransport` is registered as a pluvio `Reactor` and its
///     `poll()` calls `inner.tick()` each runtime tick.
///   * `WaitForResponse::poll` skips its own `inner.tick()` to avoid
///     double-ticking the same state machines from two borrows.
///   * The server-side dispatch task uses `drain_and_spawn` (no tick)
///     instead of `poll_once_spawn`.
pub fn reactor_mode_enabled() -> bool {
    crate::runtime_config::RuntimeConfig::global()
        .locusta
        .reactor_mode
}

/// Inner state held behind a `Rc<RefCell<...>>` so async futures can
/// reach into it from poll-time.
pub struct LocustaInner {
    pub client: Client,
    pub daemon: RelayDaemon,
    pub server: ServerContext<RegisteredFixedBuffer>,
    /// Server-side pool of pinned, page-aligned, RDMA-registered buffers.
    /// Each handler that needs a buffer for a Put grant or a Get reply
    /// `acquire`s a [`FixedBuffer`] from here and wraps it in a
    /// [`RegisteredFixedBuffer`].
    pub server_buffer_allocator: Option<Rc<FixedBufferAllocator>>,
    /// Owning storage for the MRs registered on `server_buffer_allocator`.
    /// Must outlive the allocator — when dropped, the underlying memory is
    /// deregistered from the HCA. The field is named with a leading
    /// underscore because nothing reads it directly; its sole purpose is
    /// to act as an RAII lifetime extender for the registration.
    _server_buffer_mrs: Vec<MemoryRegion>,
    pub node_to_dest: HashMap<NodeId, u16>,
    pub channel_base_ptr: *mut u8,
    pub channel_layout_total: usize,
    pub inflight: HashMap<u64, ResponseFuture>,
    /// Reused scratch buffer for vectored `send_eager`/`send_put`/`send_get`
    /// requests — locusta's `batch.call_*` API only accepts a contiguous
    /// `&[u8]`, so we have to flatten `IoSlice`s here. Allocating once and
    /// re-using avoids the per-RPC `Vec` churn that hurt the `LocustaCallable`
    /// path.
    pub small_req_scratch: Vec<u8>,
    /// Filesystem directory used for QP info exchange (same value passed
    /// in `LocustaConfig`). Cached so `add_peer` can find / write files
    /// after `init`.
    pub registry_dir: PathBuf,
    /// Local node id (same value passed in `LocustaConfig`).
    pub local_node_id: NodeId,
    /// QP config reused for every new peer.
    pub qp_config: RcQpConfig,
    /// Next dest_id to hand out from `add_peer`. Starts at
    /// `cfg.peer_node_ids.len()` after `init` and increments with each
    /// successful dynamic add.
    pub next_dest_id: u16,
    /// UDP socket used for the QP-info exchange that replaces the
    /// Lustre file polling protocol. Bound at `init`-time to an
    /// ephemeral port; `local_udp_addr` is published into the
    /// registry so peers can find us.
    pub udp_socket: UdpSocket,
    /// Externally-visible `ip:port` of [`udp_socket`] (matches what's
    /// published in `{registry_dir}/locusta_udp/{local_node_id}`).
    pub local_udp_addr: SocketAddrV4,
    /// Cached UDP addresses of peers we've already looked up. The
    /// registry file is one-shot — we only read it the first time we
    /// need to talk to a peer; subsequent connects reuse the cached
    /// address.
    pub peer_udp_addrs: HashMap<NodeId, SocketAddrV4>,
    /// Last RESPONSE packet we sent to each peer, keyed by node_id.
    /// On UDP, the initial RESPONSE may be lost and the peer will
    /// retransmit its REQUEST — without re-sending the cached
    /// RESPONSE we'd just drop the duplicate REQUEST (because the
    /// peer is already in `node_to_dest`) and the peer would time
    /// out forever. We don't re-run `prepare_*` / `connect_*` on
    /// retransmits — those mutate locusta slabs and would blow up.
    pub cached_responses: HashMap<NodeId, (SocketAddrV4, [u8; PACKET_SIZE])>,
}

impl LocustaInner {
    /// Pump the polling state machines. Called both from the async wait
    /// futures and (in Phase 1c) from a registered Reactor.
    pub fn tick(&mut self) {
        let t0 = std::time::Instant::now();
        self.daemon.poll_client_requests();
        let t1 = t0.elapsed().as_micros() as u64;
        self.daemon.process_pending_dma_writes();
        let t2 = t0.elapsed().as_micros() as u64;
        self.daemon.flush_all_destinations();
        let t3 = t0.elapsed().as_micros() as u64;
        self.daemon.poll_server_completions();
        let t4 = t0.elapsed().as_micros() as u64;
        self.client.poll();
        let t5 = t0.elapsed().as_micros() as u64;
        self.server.poll();
        let t6 = t0.elapsed().as_micros() as u64;
        // Sample slow ticks (≥1ms) to keep log volume manageable. Under
        // a healthy system every tick should be sub-100µs.
        if t6 >= 1000 && crate::stats::is_stats_enabled() {
            tracing::info!(
                target: "pluvio_tick_timing",
                kind = "locusta_tick",
                cli_req_us = t1,
                dma_write_us = t2 - t1,
                flush_dest_us = t3 - t2,
                srv_complete_us = t4 - t3,
                client_poll_us = t5 - t4,
                server_poll_us = t6 - t5,
                total_us = t6,
                "TICK_TIMING_SLOW"
            );
        }
        // Periodically dump the daemon's credit_reads_issued counter so we
        // can see whether the relay ring is going full and stalling (credit
        // reads are an RDMA round trip; under congestion they balloon).
        if crate::stats::is_stats_enabled() {
            use std::sync::atomic::{AtomicU64, Ordering};
            static TICK_COUNT: AtomicU64 = AtomicU64::new(0);
            let n = TICK_COUNT.fetch_add(1, Ordering::Relaxed);
            if n.is_multiple_of(100_000) {
                tracing::info!(
                    target: "pluvio_tick_timing",
                    kind = "credit_reads",
                    tick_n = n,
                    credit_reads_issued = self.daemon.debug_credit_reads_issued,
                    "CREDIT_READ_COUNT"
                );
            }
        }
    }

    /// Set up a peer over UDP, replacing the old Lustre file-polling
    /// protocol. Called from both [`LocustaTransport::init`] (for
    /// statically-configured peers) and [`LocustaTransport::add_peer`]
    /// (the lazy `get_or_connect` path).
    ///
    /// Steps (initiator side):
    ///   1. Allocate a fresh `dest_id` (advance counter before any
    ///      fallible work — same rationale as the old slab-key fix).
    ///   2. `prepare_peer` + `prepare_destination` on the local QPs.
    ///   3. Look up the peer's UDP `ip:port` from
    ///      `{registry_dir}/locusta_udp/{peer}` (one-shot read).
    ///   4. Send REQUEST containing our two QP infos to peer's UDP.
    ///   5. Receive RESPONSE containing the peer's two QP infos.
    ///      The server-side handler completes its `connect_*` BEFORE
    ///      replying, so by the time we read this byte the peer's QPs
    ///      are already RTR/RTS — the first RDMA send won't RNR.
    ///   6. `connect_peer` + `connect_destination` on our QPs.
    fn add_peer_blocking(&mut self, peer: &str, deadline: Instant) -> Result<u16, RpcError> {
        if let Some(&existing) = self.node_to_dest.get(peer) {
            return Ok(existing);
        }

        let dest_id = self.next_dest_id;
        self.next_dest_id = self
            .next_dest_id
            .checked_add(1)
            .ok_or_else(|| RpcError::ConnectionError("dest_id space exhausted".to_string()))?;

        // Tuning comes from [locusta] section in benchfs.toml.
        let rc = crate::runtime_config::RuntimeConfig::global();
        let recv_ring_size = rc.locusta.recv_ring_size;
        let send_buf_size = rc.locusta.send_buf_size;
        let max_inflight = rc.locusta.max_inflight;
        let server_local = self
            .server
            .prepare_peer(
                dest_id,
                recv_ring_size,
                send_buf_size,
                512,
                max_inflight,
                &self.qp_config,
            )
            .map_err(|e| {
                RpcError::ConnectionError(format!("server.prepare_peer({dest_id}): {e:?}"))
            })?;
        let relay_local = self
            .daemon
            .prepare_destination(dest_id, recv_ring_size, send_buf_size, &self.qp_config)
            .map_err(|e| {
                RpcError::ConnectionError(format!("daemon.prepare_destination({dest_id}): {e:?}"))
            })?;

        let peer_addr = self.resolve_peer_udp_addr(peer, deadline)?;

        let request = ExchangePacket {
            msg_type: MSG_REQUEST,
            relay_qp: relay_local,
            server_qp: server_local,
            node_id: self.local_node_id.clone(),
        };
        let request_bytes = request.encode();

        // Per-attempt UDP recv timeout. Starts at 500 ms (matches
        // typical UDP round-trip + server QP setup) and grows
        // exponentially after each retransmit, capped at 8 s. At
        // 10 phys × ppn=8 = 6400 inbound REQUESTs per server, a
        // fixed 500 ms retransmit makes 80 clients send 12 800
        // pkt/s into a single server that can only drain ~100-1000
        // QP-setups/s, so the kernel UDP rcvbuf overflows and the
        // storm self-amplifies (job 20492-class). Exponential
        // backoff caps the steady-state ingress while still letting
        // a single dropped packet recover in ≤1 s.
        let initial_recv_timeout = Duration::from_millis(500);
        const MAX_RECV_TIMEOUT: Duration = Duration::from_secs(8);
        self.udp_socket
            .set_read_timeout(Some(initial_recv_timeout))
            .map_err(|e| RpcError::ConnectionError(format!("set_read_timeout: {e}")))?;

        // Send REQUEST and wait for the matching RESPONSE. While
        // waiting we also service incoming REQUESTs (server-to-server
        // setups may have peers handshaking us concurrently). A lost
        // datagram retransmits on `recv_timeout`.
        let mut recv_timeout = initial_recv_timeout;
        let mut last_send = Instant::now() - recv_timeout;
        let response = loop {
            if last_send.elapsed() >= recv_timeout {
                self.udp_socket
                    .send_to(&request_bytes, peer_addr)
                    .map_err(|e| {
                        RpcError::ConnectionError(format!("send_to({peer_addr}, {peer}): {e}"))
                    })?;
                last_send = Instant::now();
                // Double the backoff for the next attempt, capped at
                // MAX_RECV_TIMEOUT. Also reflect it into the socket
                // so recv_from blocks at most that long before we
                // re-decide to retransmit.
                recv_timeout = std::cmp::min(recv_timeout * 2, MAX_RECV_TIMEOUT);
                let _ = self.udp_socket.set_read_timeout(Some(recv_timeout));
            }
            let mut buf = [0u8; PACKET_SIZE];
            match self.udp_socket.recv_from(&mut buf) {
                Ok((n, from)) => {
                    let from_v4 = match from {
                        std::net::SocketAddr::V4(v4) => v4,
                        std::net::SocketAddr::V6(_) => continue,
                    };
                    let pkt = match ExchangePacket::decode(&buf[..n]) {
                        Ok(p) => p,
                        Err(e) => {
                            tracing::debug!("dropping malformed udp from {from}: {e}");
                            continue;
                        }
                    };
                    match pkt.msg_type {
                        // Match by node_id, NOT by source IP. The
                        // kernel may pick a different source interface
                        // for the return path than what the registry
                        // advertised (Sirius has both IPoIB and a
                        // separate 1 GbE management network), so the
                        // RESPONSE's `from` address won't necessarily
                        // equal `peer_addr`. The node_id in the packet
                        // identifies the sender unambiguously.
                        MSG_RESPONSE if pkt.node_id == peer => break pkt,
                        MSG_ACK if pkt.node_id == peer => {
                            // Server says "REQUEST received, QP setup
                            // in progress". Don't retransmit — bump the
                            // next-send deadline well past any plausible
                            // QP setup time. `recv_from` keeps blocking
                            // on `recv_timeout`, so we still get the
                            // eventual RESPONSE.
                            last_send = Instant::now() + Duration::from_secs(30);
                            tracing::debug!("udp ACK from {peer}; suppressing retransmits");
                            continue;
                        }
                        MSG_REQUEST => {
                            // Inline-accept an unsolicited request from
                            // another peer so we don't drop it.
                            self.peer_udp_addrs.insert(pkt.node_id.clone(), from_v4);
                            // Send ACK immediately to suppress the other
                            // peer's retransmits (same logic as the
                            // server-accept path in handle_one_udp_request).
                            let ack = ExchangePacket {
                                msg_type: MSG_ACK,
                                relay_qp: unsafe { std::mem::zeroed() },
                                server_qp: unsafe { std::mem::zeroed() },
                                node_id: self.local_node_id.clone(),
                            };
                            let ack_bytes = ack.encode();
                            if let Err(e) = self.udp_socket.send_to(&ack_bytes, from_v4) {
                                tracing::warn!(
                                    "ack send to {} ({}) failed: {e}",
                                    pkt.node_id,
                                    from_v4
                                );
                            }
                            if let Err(e) = self.accept_request_inline(pkt, from_v4) {
                                tracing::warn!("inline accept failed: {e:?}");
                            }
                            continue;
                        }
                        _ => {
                            tracing::debug!(
                                "stray packet from {from} (node_id={}) type={} (waiting on {peer})",
                                pkt.node_id,
                                pkt.msg_type
                            );
                            continue;
                        }
                    }
                }
                Err(ref e)
                    if e.kind() == std::io::ErrorKind::WouldBlock
                        || e.kind() == std::io::ErrorKind::TimedOut =>
                {
                    if Instant::now() >= deadline {
                        return Err(RpcError::ConnectionError(format!(
                            "udp exchange timeout for {peer} (no response within deadline)"
                        )));
                    }
                    continue;
                }
                Err(e) => {
                    return Err(RpcError::ConnectionError(format!(
                        "recv_from waiting for {peer}: {e}"
                    )));
                }
            }
        };

        self.server
            .connect_peer(dest_id, &response.relay_qp)
            .map_err(|e| {
                RpcError::ConnectionError(format!("server.connect_peer({dest_id}): {e:?}"))
            })?;
        self.daemon
            .connect_destination(dest_id, &response.server_qp)
            .map_err(|e| {
                RpcError::ConnectionError(format!("daemon.connect_destination({dest_id}): {e:?}"))
            })?;

        self.node_to_dest.insert(peer.to_string(), dest_id);
        Ok(dest_id)
    }

    /// Look up `peer`'s UDP `ip:port`, blocking until the registry
    /// file appears (with backoff). Cached on first hit.
    fn resolve_peer_udp_addr(
        &mut self,
        peer: &str,
        deadline: Instant,
    ) -> Result<SocketAddrV4, RpcError> {
        if let Some(&cached) = self.peer_udp_addrs.get(peer) {
            return Ok(cached);
        }
        let addr = udp_handshake::read_peer_udp_addr(&self.registry_dir, peer, deadline)
            .map_err(|e| RpcError::ConnectionError(format!("read_peer_udp_addr({peer}): {e}")))?;
        self.peer_udp_addrs.insert(peer.to_string(), addr);
        Ok(addr)
    }

    /// Process one incoming UDP REQUEST if any are queued. Polled
    /// repeatedly by the server's accept loop. Returns the connected
    /// peer's node_id on success, or `None` if no packet was waiting.
    pub fn handle_one_udp_request(
        &mut self,
        recv_timeout: Duration,
    ) -> Result<Option<NodeId>, RpcError> {
        self.udp_socket
            .set_read_timeout(Some(recv_timeout))
            .map_err(|e| RpcError::ConnectionError(format!("set_read_timeout: {e}")))?;
        let mut buf = [0u8; PACKET_SIZE];
        let (n, from) = match self.udp_socket.recv_from(&mut buf) {
            Ok(v) => v,
            Err(ref e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                return Ok(None);
            }
            Err(e) => {
                return Err(RpcError::ConnectionError(format!("udp recv_from: {e}")));
            }
        };

        let pkt = ExchangePacket::decode(&buf[..n])
            .map_err(|e| RpcError::ConnectionError(format!("decode request: {e}")))?;
        let from_v4 = match from {
            std::net::SocketAddr::V4(v4) => v4,
            std::net::SocketAddr::V6(_) => {
                return Err(RpcError::ConnectionError(
                    "received v6 sender; expected v4".to_string(),
                ));
            }
        };
        if pkt.msg_type != MSG_REQUEST {
            tracing::debug!(
                "ignoring non-REQUEST UDP packet from {} type={}",
                from,
                pkt.msg_type
            );
            return Ok(None);
        }
        let peer = pkt.node_id.clone();
        if peer == self.local_node_id {
            return Ok(None);
        }
        self.peer_udp_addrs.insert(peer.clone(), from_v4);
        if self.node_to_dest.contains_key(&peer) {
            // Already connected. The peer is retransmitting because
            // our first RESPONSE was lost — re-send the cached one so
            // it can move on.
            if let Some((addr, bytes)) = self.cached_responses.get(&peer).cloned() {
                tracing::debug!("udp REQUEST retry from {peer}; re-sending cached RESPONSE");
                if let Err(e) = self.udp_socket.send_to(&bytes, addr) {
                    tracing::warn!("re-send to {peer} failed: {e}");
                }
            } else {
                tracing::warn!("udp REQUEST retry from {peer} but no cached response");
            }
            return Ok(None);
        }
        // Send ACK immediately — before the slow QP-setup work in
        // `accept_request_inline` — so the client's retransmit timer
        // stalls during the seconds we may take to finish prepare/
        // connect. At 10 phys × ppn=8 = 80 inbound REQUESTs per server
        // tick, even with exponential backoff the kernel UDP queue
        // grows uncomfortably; an ACK is a 201 B reply we can fire in
        // microseconds.
        let ack = ExchangePacket {
            msg_type: MSG_ACK,
            relay_qp: unsafe { std::mem::zeroed() },
            server_qp: unsafe { std::mem::zeroed() },
            node_id: self.local_node_id.clone(),
        };
        let ack_bytes = ack.encode();
        if let Err(e) = self.udp_socket.send_to(&ack_bytes, from_v4) {
            tracing::warn!("ack send to {peer} ({from_v4}) failed: {e}");
        }
        self.accept_request_inline(pkt, from_v4)?;
        Ok(Some(peer))
    }

    /// Server-side QP setup for an incoming REQUEST. Allocates a fresh
    /// `dest_id`, calls `prepare_peer` + `prepare_destination` +
    /// `connect_peer` + `connect_destination` (in that order — the
    /// connect steps run **before** the UDP RESPONSE goes out so the
    /// peer's first RDMA send doesn't race against our QPs still being
    /// in INIT). Used both by [`handle_one_udp_request`] and inline
    /// from [`add_peer_blocking`] when an unsolicited REQUEST arrives
    /// while we're awaiting a RESPONSE.
    fn accept_request_inline(
        &mut self,
        request: ExchangePacket,
        from_v4: SocketAddrV4,
    ) -> Result<(), RpcError> {
        let peer = request.node_id.clone();
        if peer == self.local_node_id {
            return Ok(());
        }
        if self.node_to_dest.contains_key(&peer) {
            // Already-connected: retransmit cached RESPONSE.
            if let Some((addr, bytes)) = self.cached_responses.get(&peer).cloned() {
                if let Err(e) = self.udp_socket.send_to(&bytes, addr) {
                    tracing::warn!("re-send to {peer} failed: {e}");
                }
            }
            return Ok(());
        }

        let dest_id = self.next_dest_id;
        self.next_dest_id = self
            .next_dest_id
            .checked_add(1)
            .ok_or_else(|| RpcError::ConnectionError("dest_id space exhausted".to_string()))?;

        // Tuning comes from [locusta] section in benchfs.toml.
        let rc = crate::runtime_config::RuntimeConfig::global();
        let recv_ring_size = rc.locusta.recv_ring_size;
        let send_buf_size = rc.locusta.send_buf_size;
        let max_inflight = rc.locusta.max_inflight;
        let server_local = self
            .server
            .prepare_peer(
                dest_id,
                recv_ring_size,
                send_buf_size,
                512,
                max_inflight,
                &self.qp_config,
            )
            .map_err(|e| {
                RpcError::ConnectionError(format!("server.prepare_peer({dest_id}): {e:?}"))
            })?;
        let relay_local = self
            .daemon
            .prepare_destination(dest_id, recv_ring_size, send_buf_size, &self.qp_config)
            .map_err(|e| {
                RpcError::ConnectionError(format!("daemon.prepare_destination({dest_id}): {e:?}"))
            })?;

        self.server
            .connect_peer(dest_id, &request.relay_qp)
            .map_err(|e| {
                RpcError::ConnectionError(format!("server.connect_peer({dest_id}): {e:?}"))
            })?;
        self.daemon
            .connect_destination(dest_id, &request.server_qp)
            .map_err(|e| {
                RpcError::ConnectionError(format!("daemon.connect_destination({dest_id}): {e:?}"))
            })?;

        let response = ExchangePacket {
            msg_type: MSG_RESPONSE,
            relay_qp: relay_local,
            server_qp: server_local,
            node_id: self.local_node_id.clone(),
        };
        let response_bytes = response.encode();
        self.udp_socket
            .send_to(&response_bytes, from_v4)
            .map_err(|e| RpcError::ConnectionError(format!("send_to({from_v4}, response): {e}")))?;

        self.node_to_dest.insert(peer.clone(), dest_id);
        self.cached_responses
            .insert(peer, (from_v4, response_bytes));
        Ok(())
    }

    /// Returns true once the response for `completion_id` is ready.
    /// Removes the entry on success and returns the response.
    pub fn try_take(&mut self, completion_id: u64) -> Option<Response> {
        match self.inflight.get(&completion_id) {
            Some(fut) if fut.is_ready() => {
                let fut = self.inflight.remove(&completion_id).unwrap();
                fut.get(&mut self.client)
            }
            _ => None,
        }
    }
}

impl Drop for LocustaInner {
    fn drop(&mut self) {
        if !self.channel_base_ptr.is_null() && self.channel_layout_total > 0 {
            let layout = Layout::from_size_align(self.channel_layout_total, 4096)
                .expect("invalid channel layout");
            unsafe {
                std::alloc::dealloc(self.channel_base_ptr, layout);
            }
        }
    }
}

/// Handle exposed to BenchFS. Cheap to clone (`Rc` underneath).
#[derive(Clone)]
pub struct LocustaTransport {
    pub inner: Rc<RefCell<LocustaInner>>,
}

/// Configuration for initializing a locusta transport on one node.
pub struct LocustaConfig {
    /// Filesystem path shared across nodes used to exchange `QpExchangeInfo`.
    pub registry_dir: PathBuf,
    /// Local node identifier (matches BenchFS node ids).
    pub local_node_id: NodeId,
    /// Ordered peer node ids. Each peer gets dest_id = its index.
    pub peer_node_ids: Vec<NodeId>,
    /// Channel ring capacity in slots.
    pub ring_capacity: u32,
    /// DMA arena size in bytes.
    pub arena_size: u32,
    /// SRQ recv buffer chunks.
    pub recv_chunks: u16,
    /// CQ size.
    pub cq_size: i32,
    /// HCA port.
    pub port: u8,
    /// QP info exchange timeout (seconds).
    pub exchange_timeout_secs: u64,
    /// Number of pluvio FixedBuffer slots for the server-side RDMA pool.
    /// 0 disables the pool (Put/Get server handlers will not be usable).
    /// Phase 2.1 default: 4 slots × 4 MiB each. Ignored when
    /// `external_server_allocator` is `Some`.
    pub server_buf_slots: usize,
    /// Per-slot size for the server-side buffer pool. Set ≥ the largest
    /// expected chunk (4 MiB for BenchFS). Ignored when
    /// `external_server_allocator` is `Some`.
    pub server_buf_size: usize,
    /// Phase 2.4 production wiring: if `Some`, locusta registers this
    /// caller-owned `FixedBufferAllocator` with the server PD instead
    /// of allocating a separate pool. Lets the chunk store and the
    /// locusta server share one set of pinned buffers, enabling
    /// zero-copy `network ↔ io_uring`.
    pub external_server_allocator: Option<Rc<FixedBufferAllocator>>,
}

impl Default for LocustaConfig {
    fn default() -> Self {
        Self {
            registry_dir: PathBuf::from("/tmp/benchfs_locusta_registry"),
            local_node_id: "node_0".to_string(),
            peer_node_ids: Vec::new(),
            ring_capacity: 128,
            arena_size: 8 * 1024 * 1024, // 8 MiB
            recv_chunks: 4096,
            cq_size: 8192,
            port: 1,
            exchange_timeout_secs: 120,
            server_buf_slots: 4,
            server_buf_size: 4 * 1024 * 1024,
            external_server_allocator: None,
        }
    }
}

/// Open an mlx5 device.
///
/// Device selection priority (highest first):
/// 1. `BENCHFS_MLX5_DEVICE` — explicit name (e.g. `"mlx5_2"`).
/// 2. `BENCHFS_MLX5_DEVICE_INDEX` — 0-based index into the device list.
/// 3. Auto-spread from `OMPI_COMM_WORLD_LOCAL_RANK` modulo `num_devices` —
///    so 4-HCA Sirius nodes get balanced HCA assignment across ranks on
///    the same phys node (matches per-vnode NUMA layout when ppn ≤ HCA
///    count, balances send-side load when ppn > HCA count). Disable by
///    setting `BENCHFS_MLX5_AUTO_SPREAD=0`.
/// 4. Fallback: first device that opens.
fn open_mlx5_device() -> Result<mlx5::device::Context, RpcError> {
    let device_list = mlx5::device::DeviceList::list()
        .map_err(|e| RpcError::ConnectionError(format!("mlx5::device::DeviceList::list: {e:?}")))?;
    if device_list.is_empty() {
        return Err(RpcError::ConnectionError(
            "no mlx5 devices found".to_string(),
        ));
    }
    let available: Vec<String> = device_list.iter().map(|d| d.name()).collect();
    let rc = crate::runtime_config::RuntimeConfig::global();
    let want_name = rc.locusta.mlx5_device.clone();
    let want_index = rc.locusta.mlx5_device_index.map(|v| v as usize);

    if let Some(name) = want_name.as_deref() {
        for device in device_list.iter() {
            if device.name() == name {
                let ctx = device
                    .open()
                    .map_err(|e| RpcError::ConnectionError(format!("open device {name}: {e:?}")))?;
                eprintln!("[locusta] selected mlx5 device {name} (locusta.mlx5_device)");
                return Ok(ctx);
            }
        }
        return Err(RpcError::ConnectionError(format!(
            "locusta.mlx5_device={name} not found; available: {available:?}"
        )));
    }

    if let Some(idx) = want_index {
        let device = device_list.get(idx).ok_or_else(|| {
            RpcError::ConnectionError(format!(
                "locusta.mlx5_device_index={idx} out of range; available: {available:?}"
            ))
        })?;
        let name = device.name();
        let ctx = device.open().map_err(|e| {
            RpcError::ConnectionError(format!("open device idx={idx} ({name}): {e:?}"))
        })?;
        eprintln!("[locusta] selected mlx5 device {name} (locusta.mlx5_device_index={idx})");
        return Ok(ctx);
    }

    let auto_spread = rc.locusta.mlx5_auto_spread;
    if auto_spread {
        if let Some(local_rank) = std::env::var("OMPI_COMM_WORLD_LOCAL_RANK")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
        {
            let idx = local_rank % device_list.len();
            if let Ok(ctx) = device_list[idx].open() {
                let name = device_list[idx].name();
                eprintln!(
                    "[locusta] selected mlx5 device {name} (auto-spread idx={idx} from OMPI_COMM_WORLD_LOCAL_RANK={local_rank}, devices={available:?})"
                );
                return Ok(ctx);
            }
        }
    }

    for device in device_list.iter() {
        if let Ok(ctx) = device.open() {
            let name = device.name();
            eprintln!(
                "[locusta] selected mlx5 device {name} (default fallback); available: {available:?}"
            );
            return Ok(ctx);
        }
    }
    Err(RpcError::ConnectionError(
        "no mlx5 device could be opened".to_string(),
    ))
}

fn build_relay_rdma_context(
    ctx: &mlx5::device::Context,
    port: u8,
    recv_chunks: u16,
    cq_size: i32,
) -> Result<RdmaContext, RpcError> {
    let port_attr = ctx
        .query_port(port)
        .map_err(|e| RpcError::ConnectionError(format!("query_port({port}): {e:?}")))?;
    let pd = ctx
        .alloc_pd()
        .map_err(|e| RpcError::ConnectionError(format!("alloc_pd: {e:?}")))?;
    let cq = Rc::new(
        ctx.create_cq(cq_size, &mlx5::cq::CqConfig::default())
            .map_err(|e| RpcError::ConnectionError(format!("create_cq: {e:?}")))?,
    );
    let srq_config = mlx5::srq::SrqConfig {
        max_wr: recv_chunks as u32,
        max_sge: 1,
    };
    let srq = Rc::new(
        pd.create_srq::<u16>(&srq_config)
            .map_err(|e| RpcError::ConnectionError(format!("create_srq: {e:?}")))?,
    );
    let recv_pool = RecvBufferPool::new(&pd, recv_chunks, 64)
        .map_err(|e| RpcError::ConnectionError(format!("RecvBufferPool::new: {e:?}")))?;
    for idx in 0..recv_pool.num_chunks {
        srq.post_recv(
            idx,
            recv_pool.chunk_addr(idx),
            recv_pool.chunk_size,
            recv_pool.lkey(),
        )
        .map_err(|e| RpcError::ConnectionError(format!("srq.post_recv: {e:?}")))?;
    }
    srq.ring_doorbell();
    Ok(RdmaContext {
        ctx: ctx.clone(),
        pd,
        cq,
        srq,
        recv_pool,
        event_queue: Rc::new(RefCell::new(Vec::new())),
        port,
        lid: port_attr.lid,
    })
}

fn build_server_rdma_context(
    ctx: &mlx5::device::Context,
    port: u8,
    recv_chunks: u16,
    cq_size: i32,
) -> Result<ServerRdmaContext, RpcError> {
    let port_attr = ctx
        .query_port(port)
        .map_err(|e| RpcError::ConnectionError(format!("query_port({port}): {e:?}")))?;
    let pd = ctx
        .alloc_pd()
        .map_err(|e| RpcError::ConnectionError(format!("alloc_pd: {e:?}")))?;
    let cq = Rc::new(
        ctx.create_cq(cq_size, &mlx5::cq::CqConfig::default())
            .map_err(|e| RpcError::ConnectionError(format!("create_cq: {e:?}")))?,
    );
    let srq_config = mlx5::srq::SrqConfig {
        max_wr: recv_chunks as u32,
        max_sge: 1,
    };
    let srq = Rc::new(
        pd.create_srq::<u16>(&srq_config)
            .map_err(|e| RpcError::ConnectionError(format!("create_srq: {e:?}")))?,
    );
    let recv_pool = RecvBufferPool::new(&pd, recv_chunks, 64)
        .map_err(|e| RpcError::ConnectionError(format!("RecvBufferPool::new: {e:?}")))?;
    for idx in 0..recv_pool.num_chunks {
        srq.post_recv(
            idx,
            recv_pool.chunk_addr(idx),
            recv_pool.chunk_size,
            recv_pool.lkey(),
        )
        .map_err(|e| RpcError::ConnectionError(format!("srq.post_recv: {e:?}")))?;
    }
    srq.ring_doorbell();
    Ok(ServerRdmaContext {
        ctx: ctx.clone(),
        pd,
        cq,
        srq,
        recv_pool,
        event_queue: Rc::new(RefCell::new(Vec::new())),
        port,
        lid: port_attr.lid,
    })
}

impl LocustaTransport {
    /// Initialize locusta state for `cfg.local_node_id`, connecting to every
    /// `cfg.peer_node_ids` via the shared file registry.
    ///
    /// For each peer `p` (assigned dest_id = index):
    ///   - Server prepares QP for "relay on p" → writes
    ///     `{registry}/server_{me}_to_relay_{p}.qpinfo`
    ///     and waits for `{registry}/relay_{p}_to_server_{me}.qpinfo`.
    ///   - Relay prepares QP for "server on p" → writes
    ///     `{registry}/relay_{me}_to_server_{p}.qpinfo`
    ///     and waits for `{registry}/server_{p}_to_relay_{me}.qpinfo`.
    ///
    /// Once all peers are connected the function returns. Subsequent calls
    /// use `dest_id = index` to identify the remote peer.
    pub fn init(cfg: &LocustaConfig) -> Result<Self, RpcError> {
        fs::create_dir_all(&cfg.registry_dir).map_err(|e| {
            RpcError::ConnectionError(format!("mkdir {}: {e}", cfg.registry_dir.display()))
        })?;
        let ctx = open_mlx5_device()?;

        // ------ SHM-like channel (single channel, in-process) ------
        let layout = ShmLayout::compute(cfg.ring_capacity, cfg.arena_size);
        let alloc_layout = Layout::from_size_align(layout.total_size, 4096)
            .map_err(|e| RpcError::ConnectionError(format!("invalid layout: {e}")))?;
        let shm_base = unsafe { alloc(alloc_layout) };
        if shm_base.is_null() {
            return Err(RpcError::ConnectionError("alloc failed".to_string()));
        }
        unsafe {
            std::ptr::write_bytes(shm_base, 0, layout.total_size);
            init_channel_header(shm_base, &layout, cfg.ring_capacity);
        }
        let header = unsafe { &*(shm_base as *const ChannelHeader) };
        header.set_state(ChannelState::Active);

        let req_queue = Arc::new(JiffyQueue::<RequestSlot>::new(256));

        // ------ Relay daemon ------
        let mut daemon = RelayDaemon::new(RelayConfig::default());
        daemon.req_queue = Arc::clone(&req_queue);
        daemon.rdma = Some(build_relay_rdma_context(
            &ctx,
            cfg.port,
            cfg.recv_chunks,
            cfg.cq_size,
        )?);

        let arena_base = unsafe { shm_base.add(layout.dma_arena_off) };
        let arena_mr = {
            let pd = &daemon.rdma.as_ref().unwrap().pd;
            unsafe {
                pd.register(
                    arena_base,
                    layout.dma_arena_len,
                    AccessFlags::LOCAL_WRITE | AccessFlags::REMOTE_WRITE | AccessFlags::REMOTE_READ,
                )
            }
            .map_err(|e| RpcError::ConnectionError(format!("arena MR register: {e:?}")))?
        };
        let cq_ring_for_relay = unsafe {
            FastForwardRing::new(
                shm_base.add(layout.cq_ring_off) as *mut CompletionSlot,
                cfg.ring_capacity,
                (&header.cq_head) as *const _ as *mut u32,
                (&header.cq_tail) as *const _ as *mut u32,
            )
        };
        let cq_ring_for_client = unsafe {
            FastForwardRing::new(
                shm_base.add(layout.cq_ring_off) as *mut CompletionSlot,
                cfg.ring_capacity,
                (&header.cq_head) as *const _ as *mut u32,
                (&header.cq_tail) as *const _ as *mut u32,
            )
        };
        daemon.add_channel(Channel {
            id: 0,
            state: ChannelState::Active,
            shm_base,
            shm_size: layout.total_size,
            cq_ring: cq_ring_for_relay,
            arena_base,
            arena_rkey: arena_mr.rkey(),
            mr_addr: arena_mr.addr() as u64,
            arena_mr: Some(arena_mr),
        });

        // ------ Server ------
        let mut server: ServerContext<RegisteredFixedBuffer> =
            ServerContext::new(ServerConfig::default());
        server.rdma = Some(build_server_rdma_context(
            &ctx,
            cfg.port,
            cfg.recv_chunks,
            cfg.cq_size,
        )?);

        // ------ Server-side RDMA buffer pool ------
        //
        // Phase 2.4 prefers an externally-supplied allocator (which is
        // already io_uring-registered by the chunk store). Otherwise
        // fall back to a freshly-allocated standalone pool — used by
        // the demo binary which has no real chunk_store.
        let (server_buffer_allocator, server_buffer_mrs) = if let Some(alloc) =
            cfg.external_server_allocator.clone()
        {
            let pd = &server.rdma.as_ref().unwrap().pd;
            let mrs = register_with_pd(&alloc, pd)?;
            (Some(alloc), mrs)
        } else if cfg.server_buf_slots > 0 {
            let pd = &server.rdma.as_ref().unwrap().pd;
            let alloc =
                FixedBufferAllocator::new_without_uring(cfg.server_buf_slots, cfg.server_buf_size);
            let mrs = register_with_pd(&alloc, pd)?;
            (Some(alloc), mrs)
        } else {
            (None, Vec::new())
        };

        let qp_config = RcQpConfig {
            max_send_wr: 1024,
            ..RcQpConfig::default()
        };

        // ------ Client ------
        let arena = unsafe { DmaArena::new(arena_base, cfg.arena_size) };
        let client = Client::new_for_test(
            arena,
            req_queue,
            0,
            cq_ring_for_client,
            cfg.ring_capacity as u16,
        );

        // ------ UDP control-plane socket ------
        //
        // Bind ephemeral; publish ip:port into the registry. Peers
        // connect to us via this socket for the QP-info handshake,
        // replacing the previous Lustre file-polling protocol that
        // doesn't scale on Sirius (job 18060 hit 4213 read timeouts).
        let (udp_socket, local_udp_addr) = udp_handshake::bind_udp_socket()
            .map_err(|e| RpcError::ConnectionError(format!("bind udp socket: {e}")))?;
        udp_handshake::publish_udp_addr(&cfg.registry_dir, &cfg.local_node_id, local_udp_addr)
            .map_err(|e| RpcError::ConnectionError(format!("publish_udp_addr: {e}")))?;
        tracing::info!(
            "locusta UDP control-plane: {} → {}",
            cfg.local_node_id,
            local_udp_addr
        );

        let inner = LocustaInner {
            client,
            daemon,
            server,
            server_buffer_allocator,
            _server_buffer_mrs: server_buffer_mrs,
            node_to_dest: HashMap::new(),
            channel_base_ptr: shm_base,
            channel_layout_total: layout.total_size,
            inflight: HashMap::new(),
            // Pre-size for typical metadata header + path (≤256B). Will
            // grow once if larger inputs arrive.
            small_req_scratch: Vec::with_capacity(256),
            registry_dir: cfg.registry_dir.clone(),
            local_node_id: cfg.local_node_id.clone(),
            qp_config,
            next_dest_id: 0,
            udp_socket,
            local_udp_addr,
            peer_udp_addrs: HashMap::new(),
            cached_responses: HashMap::new(),
        };
        let inner = Rc::new(RefCell::new(inner));

        // ------ Per-peer QP exchange (sequential; each peer gets a dest_id) ------
        let deadline = Instant::now() + Duration::from_secs(cfg.exchange_timeout_secs);
        {
            let mut inner_mut = inner.borrow_mut();
            for peer in &cfg.peer_node_ids {
                inner_mut.add_peer_blocking(peer, deadline)?;
            }
        }
        Ok(Self { inner })
    }

    /// Run the full QP handshake against `peer` at runtime, blocking
    /// the calling thread until the handshake completes or `timeout`
    /// elapses. Returns the freshly-allocated dest_id. Idempotent —
    /// if `peer` is already connected returns the existing dest_id.
    ///
    /// This is the building block that lets BenchFS dynamically accept
    /// connections from late-joining clients (the FFI / IOR path) that
    /// were not known at `init` time.
    pub fn add_peer(&self, peer: &NodeId, timeout: Duration) -> Result<u16, RpcError> {
        let deadline = Instant::now() + timeout;
        let mut inner = self.inner.borrow_mut();
        inner.add_peer_blocking(peer, deadline)
    }

    /// Scan `registry_dir` for peer files belonging to nodes not yet
    /// in `node_to_dest` whose **both** publications (`server_*` and
    /// `relay_*` pointing at us) have already appeared. For each such
    /// peer, run the full handshake. Returns the list of node ids
    /// newly connected on this call.
    ///
    /// Designed to be polled by a long-running accept task on the
    /// server side. Re-entrant: a partially-published peer (only one
    /// of the two files present) is skipped this round and re-checked
    /// on the next call.
    ///
    /// Skips any file whose basename is the local node — peers don't
    /// connect to themselves.
    pub fn try_accept_pending_peers(
        &self,
        _per_peer_timeout: Duration,
    ) -> Result<Vec<NodeId>, RpcError> {
        // UDP-based accept: drain whatever is queued on the local
        // socket. The first call uses a longer recv timeout (so the
        // initial REQUEST burst doesn't get split across multiple
        // 100 ms sleep cycles); subsequent calls in the same drain go
        // non-blocking so we exit as soon as the queue empties.
        let mut added = Vec::new();
        // Drain aggressively — at io500 4 phys × ppn=8 = 128 clients
        // each prewarming to 16 servers, every server has to accept
        // ~128 REQUESTs in a short burst. Old 32-per-tick × 100 ms
        // scan-interval ≈ 320/s wasn't keeping up; bump to 256 so the
        // burst clears in one tick.
        const MAX_PER_TICK: usize = 256;
        let mut first = true;
        for _ in 0..MAX_PER_TICK {
            // First recv uses a 2 ms timeout (catches a packet that
            // arrived just before we entered this tick); subsequent
            // recvs use 1 µs (effectively non-blocking, the kernel
            // returns EAGAIN immediately when the queue is empty).
            // `set_read_timeout` rejects `Duration::ZERO`, so we use
            // 1 µs as the floor.
            let recv_timeout = if first {
                Duration::from_millis(2)
            } else {
                Duration::from_micros(1)
            };
            let mut inner = self.inner.borrow_mut();
            match inner.handle_one_udp_request(recv_timeout) {
                Ok(Some(peer)) => {
                    tracing::info!("udp accept: handshaked new peer {peer}");
                    added.push(peer);
                    first = false;
                }
                Ok(None) => break,
                Err(e) => {
                    tracing::warn!("udp accept error: {e:?}");
                    break;
                }
            }
        }
        Ok(added)
    }

    fn lookup_dest(&self, dest: &NodeId) -> Result<u16, RpcError> {
        self.inner
            .borrow()
            .node_to_dest
            .get(dest)
            .copied()
            .ok_or_else(|| RpcError::ConnectionError(format!("unknown peer node: {dest}")))
    }

    /// Returns true if `peer` is already in `node_to_dest`.
    pub fn has_peer(&self, peer: &NodeId) -> bool {
        self.inner.borrow().node_to_dest.contains_key(peer)
    }

    /// Async future that polls `LocustaInner` until the response for
    /// `completion_id` is available, then yields it.
    async fn wait_for(&self, completion_id: u64) -> Result<Response, RpcError> {
        WaitForResponse {
            inner: Rc::clone(&self.inner),
            completion_id,
        }
        .await
    }

    /// Allocate a buffer from the locusta client arena.
    ///
    /// The returned `DmaBuffer` is RDMA-registered and can be used directly
    /// as the target of `send_get_with_buffer` (zero-copy read) or the
    /// source of `send_put_with_buffer` (zero-copy write).
    pub fn arena_alloc(&self, size: u32) -> Option<rrrpc::relay::client::DmaBuffer> {
        let mut inner = self.inner.borrow_mut();
        inner.client.alloc(size)
    }

    /// Zero-copy variant of `send_get` — the server's RDMA WRITE lands
    /// directly in the caller-provided `dma_buf`, no internal allocation,
    /// no `dma_buf → recv` memcpy.
    pub async fn send_get_with_buffer<'a>(
        &'a self,
        dest: &'a NodeId,
        rpc_id: u16,
        parts: &'a [std::io::IoSlice<'a>],
        dma_buf: &'a rrrpc::relay::client::DmaBuffer,
    ) -> Result<RpcResponse, RpcError> {
        let dest_id = self.lookup_dest(dest)?;
        let completion_id = {
            let mut inner = self.inner.borrow_mut();
            let small_req = stage_small_req(&mut inner, rpc_id, parts);
            let small_req_slice =
                unsafe { std::slice::from_raw_parts(small_req.as_ptr(), small_req.len()) };
            let (id, fut) = {
                let mut batch = inner.client.batch();
                let fut = batch
                    .call_get(dest_id, small_req_slice, dma_buf)
                    .map_err(|e| {
                        RpcError::TransportError(format!("call_get (zero-copy): {e:?}"))
                    })?;
                let id = fut.req_id();
                (id, fut)
            };
            inner.inflight.insert(id, fut);
            id
        };
        let response = self.wait_for(completion_id).await?;
        let dma_len = match &response {
            rrrpc::relay::client::Response::DmaAndSmallRes { dma_len, .. } => *dma_len as usize,
            rrrpc::relay::client::Response::SmallRes { .. } => 0,
        };
        let header_bytes = {
            let inner = self.inner.borrow();
            copy_small_res_data(&inner, response.small_res_data())
        };
        Ok(RpcResponse {
            header_bytes,
            data_len: dma_len,
        })
    }

    /// Zero-copy variant of `send_put` — the caller-provided `dma_buf`
    /// (already in the locusta arena, RDMA-registered) is RDMA-written
    /// directly to the server. Skips the alloc + memcpy on the hot path
    /// (job 20146 timing: `alloc_us` ≈ 95 µs/4 MiB = ~42 GB/s memcpy
    /// bottleneck for write).
    pub async fn send_put_with_buffer<'a>(
        &'a self,
        dest: &'a NodeId,
        rpc_id: u16,
        parts: &'a [std::io::IoSlice<'a>],
        dma_buf: &'a rrrpc::relay::client::DmaBuffer,
    ) -> Result<RpcResponse, RpcError> {
        let dest_id = self.lookup_dest(dest)?;
        let completion_id = {
            let mut inner = self.inner.borrow_mut();
            let small_req = stage_small_req(&mut inner, rpc_id, parts);
            let small_req_slice =
                unsafe { std::slice::from_raw_parts(small_req.as_ptr(), small_req.len()) };
            let (id, fut) = {
                let mut batch = inner.client.batch();
                let fut = batch
                    .call_put(dest_id, small_req_slice, dma_buf)
                    .map_err(|e| {
                        RpcError::TransportError(format!("call_put (zero-copy): {e:?}"))
                    })?;
                let id = fut.req_id();
                (id, fut)
            };
            inner.inflight.insert(id, fut);
            id
        };
        let response = self.wait_for(completion_id).await?;
        let header_bytes = {
            let inner = self.inner.borrow();
            copy_small_res_data(&inner, response.small_res_data())
        };
        Ok(RpcResponse::header_only(header_bytes))
    }
}

struct WaitForResponse {
    inner: Rc<RefCell<LocustaInner>>,
    completion_id: u64,
}

thread_local! {
    /// Timestamp of the previous WaitForResponse poll that returned
    /// `Pending`. Reset to None whenever a poll returns `Ready` so the
    /// next `wait_for`'s first poll doesn't see a phantom gap.
    static LAST_WAIT_FOR_PENDING: std::cell::Cell<Option<std::time::Instant>> =
        const { std::cell::Cell::new(None) };
    /// Per-wait poll counter — counts how many busy-poll iterations the
    /// current wait_for has executed without yet seeing its completion.
    /// Reset to 0 on every Ready return.
    static WAIT_FOR_POLL_COUNT: std::cell::Cell<u64> =
        const { std::cell::Cell::new(0) };
    /// Per-wait start timestamp so we can compute total wait time as
    /// observed by WaitForResponse itself (cross-check against
    /// SEND_PUT_TIMING's rtt_us).
    static WAIT_FOR_START: std::cell::Cell<Option<std::time::Instant>> =
        const { std::cell::Cell::new(None) };
}

impl std::future::Future for WaitForResponse {
    type Output = Result<Response, RpcError>;
    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let stats = crate::stats::is_stats_enabled();
        let now_for_gap = if stats {
            let now = std::time::Instant::now();
            if let Some(prev) = LAST_WAIT_FOR_PENDING.with(|c| c.get()) {
                let gap_us = now.duration_since(prev).as_micros() as u64;
                if gap_us >= 1000 {
                    tracing::info!(
                        target: "rpc_client_timing",
                        kind = "wait_for_intra_gap",
                        gap_us = gap_us,
                        "WAIT_FOR_INTRA_GAP_SLOW"
                    );
                }
            }
            // First poll of this wait_for? Record start.
            if WAIT_FOR_START.with(|c| c.get()).is_none() {
                WAIT_FOR_START.with(|c| c.set(Some(now)));
                WAIT_FOR_POLL_COUNT.with(|c| c.set(0));
            }
            Some(now)
        } else {
            None
        };
        let mut inner = self.inner.borrow_mut();
        // In reactor mode the pluvio Reactor is the sole tick driver, so
        // skip it here to avoid double-ticking the same locusta state
        // machines from two distinct borrows. The reactor's poll() runs
        // every `reactor_poll_interval` runtime iterations (= 2 with the
        // locusta profile), so completions become visible quickly.
        if !reactor_mode_enabled() {
            inner.tick();
        }
        if let Some(resp) = inner.try_take(self.completion_id) {
            if stats {
                LAST_WAIT_FOR_PENDING.with(|c| c.set(None));
                let n_polls = WAIT_FOR_POLL_COUNT.with(|c| c.get()) + 1;
                let elapsed_us = WAIT_FOR_START
                    .with(|c| c.get())
                    .map(|t| t.elapsed().as_micros() as u64)
                    .unwrap_or(0);
                WAIT_FOR_START.with(|c| c.set(None));
                WAIT_FOR_POLL_COUNT.with(|c| c.set(0));
                use std::sync::atomic::{AtomicU64, Ordering};
                static N: AtomicU64 = AtomicU64::new(0);
                let n = N.fetch_add(1, Ordering::Relaxed);
                if n.is_multiple_of(100) {
                    tracing::info!(
                        target: "rpc_client_timing",
                        kind = "wait_for_complete",
                        n = n,
                        poll_count = n_polls,
                        elapsed_us = elapsed_us,
                        "WAIT_FOR_COMPLETE"
                    );
                }
            }
            return std::task::Poll::Ready(Ok(resp));
        }
        if let Some(now) = now_for_gap {
            LAST_WAIT_FOR_PENDING.with(|c| c.set(Some(now)));
            WAIT_FOR_POLL_COUNT.with(|c| c.set(c.get() + 1));
        }
        cx.waker().wake_by_ref();
        std::task::Poll::Pending
    }
}

/// Helper for the server side of the loop: drain incoming requests and
/// invoke a user-provided handler. The 2-byte `rpc_id` prefix encoded by
/// [`stage_small_req`] is **not** stripped — handlers reading
/// `h.small_req()` will see `[rpc_id_lo, rpc_id_hi, ...body]`. Use
/// [`extract_rpc_id`] (or the higher-level `LocustaServerDispatch`) to
/// decode it.
pub fn drain_server_requests<F>(inner: &mut LocustaInner, mut handler: F)
where
    F: FnMut(Request<RegisteredFixedBuffer>),
{
    inner.server.poll();
    let ready = inner.server.drain_ready();
    for req in ready {
        handler(req);
    }
    inner.server.flush_all();
}

/// Size of the BenchFS-level `rpc_id` prefix that
/// [`LocustaTransport::send_*`] prepends to every locusta `small_req`.
/// Server-side dispatchers strip this before handing the body to the
/// RPC's parser.
pub const LOCUSTA_RPC_ID_PREFIX_LEN: usize = 2;

/// Decode the 2-byte little-endian `rpc_id` prefix from a `small_req`.
/// Returns `(rpc_id, body_slice)` where `body_slice = small_req[2..]`.
///
/// Returns `None` if `small_req` is shorter than 2 bytes (malformed).
pub fn extract_rpc_id(small_req: &[u8]) -> Option<(u16, &[u8])> {
    if small_req.len() < LOCUSTA_RPC_ID_PREFIX_LEN {
        return None;
    }
    let rpc_id = u16::from_le_bytes([small_req[0], small_req[1]]);
    Some((rpc_id, &small_req[LOCUSTA_RPC_ID_PREFIX_LEN..]))
}

/// Flatten `parts` into `inner.small_req_scratch`, prepended by a
/// 2-byte little-endian `rpc_id`. Returns the slice view of the
/// populated bytes — borrows `inner` for the duration of the immediate
/// caller's expression.
///
/// Every locusta send path goes through this so the wire format is
/// uniform: server-side handlers can always read `small_req[0..2]` as
/// the rpc_id and dispatch accordingly.
/// `memcpy` wrapper. Two prefetch strategies tried (job 18258 T0+256B,
/// job 18263 T1+64KB) both *regressed* p50 vs plain `copy_from_slice`
/// because the CPU's L2 streamer already prefetches sequential DRAM reads
/// well; the explicit `_mm_prefetch` instructions only added decode/issue
/// overhead. glibc's `__memcpy_avx_unaligned_erms` already issues
/// non-temporal stores + relies on the hardware stream prefetcher for
/// cold sources — there's no software win at this scale.
///
/// Keeping the function as a thin wrapper so the perf-relevant call site
/// is one line and future memcpy strategies (e.g. AVX-512 NT loads when
/// the host has them) plug in here.
#[inline(always)]
unsafe fn prefetched_memcpy(dst: *mut u8, src: *const u8, len: usize) {
    unsafe {
        std::ptr::copy_nonoverlapping(src, dst, len);
    }
}

fn stage_small_req<'b>(
    inner: &'b mut LocustaInner,
    rpc_id: u16,
    parts: &[std::io::IoSlice<'_>],
) -> &'b [u8] {
    inner.small_req_scratch.clear();
    let total: usize = LOCUSTA_RPC_ID_PREFIX_LEN + parts.iter().map(|p| p.len()).sum::<usize>();
    inner.small_req_scratch.reserve(total);
    inner
        .small_req_scratch
        .extend_from_slice(&rpc_id.to_le_bytes());
    for slice in parts {
        inner.small_req_scratch.extend_from_slice(slice);
    }
    &inner.small_req_scratch
}

/// Copy a small-response payload out of locusta into an owned `Vec<u8>`.
///
/// Inline responses (≤52 B) are embedded in the completion slot. Buffered
/// responses live in the client's DMA arena at `(offset, len)`; the lease
/// is held inside `data` (the `_buffer` field of `SmallResData::Buffered`)
/// so the arena slot stays valid for the duration of this call.
///
/// 以前は Buffered パスでゼロを返していたため、readdir のように >52B
/// になるレスポンスが空読みされて pfind が無限ループする不具合があった。
fn copy_small_res_data(inner: &LocustaInner, data: &rrrpc::relay::client::SmallResData) -> Vec<u8> {
    match data {
        rrrpc::relay::client::SmallResData::Inline { buf, len } => buf.0[..*len as usize].to_vec(),
        rrrpc::relay::client::SmallResData::Buffered { offset, len, .. } => unsafe {
            std::slice::from_raw_parts(inner.client.arena.ptr(*offset), *len as usize).to_vec()
        },
    }
}

/// Pluvio Reactor impl. In reactor mode (`BENCHFS_LOCUSTA_REACTOR=1`)
/// this is the **sole** driver of `LocustaInner::tick()`:
/// `WaitForResponse::poll` and the server-side dispatch task both skip
/// their own ticks. That avoids the try_borrow_mut race that surfaced
/// in jobs 17035/17038 when two tick paths fought over the same
/// RefCell. In legacy mode (env unset) the reactor is not registered;
/// `WaitForResponse` and the dispatch task tick locusta themselves.
///
/// `status()` is `Running` iff there is observable in-flight work
/// (outstanding client RPCs). The server side cannot reliably peek
/// for incoming requests without polling, so we keep `Running` while
/// any client RPC is open OR until we've polled at least once with no
/// outstanding work — see the `last_observed_progress` flag.
impl pluvio_runtime::reactor::Reactor for LocustaTransport {
    fn poll(&self) {
        // `borrow_mut` (not try_) is correct here because in reactor
        // mode we guarantee no other tick caller. If reactor mode is
        // off but the impl is somehow still being polled (no production
        // path does this today), fall back to try_borrow_mut to keep
        // backward-compatible.
        if reactor_mode_enabled() {
            let mut inner = self.inner.borrow_mut();
            inner.tick();
        } else if let Ok(mut inner) = self.inner.try_borrow_mut() {
            inner.tick();
        }
    }
    fn status(&self) -> pluvio_runtime::reactor::ReactorStatus {
        // In legacy mode we keep the "always Running" behaviour so the
        // pluvio idle-park watchdog never trips.
        if !reactor_mode_enabled() {
            return pluvio_runtime::reactor::ReactorStatus::Running;
        }
        // Reactor mode: ask cheaply whether we have anything to do.
        // `try_borrow` lets us skip a status answer that would race the
        // tick path; pluvio caches status() for `status_cache_iterations`
        // (default 100) ticks, so a momentary Running answer is harmless.
        match self.inner.try_borrow() {
            Ok(inner) => {
                if !inner.inflight.is_empty() {
                    pluvio_runtime::reactor::ReactorStatus::Running
                } else {
                    // No client-side work pending. We cannot peek into
                    // the server's ready queue without polling, so we
                    // stay Running on the server side (always-on
                    // benchfsd) but Stopped on the client side (FFI/
                    // pfind), distinguished by whether server_buffer_
                    // allocator is present.
                    if inner.server_buffer_allocator.is_some() {
                        pluvio_runtime::reactor::ReactorStatus::Running
                    } else {
                        pluvio_runtime::reactor::ReactorStatus::Stopped
                    }
                }
            }
            Err(_) => pluvio_runtime::reactor::ReactorStatus::Running,
        }
    }
}

impl RpcTransport for LocustaTransport {
    fn send_eager<'a>(
        &'a self,
        dest: &'a NodeId,
        rpc_id: u16,
        parts: &'a [std::io::IoSlice<'a>],
    ) -> impl std::future::Future<Output = Result<RpcResponse, RpcError>> + 'a {
        async move {
            let dest_id = self.lookup_dest(dest)?;
            let completion_id = {
                let mut inner = self.inner.borrow_mut();
                let small_req = stage_small_req(&mut inner, rpc_id, parts);
                // Re-borrow trick: stage_small_req keeps the borrow on the
                // scratch field; submitting needs a separate mutable borrow
                // on `client`. Both fields are disjoint members of inner so
                // we can split them via the raw `*mut LocustaInner`.
                let small_req_ptr = small_req.as_ptr();
                let small_req_len = small_req.len();
                let small_req_slice =
                    unsafe { std::slice::from_raw_parts(small_req_ptr, small_req_len) };
                let (id, fut) = {
                    let mut batch = inner.client.batch();
                    let fut = batch
                        .call_eager(dest_id, small_req_slice)
                        .map_err(|e| RpcError::TransportError(format!("call_eager: {e:?}")))?;
                    let id = fut.req_id();
                    (id, fut)
                };
                inner.inflight.insert(id, fut);
                id
            };
            let response = self.wait_for(completion_id).await?;
            let header_bytes = {
                let inner = self.inner.borrow();
                copy_small_res_data(&inner, response.small_res_data())
            };
            Ok(RpcResponse::header_only(header_bytes))
        }
    }

    fn send_put<'a>(
        &'a self,
        dest: &'a NodeId,
        rpc_id: u16,
        parts: &'a [std::io::IoSlice<'a>],
        payload: &'a [u8],
    ) -> impl std::future::Future<Output = Result<RpcResponse, RpcError>> + 'a {
        async move {
            let t0 = std::time::Instant::now();
            let dest_id = self.lookup_dest(dest)?;
            let dma_buf = {
                let mut inner = self.inner.borrow_mut();
                let mut buf = inner.client.alloc(payload.len() as u32).ok_or_else(|| {
                    RpcError::TransportError(format!(
                        "DMA arena exhausted for {}B payload",
                        payload.len()
                    ))
                })?;
                buf.as_mut_slice().copy_from_slice(payload);
                buf
            };
            let t_alloc = t0.elapsed().as_micros() as u64;

            let completion_id = {
                let mut inner = self.inner.borrow_mut();
                let small_req = stage_small_req(&mut inner, rpc_id, parts);
                let small_req_slice =
                    unsafe { std::slice::from_raw_parts(small_req.as_ptr(), small_req.len()) };
                let (id, fut) = {
                    let mut batch = inner.client.batch();
                    let fut = batch
                        .call_put(dest_id, small_req_slice, &dma_buf)
                        .map_err(|e| RpcError::TransportError(format!("call_put: {e:?}")))?;
                    let id = fut.req_id();
                    (id, fut)
                };
                inner.inflight.insert(id, fut);
                id
            };
            let t_submit = t0.elapsed().as_micros() as u64;
            let response = self.wait_for(completion_id).await?;
            let t_wait = t0.elapsed().as_micros() as u64;
            drop(dma_buf);

            if crate::stats::is_stats_enabled() && payload.len() >= 4 * 1024 * 1024 {
                use std::sync::atomic::{AtomicU64, Ordering};
                static N: AtomicU64 = AtomicU64::new(0);
                let n = N.fetch_add(1, Ordering::Relaxed);
                if n.is_multiple_of(100) {
                    tracing::info!(
                        target: "rpc_client_timing",
                        kind = "send_put_4m",
                        n = n,
                        alloc_us = t_alloc,
                        submit_us = t_submit,
                        rtt_us = t_wait - t_submit,
                        total_us = t_wait,
                        "SEND_PUT_TIMING"
                    );
                }
            }

            let header_bytes = {
                let inner = self.inner.borrow();
                copy_small_res_data(&inner, response.small_res_data())
            };
            Ok(RpcResponse::header_only(header_bytes))
        }
    }

    fn send_get<'a>(
        &'a self,
        dest: &'a NodeId,
        rpc_id: u16,
        parts: &'a [std::io::IoSlice<'a>],
        recv: &'a mut [u8],
    ) -> impl std::future::Future<Output = Result<RpcResponse, RpcError>> + 'a {
        async move {
            let t0 = std::time::Instant::now();
            let dest_id = self.lookup_dest(dest)?;
            let dma_buf = {
                let mut inner = self.inner.borrow_mut();
                inner.client.alloc(recv.len() as u32).ok_or_else(|| {
                    RpcError::TransportError(format!(
                        "DMA arena exhausted for {}B recv",
                        recv.len()
                    ))
                })?
            };
            let t_alloc = t0.elapsed().as_micros() as u64;

            let completion_id = {
                let mut inner = self.inner.borrow_mut();
                let small_req = stage_small_req(&mut inner, rpc_id, parts);
                let small_req_slice =
                    unsafe { std::slice::from_raw_parts(small_req.as_ptr(), small_req.len()) };
                let (id, fut) = {
                    let mut batch = inner.client.batch();
                    let fut = batch
                        .call_get(dest_id, small_req_slice, &dma_buf)
                        .map_err(|e| RpcError::TransportError(format!("call_get: {e:?}")))?;
                    let id = fut.req_id();
                    (id, fut)
                };
                inner.inflight.insert(id, fut);
                id
            };
            let t_submit = t0.elapsed().as_micros() as u64;
            let response = self.wait_for(completion_id).await?;
            let t_wait = t0.elapsed().as_micros() as u64;
            let dma_len = match &response {
                rrrpc::relay::client::Response::DmaAndSmallRes { dma_len, .. } => *dma_len as usize,
                rrrpc::relay::client::Response::SmallRes { .. } => 0,
            };
            if dma_len > recv.len() {
                return Err(RpcError::TransportError(format!(
                    "server returned {dma_len}B but recv buffer is {}B",
                    recv.len()
                )));
            }
            // Server's RDMA write landed in dma_buf; surface it to the caller.
            //
            // The dma_buf is cache-cold (HCA just deposited bytes via RDMA),
            // so a plain `copy_from_slice` runs at single-thread DRAM read
            // bandwidth — ~15 GB/s for 4 MiB, ≈ 274 µs measured (18249).
            // `prefetched_memcpy` interleaves software prefetch on the source
            // so reads stay ahead of the load-issue queue, and uses AVX-512
            // non-temporal stores on the destination so we don't evict
            // useful lines from L2 (IOR's `-R` verification will re-read
            // these bytes immediately afterwards, so a small amount of
            // prefetch-back-into-cache is fine).
            //
            // EXPERIMENT knob: `[locusta] skip_recv_copy = true` skips
            // the memcpy entirely to derive the network/disk-only upper
            // bound (data is invalid; IOR `-R` will fail).
            let t_copy_start = std::time::Instant::now();
            if !crate::runtime_config::RuntimeConfig::global()
                .locusta
                .skip_recv_copy
            {
                unsafe {
                    prefetched_memcpy(recv.as_mut_ptr(), dma_buf.as_slice().as_ptr(), dma_len);
                }
            }
            let t_copy = t_copy_start.elapsed().as_micros() as u64;
            drop(dma_buf);
            if crate::stats::is_stats_enabled() && recv.len() >= 4 * 1024 * 1024 {
                use std::sync::atomic::{AtomicU64, Ordering};
                static N: AtomicU64 = AtomicU64::new(0);
                let n = N.fetch_add(1, Ordering::Relaxed);
                if n.is_multiple_of(100) {
                    tracing::info!(
                        target: "rpc_client_timing",
                        kind = "send_get_4m",
                        n = n,
                        alloc_us = t_alloc,
                        submit_us = t_submit,
                        rtt_us = t_wait - t_submit,
                        copy_us = t_copy,
                        total_us = t_wait + t_copy,
                        "SEND_GET_TIMING"
                    );
                }
            }

            let header_bytes = {
                let inner = self.inner.borrow();
                copy_small_res_data(&inner, response.small_res_data())
            };
            Ok(RpcResponse {
                header_bytes,
                data_len: dma_len,
            })
        }
    }

    fn send_oneway<'a>(
        &'a self,
        dest: &'a NodeId,
        rpc_id: u16,
        parts: &'a [std::io::IoSlice<'a>],
    ) -> impl std::future::Future<Output = Result<(), RpcError>> + 'a {
        async move {
            let dest_id = self.lookup_dest(dest)?;
            let mut inner = self.inner.borrow_mut();
            let small_req = stage_small_req(&mut inner, rpc_id, parts);
            let small_req_slice =
                unsafe { std::slice::from_raw_parts(small_req.as_ptr(), small_req.len()) };
            inner
                .client
                .send_oneway_eager(dest_id, small_req_slice)
                .map_err(|e| RpcError::TransportError(format!("send_oneway_eager: {e:?}")))?;
            Ok(())
        }
    }
}
