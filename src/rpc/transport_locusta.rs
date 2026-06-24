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
    /// Embedded mode: in-process relay daemon. `None` in standalone
    /// shared-daemon mode (POC iter191+) where the daemon lives in a
    /// separate `benchfsd_mpi` process and this client talks to it via
    /// a POSIX SHM control_ring instead of an in-process `Arc<JiffyQueue>`.
    pub daemon: Option<RelayDaemon>,
    /// Embedded mode: in-process RPC server. `None` in standalone
    /// shared-daemon mode — the server runs in the daemon process.
    pub server: Option<ServerContext<RegisteredFixedBuffer>>,
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
    /// Per-peer count of duplicate REQUESTs we've received after
    /// already being connected. Used by `accept_request_inline` to
    /// detect "the peer reset its state" — when this hits a threshold
    /// (currently 5) we drop the old dest_id and run a fresh
    /// handshake. Cleared when a fresh handshake completes.
    pub peer_request_retries: HashMap<NodeId, u32>,
    /// Throttle counter for the registry-mode peer discovery scan
    /// performed inside `tick()`. Incremented every tick; the actual
    /// directory walk runs once per `REGISTRY_SCAN_TICK_INTERVAL`.
    pub registry_scan_counter: u32,
    /// UDP accept-path instrumentation (sums of µs spent in each
    /// phase). Used by the periodic dump in `udp_accept_stats_dump`
    /// to actually measure where server-side handshake time goes
    /// rather than guess.
    pub udp_stats_recvs: u64,
    pub udp_stats_accepts: u64,
    pub udp_stats_resend_cached: u64,
    pub udp_stats_prepare_us: u64,
    pub udp_stats_connect_us: u64,
    pub udp_stats_send_resp_us: u64,
    pub udp_stats_total_us: u64,
    pub udp_stats_max_total_us: u64,
    /// Server-side registry-handshake state machine. Each tick advances
    /// at most `MAX_DISCOVERY_STEPS_PER_TICK` entries by one step, so
    /// blocking inside the tick is bounded even when individual Lustre
    /// ops are slow (the `locusta_qp/` directory grows to tens of
    /// thousands of files at 400-client scale).
    pub pending_discoveries: Vec<PendingDiscovery>,
    /// Standalone-daemon client mode (iter193+): the node identity of
    /// the daemon process whose SHM control_ring this client opened.
    /// Populated from the daemon's `*.peers` manifest file. The client
    /// excludes this node from its BenchFS data/metadata routing list
    /// because the local daemon currently has no loopback path (when
    /// dest_id points to self, the daemon would have no destinations
    /// slab entry to forward through). `None` in embedded mode.
    pub owner_node_id: Option<NodeId>,
}

/// A server-side handshake in progress. Created when discover spots a
/// fresh `{peer}__{self}.qp` slot, removed after `connect_*` succeeds.
pub struct PendingDiscovery {
    pub peer: NodeId,
    pub dest_id: u16,
    pub relay_local: rrrpc::wire::QpExchangeInfo,
    pub server_local: rrrpc::wire::QpExchangeInfo,
    pub step: DiscoveryStep,
}

#[derive(Debug)]
pub enum DiscoveryStep {
    /// Local QPs allocated; need to write `{self}__{peer}.qp`.
    NeedPublish,
    /// Self published; need to read `{peer}__{self}.qp` (one-shot, no polling).
    NeedRead,
    /// Got peer's QP info; need `connect_peer` + `connect_destination`
    /// + `publish_ack`. Holds the decoded peer QPs.
    NeedConnect {
        peer_relay: rrrpc::wire::QpExchangeInfo,
        peer_server: rrrpc::wire::QpExchangeInfo,
    },
}

impl LocustaInner {
    /// Pump the polling state machines. Called both from the async wait
    /// futures and (in Phase 1c) from a registered Reactor.
    /// Registry-mode peer discovery cadence: walk the registry once per
    /// 100 ticks (~10 ms at the default reactor pace) to spot peers that
    /// initiated a handshake against us. Cheap (readdir of a small dir
    /// on Lustre) and amortised across many ticks.
    const REGISTRY_SCAN_TICK_INTERVAL: u32 = 100;

    /// Scan `{registry_dir}/locusta_qp/` for slots of the form
    /// `{peer}__{self}.qp` whose `peer` we have not yet connected to,
    /// then run `add_peer_blocking(peer)` against each one. This is the
    /// server-side counterpart to a client's `add_peer(server)` call:
    /// without it the client's `read_peer_qp` would never see our slot
    /// because nothing ever drove `add_peer` on our end.
    /// Cap on `enqueue` calls per scan. iter120 (ppn=20) regressed
    /// from iter119's 302 fails to 552 fails when this was bumped
    /// to 256, suggesting that spawning many tasks at once serialises
    /// through `inner.borrow_mut()` during `prepare_peer`/`connect_*`
    /// and the contention degrades throughput. Back to the iter118-
    /// proven value of 64 (compromise between iter105's 16 and the
    /// failed 256). Each task still runs async so 64 tasks/scan ×
    /// 10 scans/sec = 640 handshakes/sec/server which more than
    /// covers 800 peers per server in 1.25 s.
    const MAX_DISCOVERY_ENQUEUE_PER_SCAN: usize = 64;
    const MAX_DISCOVERY_STEPS_PER_TICK: usize = 1;

    /// Walk the registry to spot new peers that have published their QP
    /// info for us. Each freshly-discovered peer gets its local QPs
    /// prepared and is enqueued in `pending_discoveries` with state
    /// `NeedPublish`. The actual file I/O for publish/read/connect/ack
    /// happens incrementally in `step_pending_discoveries`, one short
    /// op per peer per tick — that way Lustre slowness on the
    /// 16k-entry `locusta_qp/` directory cannot stall the reactor.
    fn discover_registry_peers(&mut self) {
        if crate::runtime_config::RuntimeConfig::global().locusta.handshake_mode != "registry" {
            return;
        }
        let suffix = format!("__{}.qp", self.local_node_id);
        // Build a set of already-known/already-pending peers up front so
        // we don't enqueue the same peer twice.
        let pending_set: std::collections::HashSet<String> = self
            .pending_discoveries
            .iter()
            .map(|p| p.peer.clone())
            .collect();
        let mut new_peers: Vec<String> = Vec::new();
        // Walk every shard. Each shard holds ~ 1/256 of all peer files,
        // so the per-shard readdir is bounded to ~hundreds of entries
        // even at full mesh scale.
        'shards: for shard in crate::rpc::registry_handshake::all_shard_dirs(&self.registry_dir) {
            let entries = match std::fs::read_dir(&shard) {
                Ok(e) => e,
                Err(_) => continue,
            };
            for entry in entries.flatten() {
                let name = match entry.file_name().into_string() {
                    Ok(n) => n,
                    Err(_) => continue,
                };
                if name.starts_with('.') || !name.ends_with(&suffix) {
                    continue;
                }
                let owner = &name[..name.len() - suffix.len()];
                if owner.is_empty() || owner == self.local_node_id {
                    continue;
                }
                if self.node_to_dest.contains_key(owner) {
                    continue;
                }
                if pending_set.contains(owner) {
                    continue;
                }
                new_peers.push(owner.to_string());
                if new_peers.len() >= Self::MAX_DISCOVERY_ENQUEUE_PER_SCAN {
                    break 'shards;
                }
            }
        }
        for peer in new_peers {
            // Allocate local QPs (locusta-internal allocations, ~ms).
            let dest_id = match self.next_dest_id.checked_add(1) {
                Some(_) => {
                    let id = self.next_dest_id;
                    self.next_dest_id += 1;
                    id
                }
                None => {
                    tracing::warn!(
                        target: "rpc_registry_discover",
                        peer = %peer,
                        "dest_id space exhausted"
                    );
                    continue;
                }
            };
            let rc = crate::runtime_config::RuntimeConfig::global();
            let recv_ring_size = rc.locusta.recv_ring_size;
            let send_buf_size = rc.locusta.send_buf_size;
            let max_inflight = rc.locusta.max_inflight;
            let server_local = match self.server.as_mut().expect("server present in embedded mode").prepare_peer(
                dest_id,
                recv_ring_size,
                send_buf_size,
                512,
                max_inflight,
                &self.qp_config,
            ) {
                Ok(q) => q,
                Err(e) => {
                    tracing::warn!(
                        target: "rpc_registry_discover",
                        peer = %peer,
                        error = ?e,
                        "prepare_peer failed during discover enqueue"
                    );
                    continue;
                }
            };
            let relay_local = match self.daemon.as_mut().expect("daemon present in embedded mode").prepare_destination(
                dest_id,
                recv_ring_size,
                send_buf_size,
                &self.qp_config,
            ) {
                Ok(q) => q,
                Err(e) => {
                    tracing::warn!(
                        target: "rpc_registry_discover",
                        peer = %peer,
                        error = ?e,
                        "prepare_destination failed during discover enqueue"
                    );
                    continue;
                }
            };
            self.pending_discoveries.push(PendingDiscovery {
                peer,
                dest_id,
                relay_local,
                server_local,
                step: DiscoveryStep::NeedPublish,
            });
        }
    }

    /// Advance each in-flight discovery by one short step. Caps total
    /// work to `MAX_DISCOVERY_STEPS_PER_TICK`; remaining pending entries
    /// roll over to the next tick. Each step does at most one Lustre op
    /// (~100ms worst case on a 16k-entry directory), so the tick stays
    /// bounded to ~1s even under heavy contention.
    fn step_pending_discoveries(&mut self) {
        if self.pending_discoveries.is_empty() {
            return;
        }
        let tick_t0 = std::time::Instant::now();
        let mut completed: Vec<usize> = Vec::new();
        let mut steps_done: usize = 0;
        let mut publish_total_us: u64 = 0;
        let mut publish_count: u32 = 0;
        let mut publish_max_us: u64 = 0;
        let mut read_total_us: u64 = 0;
        let mut read_count: u32 = 0;
        let mut read_max_us: u64 = 0;
        let mut connect_total_us: u64 = 0;
        let mut connect_count: u32 = 0;
        let mut connect_max_us: u64 = 0;
        for (idx, pd) in self.pending_discoveries.iter_mut().enumerate() {
            if steps_done >= Self::MAX_DISCOVERY_STEPS_PER_TICK {
                break;
            }
            let step = std::mem::replace(&mut pd.step, DiscoveryStep::NeedPublish);
            match step {
                DiscoveryStep::NeedPublish => {
                    steps_done += 1;
                    let op_t = std::time::Instant::now();
                    let pres = crate::rpc::registry_handshake::publish_local_qp(
                        &self.registry_dir,
                        &self.local_node_id,
                        &pd.peer,
                        &pd.relay_local,
                        &pd.server_local,
                    );
                    let dt = op_t.elapsed().as_micros() as u64;
                    publish_total_us += dt;
                    publish_count += 1;
                    if dt > publish_max_us {
                        publish_max_us = dt;
                    }
                    if let Err(e) = pres {
                        tracing::warn!(
                            target: "rpc_registry_discover",
                            peer = %pd.peer,
                            error = %e,
                            "publish_local_qp failed; dropping pending discovery"
                        );
                        completed.push(idx);
                        continue;
                    }
                    pd.step = DiscoveryStep::NeedRead;
                }
                DiscoveryStep::NeedRead => {
                    steps_done += 1;
                    let path = crate::rpc::registry_handshake::slot_path(
                        &self.registry_dir,
                        &pd.peer,
                        &self.local_node_id,
                    );
                    let op_t = std::time::Instant::now();
                    let rres = std::fs::read(&path);
                    let dt = op_t.elapsed().as_micros() as u64;
                    read_total_us += dt;
                    read_count += 1;
                    if dt > read_max_us {
                        read_max_us = dt;
                    }
                    match rres {
                        Ok(buf)
                            if buf.len()
                                >= crate::rpc::registry_handshake::QP_PAYLOAD_SIZE =>
                        {
                            match crate::rpc::registry_handshake::decode_qp_payload(&buf) {
                                Ok((peer_relay, peer_server)) => {
                                    pd.step =
                                        DiscoveryStep::NeedConnect { peer_relay, peer_server };
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        target: "rpc_registry_discover",
                                        peer = %pd.peer,
                                        error = %e,
                                        "decode_qp_payload failed; dropping"
                                    );
                                    completed.push(idx);
                                }
                            }
                        }
                        Ok(_) | Err(_) => {
                            // Peer hasn't published yet, or partial write —
                            // try again next tick. Keep state as NeedRead.
                            pd.step = DiscoveryStep::NeedRead;
                        }
                    }
                }
                DiscoveryStep::NeedConnect { peer_relay, peer_server } => {
                    steps_done += 1;
                    let op_t = std::time::Instant::now();
                    let cres1 = self.server.as_mut().expect("server present in embedded mode").connect_peer(pd.dest_id, &peer_relay);
                    let dt1 = op_t.elapsed().as_micros() as u64;
                    connect_total_us += dt1;
                    connect_count += 1;
                    if dt1 > connect_max_us {
                        connect_max_us = dt1;
                    }
                    if let Err(e) = cres1 {
                        tracing::warn!(
                            target: "rpc_registry_discover",
                            peer = %pd.peer,
                            dest_id = pd.dest_id,
                            error = ?e,
                            "connect_peer failed; dropping"
                        );
                        completed.push(idx);
                        continue;
                    }
                    if let Err(e) = self.daemon.as_mut().expect("daemon present in embedded mode").connect_destination(pd.dest_id, &peer_server) {
                        tracing::warn!(
                            target: "rpc_registry_discover",
                            peer = %pd.peer,
                            dest_id = pd.dest_id,
                            error = ?e,
                            "connect_destination failed; dropping"
                        );
                        completed.push(idx);
                        continue;
                    }
                    if let Err(e) = crate::rpc::registry_handshake::publish_ack(
                        &self.registry_dir,
                        &self.local_node_id,
                        &pd.peer,
                    ) {
                        tracing::warn!(
                            target: "rpc_registry_discover",
                            peer = %pd.peer,
                            error = %e,
                            "publish_ack failed (continuing — peer may proceed without ack)"
                        );
                    }
                    self.node_to_dest.insert(pd.peer.clone(), pd.dest_id);
                    completed.push(idx);
                }
            }
        }
        // Remove completed (in reverse order so indices stay valid).
        for idx in completed.into_iter().rev() {
            self.pending_discoveries.swap_remove(idx);
        }
        // Periodically log accumulated step timings. Every ~1000 ticks
        // (~1s at 1 kHz) the WARN keeps log volume manageable while
        // surfacing actual Lustre op latencies — the user pushed back
        // on the unmeasured "Lustre is slow at seconds" claim and we
        // need real numbers.
        self.registry_scan_counter = self.registry_scan_counter; // (touch)
        if steps_done > 0 && tick_t0.elapsed().as_micros() as u64 > 1000 {
            tracing::warn!(
                target: "rpc_registry_step_timing",
                steps = steps_done,
                tick_us = tick_t0.elapsed().as_micros() as u64,
                publish_count,
                publish_avg_us = if publish_count > 0 { publish_total_us / publish_count as u64 } else { 0 },
                publish_max_us,
                read_count,
                read_avg_us = if read_count > 0 { read_total_us / read_count as u64 } else { 0 },
                read_max_us,
                connect_count,
                connect_avg_us = if connect_count > 0 { connect_total_us / connect_count as u64 } else { 0 },
                connect_max_us,
                pending = self.pending_discoveries.len(),
                "registry step timings"
            );
        }
    }

    pub fn tick(&mut self) {
        let t0 = std::time::Instant::now();
        // Registry-mode discover (one peer per call) runs every
        // REGISTRY_SCAN_TICK_INTERVAL ticks. In iter108 the original
        // unbounded variant froze the reactor for ~40s. With one peer
        // per scan and a short 5s per-handshake deadline, the worst
        // case is a single tick blocked for ~5s — undesirable but
        // recoverable, and only during the warm-up phase before the
        // mesh is complete.
        // NOTE: in-tick discover/step state-machine is disabled. The
        // server-side handshake now runs as a separate pluvio async
        // task (`server_discover_task`) spawned from `benchfsd_mpi`,
        // and the client-side handshake is `add_peer_async` (which
        // yields the reactor between Lustre polls). Both paths take
        // `inner.borrow_mut()` only for the short locusta-internal
        // phases, so the tick stays responsive.
        // Shared-daemon mode (POC): the daemon lives in another process,
        // so skip its polls entirely. The client cq_ring is still driven
        // from this process via `self.client.poll()` below.
        // poll_control_ring must run on every daemon tick so cross-process
        // clients can `Connect` (standalone-daemon mode, iter191+). Without
        // this, clients' `Client::connect` sends a Connect message into the
        // SHM control_ring but the daemon never drains it → handle_connect
        // never fires → channel never transitions to Active → client spins
        // forever in `Client::connect`'s "Active 待ち" loop. Job 21635
        // observed: `total_drained=0` heartbeats indefinitely.
        if let Some(daemon) = self.daemon.as_mut() {
            daemon.poll_control_ring();
            daemon.poll_client_requests();
        }
        let t1 = t0.elapsed().as_micros() as u64;
        if let Some(daemon) = self.daemon.as_mut() {
            daemon.process_pending_dma_writes();
        }
        let t2 = t0.elapsed().as_micros() as u64;
        if let Some(daemon) = self.daemon.as_mut() {
            daemon.flush_all_destinations();
        }
        let t3 = t0.elapsed().as_micros() as u64;
        if let Some(daemon) = self.daemon.as_mut() {
            daemon.poll_server_completions();
        }
        let t4 = t0.elapsed().as_micros() as u64;
        self.client.poll();
        let t5 = t0.elapsed().as_micros() as u64;
        if let Some(server) = self.server.as_mut() {
            server.poll();
        }
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
                    credit_reads_issued = self.daemon.as_ref().map(|d| d.debug_credit_reads_issued).unwrap_or(0),
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
    /// **iter199+ standalone-daemon loopback support**: set up an
    /// in-process RDMA loopback peer for `self.local_node_id`. The
    /// server's QP and the daemon's QP are wired to each other via
    /// the local HCA (same LID, same port — RDMA loopback). dest_id
    /// = next vacant slab key. After this, slots from co-resident
    /// clients targeting `self.local_node_id` route via destinations[dest_id]
    /// → RDMA WRITE to self → server's RDMA recv → handler →
    /// reply back via the same loopback. Solves the cross-client
    /// metadata visibility issue (iter197) without modifying locusta.
    fn add_self_loopback(&mut self) -> Result<u16, RpcError> {
        let self_id = self.local_node_id.clone();
        if let Some(&existing) = self.node_to_dest.get(&self_id) {
            return Ok(existing);
        }
        if self.server.is_none() || self.daemon.is_none() {
            return Err(RpcError::ConnectionError(
                "add_self_loopback requires embedded daemon+server".to_string(),
            ));
        }
        let dest_id = self.next_dest_id;
        self.next_dest_id = self
            .next_dest_id
            .checked_add(1)
            .ok_or_else(|| RpcError::ConnectionError("dest_id space exhausted".to_string()))?;

        let rc = crate::runtime_config::RuntimeConfig::global();
        let recv_ring_size = rc.locusta.recv_ring_size;
        let send_buf_size = rc.locusta.send_buf_size;
        let max_inflight = rc.locusta.max_inflight;
        let server_local = self
            .server
            .as_mut()
            .unwrap()
            .prepare_peer(
                dest_id,
                recv_ring_size,
                send_buf_size,
                512,
                max_inflight,
                &self.qp_config,
            )
            .map_err(|e| {
                RpcError::ConnectionError(format!(
                    "server.prepare_peer({dest_id}, self-loopback): {e:?}"
                ))
            })?;
        let relay_local = self
            .daemon
            .as_mut()
            .unwrap()
            .prepare_destination(dest_id, recv_ring_size, send_buf_size, &self.qp_config)
            .map_err(|e| {
                RpcError::ConnectionError(format!(
                    "daemon.prepare_destination({dest_id}, self-loopback): {e:?}"
                ))
            })?;
        // Connect each side to the OTHER's local QP info — RDMA
        // loopback through the local HCA (same LID).
        self.server
            .as_mut()
            .unwrap()
            .connect_peer(dest_id, &relay_local)
            .map_err(|e| {
                RpcError::ConnectionError(format!(
                    "server.connect_peer({dest_id}, self-loopback): {e:?}"
                ))
            })?;
        self.daemon
            .as_mut()
            .unwrap()
            .connect_destination(dest_id, &server_local)
            .map_err(|e| {
                RpcError::ConnectionError(format!(
                    "daemon.connect_destination({dest_id}, self-loopback): {e:?}"
                ))
            })?;
        self.node_to_dest.insert(self_id.clone(), dest_id);
        tracing::info!(
            "locusta self-loopback peer wired: node={} dest_id={}",
            self_id,
            dest_id
        );
        Ok(dest_id)
    }

    fn add_peer_blocking(&mut self, peer: &str, deadline: Instant) -> Result<u16, RpcError> {
        if let Some(&existing) = self.node_to_dest.get(peer) {
            return Ok(existing);
        }

        // Standalone-daemon clients have no local HCA resources and
        // rely on the daemon process's already-established peer mesh.
        // The daemon publishes its peer manifest at init time; any
        // peer NOT in `node_to_dest` here is either the daemon's OWN
        // identity (= local-daemon's node, which the daemon can't
        // forward to without loopback support) or an unknown peer.
        // In both cases there's no HCA to run a fresh handshake, so
        // return a clear error instead of panicking on
        // `self.server.expect(...)`.
        if self.server.is_none() {
            return Err(RpcError::ConnectionError(format!(
                "add_peer({peer}): unreachable in standalone-daemon client mode \
                 (peer is the daemon's OWN node or missing from manifest)"
            )));
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
            .server.as_mut().expect("server present in embedded mode")
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
            .daemon.as_mut().expect("daemon present in embedded mode")
            .prepare_destination(dest_id, recv_ring_size, send_buf_size, &self.qp_config)
            .map_err(|e| {
                RpcError::ConnectionError(format!("daemon.prepare_destination({dest_id}): {e:?}"))
            })?;

        // Design C: registry-based rendezvous. Opt-in via [locusta]
        // handshake_mode = "registry" in benchfs.toml.
        let hs_mode = &crate::runtime_config::RuntimeConfig::global().locusta.handshake_mode;
        let use_registry = hs_mode == "registry";
        if use_registry {
            crate::rpc::registry_handshake::publish_local_qp(
                &self.registry_dir,
                &self.local_node_id,
                peer,
                &relay_local,
                &server_local,
            )
            .map_err(|e| {
                RpcError::ConnectionError(format!(
                    "registry publish_local_qp({}_->{peer}): {e}",
                    self.local_node_id
                ))
            })?;
            let (peer_relay, peer_server) = crate::rpc::registry_handshake::read_peer_qp(
                &self.registry_dir,
                peer,
                &self.local_node_id,
                deadline,
            )
            .map_err(|e| {
                RpcError::ConnectionError(format!(
                    "registry read_peer_qp({peer}__->{}): {e}",
                    self.local_node_id
                ))
            })?;
            self.server.as_mut().expect("server present in embedded mode")
                .connect_peer(dest_id, &peer_relay)
                .map_err(|e| {
                    RpcError::ConnectionError(format!(
                        "server.connect_peer({dest_id}, registry): {e:?}"
                    ))
                })?;
            self.daemon.as_mut().expect("daemon present in embedded mode")
                .connect_destination(dest_id, &peer_server)
                .map_err(|e| {
                    RpcError::ConnectionError(format!(
                        "daemon.connect_destination({dest_id}, registry): {e:?}"
                    ))
                })?;
            // Phase 2: ACK barrier (publish only, optional wait). We
            // always publish our ACK so peers can detect that *we* have
            // connected, but waiting on theirs is gated behind
            // `BENCHFS_LOCUSTA_WAIT_PEER_ACK=1` because in a 400-client
            // prewarm pile-up the peer typically completes its own
            // connect tens of seconds after ours (client side is
            // sequential across 40 servers), and a strict wait inside
            // the server's tick-bound discover loop drains the 5s
            // deadline and fails the handshake (iter109 224 such
            // failures). Skipping the wait is safe because no RPC is
            // posted between handshake and the io500 stonewall warm-
            // up (~seconds), and by then both sides' QPs are RTR/RTS.
            crate::rpc::registry_handshake::publish_ack(
                &self.registry_dir,
                &self.local_node_id,
                peer,
            )
            .map_err(|e| {
                RpcError::ConnectionError(format!(
                    "registry publish_ack({}__->{peer}): {e}",
                    self.local_node_id
                ))
            })?;
            // Short-deadline best-effort ACK wait. With strict mode
            // ([locusta] wait_peer_ack_strict = true) the handshake
            // fails on missing ACK; default (false) does a brief 10 s
            // poll so the peer has time to finish its connect_* —
            // without this, iter111 saw rpc_hang on every put because
            // the first 4 MiB write landed before the peer's QP
            // transitioned to RTR and RNR retry exhausted.
            let strict = crate::runtime_config::RuntimeConfig::global().locusta.wait_peer_ack_strict;
            let ack_deadline = if strict {
                deadline
            } else {
                // Default best-effort 10s. Most peers publish their
                // ack within milliseconds once the server-side state
                // machine reaches NeedConnect, but Lustre readdir
                // latency can stall the server scan loop by seconds.
                std::time::Instant::now() + std::time::Duration::from_secs(10)
            };
            let ack_res = crate::rpc::registry_handshake::wait_peer_ack(
                &self.registry_dir,
                peer,
                &self.local_node_id,
                ack_deadline,
            );
            match (ack_res, strict) {
                (Ok(()), _) => {}
                (Err(e), true) => {
                    return Err(RpcError::ConnectionError(format!(
                        "registry wait_peer_ack({peer}__->{}): {e}",
                        self.local_node_id
                    )));
                }
                (Err(e), false) => {
                    tracing::warn!(
                        target: "rpc_registry_ack",
                        peer = %peer,
                        error = %e,
                        "best-effort ack wait expired; proceeding (peer may not be RTR yet)"
                    );
                }
            }
            self.node_to_dest.insert(peer.to_string(), dest_id);
            tracing::debug!(
                target: "rpc_registry_handshake",
                peer = %peer,
                dest_id,
                "registry handshake complete"
            );
            return Ok(dest_id);
        }

        let peer_addr = self.resolve_peer_udp_addr(peer, deadline)?;

        let request = ExchangePacket {
            msg_type: MSG_REQUEST,
            relay_qp: relay_local,
            server_qp: server_local,
            node_id: self.local_node_id.clone(),
        };
        let request_bytes = request.encode();

        // Per-attempt UDP recv timeout. Sized to absorb the full
        // burst-drain window of one server.
        //
        // Drain math (iter170, ppn=20, 10 phys = 800 clients × 40
        // servers = 32 000 handshakes evenly split = **800 REQUESTs
        // per server proc** at t=0):
        //   per-accept fast-path cost (pre_alloc pool hit) ≈ 1.3 ms
        //   (measured `avg_total_us=1300`, mostly `avg_connect_us`)
        //   ⇒ 800 × 1.3 ms ≈ 1040 ms to drain one server's burst
        //
        // The previous 500 ms initial timeout was sized for the
        // 6400-client ppn=8 mesh (80 incoming/server × 1.3 ms ≈
        // 100 ms). At ppn=20 it's half the drain window, so the
        // tail clients always retransmit before the server reaches
        // them — the retransmits pile into the same socket and the
        // storm self-amplifies (iter170 observed 2390 Pre-warm
        // timeouts = ~7.5 % of all handshakes). 3 s gives a 3× margin
        // over the measured drain plus headroom for occasional
        // 50 ms slow accepts (when slot-pool misses to slow path).
        let initial_recv_timeout = Duration::from_millis(3000);
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
                match self.udp_socket.send_to(&request_bytes, peer_addr) {
                    Ok(_) => {}
                    // EPERM (conntrack table full / transient firewall
                    // drop under the 80-daemon handshake storm) and
                    // ENOBUFS/EAGAIN are retryable: treat like a lost
                    // datagram and retransmit after the backoff.
                    // 28778 (2026-06-12) died at startup because one
                    // EPERM here was fatal for the whole 20-phys run.
                    Err(e)
                        if matches!(
                            e.raw_os_error(),
                            Some(libc::EPERM) | Some(libc::ENOBUFS) | Some(libc::EAGAIN)
                        ) =>
                    {
                        tracing::warn!(
                            target: "udp_handshake",
                            "send_to({peer_addr}, {peer}) transient error, will retransmit: {e}"
                        );
                    }
                    Err(e) => {
                        return Err(RpcError::ConnectionError(format!(
                            "send_to({peer_addr}, {peer}): {e}"
                        )));
                    }
                }
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

        self.server.as_mut().expect("server present in embedded mode")
            .connect_peer(dest_id, &response.relay_qp)
            .map_err(|e| {
                RpcError::ConnectionError(format!("server.connect_peer({dest_id}): {e:?}"))
            })?;
        self.daemon.as_mut().expect("daemon present in embedded mode")
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
                    || e.kind() == std::io::ErrorKind::TimedOut
                    // EINTR: a signal interrupted recv. Treat as "no
                    // packet" so the accept loop retries next tick
                    // instead of surfacing a scary ConnectionError.
                    // 28476 (2026-06-11) showed multi-minute EINTR
                    // storms on one daemon wedging its handshake path.
                    || e.kind() == std::io::ErrorKind::Interrupted =>
            {
                return Ok(None);
            }
            Err(e) => {
                return Err(RpcError::ConnectionError(format!("udp recv_from: {e}")));
            }
        };

        self.udp_stats_recvs = self.udp_stats_recvs.saturating_add(1);
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
        let t_enter = std::time::Instant::now();
        let peer = request.node_id.clone();
        if peer == self.local_node_id {
            return Ok(());
        }
        // Reset-aware: the REQUEST carries the client's freshly-built
        // relay/server QPNs. Compare against the cached RESPONSE's
        // implied "what we last connected to". If the client's QPNs
        // differ from the values we previously connected to, it has
        // reset its state and we MUST allocate a new dest_id (leaks
        // the old slot but lets the peer recover from a wedged QP).
        if let Some(&old_dest) = self.node_to_dest.get(&peer) {
            let cached_request_match = self
                .cached_responses
                .get(&peer)
                .map(|(_, _)| true)
                .unwrap_or(false);
            // We can't directly compare against the old client QPN
            // (it was never stored), so use a simpler heuristic:
            // re-resend cached RESPONSE; if the client is in the same
            // state it acks and moves on. If we see N retransmits
            // from this peer in a row, assume reset and allocate new
            // dest_id below.
            let retry_count = self.peer_request_retries.entry(peer.clone()).or_insert(0);
            *retry_count += 1;
            let retries = *retry_count;
            if retries < 5 && cached_request_match {
                // Probably just a lost RESPONSE; resend the cached
                // one and let the existing dest_id stand.
                if let Some((addr, bytes)) = self.cached_responses.get(&peer).cloned() {
                    if let Err(e) = self.udp_socket.send_to(&bytes, addr) {
                        tracing::warn!("re-send to {peer} failed: {e}");
                    }
                }
                self.udp_stats_resend_cached += 1;
                return Ok(());
            }
            // 5+ retransmits despite our resent RESPONSE → assume the
            // client reset itself. Drop the old dest_id mapping and
            // fall through to allocate a fresh one. The old QP slots
            // on server/daemon leak until process exit.
            tracing::warn!(
                "peer {peer} request retried {retries}× through cached RESPONSE — \
                 treating as peer-reset, reallocating dest_id (old={old_dest})"
            );
            self.node_to_dest.remove(&peer);
            self.cached_responses.remove(&peer);
            self.peer_request_retries.remove(&peer);
        }

        // Tuning comes from [locusta] section in benchfs.toml.
        let rc = crate::runtime_config::RuntimeConfig::global();
        let recv_ring_size = rc.locusta.recv_ring_size;
        let send_buf_size = rc.locusta.send_buf_size;
        let max_inflight = rc.locusta.max_inflight;
        // Try the pre-allocated peer pool first. If a slot is available
        // we skip 3-5 ibv_reg_mr calls (~57-145 ms measured in
        // iter135), drastically cutting per-accept time and letting
        // the server keep up with 800-client bursts.
        let t_prep_start = std::time::Instant::now();
        let claim_server = self.server.as_mut().expect("accept_pre_allocated_inline: server present").try_claim_pre_allocated_peer();
        let claim_daemon = self.daemon.as_mut().expect("accept_pre_allocated_inline: daemon present").try_claim_pre_allocated_destination();
        let (dest_id, server_local, relay_local) = match (claim_server, claim_daemon) {
            (Some((sid, server_local)), Some((did, relay_local))) if sid == did => {
                (sid, server_local, relay_local)
            }
            (server_slot, daemon_slot) => {
                // Pool empty or ids out of sync — fall back to the
                // slow path. Re-push any partially claimed slot so we
                // don't leak it.
                if let Some((sid, info)) = server_slot {
                    let server_mut = self.server.as_mut().unwrap();
                    server_mut.pre_allocated_peer_ids.push_front(sid);
                    server_mut.pre_allocated_local_info.insert(sid, info);
                }
                if let Some((did, info)) = daemon_slot {
                    let daemon_mut = self.daemon.as_mut().unwrap();
                    daemon_mut.pre_allocated_dest_ids.push_front(did);
                    daemon_mut.pre_allocated_dest_info.insert(did, info);
                }
                let dest_id = self.next_dest_id;
                self.next_dest_id = self.next_dest_id.checked_add(1).ok_or_else(|| {
                    RpcError::ConnectionError("dest_id space exhausted".to_string())
                })?;
                let server_local = self
                    .server.as_mut().expect("server present in embedded mode")
                    .prepare_peer(
                        dest_id,
                        recv_ring_size,
                        send_buf_size,
                        512,
                        max_inflight,
                        &self.qp_config,
                    )
                    .map_err(|e| {
                        RpcError::ConnectionError(format!(
                            "server.prepare_peer({dest_id}): {e:?}"
                        ))
                    })?;
                let relay_local = self
                    .daemon.as_mut().expect("daemon present in embedded mode")
                    .prepare_destination(dest_id, recv_ring_size, send_buf_size, &self.qp_config)
                    .map_err(|e| {
                        RpcError::ConnectionError(format!(
                            "daemon.prepare_destination({dest_id}): {e:?}"
                        ))
                    })?;
                (dest_id, server_local, relay_local)
            }
        };
        let prep_us = t_prep_start.elapsed().as_micros() as u64;
        self.udp_stats_prepare_us = self.udp_stats_prepare_us.saturating_add(prep_us);

        let t_connect_start = std::time::Instant::now();
        self.server.as_mut().expect("server present in embedded mode")
            .connect_peer(dest_id, &request.relay_qp)
            .map_err(|e| {
                RpcError::ConnectionError(format!("server.connect_peer({dest_id}): {e:?}"))
            })?;
        self.daemon.as_mut().expect("daemon present in embedded mode")
            .connect_destination(dest_id, &request.server_qp)
            .map_err(|e| {
                RpcError::ConnectionError(format!("daemon.connect_destination({dest_id}): {e:?}"))
            })?;
        let connect_us = t_connect_start.elapsed().as_micros() as u64;
        self.udp_stats_connect_us = self.udp_stats_connect_us.saturating_add(connect_us);

        let response = ExchangePacket {
            msg_type: MSG_RESPONSE,
            relay_qp: relay_local,
            server_qp: server_local,
            node_id: self.local_node_id.clone(),
        };
        let response_bytes = response.encode();
        let t_send_start = std::time::Instant::now();
        let send_res = self.udp_socket
            .send_to(&response_bytes, from_v4);
        let send_us = t_send_start.elapsed().as_micros() as u64;
        self.udp_stats_send_resp_us = self.udp_stats_send_resp_us.saturating_add(send_us);
        // A transient send failure (EPERM from a full conntrack table,
        // ENOBUFS, EAGAIN) must not poison the accept loop: the peer
        // retransmits its REQUEST on its own backoff and we re-answer
        // from `cached_responses`. Only treat other errors as fatal.
        match send_res {
            Ok(_) => {}
            Err(ref e)
                if matches!(
                    e.raw_os_error(),
                    Some(libc::EPERM) | Some(libc::ENOBUFS) | Some(libc::EAGAIN)
                ) =>
            {
                tracing::warn!(
                    target: "udp_handshake",
                    "send_to({from_v4}, response) transient error, peer will retransmit: {e}"
                );
            }
            Err(e) => {
                return Err(RpcError::ConnectionError(format!(
                    "send_to({from_v4}, response): {e}"
                )));
            }
        }

        self.node_to_dest.insert(peer.clone(), dest_id);
        self.cached_responses
            .insert(peer, (from_v4, response_bytes));
        let total_us = t_enter.elapsed().as_micros() as u64;
        self.udp_stats_accepts = self.udp_stats_accepts.saturating_add(1);
        self.udp_stats_total_us = self.udp_stats_total_us.saturating_add(total_us);
        if total_us > self.udp_stats_max_total_us {
            self.udp_stats_max_total_us = total_us;
        }
        Ok(())
    }

    /// Periodic dump of UDP accept-path stats (every ~5 s). Writes
    /// a single `WARN target=udp_accept_stats` line with averages so
    /// we can actually measure where server-side handshake time goes
    /// instead of guessing parameters.
    pub fn maybe_dump_udp_accept_stats(&self) {
        let accepts = self.udp_stats_accepts;
        let recvs = self.udp_stats_recvs;
        if accepts == 0 && recvs == 0 {
            return;
        }
        let avg_total_us = if accepts > 0 {
            self.udp_stats_total_us / accepts
        } else {
            0
        };
        let avg_prep_us = if accepts > 0 {
            self.udp_stats_prepare_us / accepts
        } else {
            0
        };
        let avg_connect_us = if accepts > 0 {
            self.udp_stats_connect_us / accepts
        } else {
            0
        };
        let avg_send_us = if accepts > 0 {
            self.udp_stats_send_resp_us / accepts
        } else {
            0
        };
        tracing::warn!(
            target: "udp_accept_stats",
            recvs,
            accepts,
            resend_cached = self.udp_stats_resend_cached,
            avg_total_us,
            avg_prep_us,
            avg_connect_us,
            avg_send_us,
            max_total_us = self.udp_stats_max_total_us,
            "udp accept timing summary"
        );
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
    /// Override the global `[locusta] pre_allocated_peer_count` for
    /// this transport instance. `None` means use the config-level
    /// value (server default). `Some(0)` disables pre-allocation,
    /// required on the **client side** where the global config gives
    /// the server-side count but each client process would otherwise
    /// pin `N × 96 MB` (= 9.6 GB at N=100) of memory for slots that
    /// never get used — clients connect, they don't accept. At ppn=20
    /// (80 client procs/host × 9.6 GB = 768 GB locked memory) this
    /// busts the per-host physical-RAM budget and triggers OOM-killer
    /// (observed iter141..iter167: rank death on first init, vnode 3
    /// dies first because PBS/numactl maps higher-numbered ranks to
    /// later NUMA nodes which fill last as the kernel runs out of
    /// pinnable pages).
    pub pre_allocated_peer_count_override: Option<u16>,
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
            pre_allocated_peer_count_override: None,
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
            pending_cq: std::collections::VecDeque::new(),
            // Embedded mode: client and daemon share the same process
            // and use the in-process JiffyQueue (`req_queue`). The SHM
            // req_ring is only used in standalone-daemon mode.
            req_ring: None,
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
        //
        // NOTE: Bind here, but **publish the addr only after
        // `pre_allocate_peers` finishes** (see `delayed_publish` block
        // below the pre_alloc loop). Iter170-172 hit a Pre-warm storm
        // because clients saw the server's UDP addr **before** the
        // server had finished its 30-60 sec pre_alloc, sent REQUESTs
        // that overflowed the kernel UDP recv buffer, and the
        // exponential retransmits kept the queue saturated even after
        // the server became ready. Holding back the publish until the
        // accept-side pool exists eliminates that pre-ready REQUEST
        // burst entirely.
        let (udp_socket, local_udp_addr) = udp_handshake::bind_udp_socket()
            .map_err(|e| RpcError::ConnectionError(format!("bind udp socket: {e}")))?;
        tracing::info!(
            "locusta UDP control-plane bound: {} → {} (publish deferred until pre_alloc done)",
            cfg.local_node_id,
            local_udp_addr
        );

        let inner = LocustaInner {
            client,
            daemon: Some(daemon),
            server: Some(server),
            server_buffer_allocator,
            _server_buffer_mrs: server_buffer_mrs,
            registry_scan_counter: 0,
            udp_stats_recvs: 0,
            udp_stats_accepts: 0,
            udp_stats_resend_cached: 0,
            udp_stats_prepare_us: 0,
            udp_stats_connect_us: 0,
            udp_stats_send_resp_us: 0,
            udp_stats_total_us: 0,
            udp_stats_max_total_us: 0,
            pending_discoveries: Vec::new(),
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
            peer_request_retries: HashMap::new(),
            owner_node_id: None,
        };
        let inner = Rc::new(RefCell::new(inner));

        // ------ Per-peer QP exchange (sequential; each peer gets a dest_id) ------
        // `[locusta] exchange_timeout_secs` in benchfs.toml overrides the
        // config-default 120 s; runtime-supplied cfg.exchange_timeout_secs
        // (rare) is consulted only when the toml is unset.
        let rc_locusta = &crate::runtime_config::RuntimeConfig::global().locusta;
        let timeout_secs = if rc_locusta.exchange_timeout_secs > 0 {
            rc_locusta.exchange_timeout_secs
        } else {
            cfg.exchange_timeout_secs
        };
        let deadline = Instant::now() + Duration::from_secs(timeout_secs);
        // `[locusta] defer_init_prewarm = true` skips the sync prewarm
        // in init. Callers are then responsible for calling `add_peer`
        // per peer later, after `register_self` has published their
        // address (so the job script's `check_server_ready` barrier
        // passes and io500 clients can start, ending the chicken-and-
        // egg deadlock seen in iter108).
        // Pre-allocate `pre_allocated_peer_count` peer/destination
        // slots so subsequent accepts don't pay the ~57-145 ms
        // ibv_reg_mr × 3-5 cost on the hot path. Done BEFORE the
        // init-time prewarm so the server-server handshake bursts
        // (40 × 39 inbound REQUESTs at once) also benefit from the
        // pool instead of going through the slow path.
        // Per-instance override (client passes Some(0) so it skips
        // the 9.6 GB/process pre-allocation — see field doc on
        // LocustaConfig::pre_allocated_peer_count_override for why
        // this is critical at ppn≥10).
        let pre_alloc_count = cfg
            .pre_allocated_peer_count_override
            .unwrap_or(rc_locusta.pre_allocated_peer_count);
        if pre_alloc_count > 0 {
            let mut inner_mut = inner.borrow_mut();
            let start_id = inner_mut.next_dest_id;
            let recv_ring_size = rc_locusta.recv_ring_size;
            let send_buf_size = rc_locusta.send_buf_size;
            let max_inflight = rc_locusta.max_inflight;
            let qp_config = inner_mut.qp_config.clone();
            let t0 = std::time::Instant::now();
            inner_mut
                .server
                .as_mut().expect("server present in embedded mode")
                .pre_allocate_peers(
                    start_id,
                    pre_alloc_count,
                    recv_ring_size,
                    send_buf_size,
                    512,
                    max_inflight,
                    &qp_config,
                )
                .map_err(|e| {
                    RpcError::ConnectionError(format!(
                        "server.pre_allocate_peers({pre_alloc_count}): {e}"
                    ))
                })?;
            inner_mut
                .daemon
                .as_mut().expect("daemon present in embedded mode")
                .pre_allocate_destinations(
                    start_id,
                    pre_alloc_count,
                    recv_ring_size,
                    send_buf_size,
                    &qp_config,
                )
                .map_err(|e| {
                    RpcError::ConnectionError(format!(
                        "daemon.pre_allocate_destinations({pre_alloc_count}): {e}"
                    ))
                })?;
            inner_mut.next_dest_id = start_id.saturating_add(pre_alloc_count);
            tracing::warn!(
                target: "udp_accept_stats",
                pre_alloc_count,
                elapsed_ms = t0.elapsed().as_millis() as u64,
                "pre-allocated peer/destination slots"
            );
        }

        // Now publish the UDP addr — server is ready to accept REQUESTs
        // (pool primed, accept loop will hit fast path immediately). This
        // is the key ordering fix vs the pre-iter183 design that wrote
        // the addr early and forced clients into a retransmit storm
        // against a server still busy in `pre_allocate_peers`.
        udp_handshake::publish_udp_addr(&cfg.registry_dir, &cfg.local_node_id, local_udp_addr)
            .map_err(|e| RpcError::ConnectionError(format!("publish_udp_addr: {e}")))?;
        tracing::info!(
            "locusta UDP addr published post-prealloc: {} → {}",
            cfg.local_node_id,
            local_udp_addr
        );

        if !rc_locusta.defer_init_prewarm {
            let mut inner_mut = inner.borrow_mut();
            // iter199+: self-loopback peer FIRST so dest_id 0 is the
            // local node and shared-daemon clients can route to OWNER
            // via the same destinations slab as remote peers. The
            // resulting `node_to_dest` published in the manifest
            // includes self (with dest_id 0), eliminating the
            // "consistent_hash hits OWNER → fail" pattern that
            // dominated iters 5-9.
            inner_mut.add_self_loopback()?;
            for peer in &cfg.peer_node_ids {
                inner_mut.add_peer_blocking(peer, deadline)?;
            }
        } else {
            tracing::info!(
                target: "rpc_handshake_mode",
                "[locusta] defer_init_prewarm=true — skipping init-time prewarm; \
                 caller must invoke add_peer per peer after register_self"
            );
        }
        Ok(Self { inner })
    }

    /// **iter191+ shared-daemon mode**. Initialize a *client-only*
    /// LocustaTransport that talks to a daemon already running in
    /// another process via the daemon's control_ring SHM.
    ///
    /// The companion to [`expose_daemon_control_ring`] on the daemon
    /// side: the daemon creates the SHM and registers it on its
    /// `RelayDaemon`, then this constructor opens the same SHM, sends
    /// a `Connect` message, and receives a per-client channel back.
    ///
    /// Differences vs [`init`]:
    /// * No mlx5 device opened (the daemon owns the HCA).
    /// * No `RelayDaemon` / `ServerContext` constructed —
    ///   `LocustaInner.{daemon, server}` are `None`.
    /// * No UDP control-plane bound (no peer handshake from this
    ///   process; the daemon already has warm QPs).
    /// * `node_to_dest` is populated from `peer_node_ids` with
    ///   `dest_id = index`. The caller must pass the SAME peer order
    ///   used by the daemon so dest_ids match.
    ///
    /// **Request-path IPC**: `Client::connect` constructs a SHM-backed
    /// `FastForwardRing<RequestSlot>` (producer side) over the
    /// per-client channel SHM. The daemon (in another process) sees
    /// the same ring as a consumer in its `Channel::req_ring` and
    /// drains it in `poll_client_requests`. The `JiffyQueue` handed
    /// to `Client::connect` for signature compatibility is unused
    /// when `req_ring` is `Some`.
    pub fn init_shared_client(
        shm_name: &str,
        config: &LocustaConfig,
    ) -> Result<Self, RpcError> {
        use rrrpc::relay::client::ClientConfig;
        use rrrpc::relay::control_ring::ControlRingProducer;
        use rrrpc::relay::protocol::{ControlRingHeader, ControlSlot};
        use rrrpc::relay::shm::open_control_ring_shm;

        // ---- Open the daemon's control_ring SHM ----
        let (_ctrl_fd, ctrl_base, _ctrl_size) =
            open_control_ring_shm(shm_name).map_err(|e| {
                RpcError::ConnectionError(format!(
                    "open_control_ring_shm({shm_name}): {e}"
                ))
            })?;
        // Layout matches `create_control_ring_shm`: header | slots[N].
        let header_ptr = ctrl_base as *const ControlRingHeader;
        let slots_ptr = unsafe {
            ctrl_base.add(std::mem::size_of::<ControlRingHeader>()) as *mut ControlSlot
        };
        let control_ring = unsafe { ControlRingProducer::new(header_ptr, slots_ptr) };

        // ---- Build a dummy req_queue ----
        // `Client::connect` requires this arg by signature but the
        // SHM `req_ring` constructed inside `connect` takes precedence
        // when present (`req_ring.is_some()`), so this JiffyQueue is
        // never enqueued in standalone mode. Kept for ABI parity with
        // the embedded path.
        let req_queue = Arc::new(JiffyQueue::<RequestSlot>::new(256));

        // ---- Call Client::connect to allocate a per-client channel ----
        let client_cfg = ClientConfig {
            arena_size: config.arena_size,
            ring_capacity: config.ring_capacity,
            channel_name: None,
        };
        let client = Client::connect(&control_ring, &client_cfg, Arc::clone(&req_queue))
            .map_err(|e| {
                RpcError::ConnectionError(format!(
                    "Client::connect(shared daemon shm={shm_name}): {e:?}"
                ))
            })?;

        // ---- Read the daemon's peer→dest_id manifest ----
        // The daemon writes `${registry}/standalone_daemon/${shm_basename}.peers`
        // after `expose_daemon_control_ring`. Each line is one of:
        //   <peer_node_id>\t<dest_id>
        //   OWNER\t<node_id_of_this_daemon>
        // We use this verbatim instead of `peer_idx → dest_id` because
        // each daemon skips its own slot when allocating dest_ids, so
        // the index ≠ rank mapping varies per daemon (iter193 bug).
        let shm_basename = shm_name.trim_start_matches('/');
        let manifest_path = config
            .registry_dir
            .join("standalone_daemon")
            .join(format!("{shm_basename}.peers"));
        // Poll for the manifest with a short timeout — the daemon may
        // not have written it yet at the moment a client races ahead.
        let mut manifest_body: Option<String> = None;
        let deadline_manifest =
            std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            match std::fs::read_to_string(&manifest_path) {
                Ok(b) if b.contains("OWNER\t") => {
                    manifest_body = Some(b);
                    break;
                }
                _ => {}
            }
            if std::time::Instant::now() >= deadline_manifest {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        let manifest_body = manifest_body.ok_or_else(|| {
            RpcError::ConnectionError(format!(
                "standalone-daemon manifest not available at {}",
                manifest_path.display()
            ))
        })?;

        let mut node_to_dest: HashMap<NodeId, u16> = HashMap::new();
        let mut owner: Option<NodeId> = None;
        for line in manifest_body.lines() {
            let mut parts = line.splitn(2, '\t');
            let key = match parts.next() {
                Some(k) => k,
                None => continue,
            };
            let val = match parts.next() {
                Some(v) => v,
                None => continue,
            };
            if key == "OWNER" {
                owner = Some(val.to_string());
            } else {
                let dest_id: u16 = val.parse().map_err(|e| {
                    RpcError::ConnectionError(format!(
                        "manifest parse {key}={val}: {e}"
                    ))
                })?;
                node_to_dest.insert(key.to_string(), dest_id);
            }
        }
        tracing::info!(
            target: "rpc_handshake_mode",
            owner = owner.as_deref().unwrap_or("?"),
            entries = node_to_dest.len(),
            "init_shared_client loaded manifest"
        );
        let mut entry_dump: Vec<(NodeId, u16)> =
            node_to_dest.iter().map(|(k, v)| (k.clone(), *v)).collect();
        entry_dump.sort_by_key(|(_, v)| *v);
        tracing::warn!(
            target: "init_probe",
            self_node = %config.local_node_id,
            manifest_path = %manifest_path.display(),
            entries = node_to_dest.len(),
            owner = owner.as_deref().unwrap_or("?"),
            body_len = manifest_body.len(),
            dump = ?entry_dump,
            "[shared_client] manifest loaded"
        );

        // ---- Bind a dummy UDP socket. Never used in shared-client
        //      mode (no peer handshake from this process), but the
        //      LocustaInner field is non-Option. Bind ephemeral on
        //      loopback so it can't accidentally receive traffic. ----
        let udp_socket = UdpSocket::bind("127.0.0.1:0").map_err(|e| {
            RpcError::ConnectionError(format!("bind dummy udp socket: {e}"))
        })?;
        let local_udp_addr = match udp_socket.local_addr() {
            Ok(std::net::SocketAddr::V4(v4)) => v4,
            _ => SocketAddrV4::new(std::net::Ipv4Addr::LOCALHOST, 0),
        };

        let qp_config = RcQpConfig {
            max_send_wr: 1024,
            ..RcQpConfig::default()
        };

        let inner = LocustaInner {
            client,
            daemon: None,
            server: None,
            server_buffer_allocator: None,
            _server_buffer_mrs: Vec::new(),
            registry_scan_counter: 0,
            udp_stats_recvs: 0,
            udp_stats_accepts: 0,
            udp_stats_resend_cached: 0,
            udp_stats_prepare_us: 0,
            udp_stats_connect_us: 0,
            udp_stats_send_resp_us: 0,
            udp_stats_total_us: 0,
            udp_stats_max_total_us: 0,
            pending_discoveries: Vec::new(),
            node_to_dest,
            // No locally-allocated channel SHM in shared-client mode —
            // the per-client channel SHM is owned by `Client` itself
            // (Drop on Client closes it). Leave these null/zero so
            // `Drop for LocustaInner` skips the dealloc path.
            channel_base_ptr: std::ptr::null_mut(),
            channel_layout_total: 0,
            inflight: HashMap::new(),
            small_req_scratch: Vec::with_capacity(256),
            registry_dir: config.registry_dir.clone(),
            local_node_id: config.local_node_id.clone(),
            qp_config,
            next_dest_id: config.peer_node_ids.len() as u16,
            udp_socket,
            local_udp_addr,
            peer_udp_addrs: HashMap::new(),
            cached_responses: HashMap::new(),
            peer_request_retries: HashMap::new(),
            owner_node_id: owner.clone(),
        };

        tracing::info!(
            target: "rpc_handshake_mode",
            shm_name,
            owner = owner.as_deref().unwrap_or("?"),
            peer_count = config.peer_node_ids.len(),
            "LocustaTransport::init_shared_client connected to daemon SHM"
        );
        Ok(Self {
            inner: Rc::new(RefCell::new(inner)),
        })
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

    /// Server-server QP exchange via MPI_Allgather. Bypasses the
    /// file-based registry (and its Lustre overhead) for the small
    /// set of peers known at startup. The collective layout is:
    ///
    /// - Each rank packs `mpi_size` slots of 128 bytes (relay_qp 64 +
    ///   server_qp 64). The slot at index `peer` holds the local QPs
    ///   prepared for talking to `peer`; the slot at index `mpi_rank`
    ///   itself is zeroed.
    /// - `all_gather_into` collects every rank's send buffer into a
    ///   single `mpi_size² · 128`-byte recv buffer.
    /// - To connect to peer `P`, this rank reads `recvbuf[P][mpi_rank]`
    ///   — peer P's local QPs that it prepared specifically for us.
    ///
    /// Caller is responsible for skipping the init-time file prewarm
    /// (set `[locusta] defer_init_prewarm = true` or use the
    /// `mpi_server_mesh` flag in benchfsd_mpi).
    #[cfg(feature = "mpi-support")]
    pub fn mpi_handshake_servers<C>(
        &self,
        world: &C,
        mpi_rank: i32,
        mpi_size: i32,
        peer_name: impl Fn(i32) -> String,
    ) -> Result<(), RpcError>
    where
        C: mpi::topology::Communicator,
    {
        use mpi::traits::CommunicatorCollectives;
        use rrrpc::wire::QpExchangeInfo;

        const SLOT_BYTES: usize = 128; // relay (64) + server (64)
        let n = mpi_size as usize;
        let me = mpi_rank as usize;

        let rc = crate::runtime_config::RuntimeConfig::global();
        let recv_ring_size = rc.locusta.recv_ring_size;
        let send_buf_size = rc.locusta.send_buf_size;
        let max_inflight = rc.locusta.max_inflight;

        // ---- Phase 1: prepare local QPs for each peer (skip self) ----
        let mut sendbuf: Vec<u8> = vec![0u8; n * SLOT_BYTES];
        let mut dest_ids: Vec<u16> = vec![0u16; n];
        {
            let mut inner = self.inner.borrow_mut();
            for peer in 0..n {
                if peer == me {
                    continue;
                }
                let dest_id = inner.next_dest_id;
                inner.next_dest_id = inner.next_dest_id.checked_add(1).ok_or_else(|| {
                    RpcError::ConnectionError("dest_id space exhausted".to_string())
                })?;
                dest_ids[peer] = dest_id;
                let qp_config = inner.qp_config.clone();
                let server_local = inner
                    .server
                    .as_mut().expect("server present in embedded mode")
                    .prepare_peer(
                        dest_id,
                        recv_ring_size,
                        send_buf_size,
                        512,
                        max_inflight,
                        &qp_config,
                    )
                    .map_err(|e| {
                        RpcError::ConnectionError(format!(
                            "server.prepare_peer({dest_id}, mpi): {e:?}"
                        ))
                    })?;
                let relay_local = inner
                    .daemon
                    .as_mut().expect("daemon present in embedded mode")
                    .prepare_destination(dest_id, recv_ring_size, send_buf_size, &qp_config)
                    .map_err(|e| {
                        RpcError::ConnectionError(format!(
                            "daemon.prepare_destination({dest_id}, mpi): {e:?}"
                        ))
                    })?;
                // Pack [relay (64) | server (64)] at slot `peer`.
                let off = peer * SLOT_BYTES;
                unsafe {
                    let r: &[u8; 64] =
                        &*(&relay_local as *const QpExchangeInfo as *const [u8; 64]);
                    let s: &[u8; 64] =
                        &*(&server_local as *const QpExchangeInfo as *const [u8; 64]);
                    sendbuf[off..off + 64].copy_from_slice(r);
                    sendbuf[off + 64..off + 128].copy_from_slice(s);
                }
            }
        }

        // ---- Phase 2: MPI_Allgather ----
        let mut recvbuf: Vec<u8> = vec![0u8; n * n * SLOT_BYTES];
        world.all_gather_into(&sendbuf[..], &mut recvbuf[..]);

        // ---- Phase 3: connect to each peer's QPs (skip self) ----
        let mut inner = self.inner.borrow_mut();
        for peer in 0..n {
            if peer == me {
                continue;
            }
            // peer's send slot for me lives at recvbuf[peer][me].
            let off = (peer * n + me) * SLOT_BYTES;
            let peer_relay: QpExchangeInfo = unsafe {
                let mut q: QpExchangeInfo = std::mem::zeroed();
                std::ptr::copy_nonoverlapping(
                    recvbuf[off..off + 64].as_ptr(),
                    &mut q as *mut QpExchangeInfo as *mut u8,
                    64,
                );
                q
            };
            let peer_server: QpExchangeInfo = unsafe {
                let mut q: QpExchangeInfo = std::mem::zeroed();
                std::ptr::copy_nonoverlapping(
                    recvbuf[off + 64..off + 128].as_ptr(),
                    &mut q as *mut QpExchangeInfo as *mut u8,
                    64,
                );
                q
            };
            let dest_id = dest_ids[peer];
            inner.server.as_mut().expect("server present in embedded mode").connect_peer(dest_id, &peer_relay).map_err(|e| {
                RpcError::ConnectionError(format!(
                    "server.connect_peer({dest_id}, mpi peer={peer}): {e:?}"
                ))
            })?;
            inner
                .daemon.as_mut().expect("daemon present in embedded mode")
                .connect_destination(dest_id, &peer_server)
                .map_err(|e| {
                    RpcError::ConnectionError(format!(
                        "daemon.connect_destination({dest_id}, mpi peer={peer}): {e:?}"
                    ))
                })?;
            let peer_id = peer_name(peer as i32);
            inner.node_to_dest.insert(peer_id, dest_id);
        }
        tracing::info!(
            target: "rpc_handshake_mode",
            mpi_rank,
            mpi_size,
            peers_connected = n - 1,
            "MPI_Allgather server-mesh handshake complete"
        );
        Ok(())
    }

    /// Async variant of `add_peer` that yields the reactor between
    /// Lustre polling attempts. The sync `add_peer` holds
    /// `inner.borrow_mut()` for the entire handshake (~100 ms for the
    /// registry path), starving the locusta tick of RDMA polling — at
    /// 400-client scale that crushed ior-easy-write to 0.92 GiB/s
    /// (iter114-116). This variant takes `inner.borrow_mut()` only for
    /// the short locusta-internal phases (prepare_peer / connect_peer)
    /// and drops it across `pluvio_timer::sleep().await` boundaries so
    /// the reactor keeps ticking.
    ///
    /// Only the registry mode is exposed here; if
    /// `[locusta] handshake_mode != "registry"` the call falls back
    /// to the sync UDP path (which is what default deployments use).
    pub async fn add_peer_async(
        &self,
        peer: &NodeId,
        timeout: Duration,
    ) -> Result<u16, RpcError> {
        let deadline = Instant::now() + timeout;
        let use_registry =
            crate::runtime_config::RuntimeConfig::global().locusta.handshake_mode == "registry";
        if !use_registry {
            let mut inner = self.inner.borrow_mut();
            return inner.add_peer_blocking(peer, deadline);
        }

        // Already added?
        if let Some(existing) = self.try_get_dest_id(peer) {
            return Ok(existing);
        }

        // ---- Phase 1: prepare local QPs (short borrow_mut) ----
        let (dest_id, server_local, relay_local, registry_dir, local_node_id) = {
            let mut inner = self.inner.borrow_mut();
            if let Some(&existing) = inner.node_to_dest.get(peer) {
                return Ok(existing);
            }
            let dest_id = inner.next_dest_id;
            inner.next_dest_id = inner.next_dest_id.checked_add(1).ok_or_else(|| {
                RpcError::ConnectionError("dest_id space exhausted".to_string())
            })?;
            let rc = crate::runtime_config::RuntimeConfig::global();
            let recv_ring_size = rc.locusta.recv_ring_size;
            let send_buf_size = rc.locusta.send_buf_size;
            let max_inflight = rc.locusta.max_inflight;
            let qp_config = inner.qp_config.clone();
            let server_local = inner
                .server
                .as_mut().expect("server present in embedded mode")
                .prepare_peer(dest_id, recv_ring_size, send_buf_size, 512, max_inflight, &qp_config)
                .map_err(|e| {
                    RpcError::ConnectionError(format!(
                        "server.prepare_peer({dest_id}): {e:?}"
                    ))
                })?;
            let relay_local = inner
                .daemon
                .as_mut().expect("daemon present in embedded mode")
                .prepare_destination(dest_id, recv_ring_size, send_buf_size, &qp_config)
                .map_err(|e| {
                    RpcError::ConnectionError(format!(
                        "daemon.prepare_destination({dest_id}): {e:?}"
                    ))
                })?;
            (
                dest_id,
                server_local,
                relay_local,
                inner.registry_dir.clone(),
                inner.local_node_id.clone(),
            )
        };

        // ---- Phase 2: publish our QP info (no borrow) ----
        crate::rpc::registry_handshake::publish_local_qp(
            &registry_dir,
            &local_node_id,
            peer,
            &relay_local,
            &server_local,
        )
        .map_err(|e| {
            RpcError::ConnectionError(format!(
                "registry publish_local_qp({local_node_id}__->{peer}): {e}"
            ))
        })?;

        // ---- Phase 3: poll for peer's QP info (yields reactor) ----
        let (peer_relay, peer_server) = loop {
            let slot = crate::rpc::registry_handshake::slot_path(
                &registry_dir,
                peer,
                &local_node_id,
            );
            match std::fs::read(&slot) {
                Ok(buf)
                    if buf.len() >= crate::rpc::registry_handshake::QP_PAYLOAD_SIZE =>
                {
                    break crate::rpc::registry_handshake::decode_qp_payload(&buf)
                        .map_err(|e| {
                            RpcError::ConnectionError(format!("decode_qp_payload: {e}"))
                        })?;
                }
                _ => {}
            }
            if Instant::now() >= deadline {
                return Err(RpcError::ConnectionError(format!(
                    "registry read_peer_qp({peer}__->{local_node_id}): not visible before deadline"
                )));
            }
            pluvio_timer::sleep(Duration::from_millis(50)).await;
        };

        // ---- Phase 4: connect (short borrow_mut) ----
        {
            let mut inner = self.inner.borrow_mut();
            inner
                .server
                .as_mut().expect("server present in embedded mode")
                .connect_peer(dest_id, &peer_relay)
                .map_err(|e| {
                    RpcError::ConnectionError(format!(
                        "server.connect_peer({dest_id}, registry): {e:?}"
                    ))
                })?;
            inner
                .daemon
                .as_mut().expect("daemon present in embedded mode")
                .connect_destination(dest_id, &peer_server)
                .map_err(|e| {
                    RpcError::ConnectionError(format!(
                        "daemon.connect_destination({dest_id}, registry): {e:?}"
                    ))
                })?;
            inner.node_to_dest.insert(peer.to_string(), dest_id);
        }

        // ---- Phase 5: publish our ACK (no borrow) ----
        crate::rpc::registry_handshake::publish_ack(&registry_dir, &local_node_id, peer)
            .map_err(|e| {
                RpcError::ConnectionError(format!(
                    "registry publish_ack({local_node_id}__->{peer}): {e}"
                ))
            })?;

        // ---- Phase 6: best-effort wait for peer ACK (yields reactor) ----
        let ack_deadline =
            std::cmp::min(deadline, Instant::now() + Duration::from_secs(10));
        let ack_path = crate::rpc::registry_handshake::shard_dir(&registry_dir, peer)
            .join(format!("{peer}__{local_node_id}.ack"));
        loop {
            if std::fs::metadata(&ack_path).is_ok() {
                break;
            }
            if Instant::now() >= ack_deadline {
                tracing::warn!(
                    target: "rpc_registry_ack",
                    peer = %peer,
                    "ack wait expired (best-effort); proceeding"
                );
                break;
            }
            pluvio_timer::sleep(Duration::from_millis(20)).await;
        }

        Ok(dest_id)
    }

    /// Long-running async task: periodically scan the registry for
    /// peers that have published `{peer}__{self}.qp` against us and
    /// haven't been handshaked yet. For each new peer, kick off an
    /// independent `add_peer_async` task. Sleeps on `pluvio_timer`
    /// between scans so the reactor keeps ticking.
    ///
    /// Designed to be `pluvio_runtime::spawn`ed from benchfsd_mpi
    /// after `register_self`, so this server-side discover thread
    /// only starts taking peers after the address is announced.
    pub async fn server_discover_task(self: Rc<Self>) {
        // Adaptive cadence: fast (100 ms) while peers are arriving,
        // gradually slow (up to 2 s) only on prolonged idleness.
        // iter120 (ppn=20, 800 clients) saw mid-rank clients fail
        // because the previous aggressive 5 s slowdown missed late-
        // arriving peers; clients arrive in bursts that overlap with
        // server-side idle windows. The slower upper bound (2 s vs
        // 5 s) still tames 256-shard readdir at steady-state but
        // recovers quickly when a new burst arrives.
        const FAST_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);
        const SLOW_INTERVAL: std::time::Duration = std::time::Duration::from_millis(2000);
        const IDLE_SCANS_BEFORE_SLOWDOWN: u32 = 100;
        let mut idle_scans: u32 = 0;
        let suffix = {
            let inner = self.inner.borrow();
            format!("__{}.qp", inner.local_node_id)
        };
        loop {
            // Snapshot config we need across the scan without holding
            // borrow during file I/O.
            let (registry_dir, local_node_id) = {
                let inner = self.inner.borrow();
                (inner.registry_dir.clone(), inner.local_node_id.clone())
            };
            // Walk all shards and collect candidates.
            let mut candidates: Vec<String> = Vec::new();
            for shard in crate::rpc::registry_handshake::all_shard_dirs(&registry_dir) {
                let entries = match std::fs::read_dir(&shard) {
                    Ok(e) => e,
                    Err(_) => continue,
                };
                for entry in entries.flatten() {
                    let name = match entry.file_name().into_string() {
                        Ok(n) => n,
                        Err(_) => continue,
                    };
                    if name.starts_with('.') || !name.ends_with(&suffix) {
                        continue;
                    }
                    let owner = &name[..name.len() - suffix.len()];
                    if owner.is_empty() || owner == local_node_id {
                        continue;
                    }
                    candidates.push(owner.to_string());
                }
            }
            // Filter against known peers (read-only borrow, brief).
            let new_peers: Vec<String> = {
                let inner = self.inner.borrow();
                candidates
                    .into_iter()
                    .filter(|p| !inner.node_to_dest.contains_key(p))
                    .collect()
            };
            // Adaptive cadence: if no new peers found, ramp toward slow
            // interval; reset on any activity.
            if new_peers.is_empty() {
                idle_scans = idle_scans.saturating_add(1);
            } else {
                idle_scans = 0;
            }
            let next_interval = if idle_scans >= IDLE_SCANS_BEFORE_SLOWDOWN {
                SLOW_INTERVAL
            } else {
                FAST_INTERVAL
            };
            // For each new peer, kick off an independent async handshake
            // so they progress in parallel (each yields the reactor
            // between Lustre polls).
            for peer in new_peers {
                let me = Rc::clone(&self);
                let peer_id = peer.clone();
                pluvio_runtime::spawn_with_name(
                    {
                        let me = me.clone();
                        async move {
                            match me
                                .add_peer_async(&peer_id, std::time::Duration::from_secs(60))
                                .await
                            {
                                Ok(dest_id) => {
                                    tracing::info!(
                                        target: "rpc_registry_discover",
                                        peer = %peer_id,
                                        dest_id,
                                        "server accepted registry-initiated peer"
                                    );
                                    Ok::<(), std::io::Error>(())
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        target: "rpc_registry_discover",
                                        peer = %peer_id,
                                        error = ?e,
                                        "server registry handshake failed"
                                    );
                                    Ok(())
                                }
                            }
                        }
                    },
                    format!("server_handshake_{}", peer),
                );
            }
            pluvio_timer::sleep(next_interval).await;
        }
    }

    /// Public wrapper to dump the UDP accept stats. Borrow is short-lived.
    pub fn dump_udp_accept_stats(&self) {
        if let Ok(inner) = self.inner.try_borrow() {
            inner.maybe_dump_udp_accept_stats();
        }
    }

    /// Best-effort lookup of an existing dest_id (read-only borrow that
    /// stays out of the way of in-flight handshakes). Returns `None` if
    /// the peer isn't connected yet.
    fn try_get_dest_id(&self, peer: &NodeId) -> Option<u16> {
        self.inner
            .try_borrow()
            .ok()
            .and_then(|i| i.node_to_dest.get(peer).copied())
    }

    /// Forget all client-side state for `peer` so the next RPC has to
    /// re-issue the full handshake. Used by `call_locusta` when a peer
    /// goes silent (no completion for ≥30 s) — the underlying RC QP is
    /// probably wedged, and a fresh dest_id + fresh QP gives us a way
    /// out without restarting the entire process.
    ///
    /// Caveats:
    /// * Old QP slots on `server` / `daemon` are NOT released — they
    ///   leak until the process exits. Short-lived IO500 phases tolerate
    ///   this.
    /// * The next `add_peer(peer, ...)` will allocate a fresh dest_id
    ///   and run the UDP exchange again. If the server still
    ///   remembers the old QP, the new REQUEST is treated as a
    ///   retransmit and the server replies with its cached
    ///   (old-QP) RESPONSE — we then fail to set up the new QP.
    ///   To handle that, server side `accept_request_inline` was
    ///   extended (separate edit) to recognise "node_id already
    ///   present" + fresh REQUEST as a peer-reset request.
    pub fn reset_peer(&self, peer: &NodeId) -> bool {
        let mut inner = self.inner.borrow_mut();
        let had = inner.node_to_dest.remove(peer).is_some();
        if had {
            inner.peer_udp_addrs.remove(peer);
            inner.cached_responses.remove(peer);
            inner.peer_request_retries.remove(peer);
            tracing::warn!(
                target: "rpc_peer_reset",
                peer = %peer,
                "client reset peer state — next RPC will re-handshake"
            );
        }
        had
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
        //
        // ppn=20 tuning (iter132): the previous MAX_PER_TICK=256
        // held inner.borrow_mut() for the entire 256-accept loop, and
        // each `handle_one_udp_request` does QP prepare/connect at
        // ~30-50 ms, totalling ~10 s of reactor blocking. That stalls
        // the locusta dispatch task (memory `iter130 dispatch STALL
        // 10s`) and starves client RPC processing during prewarm. We
        // now cap at 16 per call and pair this with the async drain
        // helper (`drain_pending_peers_async`) which awaits between
        // sub-batches; the caller's scan_interval is also dropped to
        // 10 ms to keep aggregate accept throughput at the same
        // 1.6 kpeers/s while bounding tick stall to ~160 ms.
        const MAX_PER_TICK: usize = 16;
        let mut added = Vec::new();
        let mut first = true;
        for _ in 0..MAX_PER_TICK {
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

    /// Async variant of `try_accept_pending_peers` that yields the
    /// reactor between every accept so other tasks (locusta dispatch,
    /// client RPCs) can interleave during the prewarm burst. Caps each
    /// invocation at `max_per_call` accepts to keep wall-time bounded
    /// even when the kernel UDP queue is huge.
    pub async fn try_accept_pending_peers_async(
        &self,
        _per_peer_timeout: Duration,
        max_per_call: usize,
    ) -> Result<Vec<NodeId>, RpcError> {
        let mut added = Vec::new();
        let mut first = true;
        for i in 0..max_per_call {
            let recv_timeout = if first {
                Duration::from_millis(2)
            } else {
                Duration::from_micros(1)
            };
            let res = {
                let mut inner = self.inner.borrow_mut();
                inner.handle_one_udp_request(recv_timeout)
            };
            match res {
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
            // Yield every 4 accepts so the reactor can drain other
            // tasks (locusta tick, client RPC dispatch). 4 was chosen
            // so per-accept Lustre / QP setup work (~30 ms × 4 =
            // 120 ms) stays under the io500 200 ms heartbeat budget.
            if i % 4 == 3 {
                pluvio_timer::sleep(Duration::from_micros(10)).await;
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

    /// Returns the node_id of the daemon process whose SHM this
    /// transport is attached to (standalone-daemon client mode).
    /// `None` in embedded mode.
    pub fn owner_node_id(&self) -> Option<NodeId> {
        self.inner.borrow().owner_node_id.clone()
    }

    /// Snapshot the daemon's current `node_to_dest` mapping. Used by
    /// the standalone-daemon orchestration in `benchfsd_mpi.rs` to
    /// publish a manifest file so co-resident clients can mirror the
    /// same `peer → dest_id` map (each daemon allocates dest_ids
    /// sequentially in `peer_node_ids` order, skipping self — without
    /// the manifest a client attached to daemon at rank R would
    /// misroute every RPC for peer ranks > R).
    pub fn node_to_dest_snapshot(&self) -> Vec<(NodeId, u16)> {
        self.inner
            .borrow()
            .node_to_dest
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect()
    }

    /// Standalone-daemon mode (POC, iter191+):
    ///
    /// Expose this transport's relay daemon to LOCAL processes via a
    /// POSIX shared-memory control ring. Co-resident clients (the
    /// `libbenchfs.so` loaded by IOR) can then call `Client::connect`
    /// against `shm_name` and route their RPCs through this daemon's
    /// already-warm QPs to the remote peers — eliminating the
    /// per-process `ibv_reg_mr × N peers` cost that was making ppn=20
    /// scale fragile (see iter141..iter190 server-side SIGKILLs).
    ///
    /// Capacity must be a power of two; 64 is enough for connect /
    /// disconnect bursts even at ppn=20 (clients reuse the channel
    /// for the lifetime of the IOR phase).
    ///
    /// Idempotent w.r.t. stale SHM left from a crashed prior daemon
    /// — `create_control_ring_shm` unlinks before creating.
    ///
    /// Returns the held SHM fd + base + size so the caller keeps the
    /// region alive for the daemon's lifetime.
    pub fn expose_daemon_control_ring(
        &self,
        shm_name: &str,
        capacity: u32,
    ) -> Result<(std::os::fd::OwnedFd, *mut u8, usize), RpcError> {
        use rrrpc::relay::control_ring::ControlRingConsumer;
        use rrrpc::relay::protocol::{ControlRingHeader, ControlSlot};
        use rrrpc::relay::shm::create_control_ring_shm;

        let (fd, base, size) = create_control_ring_shm(shm_name, capacity).map_err(|e| {
            RpcError::ConnectionError(format!(
                "create_control_ring_shm({shm_name}, {capacity}): {e}"
            ))
        })?;

        // The control_ring layout is `header | slots[capacity]` and the
        // ControlRingConsumer takes raw pointers to those — see
        // `rrrpc::relay::control_ring::ControlRingConsumer::new`.
        let header_ptr = base as *const ControlRingHeader;
        let slots_ptr = unsafe {
            base.add(std::mem::size_of::<ControlRingHeader>()) as *mut ControlSlot
        };
        let consumer = unsafe { ControlRingConsumer::new(header_ptr, slots_ptr) };

        let mut inner = self.inner.borrow_mut();
        inner
            .daemon
            .as_mut()
            .expect("expose_daemon_control_ring requires embedded daemon (not shared-client mode)")
            .control_ring = Some(consumer);
        tracing::info!(
            "locusta: exposed daemon control_ring at {} (capacity={})",
            shm_name,
            capacity
        );
        Ok((fd, base, size))
    }

    /// Async future that polls `LocustaInner` until the response for
    /// `completion_id` is available, then yields it.
    ///
    /// Includes a per-RPC wall-clock timeout (config
    /// `locusta.rpc_wait_timeout_secs`, default 120 s). When a
    /// peer's RC QP wedges (host went DOWN mid-run, mlx5 returns
    /// `IBV_WC_RETRY_EXC_ERR` but the recovery path is unimplemented
    /// in `lib/locusta/mlx5/src/qp/mod.rs:983`), this prevents the
    /// future from hanging until PBS walltime — instead, the call
    /// returns `RpcError::TransportError("locusta wait timeout …")`
    /// so the upper layer (IOR / mdtest) sees the failure on this
    /// file and can move on.
    ///
    /// `dest_node` is captured for diagnostics so the timeout log
    /// identifies which peer was unresponsive.
    async fn wait_for(
        &self,
        completion_id: u64,
        dest_node: NodeId,
    ) -> Result<Response, RpcError> {
        let timeout_secs = crate::runtime_config::RuntimeConfig::global()
            .locusta
            .rpc_wait_timeout_secs;
        let deadline = if timeout_secs == 0 {
            None
        } else {
            Some(std::time::Instant::now() + Duration::from_secs(timeout_secs))
        };
        WaitForResponse {
            inner: Rc::clone(&self.inner),
            completion_id,
            dest_node,
            deadline,
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
        let response = self.wait_for(completion_id, dest.clone()).await?;
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
        let response = self.wait_for(completion_id, dest.clone()).await?;
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
    /// Peer this RPC targets — only used in the timeout diagnostic
    /// so the log line identifies which dead host caused the wedge.
    dest_node: NodeId,
    /// `Instant` at which `poll` should bail out with a timeout
    /// error. `None` means timeout is disabled (legacy hang-forever
    /// behaviour). Set by `LocustaTransport::wait_for` from
    /// `locusta.rpc_wait_timeout_secs`.
    deadline: Option<std::time::Instant>,
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
        // Per-RPC wall-clock timeout. When a peer's RC QP has wedged
        // (mlx5 reports `IBV_WC_RETRY_EXC_ERR` server-side but the
        // client's local QP is healthy and just waiting for a reply
        // that will never arrive — see `lib/locusta/mlx5/src/qp/
        // mod.rs:977-984`), the in-flight `ResponseFuture` would
        // otherwise hang until PBS walltime. Drop the entry from
        // `inflight` (the underlying locusta slot leaks until process
        // exit, but iter154's hang showed this is preferable to
        // wedging every subsequent RPC), then return a transport
        // error so the upper layer (IOR / mdtest) can fail-fast on
        // this file and continue with the next one.
        if let Some(deadline) = self.deadline
            && std::time::Instant::now() >= deadline
        {
            let cid = self.completion_id;
            inner.inflight.remove(&cid);
            tracing::warn!(
                target: "rpc_wait_timeout",
                peer = %self.dest_node,
                completion_id = cid,
                "locusta wait_for timed out — abandoning RPC (peer likely dead)"
            );
            return std::task::Poll::Ready(Err(RpcError::TransportError(format!(
                "locusta wait timeout for peer {}",
                self.dest_node
            ))));
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
    let ready = {
        let Some(server) = inner.server.as_mut() else {
            // Shared-client mode has no in-process server; nothing to drain.
            return;
        };
        server.poll();
        server.drain_ready()
    };
    for req in ready {
        handler(req);
    }
    inner.server.as_mut().unwrap().flush_all();
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
            let response = self.wait_for(completion_id, dest.clone()).await?;
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
            let response = self.wait_for(completion_id, dest.clone()).await?;
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
            let response = self.wait_for(completion_id, dest.clone()).await?;
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
