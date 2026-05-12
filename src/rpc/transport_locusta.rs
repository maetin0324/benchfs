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
use std::path::{Path, PathBuf};
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

const QP_INFO_SIZE: usize = std::mem::size_of::<QpExchangeInfo>();
const _: () = assert!(QP_INFO_SIZE == 64);

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
}

impl LocustaInner {
    /// Pump the polling state machines. Called both from the async wait
    /// futures and (in Phase 1c) from a registered Reactor.
    pub fn tick(&mut self) {
        self.daemon.poll_client_requests();
        self.daemon.process_pending_dma_writes();
        self.daemon.flush_all_destinations();
        self.daemon.poll_server_completions();
        self.client.poll();
        self.server.poll();
    }

    /// Run the full 4-step QP handshake against `peer` and register the
    /// resulting dest_id under `peer` in `node_to_dest`. Blocks the
    /// calling thread (with 50ms sleeps) until both of the peer's QP
    /// info files appear, or `deadline` is reached.
    ///
    /// Used by `LocustaTransport::init` for each pre-configured peer
    /// and by `LocustaTransport::add_peer` at runtime. The protocol is
    /// symmetric — whichever side runs first writes its files and
    /// spin-reads for the other side; whichever side runs second sees
    /// the files immediately and returns quickly.
    fn add_peer_blocking(&mut self, peer: &str, deadline: Instant) -> Result<u16, RpcError> {
        if let Some(&existing) = self.node_to_dest.get(peer) {
            return Ok(existing);
        }
        let dest_id = self.next_dest_id;

        // Server side: prepare peer for the remote relay
        let server_local = self
            .server
            .prepare_peer(dest_id, 64 * 1024, 64 * 1024, 512, 4096, &self.qp_config)
            .map_err(|e| {
                RpcError::ConnectionError(format!("server.prepare_peer({dest_id}): {e:?}"))
            })?;
        let server_out = server_qp_path(&self.registry_dir, &self.local_node_id, peer);
        write_qp_info(&server_out, &server_local).map_err(|e| {
            RpcError::ConnectionError(format!("write_qp_info({}): {e}", server_out.display()))
        })?;

        // Relay side: prepare destination for the remote server
        let relay_local = self
            .daemon
            .prepare_destination(dest_id, 64 * 1024, 64 * 1024, &self.qp_config)
            .map_err(|e| {
                RpcError::ConnectionError(format!("daemon.prepare_destination({dest_id}): {e:?}"))
            })?;
        let relay_out = relay_qp_path(&self.registry_dir, &self.local_node_id, peer);
        write_qp_info(&relay_out, &relay_local).map_err(|e| {
            RpcError::ConnectionError(format!("write_qp_info({}): {e}", relay_out.display()))
        })?;

        // Wait for peer's published info and finalize connections.
        let peer_relay_path = relay_qp_path(&self.registry_dir, peer, &self.local_node_id);
        let peer_relay_info = read_qp_info_when_ready(&peer_relay_path, deadline)?;
        self.server
            .connect_peer(dest_id, &peer_relay_info)
            .map_err(|e| {
                RpcError::ConnectionError(format!("server.connect_peer({dest_id}): {e:?}"))
            })?;

        let peer_server_path = server_qp_path(&self.registry_dir, peer, &self.local_node_id);
        let peer_server_info = read_qp_info_when_ready(&peer_server_path, deadline)?;
        self.daemon
            .connect_destination(dest_id, &peer_server_info)
            .map_err(|e| {
                RpcError::ConnectionError(format!("daemon.connect_destination({dest_id}): {e:?}"))
            })?;

        self.node_to_dest.insert(peer.to_string(), dest_id);
        self.next_dest_id = self
            .next_dest_id
            .checked_add(1)
            .ok_or_else(|| RpcError::ConnectionError("dest_id space exhausted".to_string()))?;
        Ok(dest_id)
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

fn open_mlx5_device() -> Result<mlx5::device::Context, RpcError> {
    let device_list = mlx5::device::DeviceList::list()
        .map_err(|e| RpcError::ConnectionError(format!("mlx5::device::DeviceList::list: {e:?}")))?;
    for device in device_list.iter() {
        if let Ok(ctx) = device.open() {
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

fn write_qp_info(path: &Path, info: &QpExchangeInfo) -> std::io::Result<()> {
    let tmp = path.with_extension("tmp");
    let bytes = unsafe {
        std::slice::from_raw_parts(info as *const QpExchangeInfo as *const u8, QP_INFO_SIZE)
    };
    fs::write(&tmp, bytes)?;
    fs::rename(&tmp, path)?;
    Ok(())
}

fn try_read_qp_info(path: &Path) -> Option<QpExchangeInfo> {
    let bytes = fs::read(path).ok()?;
    if bytes.len() != QP_INFO_SIZE {
        return None;
    }
    let mut info = std::mem::MaybeUninit::<QpExchangeInfo>::uninit();
    unsafe {
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), info.as_mut_ptr() as *mut u8, QP_INFO_SIZE);
        Some(info.assume_init())
    }
}

fn read_qp_info_when_ready(path: &Path, deadline: Instant) -> Result<QpExchangeInfo, RpcError> {
    loop {
        if let Some(info) = try_read_qp_info(path) {
            return Ok(info);
        }
        if Instant::now() > deadline {
            return Err(RpcError::ConnectionError(format!(
                "timed out waiting for {}",
                path.display()
            )));
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn server_qp_path(registry: &Path, server_node: &str, client_node: &str) -> PathBuf {
    registry.join(format!(
        "server_{server_node}_to_relay_{client_node}.qpinfo"
    ))
}

fn relay_qp_path(registry: &Path, client_node: &str, server_node: &str) -> PathBuf {
    registry.join(format!(
        "relay_{client_node}_to_server_{server_node}.qpinfo"
    ))
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
        per_peer_timeout: Duration,
    ) -> Result<Vec<NodeId>, RpcError> {
        let (registry_dir, local_node_id, known_peers) = {
            let inner = self.inner.borrow();
            (
                inner.registry_dir.clone(),
                inner.local_node_id.clone(),
                inner.node_to_dest.keys().cloned().collect::<Vec<_>>(),
            )
        };
        // Two prefixes mark "peer X published its half pointing at us":
        //   server_<X>_to_relay_<self>.qpinfo
        //   relay_<X>_to_server_<self>.qpinfo
        // Both must exist before we attempt the handshake (so
        // read_qp_info_when_ready never has to wait).
        let server_suffix = format!("_to_relay_{local_node_id}.qpinfo");
        let relay_suffix = format!("_to_server_{local_node_id}.qpinfo");
        let mut candidates_server: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        let mut candidates_relay: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        let dir_iter = fs::read_dir(&registry_dir).map_err(|e| {
            RpcError::ConnectionError(format!("read_dir({}): {e}", registry_dir.display()))
        })?;
        for entry in dir_iter.flatten() {
            let Ok(name) = entry.file_name().into_string() else {
                continue;
            };
            if let Some(rest) = name.strip_prefix("server_")
                && let Some(peer) = rest.strip_suffix(&server_suffix)
            {
                candidates_server.insert(peer.to_string());
            } else if let Some(rest) = name.strip_prefix("relay_")
                && let Some(peer) = rest.strip_suffix(&relay_suffix)
            {
                candidates_relay.insert(peer.to_string());
            }
        }
        let mut added = Vec::new();
        let candidates_count =
            candidates_server.intersection(&candidates_relay).count();
        if candidates_count > 0 {
            tracing::debug!(
                "try_accept_pending_peers: {} server-side, {} relay-side, {} intersect; known={}",
                candidates_server.len(),
                candidates_relay.len(),
                candidates_count,
                known_peers.len()
            );
        }
        for peer in candidates_server.intersection(&candidates_relay) {
            if peer == &local_node_id {
                continue;
            }
            if known_peers.iter().any(|k| k == peer) {
                continue;
            }
            // Both publications exist — handshake should be quick. Use
            // the supplied timeout as a sanity cap.
            tracing::info!(
                "try_accept_pending_peers: handshaking new peer {}",
                peer
            );
            self.add_peer(peer, per_peer_timeout)?;
            added.push(peer.clone());
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
}

struct WaitForResponse {
    inner: Rc<RefCell<LocustaInner>>,
    completion_id: u64,
}

impl std::future::Future for WaitForResponse {
    type Output = Result<Response, RpcError>;
    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let mut inner = self.inner.borrow_mut();
        inner.tick();
        if let Some(resp) = inner.try_take(self.completion_id) {
            return std::task::Poll::Ready(Ok(resp));
        }
        // Not ready: wake immediately so the runtime re-polls us. This is
        // a busy-yield bridge — Phase 1c will replace it with a Reactor
        // that wakes a stored Waker only when locusta state changes.
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

/// Register `LocustaTransport` as a pluvio reactor: each runtime tick
/// will call `inner.tick()`. This makes the runtime see locusta as
/// "making progress" so the 1M-iteration stuck watchdog doesn't fire
/// while a long DMA RPC is in flight, and removes the need for
/// `WaitForResponse` to busy-poll inline.
impl pluvio_runtime::reactor::Reactor for LocustaTransport {
    fn poll(&self) {
        // try_borrow_mut so we don't panic if a future is currently
        // holding the inner borrow (it will tick on its own anyway).
        if let Ok(mut inner) = self.inner.try_borrow_mut() {
            inner.tick();
        }
    }
    fn status(&self) -> pluvio_runtime::reactor::ReactorStatus {
        pluvio_runtime::reactor::ReactorStatus::Running
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
            let header_bytes = match response.small_res_data() {
                rrrpc::relay::client::SmallResData::Inline { buf, len } => {
                    buf.0[..*len as usize].to_vec()
                }
                rrrpc::relay::client::SmallResData::Buffered { len, .. } => {
                    // For Phase 1 we don't expose the buffered slot's
                    // contents (would require holding the lease alive).
                    // Eager paths in BenchFS use ≤52B inline.
                    vec![0u8; *len as usize]
                }
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
            let dest_id = self.lookup_dest(dest)?;
            // Allocate a DMA buffer and stage the payload into it. The
            // arena is shared with the relay's MR so this is what
            // call_put will RDMA-write to the server.
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
            let response = self.wait_for(completion_id).await?;
            // Buffer must outlive the relay's DMA write — we held it on
            // the stack until completion, so it's safe to release now.
            drop(dma_buf);

            let header_bytes = match response.small_res_data() {
                rrrpc::relay::client::SmallResData::Inline { buf, len } => {
                    buf.0[..*len as usize].to_vec()
                }
                rrrpc::relay::client::SmallResData::Buffered { len, .. } => {
                    vec![0u8; *len as usize]
                }
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
            let response = self.wait_for(completion_id).await?;
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
            recv[..dma_len].copy_from_slice(&dma_buf.as_slice()[..dma_len]);
            drop(dma_buf);

            let header_bytes = match response.small_res_data() {
                rrrpc::relay::client::SmallResData::Inline { buf, len } => {
                    buf.0[..*len as usize].to_vec()
                }
                rrrpc::relay::client::SmallResData::Buffered { len, .. } => {
                    vec![0u8; *len as usize]
                }
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
