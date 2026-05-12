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

use std::alloc::{alloc, Layout};
use std::cell::RefCell;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use mlx5::pd::AccessFlags;
use rrrpc::arena::DmaArena;
use rrrpc::rdma_util::RecvBufferPool;
use rrrpc::relay::client::{Client, Response, ResponseFuture};
use rrrpc::relay::daemon::{Channel, RdmaContext, RelayConfig, RelayDaemon};
use rrrpc::relay::jiffy::JiffyQueue;
use rrrpc::relay::shm::{init_channel_header, ChannelHeader, ChannelState, ShmLayout};
use rrrpc::relay::spsc::FastForwardRing;
use rrrpc::server::{RdmaBuffer, Request, ServerConfig, ServerContext, ServerRdmaContext};
use rrrpc::wire::{CompletionSlot, QpExchangeInfo, RequestSlot};

use mlx5::pd::MemoryRegion;
use pluvio_uring::allocator::FixedBufferAllocator;

use crate::rpc::locusta_buffer::{register_with_pd, RegisteredFixedBuffer};
use crate::rpc::transport::{NodeId, RpcResponse, RpcTransport};
use crate::rpc::RpcError;

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
    /// Phase 2.1 default: 4 slots × 4 MiB each.
    pub server_buf_slots: usize,
    /// Per-slot size for the server-side buffer pool. Set ≥ the largest
    /// expected chunk (4 MiB for BenchFS).
    pub server_buf_size: usize,
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
        }
    }
}

fn open_mlx5_device() -> Result<mlx5::device::Context, RpcError> {
    let device_list = mlx5::device::DeviceList::list().map_err(|e| {
        RpcError::ConnectionError(format!("mlx5::device::DeviceList::list: {e:?}"))
    })?;
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

fn read_qp_info_when_ready(path: &Path, deadline: Instant) -> Result<QpExchangeInfo, RpcError> {
    loop {
        if let Ok(bytes) = fs::read(path) {
            if bytes.len() == QP_INFO_SIZE {
                let mut info = std::mem::MaybeUninit::<QpExchangeInfo>::uninit();
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        bytes.as_ptr(),
                        info.as_mut_ptr() as *mut u8,
                        QP_INFO_SIZE,
                    );
                    return Ok(info.assume_init());
                }
            }
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
    registry.join(format!("server_{server_node}_to_relay_{client_node}.qpinfo"))
}

fn relay_qp_path(registry: &Path, client_node: &str, server_node: &str) -> PathBuf {
    registry.join(format!("relay_{client_node}_to_server_{server_node}.qpinfo"))
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
            RpcError::ConnectionError(format!(
                "mkdir {}: {e}",
                cfg.registry_dir.display()
            ))
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
                    AccessFlags::LOCAL_WRITE
                        | AccessFlags::REMOTE_WRITE
                        | AccessFlags::REMOTE_READ,
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

        // ------ Server-side RDMA buffer pool (Phase 2.1) ------
        //
        // Allocates pluvio FixedBuffers without io_uring registration, then
        // registers them with the server's PD. The same pluvio handle type
        // can later be threaded through io_uring once the chunk-store
        // integration (Phase 2.2) lands.
        let (server_buffer_allocator, server_buffer_mrs) = if cfg.server_buf_slots > 0 {
            let pd = &server.rdma.as_ref().unwrap().pd;
            let alloc = FixedBufferAllocator::new_without_uring(
                cfg.server_buf_slots,
                cfg.server_buf_size,
            );
            let mrs = register_with_pd(&alloc, pd)?;
            (Some(alloc), mrs)
        } else {
            (None, Vec::new())
        };

        let qp_config = mlx5::qp::RcQpConfig {
            max_send_wr: 1024,
            ..mlx5::qp::RcQpConfig::default()
        };

        // ------ Per-peer QP exchange (sequential; each peer gets a dest_id) ------
        let deadline = Instant::now() + Duration::from_secs(cfg.exchange_timeout_secs);
        let mut node_to_dest = HashMap::new();
        for (idx, peer) in cfg.peer_node_ids.iter().enumerate() {
            let dest_id = idx as u16;
            // Server side: prepare peer for the remote relay
            let server_local = server
                .prepare_peer(dest_id, 64 * 1024, 64 * 1024, 512, 4096, &qp_config)
                .map_err(|e| {
                    RpcError::ConnectionError(format!("server.prepare_peer({dest_id}): {e:?}"))
                })?;
            let server_out =
                server_qp_path(&cfg.registry_dir, &cfg.local_node_id, peer);
            write_qp_info(&server_out, &server_local).map_err(|e| {
                RpcError::ConnectionError(format!(
                    "write_qp_info({}): {e}",
                    server_out.display()
                ))
            })?;

            // Relay side: prepare destination for the remote server
            let relay_local = daemon
                .prepare_destination(dest_id, 64 * 1024, 64 * 1024, &qp_config)
                .map_err(|e| {
                    RpcError::ConnectionError(format!(
                        "daemon.prepare_destination({dest_id}): {e:?}"
                    ))
                })?;
            let relay_out =
                relay_qp_path(&cfg.registry_dir, &cfg.local_node_id, peer);
            write_qp_info(&relay_out, &relay_local).map_err(|e| {
                RpcError::ConnectionError(format!(
                    "write_qp_info({}): {e}",
                    relay_out.display()
                ))
            })?;

            // Read peer's published info and finalize connections.
            let peer_relay_path = relay_qp_path(&cfg.registry_dir, peer, &cfg.local_node_id);
            let peer_relay_info = read_qp_info_when_ready(&peer_relay_path, deadline)?;
            server.connect_peer(dest_id, &peer_relay_info).map_err(|e| {
                RpcError::ConnectionError(format!(
                    "server.connect_peer({dest_id}): {e:?}"
                ))
            })?;

            let peer_server_path = server_qp_path(&cfg.registry_dir, peer, &cfg.local_node_id);
            let peer_server_info = read_qp_info_when_ready(&peer_server_path, deadline)?;
            daemon
                .connect_destination(dest_id, &peer_server_info)
                .map_err(|e| {
                    RpcError::ConnectionError(format!(
                        "daemon.connect_destination({dest_id}): {e:?}"
                    ))
                })?;

            node_to_dest.insert(peer.clone(), dest_id);
        }

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
            node_to_dest,
            channel_base_ptr: shm_base,
            channel_layout_total: layout.total_size,
            inflight: HashMap::new(),
        };
        Ok(Self {
            inner: Rc::new(RefCell::new(inner)),
        })
    }

    fn lookup_dest(&self, dest: &NodeId) -> Result<u16, RpcError> {
        self.inner
            .borrow()
            .node_to_dest
            .get(dest)
            .copied()
            .ok_or_else(|| RpcError::ConnectionError(format!("unknown peer node: {dest}")))
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
/// invoke a user-provided handler. Phase 2 will replace this with the
/// AmRpc dispatch table.
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

impl RpcTransport for LocustaTransport {
    fn send_eager<'a>(
        &'a self,
        dest: &'a NodeId,
        _rpc_id: u16,
        header: &'a [u8],
    ) -> impl std::future::Future<Output = Result<RpcResponse, RpcError>> + 'a {
        async move {
            let dest_id = self.lookup_dest(dest)?;
            let completion_id = {
                let mut inner = self.inner.borrow_mut();
                let (id, fut) = {
                    let mut batch = inner.client.batch();
                    let fut = batch.call_eager(dest_id, header).map_err(|e| {
                        RpcError::TransportError(format!("call_eager: {e:?}"))
                    })?;
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
        _rpc_id: u16,
        header: &'a [u8],
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
                let (id, fut) = {
                    let mut batch = inner.client.batch();
                    let fut = batch
                        .call_put(dest_id, header, &dma_buf)
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
        _rpc_id: u16,
        header: &'a [u8],
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
                let (id, fut) = {
                    let mut batch = inner.client.batch();
                    let fut = batch
                        .call_get(dest_id, header, &dma_buf)
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
        _rpc_id: u16,
        header: &'a [u8],
    ) -> impl std::future::Future<Output = Result<(), RpcError>> + 'a {
        async move {
            let dest_id = self.lookup_dest(dest)?;
            let mut inner = self.inner.borrow_mut();
            inner.client.send_oneway_eager(dest_id, header).map_err(|e| {
                RpcError::TransportError(format!("send_oneway_eager: {e:?}"))
            })?;
            Ok(())
        }
    }
}
