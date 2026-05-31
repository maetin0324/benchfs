//! UCX standalone-daemon mode for BenchFS.
//!
//! Mirrors locusta's standalone-daemon architecture but uses UCX AM as
//! the daemon-to-daemon transport. IOR client processes attach to this
//! daemon's POSIX SHM control ring using `rrrpc::relay::client::Client`
//! (= locusta wire format), so on the client side the existing
//! `LocustaTransport::init_shared_client` path is reused unchanged —
//! the SHM ring contents simply happen to be forwarded by UCX instead
//! of locusta's mlx5 RDMA path.
//!
//! Wire layout from the client's perspective:
//!
//! 1. Client serializes an `AmRpc` request as `[rpc_id u16 LE][header bytes][inline data bytes]`
//!    and writes a `RequestSlot { dest_id, flags, small_req_* }` to the
//!    channel's SHM req_ring. For Put/Get patterns the bulk payload
//!    lives in the channel arena, referenced by `dma_off/dma_len`.
//! 2. Daemon pops the slot, looks up the target peer by `dest_id`,
//!    extracts `(rpc_id, amrpc_header_bytes, amrpc_data_bytes)`, and
//!    issues `endpoint.am_send_vectorized(rpc_id, header, data,
//!    need_reply=true)` to the peer daemon's UCX worker.
//! 3. Peer daemon's existing `RpcServer::listen::<T>` task receives the
//!    AM in `am_stream(rpc_id)` and runs the registered
//!    `T::server_handler` exactly as it would for a directly-connected
//!    UCX client. The handler replies via `am_msg.reply(...)`.
//! 4. Originating daemon's reply listener pops the AM reply from
//!    `am_stream(reply_stream_id = rpc_id + 100)`, copies the response
//!    header + data into the client's SHM arena (or inline), and pushes
//!    a `CompletionSlot` onto the channel's cq_ring.

use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::io::IoSlice;
use std::rc::Rc;

use rrrpc::relay::control_ring::ControlRingConsumer;
use rrrpc::relay::protocol::{ConnectPayload, ControlMessage, ControlMsgType, ControlRingHeader, ControlSlot};
use rrrpc::relay::shm::{
    ChannelHeader, ChannelState, ShmLayout, create_control_ring_shm, mmap_channel,
    open_channel_shm, validate_channel_header,
};
use rrrpc::relay::spsc::FastForwardRing;
use rrrpc::wire::{CompletionRespKind, CompletionSlot, RequestFlags, RequestSlot};

use pluvio_ucx::async_ucx::ucp::AmMsg;

use crate::rpc::connection::ConnectionPool;
use crate::rpc::transport::NodeId;

/// Static table: rpc_id → size_of::<RequestHeader>().
pub fn request_header_size(rpc_id: u16) -> Option<usize> {
    use std::mem::size_of;
    match rpc_id {
        12 => Some(size_of::<crate::rpc::data_ops::ReadChunkByIdRequestHeader>()),
        13 => Some(size_of::<crate::rpc::data_ops::WriteChunkByIdRequestHeader>()),
        14 => Some(size_of::<crate::rpc::data_ops::FsyncChunkRequestHeader>()),
        20 => Some(size_of::<crate::rpc::metadata_ops::MetadataLookupRequestHeader>()),
        21 => Some(size_of::<crate::rpc::metadata_ops::MetadataCreateFileRequestHeader>()),
        22 => Some(size_of::<crate::rpc::metadata_ops::MetadataCreateDirRequestHeader>()),
        23 => Some(size_of::<crate::rpc::metadata_ops::MetadataDeleteRequestHeader>()),
        24 => Some(size_of::<crate::rpc::metadata_ops::MetadataUpdateRequestHeader>()),
        25 => Some(size_of::<crate::rpc::metadata_ops::ShutdownRequestHeader>()),
        26 => Some(size_of::<crate::rpc::readdir_ops::ReaddirRequestHeader>()),
        27 => Some(size_of::<crate::rpc::dir_index_ops::DirIndexUpdateRequestHeader>()),
        30 => Some(size_of::<crate::rpc::bench_ops::BenchPingRequestHeader>()),
        32 => Some(size_of::<crate::rpc::bench_ops::BenchShutdownRequestHeader>()),
        _ => None,
    }
}

struct UcxChannel {
    cq_ring: FastForwardRing<CompletionSlot>,
    req_ring: FastForwardRing<RequestSlot>,
    arena_base: *mut u8,
}

unsafe impl Send for UcxChannel {}

struct UcxPending {
    client_req_id: u16,
    channel_id: u32,
    small_res_off: u32,
    small_res_cap: u32,
}

pub struct UcxRelayDaemon {
    inner: RefCell<Inner>,
    pub node_to_dest: HashMap<NodeId, u16>,
    pub dest_to_node: HashMap<u16, NodeId>,
    pub self_node_id: NodeId,
    pub ucx_pool: Rc<ConnectionPool>,
}

struct Inner {
    control: Option<ControlRingConsumer>,
    channels: slab::Slab<UcxChannel>,
    pending: slab::Slab<UcxPending>,
    /// Per-target FIFO of (relay_req_id, slot, arena_ptr). Forwards on
    /// the same target are serialized so concurrent `am_stream(rpc_id+100)`
    /// listeners can't cross-route each other's replies. Each target
    /// runs one in-flight forward at a time; the per-target task drains
    /// the queue.
    pending_per_target: HashMap<NodeId, VecDeque<(u16, RequestSlot, usize)>>,
    /// Targets that already have a worker task spawned. The worker
    /// runs until its queue is empty, then sets the flag back to false.
    target_busy: HashMap<NodeId, bool>,
}

impl UcxRelayDaemon {
    pub fn new(
        self_node_id: NodeId,
        peer_node_ids: Vec<NodeId>,
        ucx_pool: Rc<ConnectionPool>,
    ) -> Self {
        let mut node_to_dest: HashMap<NodeId, u16> = HashMap::new();
        let mut dest_to_node: HashMap<u16, NodeId> = HashMap::new();
        let mut next_dest: u16 = 0;
        for peer in &peer_node_ids {
            if peer == &self_node_id {
                continue;
            }
            node_to_dest.insert(peer.clone(), next_dest);
            dest_to_node.insert(next_dest, peer.clone());
            next_dest += 1;
        }
        // Locusta-style self-loopback. Clients dispatching RPCs whose
        // target happens to be this daemon's own node_id must still
        // find an entry in the manifest; `forward_one` then resolves
        // the dest back to `self_node_id` and routes via UCX's `self`
        // transport (or the published worker address).
        node_to_dest.insert(self_node_id.clone(), next_dest);
        dest_to_node.insert(next_dest, self_node_id.clone());
        Self {
            inner: RefCell::new(Inner {
                control: None,
                channels: slab::Slab::new(),
                pending: slab::Slab::new(),
                pending_per_target: HashMap::new(),
                target_busy: HashMap::new(),
            }),
            node_to_dest,
            dest_to_node,
            self_node_id,
            ucx_pool,
        }
    }

    pub fn expose_control_ring(&self, shm_name: &str, capacity: u32) -> std::io::Result<()> {
        let (fd, base, _size) = create_control_ring_shm(shm_name, capacity)?;
        let header_ptr = base as *const ControlRingHeader;
        let slots_ptr = unsafe {
            base.add(std::mem::size_of::<ControlRingHeader>()) as *mut ControlSlot
        };
        let consumer = unsafe { ControlRingConsumer::new(header_ptr, slots_ptr) };
        self.inner.borrow_mut().control = Some(consumer);
        // Leak fd so the SHM stays mapped for the daemon's lifetime.
        std::mem::forget(fd);
        tracing::info!(
            "ucx_relay: exposed control_ring at {} (capacity={})",
            shm_name,
            capacity
        );
        Ok(())
    }

    pub fn write_manifest(
        &self,
        registry_dir: &std::path::Path,
        shm_name: &str,
    ) -> std::io::Result<()> {
        let mut manifest_dir = registry_dir.to_path_buf();
        manifest_dir.push("standalone_daemon");
        std::fs::create_dir_all(&manifest_dir)?;
        let shm_base = shm_name.trim_start_matches('/');
        let manifest_path = manifest_dir.join(format!("{shm_base}.peers"));
        let mut entries: Vec<(NodeId, u16)> = self
            .node_to_dest
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect();
        entries.sort_by_key(|(_, d)| *d);
        let mut body = String::new();
        for (peer, dest_id) in &entries {
            body.push_str(&format!("{peer}\t{dest_id}\n"));
        }
        body.push_str(&format!("OWNER\t{}\n", self.self_node_id));
        std::fs::write(&manifest_path, &body)?;
        eprintln!(
            "[ucx_relay] wrote manifest at path={} self={}\nbody=\n{}",
            manifest_path.display(),
            self.self_node_id,
            body
        );
        tracing::info!(
            "ucx_relay: wrote manifest {} ({} peer entries)",
            manifest_path.display(),
            entries.len()
        );
        Ok(())
    }

    pub fn poll_control_ring(&self) {
        // Drain control ring into a Vec first so we can release the
        // borrow on `inner.control` before re-borrowing inner mutably
        // in handle_connect_inner.
        let msgs: Vec<ControlMessage> = {
            let inner = self.inner.borrow();
            let consumer = match inner.control.as_ref() {
                Some(c) => c,
                None => return,
            };
            let mut v = Vec::new();
            while let Some(msg) = consumer.try_recv() {
                v.push(msg);
            }
            v
        };
        let mut inner = self.inner.borrow_mut();
        for msg in msgs {
            match ControlMsgType::from_u8(msg.msg_type) {
                Some(ControlMsgType::Connect) => {
                    let payload: &ConnectPayload = msg.as_connect();
                    if let Err(e) = Self::handle_connect_inner(&mut inner, payload) {
                        tracing::warn!("ucx_relay: handle_connect failed: {:?}", e);
                    }
                }
                Some(ControlMsgType::Disconnect) => {}
                None => {}
            }
        }
    }

    fn handle_connect_inner(inner: &mut Inner, payload: &ConnectPayload) -> std::io::Result<()> {
        use std::os::fd::AsFd;
        let name = payload.channel_name();
        let ring_capacity = payload.ring_capacity;
        let arena_size = payload.arena_size;
        let layout = ShmLayout::compute(ring_capacity, arena_size);

        let fd = open_channel_shm(name)?;
        let base = unsafe { mmap_channel(fd.as_fd(), layout.total_size) }?;
        unsafe {
            validate_channel_header(base).map_err(|e| {
                let _ = nix::sys::mman::munmap(
                    std::ptr::NonNull::new(base as *mut _).unwrap(),
                    layout.total_size,
                );
                std::io::Error::new(std::io::ErrorKind::InvalidData, e)
            })?;
        }

        let header_ptr = base as *mut ChannelHeader;
        let cq_ring = unsafe {
            FastForwardRing::new(
                base.add(layout.cq_ring_off) as *mut CompletionSlot,
                ring_capacity,
                &raw mut (*header_ptr).cq_head,
                &raw mut (*header_ptr).cq_tail,
            )
        };
        let req_ring = unsafe {
            FastForwardRing::new(
                base.add(layout.req_ring_off) as *mut RequestSlot,
                ring_capacity,
                &raw mut (*header_ptr).req_head,
                &raw mut (*header_ptr).req_tail,
            )
        };
        let arena_base = unsafe { base.add(layout.dma_arena_off) };

        let key = inner.channels.insert(UcxChannel {
            cq_ring,
            req_ring,
            arena_base,
        });
        std::mem::forget(fd);

        let header = unsafe { &mut *header_ptr };
        header.channel_id = key as u32;
        header.mr_addr = 0;
        header.mr_rkey = 0;
        header.set_state(ChannelState::Active);
        Ok(())
    }

    pub fn drain_req_rings_and_forward(self: &Rc<Self>) {
        // Collect new jobs and bucket them into per-target queues.
        // Spawn at most one worker per target so reply demux on the
        // shared `am_stream(rpc_id+100)` listener is unambiguous.
        let mut newly_busy: Vec<NodeId> = Vec::new();
        {
            let mut inner = self.inner.borrow_mut();
            let channel_keys: Vec<usize> = inner.channels.iter().map(|(k, _)| k).collect();
            for k in channel_keys {
                let mut budget = 256usize;
                while budget > 0 {
                    let slot_opt = inner
                        .channels
                        .get(k)
                        .and_then(|c| c.req_ring.try_pop());
                    let slot = match slot_opt {
                        Some(s) => s,
                        None => break,
                    };
                    let target_node_id = match self.dest_to_node.get(&slot.dest_id).cloned() {
                        Some(n) => n,
                        None => {
                            tracing::error!(
                                "ucx_relay: unknown dest_id={} dropping",
                                slot.dest_id,
                            );
                            continue;
                        }
                    };
                    let arena_base = inner.channels[k].arena_base as usize;
                    let pending_key = inner.pending.insert(UcxPending {
                        client_req_id: slot.req_id,
                        channel_id: k as u32,
                        small_res_off: slot.small_res_off,
                        small_res_cap: slot.small_res_cap,
                    });
                    let relay_req_id = pending_key as u16;
                    inner
                        .pending_per_target
                        .entry(target_node_id.clone())
                        .or_default()
                        .push_back((relay_req_id, slot, arena_base));
                    let busy = inner.target_busy.entry(target_node_id.clone()).or_insert(false);
                    if !*busy {
                        *busy = true;
                        newly_busy.push(target_node_id);
                    }
                    budget -= 1;
                }
            }
        }
        for target in newly_busy {
            let daemon = Rc::clone(self);
            pluvio_runtime::spawn(async move {
                daemon.drain_target_queue(target).await;
            });
        }
    }

    async fn drain_target_queue(self: Rc<Self>, target: NodeId) {
        loop {
            let next = {
                let mut inner = self.inner.borrow_mut();
                let queue = inner.pending_per_target.get_mut(&target);
                match queue.and_then(|q| q.pop_front()) {
                    Some(job) => Some(job),
                    None => {
                        if let Some(b) = inner.target_busy.get_mut(&target) {
                            *b = false;
                        }
                        None
                    }
                }
            };
            let (relay_req_id, slot, arena_base) = match next {
                Some(j) => j,
                None => return,
            };
            let arena_ptr = arena_base as *const u8;
            self.forward_one(relay_req_id, slot, arena_ptr, &target).await;
        }
    }

    async fn forward_one(
        &self,
        relay_req_id: u16,
        slot: RequestSlot,
        arena_ptr: *const u8,
        target_node_id: &NodeId,
    ) {
        let small_req_len = slot.small_req_len as usize;
        if small_req_len < 2 {
            self.complete_with_status(relay_req_id, 0xfe, &[]);
            return;
        }

        let flags = RequestFlags::from_bits_truncate(slot.flags);
        let small_req_slice: Vec<u8> = if flags.contains(RequestFlags::INLINE_SMALL_REQ) {
            slot.small_req_inline[..small_req_len].to_vec()
        } else {
            unsafe {
                std::slice::from_raw_parts(
                    arena_ptr.add(slot.small_req_off as usize),
                    small_req_len,
                )
            }
            .to_vec()
        };
        let rpc_id = u16::from_le_bytes([small_req_slice[0], small_req_slice[1]]);
        let header_size = match request_header_size(rpc_id) {
            Some(s) => s,
            None => {
                tracing::error!("ucx_relay: no header_size for rpc_id={}", rpc_id);
                self.complete_with_status(relay_req_id, 0xfd, &[]);
                return;
            }
        };
        if small_req_slice.len() < 2 + header_size {
            self.complete_with_status(relay_req_id, 0xfd, &[]);
            return;
        }
        let header_bytes = small_req_slice[2..2 + header_size].to_vec();
        let inline_data = small_req_slice[2 + header_size..].to_vec();

        let dma_data: Vec<u8> = if flags.contains(RequestFlags::HAS_DMA_WRITE) && slot.dma_len > 0 {
            unsafe {
                std::slice::from_raw_parts(
                    arena_ptr.add(slot.dma_off as usize),
                    slot.dma_len as usize,
                )
            }
            .to_vec()
        } else {
            Vec::new()
        };
        let combined_data: Vec<u8> = if dma_data.is_empty() {
            inline_data
        } else if inline_data.is_empty() {
            dma_data
        } else {
            let mut v = Vec::with_capacity(inline_data.len() + dma_data.len());
            v.extend_from_slice(&inline_data);
            v.extend_from_slice(&dma_data);
            v
        };

        let client_rc = match self.ucx_pool.get_or_connect(target_node_id).await {
            Ok(c) => c,
            Err(e) => {
                tracing::error!(
                    "ucx_relay: get_or_connect({}) failed: {:?}",
                    target_node_id,
                    e
                );
                self.complete_with_status(relay_req_id, 0xfc, &[]);
                return;
            }
        };
        let conn = match client_rc.connection() {
            Some(c) => c,
            None => {
                tracing::error!("ucx_relay: client has no Connection (locusta-only mode?)");
                self.complete_with_status(relay_req_id, 0xfb, &[]);
                return;
            }
        };
        let reply_stream_id = rpc_id + 100;
        let reply_stream = match conn.worker().am_stream(reply_stream_id) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(
                    "ucx_relay: am_stream({}) failed: {:?}",
                    reply_stream_id,
                    e
                );
                self.complete_with_status(relay_req_id, 0xfa, &[]);
                return;
            }
        };
        let data_iov = [IoSlice::new(&combined_data)];
        let send_res = conn
            .endpoint()
            .am_send_vectorized(rpc_id as u32, &header_bytes, &data_iov, true, None)
            .await;
        if let Err(e) = send_res {
            tracing::error!(
                "ucx_relay: am_send rpc_id={} relay_req_id={} failed: {:?}",
                rpc_id,
                relay_req_id,
                e
            );
            self.complete_with_status(relay_req_id, 0xf9, &[]);
            return;
        }
        let msg = match reply_stream.wait_msg().await {
            Some(m) => m,
            None => {
                tracing::error!(
                    "ucx_relay: wait_msg(reply_stream={}) for relay_req_id={} returned None",
                    reply_stream_id,
                    relay_req_id
                );
                self.complete_with_status(relay_req_id, 0xf8, &[]);
                return;
            }
        };
        self.handle_reply(relay_req_id, msg).await;
    }

    async fn handle_reply(&self, relay_req_id: u16, mut msg: AmMsg) {
        let header_bytes = msg.header().to_vec();
        let data_bytes = match msg.recv_data().await {
            Ok(v) => v,
            Err(e) => {
                tracing::error!(
                    "ucx_relay: recv_data() for relay_req_id={} failed: {:?}",
                    relay_req_id,
                    e
                );
                self.complete_with_status(relay_req_id, 0xf7, &[]);
                return;
            }
        };
        let mut combined = header_bytes;
        combined.extend_from_slice(&data_bytes);
        self.complete_with_status(relay_req_id, 0, &combined);
    }

    fn complete_with_status(&self, relay_req_id: u16, status: u8, resp_payload: &[u8]) {
        let mut inner = self.inner.borrow_mut();
        let pending = match inner.pending.try_remove(relay_req_id as usize) {
            Some(p) => p,
            None => {
                tracing::warn!("ucx_relay: pending {} not found", relay_req_id);
                return;
            }
        };
        let channel = match inner.channels.get(pending.channel_id as usize) {
            Some(c) => c,
            None => return,
        };
        let resp_len = resp_payload.len() as u32;
        let completion = if resp_len <= 52 {
            let mut inline = [0u8; 52];
            let copy = (resp_len as usize).min(52);
            inline[..copy].copy_from_slice(&resp_payload[..copy]);
            CompletionSlot {
                req_id: pending.client_req_id,
                transport_status: status,
                resp_kind: CompletionRespKind::InlineSmallRes as u8,
                small_res_len: resp_len,
                small_res_off: pending.small_res_off,
                small_res_inline: inline,
            }
        } else {
            let copy_len = resp_len.min(pending.small_res_cap) as usize;
            unsafe {
                std::ptr::copy_nonoverlapping(
                    resp_payload.as_ptr(),
                    channel.arena_base.add(pending.small_res_off as usize),
                    copy_len,
                );
            }
            CompletionSlot {
                req_id: pending.client_req_id,
                transport_status: status,
                resp_kind: CompletionRespKind::BufferedSmallRes as u8,
                small_res_len: resp_len,
                small_res_off: pending.small_res_off,
                small_res_inline: [0u8; 52],
            }
        };
        if channel.cq_ring.try_push(completion).is_err() {
            tracing::error!(
                "ucx_relay: cq_ring full for channel {}; req_id={} will time out",
                pending.channel_id,
                pending.client_req_id
            );
        }
    }
}
