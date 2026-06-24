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
use std::collections::HashMap;
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
    /// 2026-06-04 zero-copy direct RDMA: client's SHM arena is registered
    /// as a UCX memory region under the daemon's context. `arena_rkey_buf`
    /// is the packed rkey we ship to peer servers so they can ucp_put
    /// chunk data directly into the client's arena, bypassing the daemon
    /// memcpy on the data path. Lifetime-tied to UcxChannel — when the
    /// channel is closed, `_arena_mh` drops and the MR is unmapped.
    #[allow(dead_code)]
    _arena_mh: Option<pluvio_ucx::async_ucx::ucp::MemoryHandle>,
    arena_rkey_buf: Vec<u8>,
}

unsafe impl Send for UcxChannel {}

struct UcxPending {
    client_req_id: u16,
    channel_id: u32,
    small_res_off: u32,
    small_res_cap: u32,
    /// Copied from the originating RequestSlot so that handle_reply
    /// can route the AM-reply data correctly: for HAS_DMA_READ the
    /// chunk bytes belong at dma_off..dma_off+dma_len and the small_res
    /// receives only the response header. (Otherwise the chunk content
    /// is truncated to small_res_cap and ior-hard-read fails its
    /// length check.)
    flags: u8,
    dma_off: u32,
    dma_len: u32,
}

pub struct UcxRelayDaemon {
    inner: RefCell<Inner>,
    pub node_to_dest: HashMap<NodeId, u16>,
    pub dest_to_node: HashMap<u16, NodeId>,
    pub self_node_id: NodeId,
    pub ucx_pool: Rc<ConnectionPool>,
    /// SHM names this daemon created (control_ring + any per-channel
    /// SHMs we opened on a Connect). `Drop` shm_unlinks them so they
    /// don't accumulate across runs — the previous "leak forever"
    /// behaviour forced the job script to do `find /dev/shm -delete`
    /// hacks instead of treating cleanup as a program responsibility.
    owned_shm_names: RefCell<Vec<String>>,
}

struct Inner {
    control: Option<ControlRingConsumer>,
    channels: slab::Slab<UcxChannel>,
    pending: slab::Slab<UcxPending>,
    /// Set of `(target_node_id, rpc_id)` for which a long-lived reply
    /// listener task has already been spawned. The listener loops
    /// `am_stream(reply_stream_id).wait_msg()` and demuxes replies via
    /// the 4-byte correlation_id prefix on each reply header
    /// (`pending` slab key). Per-rpc serialize (the old
    /// `pending_per_rpc` + `rpc_busy`) is no longer needed since the
    /// listener can correctly route concurrent replies on the shared
    /// stream by their corr_id rather than by FIFO order.
    listeners: HashMap<(NodeId, u16), ()>,
    // 2026-06-03 timing instrumentation (chase loop iter 1):
    // accumulate per-stage time on the UCX daemon reply hot path so
    // we can prove (or refute) where the 4-MB read RPC is spending
    // its 37 ms. Dump every N completions via tracing.
    stat_n: u64,
    stat_recv_data_us: u64,
    stat_memcpy_dma_us: u64,
    stat_memcpy_small_us: u64,
    stat_cq_push_us: u64,
    stat_data_bytes_total: u64,
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
                listeners: HashMap::new(),
                stat_n: 0,
                stat_recv_data_us: 0,
                stat_memcpy_dma_us: 0,
                stat_memcpy_small_us: 0,
                stat_cq_push_us: 0,
                stat_data_bytes_total: 0,
            }),
            node_to_dest,
            dest_to_node,
            self_node_id,
            ucx_pool,
            owned_shm_names: RefCell::new(Vec::new()),
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
        // Leak fd so the SHM stays mapped for the daemon's lifetime;
        // remember the name so Drop can shm_unlink it.
        std::mem::forget(fd);
        self.owned_shm_names.borrow_mut().push(shm_name.to_string());
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
        // Capture UCX context once outside the inner borrow loop. Used by
        // handle_connect_inner to register each new channel's arena as a
        // UCX MR for zero-copy direct RDMA.
        let ucx_context = self.ucx_pool.worker().ucx_context();
        let mut inner = self.inner.borrow_mut();
        for msg in msgs {
            match ControlMsgType::from_u8(msg.msg_type) {
                Some(ControlMsgType::Connect) => {
                    let payload: &ConnectPayload = msg.as_connect();
                    if let Err(e) =
                        Self::handle_connect_inner(&mut inner, payload, &ucx_context)
                    {
                        tracing::warn!("ucx_relay: handle_connect failed: {:?}", e);
                    }
                }
                Some(ControlMsgType::Disconnect) => {}
                None => {}
            }
        }
    }

    fn handle_connect_inner(
        inner: &mut Inner,
        payload: &ConnectPayload,
        ucx_context: &std::sync::Arc<pluvio_ucx::async_ucx::ucp::Context>,
    ) -> std::io::Result<()> {
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

        // 2026-06-04 Step 2: Register the client's arena region as a UCX
        // memory region under the daemon's context. The packed rkey will
        // (Step 5) be shipped to peer servers so they can ucp_put chunk
        // data directly into the client's arena, bypassing daemon memcpy.
        // For now we just register + pack so the build works; the rkey
        // is unused until the server-side handler picks it up.
        let arena_slice = unsafe {
            std::slice::from_raw_parts(arena_base, arena_size as usize)
        };
        let arena_mh =
            pluvio_ucx::async_ucx::ucp::MemoryHandle::register(ucx_context, arena_slice);
        let arena_rkey_buf: Vec<u8> = arena_mh.pack().as_ref().to_vec();
        tracing::warn!(
            target: "ucx_relay_arena_mr",
            channel_name = %name,
            arena_addr = arena_base as u64,
            arena_size = arena_size,
            rkey_buf_len = arena_rkey_buf.len(),
            "[ucx_relay_arena_mr] registered client arena for zero-copy direct RDMA"
        );

        let key = inner.channels.insert(UcxChannel {
            cq_ring,
            req_ring,
            arena_base,
            _arena_mh: Some(arena_mh),
            arena_rkey_buf,
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
        // Drain pending request slots out of every channel's SHM
        // req_ring, allocate a `pending` slab entry per slot, then spawn
        // independent `forward_one` tasks. Per-rpc serialize is gone:
        // the long-lived per-(target, rpc_id) listener spawned inside
        // `forward_one` demuxes concurrent replies by the 4-byte
        // correlation_id prefix.
        let mut forwards: Vec<(u16, RequestSlot, usize, NodeId, u16)> = Vec::new();
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
                    // Pull rpc_id from the inline small_req to key the
                    // per-rpc serialization queue.
                    let small_req_len = slot.small_req_len as usize;
                    let flags = RequestFlags::from_bits_truncate(slot.flags);
                    let rpc_id = if small_req_len >= 2 {
                        let bytes = if flags.contains(RequestFlags::INLINE_SMALL_REQ) {
                            &slot.small_req_inline[..2]
                        } else {
                            let arena_ptr = arena_base as *const u8;
                            unsafe {
                                std::slice::from_raw_parts(
                                    arena_ptr.add(slot.small_req_off as usize),
                                    2,
                                )
                            }
                        };
                        u16::from_le_bytes([bytes[0], bytes[1]])
                    } else {
                        0
                    };
                    let pending_key = inner.pending.insert(UcxPending {
                        client_req_id: slot.req_id,
                        channel_id: k as u32,
                        small_res_off: slot.small_res_off,
                        small_res_cap: slot.small_res_cap,
                        flags: slot.flags,
                        dma_off: slot.dma_off,
                        dma_len: slot.dma_len,
                    });
                    // corr_id == pending slab key. Cast to u16 is fine
                    // because slab indices stay below 65536 in practice
                    // (max_inflight at ppn=80 × 80 channels per daemon is
                    // ~6400 in-flight, far below u16 range).
                    let relay_req_id = pending_key as u16;
                    forwards.push((relay_req_id, slot, arena_base, target_node_id, rpc_id));
                    budget -= 1;
                }
            }
        }
        // Spawn one forward task per slot — no per-rpc serialize. The
        // listener task (lazily ensured per (target_node, rpc_id) inside
        // `forward_one`) demuxes concurrent replies via the corr_id
        // prefix, so multiple in-flight RPCs on the same rpc_id no
        // longer race on `am_stream(reply_stream_id).wait_msg()`.
        for (relay_req_id, slot, arena_base, target_node_id, _rpc_id) in forwards {
            let daemon = Rc::clone(self);
            pluvio_runtime::spawn(async move {
                let arena_ptr = arena_base as *const u8;
                daemon
                    .forward_one(relay_req_id, slot, arena_ptr, &target_node_id)
                    .await;
            });
        }
    }

    async fn forward_one(
        self: Rc<Self>,
        relay_req_id: u16,
        slot: RequestSlot,
        arena_ptr: *const u8,
        target_node_id: &NodeId,
    ) {
        let small_req_len = slot.small_req_len as usize;
        if small_req_len < 2 {
            self.complete_with_status(relay_req_id, 0xfe, &[], &[]);
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
                self.complete_with_status(relay_req_id, 0xfd, &[], &[]);
                return;
            }
        };
        if small_req_slice.len() < 2 + header_size {
            self.complete_with_status(relay_req_id, 0xfd, &[], &[]);
            return;
        }
        let header_bytes_raw = &small_req_slice[2..2 + header_size];
        let inline_data = small_req_slice[2 + header_size..].to_vec();

        // Prefix the AM header with the 4-byte corr_id. The server-side
        // `helpers::parse_header` skips these 4 bytes when decoding the
        // typed header, and `send_rpc_response_via_reply` echoes them
        // back on the reply so the listener can demux.
        let mut header_with_corr: Vec<u8> =
            Vec::with_capacity(crate::rpc::helpers::CORR_ID_PREFIX_LEN + header_size);
        header_with_corr.extend_from_slice(&(relay_req_id as u32).to_le_bytes());
        header_with_corr.extend_from_slice(header_bytes_raw);

        // 2026-06-04 Step 3 zero-copy direct RDMA: for READ RPCs with a
        // DMA-read target, append (rma_target_addr u64, rkey_buf_len u32,
        // rkey_buf bytes) so the server can ucp_put chunk data directly
        // into the client's SHM arena and reply with a tiny ACK instead
        // of a 4 MB Rndv. Server side detects extra suffix by checking
        // header length > corr_id+sizeof(H).
        // 2026-06-05: gate on ZERO_COPY_MIN_BYTES — ucp_put_nbx +
        // ucp_ep_rkey_unpack overhead (~10-20 µs/RPC) dominates at small
        // transfer sizes (47 KB ior-hard), so only enable for large reads
        // where the saved 4 MB Rndv/memcpy actually pays off. Default
        // 1 MiB; override via BENCHFS_UCX_ZERO_COPY_MIN.
        const ZERO_COPY_MIN_BYTES_DEFAULT: u32 = 1 << 20;
        let zero_copy_min = std::env::var("BENCHFS_UCX_ZERO_COPY_MIN")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(ZERO_COPY_MIN_BYTES_DEFAULT);
        if rpc_id == crate::rpc::data_ops::RPC_READ_CHUNK_BY_ID
            && flags.contains(RequestFlags::HAS_DMA_READ)
            && slot.dma_len >= zero_copy_min
        {
            let channel_key = {
                let inner = self.inner.borrow();
                inner
                    .pending
                    .get(relay_req_id as usize)
                    .map(|p| p.channel_id as usize)
            };
            if let Some(ckey) = channel_key {
                let inner = self.inner.borrow();
                if let Some(ch) = inner.channels.get(ckey) {
                    let rma_target_addr =
                        ch.arena_base as u64 + slot.dma_off as u64;
                    let rkey_buf = ch.arena_rkey_buf.clone();
                    let rkey_len = rkey_buf.len() as u32;
                    header_with_corr.extend_from_slice(&rma_target_addr.to_le_bytes());
                    header_with_corr.extend_from_slice(&rkey_len.to_le_bytes());
                    header_with_corr.extend_from_slice(&rkey_buf);
                }
            }
        }

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
                self.complete_with_status(relay_req_id, 0xfc, &[], &[]);
                return;
            }
        };
        let conn = match client_rc.connection() {
            Some(c) => c,
            None => {
                tracing::error!("ucx_relay: client has no Connection (locusta-only mode?)");
                self.complete_with_status(relay_req_id, 0xfb, &[], &[]);
                return;
            }
        };

        // Ensure the per-(target, rpc_id) reply listener task is running
        // before we send. After this call returns, any reply that comes
        // in on `am_stream(rpc_id + 100)` will be demuxed by corr_id and
        // dispatched to the right pending entry.
        if let Err(status) = self.ensure_listener(target_node_id, rpc_id, conn.worker()) {
            self.complete_with_status(relay_req_id, status, &[], &[]);
            return;
        }

        let data_iov = [IoSlice::new(&combined_data)];
        let send_res = conn
            .endpoint()
            .am_send_vectorized(rpc_id as u32, &header_with_corr, &data_iov, true, None)
            .await;
        if let Err(e) = send_res {
            tracing::error!(
                "ucx_relay: am_send rpc_id={} relay_req_id={} failed: {:?}",
                rpc_id,
                relay_req_id,
                e
            );
            self.complete_with_status(relay_req_id, 0xf9, &[], &[]);
            // No further action — the listener will not see a reply for
            // this corr_id, but the pending entry has already been freed
            // by complete_with_status.
        }
        // No wait_msg() here. The long-lived listener spawned by
        // `ensure_listener` consumes the reply and dispatches via
        // corr_id; this function returns as soon as the AM is on the
        // wire.
    }

    /// Spawn (or no-op if already running) a reply listener for
    /// `(target_node_id, rpc_id)`. Each listener loops
    /// `am_stream(reply_stream_id).wait_msg()`, reads the 4-byte
    /// corr_id prefix off each reply header, and calls
    /// `complete_with_status` with the matching pending slab entry.
    /// Returns Ok on success; on transport failure, returns a status
    /// byte the caller should report on the failed slot.
    fn ensure_listener(
        self: &Rc<Self>,
        target_node_id: &NodeId,
        rpc_id: u16,
        worker: &Rc<pluvio_ucx::Worker>,
    ) -> Result<(), u8> {
        let key = (target_node_id.clone(), rpc_id);
        if self.inner.borrow().listeners.contains_key(&key) {
            return Ok(());
        }
        let reply_stream_id = rpc_id + 100;
        let reply_stream = match worker.am_stream(reply_stream_id) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(
                    "ucx_relay: ensure_listener am_stream({}) failed: {:?}",
                    reply_stream_id,
                    e
                );
                return Err(0xfa);
            }
        };
        self.inner.borrow_mut().listeners.insert(key.clone(), ());
        let daemon = Rc::clone(self);
        let key_for_task = key;
        pluvio_runtime::spawn(async move {
            loop {
                let msg = match reply_stream.wait_msg().await {
                    Some(m) => m,
                    None => {
                        tracing::warn!(
                            "ucx_relay: listener({:?}) wait_msg returned None — exiting",
                            key_for_task
                        );
                        break;
                    }
                };
                // 2026-06-03 fix: spawn per-message so `recv_data()`
                // (which blocks on RDMA Rndv pull, up to 4 MiB) runs
                // concurrently across messages. Previously the
                // listener awaited recv_data inline, serializing
                // every read-RPC reply at the daemon and limiting
                // ior-easy-read to ~80 GiB/s aggregate.
                let daemon_clone = Rc::clone(&daemon);
                pluvio_runtime::spawn(async move {
                    let mut msg = msg;
                    let header_full = msg.header().to_vec();
                    let corr_id =
                        crate::rpc::helpers::read_corr_id_prefix(&header_full).unwrap_or(0);
                    let relay_req_id = corr_id as u16;
                    let t_recv = std::time::Instant::now();
                    // 2026-06-05 fix: ACK-only replies (e.g. WRITE +
                    // zero-copy READ where the server already ucp_put'd
                    // the data into the client's arena and only sends a
                    // header-only ACK) have no AM data segment. Calling
                    // recv_data() on a no-data message returns Err for
                    // some UCX transports, which used to flip the path
                    // into the `0xf7` error branch and surface as
                    // "InvalidHeader" client-side on every WRITE.
                    let data_bytes: Vec<u8> = if msg.contains_data() {
                        match msg.recv_data().await {
                            Ok(v) => v,
                            Err(e) => {
                                tracing::error!(
                                    "ucx_relay: listener recv_data() relay_req_id={} failed: {:?}",
                                    relay_req_id,
                                    e
                                );
                                let payload =
                                    &header_full[crate::rpc::helpers::CORR_ID_PREFIX_LEN..];
                                daemon_clone.complete_with_status(
                                    relay_req_id,
                                    0xf7,
                                    payload,
                                    &[],
                                );
                                return;
                            }
                        }
                    } else {
                        Vec::new()
                    };
                    let recv_us = t_recv.elapsed().as_micros() as u64;
                    let payload = &header_full[crate::rpc::helpers::CORR_ID_PREFIX_LEN..];
                    let data_len = data_bytes.len() as u64;
                    daemon_clone.complete_with_status_timed(
                        relay_req_id,
                        0,
                        payload,
                        &data_bytes,
                        recv_us,
                        data_len,
                    );
                });
            }
            daemon.inner.borrow_mut().listeners.remove(&key_for_task);
        });
        Ok(())
    }

    #[allow(dead_code)]
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
                self.complete_with_status(relay_req_id, 0xf7, &header_bytes, &[]);
                return;
            }
        };
        self.complete_with_status(relay_req_id, 0, &header_bytes, &data_bytes);
    }

    fn complete_with_status(
        &self,
        relay_req_id: u16,
        status: u8,
        header_payload: &[u8],
        data_payload: &[u8],
    ) {
        self.complete_with_status_timed(
            relay_req_id,
            status,
            header_payload,
            data_payload,
            0,
            data_payload.len() as u64,
        );
    }

    fn complete_with_status_timed(
        &self,
        relay_req_id: u16,
        status: u8,
        header_payload: &[u8],
        data_payload: &[u8],
        recv_us: u64,
        data_len: u64,
    ) {
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
        let req_flags = RequestFlags::from_bits_truncate(pending.flags);

        let t_dma = std::time::Instant::now();
        // Get-pattern (HAS_DMA_READ): chunk bytes go into the client's
        // dma_off..dma_off+dma_len arena slot; small_res carries only
        // the response header so it never overflows the small_res
        // capacity. Without this split, ior-hard-read truncates the
        // payload at small_res_cap and fails IOR's length check.
        let small_res_owned: Vec<u8>;
        let small_res_bytes: &[u8];
        let mut dma_bytes: u64 = 0;
        if req_flags.contains(RequestFlags::HAS_DMA_READ) {
            if pending.dma_len > 0 {
                let cap = pending.dma_len as usize;
                let to_copy = data_payload.len().min(cap);
                if to_copy > 0 {
                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            data_payload.as_ptr(),
                            channel.arena_base.add(pending.dma_off as usize),
                            to_copy,
                        );
                    }
                    dma_bytes = to_copy as u64;
                }
            }
            small_res_bytes = header_payload;
        } else if data_payload.is_empty() {
            small_res_bytes = header_payload;
        } else {
            small_res_owned =
                [header_payload, data_payload].concat();
            small_res_bytes = &small_res_owned;
        }
        let dma_us = t_dma.elapsed().as_micros() as u64;

        let t_small = std::time::Instant::now();
        let resp_len = small_res_bytes.len() as u32;
        let completion = if resp_len <= 52 {
            let mut inline = [0u8; 52];
            let copy = (resp_len as usize).min(52);
            inline[..copy].copy_from_slice(&small_res_bytes[..copy]);
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
                    small_res_bytes.as_ptr(),
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
        let small_us = t_small.elapsed().as_micros() as u64;

        let t_cq = std::time::Instant::now();
        if channel.cq_ring.try_push(completion).is_err() {
            tracing::error!(
                "ucx_relay: cq_ring full for channel {}; req_id={} will time out",
                pending.channel_id,
                pending.client_req_id
            );
        }
        let cq_us = t_cq.elapsed().as_micros() as u64;

        // Accumulate stats, dump every 4096 completions.
        inner.stat_n += 1;
        inner.stat_recv_data_us += recv_us;
        inner.stat_memcpy_dma_us += dma_us;
        inner.stat_memcpy_small_us += small_us;
        inner.stat_cq_push_us += cq_us;
        inner.stat_data_bytes_total += data_len.max(dma_bytes);
        if inner.stat_n.is_multiple_of(4096) {
            let n = inner.stat_n;
            let total_bytes_gib = inner.stat_data_bytes_total as f64 / (1u64 << 30) as f64;
            tracing::warn!(
                target: "ucx_relay_stats",
                n = n,
                avg_recv_us = inner.stat_recv_data_us as f64 / n as f64,
                avg_dma_memcpy_us = inner.stat_memcpy_dma_us as f64 / n as f64,
                avg_small_us = inner.stat_memcpy_small_us as f64 / n as f64,
                avg_cq_push_us = inner.stat_cq_push_us as f64 / n as f64,
                total_data_GiB = total_bytes_gib,
                "[ucx_relay_stats] hot path timing per RPC reply"
            );
        }
    }
}

impl Drop for UcxRelayDaemon {
    /// SHM unlink the control_ring (and any future per-daemon SHM names
    /// we add to `owned_shm_names`). Without this each benchfsd_mpi exit
    /// leaks `/benchfs_ucx_daemon_<host>_<lr>` so the job script has to
    /// rm them after the fact — a brittle workaround that loses to a
    /// crashed daemon. The Drop is best-effort: failures are logged but
    /// don't propagate because tear-down can't reasonably abort.
    fn drop(&mut self) {
        for name in self.owned_shm_names.borrow().iter() {
            if let Err(e) = nix::sys::mman::shm_unlink(name.as_str()) {
                tracing::warn!(
                    "ucx_relay: shm_unlink({}) failed on drop: {:?}",
                    name,
                    e,
                );
            }
        }
    }
}
