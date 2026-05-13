//! Phase 1b/1c demo: drive `LocustaTransport` from BenchFS code on 2 nodes.
//!
//! Each process initializes a `LocustaTransport` with one peer; the
//! client side then issues RoundtripEager pings via the async
//! `RpcTransport::send_eager` API and times them. The server side
//! pumps the locusta polling loop.
//!
//! Two `--mode` options:
//!   * `raw`      — Phase 1b: opaque 32-byte payload, server acks with 8 zero
//!                  bytes. Exercises the LocustaTransport plumbing.
//!   * `metadata` — Phase 1c: client encodes a real
//!                  `MetadataLookupRequestHeader` + path; server parses,
//!                  consults a dummy in-memory index, replies with a
//!                  `MetadataLookupResponseHeader`. Validates real BenchFS
//!                  wire types over Locusta.
//!
//! Build:
//!   cargo build --release --bin benchfs_locusta_demo --features transport-locusta
//!
//! Run (file-based QP exchange via a shared directory):
//!   # node B (server-only):
//!   benchfs_locusta_demo --role server --local node_b --peer node_a \
//!       --registry-dir /work/.../registry --serve-secs 60 --mode metadata
//!   # node A (client):
//!   benchfs_locusta_demo --role client --local node_a --peer node_b \
//!       --registry-dir /work/.../registry --pings 1000 --mode metadata \
//!       --path /test/file.bin

#![cfg(feature = "transport-locusta")]

use std::io::IoSlice;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use zerocopy::{FromBytes, IntoBytes};

use benchfs::rpc::data_ops::{WriteChunkByIdRequest, WriteChunkResponseHeader};
use benchfs::rpc::handlers::RpcHandlerContext;
use benchfs::rpc::locusta_call::LocustaCallable;
use benchfs::rpc::locusta_server::LocustaServerDispatch;
use benchfs::rpc::metadata_ops::{
    MetadataLookupRequest, MetadataLookupRequestHeader, MetadataLookupResponseHeader,
};
use benchfs::rpc::transport::RpcTransport;
use benchfs::rpc::transport_locusta::{
    LocustaConfig, LocustaTransport, drain_server_requests, extract_rpc_id,
};

/// RpcId used by BenchFS for `MetadataLookup`. Mirrors
/// `crate::constants::RPC_METADATA_LOOKUP` so the wire format matches.
const RPC_METADATA_LOOKUP: u16 = 20;

#[derive(Parser, Debug)]
struct Args {
    /// What this process should do.
    #[arg(long, value_enum)]
    role: Role,
    /// Local node id (must be unique across peers).
    #[arg(long)]
    local: String,
    /// Single peer node id to exchange QP info with.
    #[arg(long)]
    peer: String,
    /// Shared filesystem path used for QP info exchange.
    #[arg(long)]
    registry_dir: PathBuf,
    /// (client) number of ping iterations.
    #[arg(long, default_value_t = 1000)]
    pings: u32,
    /// (server) wall clock to stay alive for.
    #[arg(long, default_value_t = 60)]
    serve_secs: u64,
    /// Payload mode.
    #[arg(long, value_enum, default_value_t = Mode::Raw)]
    mode: Mode,
    /// (client) path string sent in metadata mode.
    #[arg(long, default_value = "/test/file.bin")]
    path: String,
    /// (client) DMA payload size for put/get modes (default 4 MiB).
    #[arg(long, default_value_t = 4 * 1024 * 1024)]
    payload_bytes: usize,
    /// Skip the static QP exchange at init time — both sides start with
    /// an empty peer list. Server uses `try_accept_pending_peers`; the
    /// client calls `add_peer` explicitly before issuing RPCs. Exercises
    /// the late-join handshake path used by FFI/IOR clients in Phase 3.
    #[arg(long, default_value_t = false)]
    dynamic: bool,
}

#[derive(Copy, Clone, ValueEnum, Debug, PartialEq, Eq)]
enum Role {
    Client,
    Server,
}

#[derive(Copy, Clone, ValueEnum, Debug, PartialEq, Eq)]
enum Mode {
    /// Phase 1b: opaque 32B eager payload + 8B reply.
    Raw,
    /// Phase 1c: real BenchFS `MetadataLookupRequestHeader` + path eager
    /// round-trip (manual marshalling via `transport.send_eager`).
    Metadata,
    /// Phase 2.2: same wire RPC as `Metadata`, but driven through the
    /// `LocustaCallable::call_locusta` blanket impl — i.e. invokes
    /// `MetadataLookupRequest::call_locusta(...)` exactly as production
    /// BenchFS code would once the RPC layer is wired up.
    Amrpc,
    /// Phase 2.1: `RoundtripPut` — client DMA-writes `--payload-bytes` of
    /// data into a server-side `SharedRdmaBuffer`; server replies with an
    /// 8B ack.
    Put,
    /// Phase 2.1: `RoundtripGet` — client requests `--payload-bytes`; the
    /// server replies with a pre-filled `SharedRdmaBuffer` that is
    /// RDMA-read into the client's recv buffer.
    Get,
    /// Phase 2.2b: production `WriteChunkByIdRequest::call_locusta(...)`
    /// — exercises the AmRpc `Put` dispatch via `LocustaCallable`.
    AmrpcPut,
    /// Phase 2.4: same wire as `Amrpc` on the client, but the server
    /// runs the production [`LocustaServerDispatch`] backed by a real
    /// `RpcHandlerContext`. Validates the full server-side dispatch
    /// path (rpc_id lookup, handler invocation, reply consumer).
    Dispatch,
}

fn main() {
    let args = Args::parse();
    println!(
        "[{:?}] local={} peer={} registry={}",
        args.role,
        args.local,
        args.peer,
        args.registry_dir.display()
    );

    let cfg = LocustaConfig {
        registry_dir: args.registry_dir.clone(),
        local_node_id: args.local.clone(),
        peer_node_ids: if args.dynamic {
            Vec::new()
        } else {
            vec![args.peer.clone()]
        },
        ..LocustaConfig::default()
    };

    let transport = LocustaTransport::init(&cfg).expect("LocustaTransport::init");
    println!(
        "[{:?}] init done (dynamic={}) — node_to_dest={:?}",
        args.role,
        args.dynamic,
        transport.inner.borrow().node_to_dest
    );

    // Dynamic path: client explicitly adds the peer after init; server
    // discovers it via the registry-scan accept loop driven inside
    // `run_server` below.
    if args.dynamic {
        match args.role {
            Role::Client => {
                let t0 = Instant::now();
                transport
                    .add_peer(&args.peer, Duration::from_secs(30))
                    .expect("dynamic add_peer");
                println!(
                    "[Client] dynamic add_peer({}) handshake took {:?}",
                    args.peer,
                    t0.elapsed()
                );
            }
            Role::Server => {
                // Server's accept loop is launched inside `run_server` so
                // it runs interleaved with the request poll loop.
            }
        }
    }

    match args.role {
        Role::Server => run_server(&transport, args.serve_secs, args.mode),
        Role::Client => run_client(
            &transport,
            &args.peer,
            args.pings,
            args.mode,
            &args.path,
            args.payload_bytes,
        ),
    }
}

fn run_server(transport: &LocustaTransport, serve_secs: u64, mode: Mode) {
    use benchfs::rpc::locusta_buffer::RegisteredFixedBuffer;

    // For Mode::Dispatch only — build a real LocustaServerDispatch backed
    // by a minimal RpcHandlerContext, pre-populating one file entry so
    // the lookup hits.
    let dispatch_setup = if mode == Mode::Dispatch {
        use benchfs::metadata::FileMetadata;
        let ctx = std::rc::Rc::new(RpcHandlerContext::new_bench());
        ctx.metadata_manager
            .store_file_metadata(FileMetadata::new("/test/file.bin".to_string(), 1024 * 1024))
            .expect("seed file metadata");
        let mut dispatch = LocustaServerDispatch::new(ctx);
        dispatch.register::<MetadataLookupRequest>();
        Some(dispatch)
    } else {
        None
    };

    let deadline = Instant::now() + Duration::from_secs(serve_secs);
    let mut reqs = 0u64;
    let mut polls = 0u64;
    let mut next_accept_at = Instant::now();
    while Instant::now() < deadline {
        // Periodic registry scan — picks up dynamically-joining clients.
        // Cheap when nothing new is present (a single readdir).
        if Instant::now() >= next_accept_at {
            match transport.try_accept_pending_peers(Duration::from_secs(10)) {
                Ok(added) if !added.is_empty() => {
                    println!("[server] try_accept_pending_peers added: {:?}", added);
                }
                Ok(_) => {}
                Err(e) => {
                    eprintln!("[server] try_accept_pending_peers error: {:?}", e);
                }
            }
            next_accept_at = Instant::now() + Duration::from_millis(100);
        }
        if let Some(dispatch) = dispatch_setup.as_ref() {
            // Mode::Dispatch — production polling path. `poll_once` ticks
            // the locusta state machines and runs every ready request
            // through the registered handler.
            dispatch.poll_once(transport);
            reqs = reqs.saturating_add(1); // dispatcher doesn't count; rough estimate
            polls += 1;
            if polls % 1_000_000 == 0 {
                eprintln!("[server] polls={polls} (dispatch mode)");
            }
            std::thread::yield_now();
            continue;
        }

        let mut inner = transport.inner.borrow_mut();
        inner.daemon.poll_client_requests();
        inner.daemon.process_pending_dma_writes();
        inner.daemon.flush_all_destinations();
        inner.daemon.poll_server_completions();
        inner.client.poll();
        // SAFETY: `inner` is held via `&mut *inner` only for the scope of
        // `drain_server_requests`. The callback borrows `inner.server_buffer_allocator`
        // for the duration of the call — that's fine since both fields live
        // in the same struct and are accessed sequentially.
        let allocator = inner.server_buffer_allocator.clone();
        drain_server_requests(&mut *inner, |req| {
            match req {
                rrrpc::server::Request::RoundtripEager(h) => {
                    // All locusta send_* paths prepend a 2-byte rpc_id.
                    // Strip it before dispatching to the per-RPC body parser.
                    let (_rpc_id, body) =
                        extract_rpc_id(h.small_req()).expect("small_req missing rpc_id prefix");
                    let reply = match mode {
                        Mode::Raw => vec![0u8; 8],
                        // Both Metadata and Amrpc send the same wire format —
                        // header bytes followed by the path. The only
                        // difference is the client-side marshalling path.
                        Mode::Metadata | Mode::Amrpc => serve_metadata_lookup(body),
                        Mode::Dispatch => {
                            unreachable!("Dispatch handled by LocustaServerDispatch path");
                        }
                        Mode::Put | Mode::Get | Mode::AmrpcPut => {
                            eprintln!("[server] unexpected RoundtripEager in {mode:?} mode");
                            vec![0u8; 8]
                        }
                    };
                    h.reply(reply);
                    reqs += 1;
                }
                rrrpc::server::Request::OnewayEager(_) => {
                    reqs += 1;
                }
                rrrpc::server::Request::PutGrant(h) => {
                    // Put phase 1: client wants to RDMA-write `dma_len` bytes.
                    // Acquire a registered buffer, hand it via `grant`.
                    let alloc = match &allocator {
                        Some(a) => a,
                        None => {
                            eprintln!("[server] PutGrant arrived but no buffer pool");
                            return;
                        }
                    };
                    match alloc.try_acquire() {
                        Some(fb) => h.grant(RegisteredFixedBuffer::from_fixed_buffer(fb)),
                        None => {
                            eprintln!("[server] buffer pool exhausted on PutGrant");
                        }
                    }
                }
                rrrpc::server::Request::RoundtripPutReady(h) => {
                    // Put phase 2: DMA write complete; buffer.as_ptr() is filled.
                    // Reply shape depends on the mode the client used:
                    // Mode::Put expects an opaque 8B ack, Mode::AmrpcPut
                    // expects a WriteChunkResponseHeader (16B).
                    let reply = match mode {
                        Mode::AmrpcPut => {
                            // Acknowledge the full DMA write — the client's
                            // LocustaCallable decoder will read this as
                            // WriteChunkResponseHeader. `RdmaBuffer::len`
                            // is a trait method, so use the explicit path.
                            use rrrpc::server::RdmaBuffer;
                            let resp = WriteChunkResponseHeader::success(h.buffer().len() as u64);
                            resp.as_bytes().to_vec()
                        }
                        _ => vec![0u8; 8],
                    };
                    h.reply(reply);
                    reqs += 1;
                }
                rrrpc::server::Request::RoundtripGet(h) => {
                    // Get: respond with a server-allocated, registered buffer.
                    let alloc = match &allocator {
                        Some(a) => a,
                        None => {
                            eprintln!("[server] RoundtripGet without buffer pool");
                            return;
                        }
                    };
                    match alloc.try_acquire() {
                        Some(mut fb) => {
                            // Fill with a recognizable pattern up to dma_len.
                            let dma_len = h.dma_len() as usize;
                            let slice = fb.as_mut_slice();
                            let n = dma_len.min(slice.len());
                            for byte in slice[..n].iter_mut() {
                                *byte = 0xCD;
                            }
                            let buf = RegisteredFixedBuffer::from_fixed_buffer(fb);
                            h.reply(buf, vec![0u8; 8]);
                            reqs += 1;
                        }
                        None => {
                            eprintln!("[server] buffer pool exhausted on RoundtripGet");
                        }
                    }
                }
                other => {
                    eprintln!(
                        "[server] unexpected request: {:?}",
                        std::mem::discriminant(&other)
                    );
                }
            }
        });
        drop(inner);
        polls += 1;
        if polls % 1_000_000 == 0 {
            eprintln!("[server] polls={} served={} reqs", polls, reqs);
        }
        std::thread::yield_now();
    }
    println!("[server] DONE polls={} reqs_served={}", polls, reqs);
}

/// Dummy server-side handler that parses a `MetadataLookupRequestHeader`
/// followed by `path_len` bytes from `small_req`, then returns a
/// `MetadataLookupResponseHeader` describing a single hard-coded file at
/// `/test/file.bin` (size 1 MiB). Every other path comes back as
/// `not_found`.
fn serve_metadata_lookup(small_req: &[u8]) -> Vec<u8> {
    let hdr_size = std::mem::size_of::<MetadataLookupRequestHeader>();
    let header = if let Ok(h) = MetadataLookupRequestHeader::read_from_bytes(&small_req[..hdr_size])
    {
        h
    } else {
        let resp = MetadataLookupResponseHeader::error(-22); // EINVAL
        return resp.as_bytes().to_vec();
    };
    let path_end = hdr_size + header.path_len as usize;
    if path_end > small_req.len() {
        let resp = MetadataLookupResponseHeader::error(-22);
        return resp.as_bytes().to_vec();
    }
    let path = std::str::from_utf8(&small_req[hdr_size..path_end]).unwrap_or("");
    let resp = if path == "/test/file.bin" {
        MetadataLookupResponseHeader::file(1024 * 1024)
    } else if path == "/test" {
        MetadataLookupResponseHeader::directory()
    } else {
        MetadataLookupResponseHeader::not_found()
    };
    resp.as_bytes().to_vec()
}

fn run_client(
    transport: &LocustaTransport,
    peer: &str,
    pings: u32,
    mode: Mode,
    path: &str,
    payload_bytes: usize,
) {
    let peer_id: String = peer.to_string();

    // Build the small_req wire payload once. Same encoding as
    // `MetadataLookupRequest::call()` for the metadata mode; the put/get
    // modes use a tiny header (zero-length) since the actual payload is
    // delivered over DMA, not in the small_req.
    let small_req: Vec<u8> = match mode {
        Mode::Raw => vec![0xABu8; 32],
        Mode::Metadata => {
            let hdr = MetadataLookupRequestHeader::new(path.len());
            let mut buf = hdr.as_bytes().to_vec();
            buf.extend_from_slice(path.as_bytes());
            buf
        }
        // Amrpc/AmrpcPut/Dispatch modes construct the request struct itself; small_req unused.
        Mode::Amrpc | Mode::AmrpcPut | Mode::Dispatch => Vec::new(),
        Mode::Put | Mode::Get => vec![0u8; 8], // tiny header for DMA modes
    };

    // Pre-build the AmRpc request once so the latency loop only measures
    // the wire round-trip, not allocation overhead.
    let amrpc_req = if matches!(mode, Mode::Amrpc | Mode::Dispatch) {
        Some(MetadataLookupRequest::new(path.to_string()))
    } else {
        None
    };

    // For Put we pre-fill a payload with a recognizable pattern so the
    // server can validate the bytes on the receive side. For Get we
    // pre-allocate a recv buffer the server will RDMA-write into.
    let put_payload: Vec<u8> = if mode == Mode::Put {
        vec![0xA5u8; payload_bytes]
    } else {
        Vec::new()
    };
    let mut get_recv: Vec<u8> = if mode == Mode::Get {
        vec![0u8; payload_bytes]
    } else {
        Vec::new()
    };

    // Single-threaded async runtime: poll the future with a NoopWaker.
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::{Context, Poll, Wake, Waker};

    struct NoopWaker;
    impl Wake for NoopWaker {
        fn wake(self: Arc<Self>) {}
    }
    let waker: Waker = Waker::from(Arc::new(NoopWaker));
    let mut cx = Context::from_waker(&waker);

    // Pre-allocated payload for AmrpcPut so the loop measures wire latency only.
    let amrpc_put_data: Vec<u8> = if mode == Mode::AmrpcPut {
        vec![0x5Au8; payload_bytes]
    } else {
        Vec::new()
    };

    let payload_summary = match mode {
        Mode::Put | Mode::Get | Mode::AmrpcPut => format!("dma={} bytes", payload_bytes),
        _ => format!("eager={} bytes", small_req.len()),
    };
    println!(
        "[client] sending {} pings (mode={:?}, {}) to {}",
        pings, mode, payload_summary, peer
    );
    let start = Instant::now();
    let mut total_latency_ns: u128 = 0;
    for i in 0..pings {
        let t0 = Instant::now();
        let result: Result<_, _> = match mode {
            Mode::Raw | Mode::Metadata => {
                let parts = [IoSlice::new(&small_req)];
                let mut fut = Box::pin(transport.send_eager(&peer_id, RPC_METADATA_LOOKUP, &parts));
                loop {
                    match Pin::as_mut(&mut fut).poll(&mut cx) {
                        Poll::Ready(r) => break r,
                        Poll::Pending => std::thread::yield_now(),
                    }
                }
            }
            Mode::Amrpc | Mode::Dispatch => {
                // Exercise the production code path: LocustaCallable
                // blanket impl on AmRpc serializes header+request_data,
                // dispatches via call_type, and deserializes the response.
                let req = amrpc_req.as_ref().expect("amrpc_req built");
                let mut fut = Box::pin(req.call_locusta(&peer_id, transport));
                let hdr_result = loop {
                    match Pin::as_mut(&mut fut).poll(&mut cx) {
                        Poll::Ready(r) => break r,
                        Poll::Pending => std::thread::yield_now(),
                    }
                };
                // Adapt to RpcResponse so the surrounding loop shape stays
                // uniform with other modes — we only care that the wire
                // round-trip succeeded and the response decoded.
                hdr_result.map(|hdr| benchfs::rpc::transport::RpcResponse {
                    header_bytes: hdr.as_bytes().to_vec(),
                    data_len: 0,
                })
            }
            Mode::Put => {
                let parts = [IoSlice::new(&small_req)];
                let mut fut = Box::pin(transport.send_put(
                    &peer_id,
                    RPC_METADATA_LOOKUP,
                    &parts,
                    &put_payload,
                ));
                loop {
                    match Pin::as_mut(&mut fut).poll(&mut cx) {
                        Poll::Ready(r) => break r,
                        Poll::Pending => std::thread::yield_now(),
                    }
                }
            }
            Mode::Get => {
                let parts = [IoSlice::new(&small_req)];
                let mut fut = Box::pin(transport.send_get(
                    &peer_id,
                    RPC_METADATA_LOOKUP,
                    &parts,
                    &mut get_recv,
                ));
                loop {
                    match Pin::as_mut(&mut fut).poll(&mut cx) {
                        Poll::Ready(r) => break r,
                        Poll::Pending => std::thread::yield_now(),
                    }
                }
            }
            Mode::AmrpcPut => {
                // Production path: WriteChunkByIdRequest::call_locusta(...)
                // — exercises the LocustaCallable `Put` default impl on the
                // real BenchFS chunk write request type.
                let req = WriteChunkByIdRequest::new(
                    1, /* file_id */
                    0, /* offset */
                    &amrpc_put_data,
                );
                let mut fut = Box::pin(req.call_locusta(&peer_id, transport));
                let hdr_result = loop {
                    match Pin::as_mut(&mut fut).poll(&mut cx) {
                        Poll::Ready(r) => break r,
                        Poll::Pending => std::thread::yield_now(),
                    }
                };
                hdr_result.map(|hdr| benchfs::rpc::transport::RpcResponse {
                    header_bytes: hdr.as_bytes().to_vec(),
                    data_len: 0,
                })
            }
        };
        let dt = t0.elapsed();
        total_latency_ns += dt.as_nanos();
        match result {
            Ok(resp) => {
                if i == 0 {
                    match mode {
                        Mode::Metadata | Mode::Amrpc | Mode::Dispatch => {
                            let bytes = &resp.header_bytes;
                            let sz = std::mem::size_of::<MetadataLookupResponseHeader>();
                            if bytes.len() >= sz {
                                let rh =
                                    MetadataLookupResponseHeader::read_from_bytes(&bytes[..sz])
                                        .expect("decode");
                                println!(
                                    "[client] first response ({:?}): entry_type={} status={} size={}",
                                    mode, rh.entry_type, rh.status, rh.size
                                );
                            }
                        }
                        Mode::Get => {
                            // Server writes the pattern 0xCD.
                            let head = get_recv.first().copied().unwrap_or(0);
                            let tail = get_recv.last().copied().unwrap_or(0);
                            println!(
                                "[client] first Get response: data_len={} head=0x{:02x} tail=0x{:02x}",
                                resp.data_len, head, tail
                            );
                        }
                        Mode::AmrpcPut => {
                            let bytes = &resp.header_bytes;
                            let sz = std::mem::size_of::<WriteChunkResponseHeader>();
                            if bytes.len() >= sz {
                                let rh = WriteChunkResponseHeader::read_from_bytes(&bytes[..sz])
                                    .expect("decode");
                                println!(
                                    "[client] first AmrpcPut response: bytes_written={} status={}",
                                    rh.bytes_written, rh.status
                                );
                            }
                        }
                        _ => {}
                    }
                }
            }
            Err(e) => {
                eprintln!("[client] ping {i} failed: {e}");
                return;
            }
        }
        if (i + 1) % 100 == 0 {
            let avg_ns = total_latency_ns / (i + 1) as u128;
            println!(
                "[client] {} pings cumulative, avg {} ns/ping ({:.1} us)",
                i + 1,
                avg_ns,
                avg_ns as f64 / 1000.0
            );
        }
    }
    let elapsed = start.elapsed();
    println!(
        "[client] DONE {} pings in {:.3}s ({:.1} kops/s, avg {} ns/ping)",
        pings,
        elapsed.as_secs_f64(),
        pings as f64 / elapsed.as_secs_f64() / 1000.0,
        total_latency_ns / pings as u128
    );
}
