//! Server-side dispatch for the locusta transport backend.
//!
//! Walks the `rrrpc::server::Request` stream and routes each request to
//! the registered per-`rpc_id` handler. The handler reads the small_req
//! body (after [`extract_rpc_id`] strips the 2-byte prefix that
//! [`LocustaTransport::send_*`] prepends) and produces a reply that
//! is sent back via the appropriate `Handle::reply()` consumer.
//!
//! Phase 2.4: this is the production replacement for the demo's
//! hand-rolled `drain_server_requests` callback. Each BenchFS RPC type
//! that wants to be servable over locusta implements
//! [`LocustaServerHandler`] and registers itself at startup:
//!
//! ```ignore
//! let mut dispatch = LocustaServerDispatch::new(ctx);
//! dispatch.register::<MetadataLookupRequest>();
//! dispatch.register::<MetadataCreateFileRequest>();
//! ...
//! loop {
//!     dispatch.poll_once(&transport);
//! }
//! ```

#![cfg(feature = "transport-locusta")]

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;

use rrrpc::server::Request;

use crate::rpc::AmRpc;
use crate::rpc::handlers::RpcHandlerContext;
use crate::rpc::locusta_buffer::RegisteredFixedBuffer;
use crate::rpc::transport_locusta::{LocustaTransport, drain_server_requests, extract_rpc_id};

/// Server-side handler for a single AmRpc type, dispatched by locusta.
///
/// Async because chunk RPCs need to interact with the io_uring-backed
/// `ChunkStore`. Sync RPCs (metadata) implement this trivially with
/// `async fn` that contains no `.await`.
///
/// Parameters are owned (not borrowed) so the returned future is
/// `'static` — needed to spawn it on `pluvio_runtime::executor`.
#[allow(async_fn_in_trait)]
pub trait LocustaServerHandler: AmRpc {
    async fn handle_locusta(
        ctx: Rc<RpcHandlerContext>,
        body: Vec<u8>,
        req: Request<RegisteredFixedBuffer>,
    );
}

/// Boxed handler launcher: takes ownership of (ctx, body, req) and
/// returns a `'static` future that drives the handler to completion.
type DynLauncher = Box<
    dyn Fn(
        Rc<RpcHandlerContext>,
        Vec<u8>,
        Request<RegisteredFixedBuffer>,
    ) -> Pin<Box<dyn Future<Output = ()> + 'static>>,
>;

/// Per-process router from `rpc_id` to its [`LocustaServerHandler`].
pub struct LocustaServerDispatch {
    ctx: Rc<RpcHandlerContext>,
    handlers: HashMap<u16, DynLauncher>,
}

impl LocustaServerDispatch {
    pub fn new(ctx: Rc<RpcHandlerContext>) -> Self {
        Self {
            ctx,
            handlers: HashMap::new(),
        }
    }

    /// Register `H` as the handler for `H::rpc_id()`.
    /// Panics on duplicate registration.
    pub fn register<H: LocustaServerHandler + 'static>(&mut self) {
        let rpc_id = H::rpc_id();
        let prev = self.handlers.insert(
            rpc_id,
            Box::new(|ctx, body, req| Box::pin(H::handle_locusta(ctx, body, req))),
        );
        assert!(
            prev.is_none(),
            "duplicate LocustaServerDispatch registration for rpc_id={rpc_id}"
        );
    }

    /// Look up the handler for a request and return the handler's
    /// future. Returns `None` if the request is malformed or no
    /// handler is registered (in the latter case, `req` is dropped,
    /// which sends an error response for roundtrip variants).
    pub fn dispatch(
        &self,
        req: Request<RegisteredFixedBuffer>,
    ) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        let small_req: &[u8] = match &req {
            Request::OnewayEager(h) => h.small_req(),
            Request::RoundtripEager(h) => h.small_req(),
            Request::RoundtripGet(h) => h.small_req(),
            Request::PutGrant(h) => h.small_req(),
            Request::OnewayPutReady(h) => h.small_req(),
            Request::RoundtripPutReady(h) => h.small_req(),
        };
        let (rpc_id, body) = match extract_rpc_id(small_req) {
            Some(pair) => pair,
            None => {
                eprintln!(
                    "[locusta_server] dropping request with malformed small_req ({} bytes)",
                    small_req.len()
                );
                return None;
            }
        };
        let body_owned = body.to_vec();
        match self.handlers.get(&rpc_id) {
            Some(launcher) => Some(launcher(Rc::clone(&self.ctx), body_owned, req)),
            None => {
                eprintln!("[locusta_server] no handler registered for rpc_id={rpc_id}");
                drop(req);
                None
            }
        }
    }

    /// Drain ready requests and spawn each handler onto the
    /// currently-active `pluvio_runtime` executor. Caller must have
    /// `set_runtime` already configured on this thread.
    ///
    /// Returns the number of requests drained (and spawned). The
    /// driver loop uses this to choose between busy-yielding (when
    /// hot) and a short sleep (when idle).
    pub fn poll_once_spawn(&self, transport: &LocustaTransport) -> usize {
        let mut inner = transport.inner.borrow_mut();
        inner.tick();
        let mut count = 0usize;
        drain_server_requests(&mut *inner, |req| {
            if let Some(fut) = self.dispatch(req) {
                // Capture timestamp at spawn-call site. The wrapper
                // future records dispatch-to-first-poll latency on the
                // very first poll — i.e. how long the future sat in the
                // pluvio task queue before the executor picked it up.
                // This is the key signal for diagnosing scheduler
                // starvation; if it spikes alongside the client-side
                // p99, the tail is in pluvio scheduling rather than in
                // handler body work.
                let t_spawn = std::time::Instant::now();
                let wrapped: Pin<Box<dyn Future<Output = ()> + 'static>> =
                    Box::pin(async move {
                        let dispatch_lat_us = t_spawn.elapsed().as_micros() as u64;
                        if crate::stats::is_stats_enabled() {
                            use std::sync::atomic::{AtomicU64, Ordering};
                            static N: AtomicU64 = AtomicU64::new(0);
                            let n = N.fetch_add(1, Ordering::Relaxed);
                            if n.is_multiple_of(1000) {
                                tracing::info!(
                                    target: "rpc_handler_timing",
                                    kind = "handler_dispatch",
                                    n = n,
                                    dispatch_lat_us = dispatch_lat_us,
                                    "HANDLER_DISPATCH_LAT"
                                );
                            }
                        }
                        fut.await;
                    });
                pluvio_runtime::executor::spawn(wrapped);
                count += 1;
            }
        });
        count
    }

    /// Drain-only variant for use when a separate Reactor handles
    /// `inner.tick()`. Skips the redundant tick so per-iteration cost
    /// is just the drain + spawn loop.
    pub fn drain_and_spawn(&self, transport: &LocustaTransport) {
        let mut inner = transport.inner.borrow_mut();
        drain_server_requests(&mut *inner, |req| {
            if let Some(fut) = self.dispatch(req) {
                pluvio_runtime::executor::spawn(fut);
            }
        });
    }

    /// Drain ready requests and drive each handler to completion
    /// inline via a tiny busy-poll loop. Suitable for the demo binary
    /// (no pluvio_runtime running) when handlers are effectively
    /// sync — i.e. metadata RPCs whose async future is just a
    /// `Ready(())` wrapper. Calling this with an actually-async
    /// handler that blocks on I/O will hang the polling thread.
    pub fn poll_once_inline(&self, transport: &LocustaTransport) {
        use std::sync::Arc;
        use std::task::{Context, Poll, Wake, Waker};
        struct NoopWaker;
        impl Wake for NoopWaker {
            fn wake(self: Arc<Self>) {}
        }
        let waker: Waker = Waker::from(Arc::new(NoopWaker));
        let mut cx = Context::from_waker(&waker);

        let mut inner = transport.inner.borrow_mut();
        inner.tick();
        let mut pending: Vec<Pin<Box<dyn Future<Output = ()>>>> = Vec::new();
        drain_server_requests(&mut *inner, |req| {
            if let Some(fut) = self.dispatch(req) {
                pending.push(fut);
            }
        });
        // Need to drop the borrow before polling — handlers may take
        // the transport's borrow (e.g. for re-acquiring buffers).
        drop(inner);
        for mut fut in pending {
            loop {
                match Pin::as_mut(&mut fut).poll(&mut cx) {
                    Poll::Ready(()) => break,
                    Poll::Pending => std::thread::yield_now(),
                }
            }
        }
    }

    /// Backwards-compat wrapper. Equivalent to [`Self::poll_once_inline`].
    pub fn poll_once(&self, transport: &LocustaTransport) {
        self.poll_once_inline(transport);
    }
}
