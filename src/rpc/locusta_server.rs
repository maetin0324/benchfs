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
use std::rc::Rc;

use rrrpc::server::Request;

use crate::rpc::handlers::RpcHandlerContext;
use crate::rpc::locusta_buffer::RegisteredFixedBuffer;
use crate::rpc::transport_locusta::{
    drain_server_requests, extract_rpc_id, LocustaTransport,
};
use crate::rpc::AmRpc;

/// Server-side handler for a single AmRpc type, dispatched by locusta.
///
/// Implementations parse the rpc-specific header from `body` (the
/// `small_req` after the rpc_id prefix has been stripped), execute the
/// business logic against `ctx`, and call the appropriate consumer on
/// the `req` handle.
///
/// The trait is intentionally generic over the `AmRpc` type so the
/// dispatcher can pick up `Self::rpc_id()` automatically when the
/// caller does `dispatch.register::<MyRequest>()`.
pub trait LocustaServerHandler: AmRpc {
    /// Run the request to completion. The handler is consumed (must
    /// call `req.reply()` / `req.grant()` etc. or let it drop, which
    /// produces an error response).
    fn handle_locusta(ctx: &Rc<RpcHandlerContext>, body: &[u8], req: Request<RegisteredFixedBuffer>);
}

/// Boxed handler function pointer used by the registry.
type DynHandler =
    Box<dyn Fn(&Rc<RpcHandlerContext>, &[u8], Request<RegisteredFixedBuffer>)>;

/// Per-process router from `rpc_id` to its [`LocustaServerHandler`].
///
/// Created once at startup, registered with every RPC type the server
/// understands, then `poll_once` is called repeatedly inside the
/// server's main loop.
pub struct LocustaServerDispatch {
    ctx: Rc<RpcHandlerContext>,
    handlers: HashMap<u16, DynHandler>,
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
            Box::new(|ctx, body, req| H::handle_locusta(ctx, body, req)),
        );
        assert!(
            prev.is_none(),
            "duplicate LocustaServerDispatch registration for rpc_id={rpc_id}"
        );
    }

    /// Dispatch a single `Request` based on the rpc_id prefix in its
    /// `small_req`. Variants without a small_req body (none exist in
    /// the current rrrpc enum) would need special handling.
    pub fn dispatch(&self, req: Request<RegisteredFixedBuffer>) {
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
                return;
            }
        };
        // `body` borrows from the handle inside `req`; we need to drop
        // that borrow before moving `req` into the handler. Reborrow as
        // an owned `Vec` for now (typical metadata bodies are ≤256B —
        // the copy is cheap). A future revision can re-use the
        // small_req_scratch pattern from the client side if this
        // shows up in profiles.
        let body_owned = body.to_vec();
        match self.handlers.get(&rpc_id) {
            Some(handler) => handler(&self.ctx, &body_owned, req),
            None => {
                eprintln!("[locusta_server] no handler registered for rpc_id={rpc_id}");
                // `req` drops here — for roundtrip variants this sends
                // an error response back to the client; for oneway
                // variants it auto-acks.
                drop(req);
            }
        }
    }

    /// Drain all currently-ready requests through the dispatcher.
    /// Caller is responsible for ticking the locusta polling state
    /// machines first via [`LocustaInner::tick`].
    pub fn poll_once(&self, transport: &LocustaTransport) {
        let mut inner = transport.inner.borrow_mut();
        inner.tick();
        drain_server_requests(&mut *inner, |req| self.dispatch(req));
    }
}
