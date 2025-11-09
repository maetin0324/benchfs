use std::cell::RefCell;
use std::marker::PhantomData;
use std::rc::Rc;

use pluvio_ucx::{Worker, async_ucx::ucp::WorkerAddress, listener::Listener};

use crate::rpc::client_registry::ClientRegistry;
use crate::rpc::handlers::RpcHandlerContext;
use crate::rpc::{AmRpc, RpcError, Serializable};

struct SizeCheck<T, const N: usize>(PhantomData<T>);
impl<T, const N: usize> SizeCheck<T, N> {
    // ここは「const 文脈」なので assert! がコンパイル時に評価される
    const OK: () = assert!(size_of::<T>() <= N);
}

pub const MAX_HEADER_SIZE: usize = 256;

/// RPC server that receives and dispatches ActiveMessages
pub struct RpcServer {
    worker: Rc<Worker>,
    handler_context: Rc<RpcHandlerContext>,
    socket_acceptor: RefCell<Option<Rc<SocketAcceptor>>>,
    connection_mode: crate::rpc::ConnectionMode,
}

impl RpcServer {
    pub fn new(
        worker: Rc<Worker>,
        handler_context: Rc<RpcHandlerContext>,
        connection_mode: crate::rpc::ConnectionMode,
    ) -> Self {
        Self {
            worker,
            handler_context,
            socket_acceptor: RefCell::new(None),
            connection_mode,
        }
    }

    pub fn handler_context(&self) -> &Rc<RpcHandlerContext> {
        &self.handler_context
    }

    pub fn get_address(&self) -> Result<WorkerAddress<'_>, RpcError> {
        self.worker
            .address()
            .map_err(|e| RpcError::TransportError(format!("Failed to get worker address: {:?}", e)))
    }

    /// Start listener based on connection mode
    ///
    /// In Socket mode, this creates a UCX Listener and spawns the accept loop.
    /// In WorkerAddress mode, this does nothing (legacy behavior).
    ///
    /// # Arguments
    /// * `registry_dir` - Directory for service discovery files (server_list.txt or *.addr)
    /// * `node_id` - Node identifier for logging
    /// * `client_registry` - Registry for managing client endpoints (Socket mode only)
    pub async fn start_listener(
        &self,
        registry_dir: &str,
        node_id: &str,
        client_registry: Rc<ClientRegistry>,
    ) -> Result<(), RpcError> {
        match &self.connection_mode {
            crate::rpc::ConnectionMode::Socket { bind_addr } => {
                // Check if listener is already started (idempotency check)
                if self.socket_acceptor.borrow().is_some() {
                    tracing::warn!(
                        "start_listener() called multiple times for node {}, ignoring duplicate call",
                        node_id
                    );
                    return Ok(());
                }

                tracing::info!(
                    "Starting Socket listener on {} for node {}",
                    bind_addr,
                    node_id
                );

                // Create UCX Listener
                tracing::debug!("About to call worker.create_listener({}) for node {}", bind_addr, node_id);
                let listener = self.worker.create_listener(*bind_addr).map_err(|e| {
                    tracing::error!("worker.create_listener({}) failed for node {}: {:?}", bind_addr, node_id, e);
                    RpcError::ConnectionError(format!("Failed to create listener: {:?}", e))
                })?;
                tracing::debug!("worker.create_listener() succeeded for node {}", node_id);

                // Get the actual bound address (important when using port 0 for dynamic allocation)
                let actual_addr = listener.socket_addr().map_err(|e| {
                    RpcError::ConnectionError(format!("Failed to get listener address: {:?}", e))
                })?;

                tracing::info!(
                    "Listener created on actual address {} for node {}",
                    actual_addr,
                    node_id
                );

                // Write socket address to server_list.txt with node_id
                let server_list_path = format!("{}/server_list.txt", registry_dir);

                // Get the actual IP address
                // If bind address is 0.0.0.0, get the container's actual IP address
                let ip_addr = if actual_addr.ip().is_unspecified() {
                    // Try to get IP from hostname -i command
                    match std::process::Command::new("hostname").arg("-i").output() {
                        Ok(output) if output.status.success() => {
                            let ip_str = String::from_utf8_lossy(&output.stdout);
                            let ip_trimmed = ip_str.trim();

                            tracing::debug!(
                                "Resolved IP from hostname -i: {} (port: {})",
                                ip_trimmed,
                                actual_addr.port()
                            );

                            // Parse IP and combine with port
                            if let Ok(ip) = ip_trimmed.parse::<std::net::IpAddr>() {
                                format!("{}:{}", ip, actual_addr.port())
                            } else {
                                tracing::warn!(
                                    "Failed to parse IP from hostname -i: {}, using 0.0.0.0",
                                    ip_trimmed
                                );
                                actual_addr.to_string()
                            }
                        }
                        Ok(_) | Err(_) => {
                            tracing::warn!("hostname -i command failed, using bind address 0.0.0.0");
                            actual_addr.to_string()
                        }
                    }
                } else {
                    actual_addr.to_string()
                };

                let addr_line = format!("{} {}\n", node_id, ip_addr);

                // Append to server_list.txt
                use std::fs::OpenOptions;
                use std::io::Write;
                let mut file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&server_list_path)
                    .map_err(|e| {
                        RpcError::ConnectionError(format!(
                            "Failed to open server_list.txt: {:?}",
                            e
                        ))
                    })?;
                file.write_all(addr_line.as_bytes()).map_err(|e| {
                    RpcError::ConnectionError(format!(
                        "Failed to write to server_list.txt: {:?}",
                        e
                    ))
                })?;

                tracing::info!(
                    "Wrote {} {} to {}",
                    node_id,
                    ip_addr,
                    server_list_path
                );

                // Create SocketAcceptor and spawn accept loop
                let acceptor = Rc::new(SocketAcceptor::new(
                    listener,
                    client_registry,
                    self.worker.clone(),
                    node_id.to_string(),
                ));

                // Store the acceptor for later cleanup if needed
                *self.socket_acceptor.borrow_mut() = Some(acceptor.clone());

                // Spawn accept loop (this consumes the Rc)
                acceptor.spawn_accept_loop();

                tracing::info!("Socket accept loop started for node {}", node_id);
            }
            crate::rpc::ConnectionMode::WorkerAddress => {
                tracing::info!(
                    "WorkerAddress mode: no listener needed for node {}",
                    node_id
                );
                // In WorkerAddress mode, we rely on *.addr files written elsewhere
                // No additional setup needed here
            }
        }

        Ok(())
    }

    /// Start listening for RPC requests on the given AM stream ID
    ///
    /// This method uses the unified AmRpc::server_handler() interface.
    /// It automatically handles both simple header-only responses and data transfer responses.
    ///
    /// # Example
    /// ```ignore
    /// use crate::rpc::data_ops::ReadChunkRequest;
    ///
    /// server.listen::<ReadChunkRequest, _, _>(runtime.clone()).await?;
    /// ```
    pub async fn listen<Rpc, ReqH, ResH>(&self) -> Result<(), RpcError>
    where
        ResH: Serializable + 'static,
        ReqH: Serializable + 'static,
        Rpc: AmRpc<RequestHeader = ReqH, ResponseHeader = ResH> + 'static,
    {
        // header size check at compile time
        let _ = SizeCheck::<ReqH, MAX_HEADER_SIZE>::OK;
        let _ = SizeCheck::<ResH, MAX_HEADER_SIZE>::OK;

        let stream = self.worker.am_stream(Rpc::rpc_id()).map_err(|e| {
            RpcError::TransportError(format!("Failed to create AM stream: {:?}", e))
        })?;

        tracing::info!("RpcServer: Listening on AM stream ID {}", Rpc::rpc_id());

        let ctx = self.handler_context.clone();

        loop {
            // Check shutdown flag before waiting for next message
            if ctx.should_shutdown() {
                tracing::info!(
                    "RpcServer: Handler terminating due to shutdown for RPC ID {}",
                    Rpc::rpc_id()
                );
                break;
            }

            tracing::debug!("RpcServer: Waiting for message on RPC ID {}", Rpc::rpc_id());
            let msg = stream.wait_msg().await;
            if msg.is_none() {
                tracing::info!("RpcServer: Stream closed for RPC ID {}", Rpc::rpc_id());
                break;
            }

            let am_msg = msg
                .ok_or_else(|| RpcError::TransportError("Failed to receive message".to_string()))?;

            tracing::debug!(
                "RpcServer: Received message on RPC ID {}, calling server_handler",
                Rpc::rpc_id()
            );

            let ctx_clone = ctx.clone();

            // Call the unified server_handler with span for tracking
            let _handler_span =
                tracing::debug_span!("rpc_server_handler", rpc_id = Rpc::rpc_id()).entered();

            match Rpc::server_handler(ctx_clone, am_msg).await {
                Ok((_response, _am_msg)) => {
                    // Response was already sent within server_handler via reply_ep
                    // reply_ep使用を回避するため、ここでは何もしない
                    tracing::debug!(
                        "RPC handler completed successfully for RPC ID {} (response sent directly)",
                        Rpc::rpc_id()
                    );
                }
                Err((e, _am_msg)) => {
                    // エラーレスポンスもserver_handler内で送信済み（またはハンドラーがエラーを返した）
                    tracing::error!("Handler failed for RPC ID {}: {:?}", Rpc::rpc_id(), e);
                }
            }

            drop(_handler_span);
        }

        Ok(())
    }

    /// Register and start all standard RPC handlers
    ///
    /// This is a convenience method that starts listeners for all standard RPC types:
    /// - ClientRegister (RPC_CLIENT_REGISTER)
    /// - ReadChunk (RPC_READ_CHUNK)
    /// - WriteChunk (RPC_WRITE_CHUNK)
    /// - MetadataLookup (RPC_METADATA_LOOKUP)
    /// - MetadataCreateFile (RPC_METADATA_CREATE_FILE)
    /// - MetadataCreateDir (RPC_METADATA_CREATE_DIR)
    /// - MetadataDelete (RPC_METADATA_DELETE)
    /// - MetadataUpdate (RPC_METADATA_UPDATE)
    ///
    /// Each handler runs in its own async task spawned by the runtime.
    ///
    /// # Example
    /// ```ignore
    /// use pluvio_runtime::executor::Runtime;
    ///
    /// server.register_all_handlers().await?;
    /// ```
    pub async fn register_all_handlers(&self) -> Result<(), RpcError> {
        use crate::rpc::client_id_protocol::ClientRegisterRpc;
        use crate::rpc::data_ops::{ReadChunkRequest, WriteChunkRequest};
        use crate::rpc::metadata_ops::{
            MetadataCreateDirRequest, MetadataCreateFileRequest, MetadataDeleteRequest,
            MetadataLookupRequest, MetadataUpdateRequest,
        };

        tracing::info!("Registering all RPC handlers...");

        // Spawn ClientRegister handler
        {
            let server = self.clone_for_handler();
            pluvio_runtime::spawn_with_name(
                async move {
                    if let Err(e) = server.listen::<ClientRegisterRpc, _, _>().await {
                        tracing::error!("ClientRegister handler error: {:?}", e);
                    }
                },
                "rpc_client_register_handler".to_string(),
            );
        }

        // Spawn ReadChunk handler with polling priority
        {
            let server = self.clone_for_handler();
            pluvio_runtime::spawn_polling_with_name(
                async move {
                    if let Err(e) = server.listen::<ReadChunkRequest, _, _>().await {
                        tracing::error!("ReadChunk handler error: {:?}", e);
                    }
                },
                "rpc_read_chunk_handler".to_string(),
            );
        }

        // Spawn WriteChunk handler with polling priority
        {
            let server = self.clone_for_handler();
            pluvio_runtime::spawn_polling_with_name(
                async move {
                    if let Err(e) = server.listen::<WriteChunkRequest, _, _>().await {
                        tracing::error!("WriteChunk handler error: {:?}", e);
                    }
                },
                "rpc_write_chunk_handler".to_string(),
            );
        }

        // Spawn MetadataLookup handler
        {
            let server = self.clone_for_handler();
            pluvio_runtime::spawn_with_name(
                async move {
                    if let Err(e) = server.listen::<MetadataLookupRequest, _, _>().await {
                        tracing::error!("MetadataLookup handler error: {:?}", e);
                    }
                },
                "rpc_metadata_lookup_handler".to_string(),
            );
        }

        // Spawn MetadataCreateFile handler
        {
            let server = self.clone_for_handler();
            pluvio_runtime::spawn_with_name(
                async move {
                    if let Err(e) = server.listen::<MetadataCreateFileRequest, _, _>().await {
                        tracing::error!("MetadataCreateFile handler error: {:?}", e);
                    }
                },
                "rpc_metadata_create_file_handler".to_string(),
            );
        }

        // Spawn MetadataCreateDir handler
        {
            let server = self.clone_for_handler();
            pluvio_runtime::spawn_with_name(
                async move {
                    if let Err(e) = server.listen::<MetadataCreateDirRequest, _, _>().await {
                        tracing::error!("MetadataCreateDir handler error: {:?}", e);
                    }
                },
                "rpc_metadata_create_dir_handler".to_string(),
            );
        }

        // Spawn MetadataDelete handler
        {
            let server = self.clone_for_handler();
            pluvio_runtime::spawn_with_name(
                async move {
                    if let Err(e) = server.listen::<MetadataDeleteRequest, _, _>().await {
                        tracing::error!("MetadataDelete handler error: {:?}", e);
                    }
                },
                "rpc_metadata_delete_handler".to_string(),
            );
        }

        // Spawn MetadataUpdate handler
        {
            let server = self.clone_for_handler();
            pluvio_runtime::spawn_with_name(
                async move {
                    if let Err(e) = server.listen::<MetadataUpdateRequest, _, _>().await {
                        tracing::error!("MetadataUpdate handler error: {:?}", e);
                    }
                },
                "rpc_metadata_update_handler".to_string(),
            );
        }

        // Spawn Shutdown handler
        {
            let server = self.clone_for_handler();
            pluvio_runtime::spawn_with_name(
                async move {
                    use crate::rpc::metadata_ops::ShutdownRequest;
                    if let Err(e) = server.listen::<ShutdownRequest, _, _>().await {
                        tracing::error!("Shutdown handler error: {:?}", e);
                    }
                },
                "rpc_shutdown_handler".to_string(),
            );
        }

        tracing::info!("All RPC handlers registered successfully");
        Ok(())
    }

    /// Register only benchmark RPC handlers (Ping-Pong, Throughput, etc.)
    pub async fn register_bench_handlers(&self) -> Result<(), RpcError> {
        use crate::rpc::bench_ops::{BenchPingRequest, BenchShutdownRequest};

        tracing::info!("Registering benchmark RPC handlers...");

        // Spawn BenchPing handler
        {
            let server = self.clone_for_handler();
            pluvio_runtime::spawn_with_name(
                async move {
                    if let Err(e) = server.listen::<BenchPingRequest, _, _>().await {
                        tracing::error!("BenchPing handler error: {:?}", e);
                    }
                },
                "bench_ping_handler".to_string(),
            );
        }

        // Spawn BenchShutdown handler
        {
            let server = self.clone_for_handler();
            pluvio_runtime::spawn_with_name(
                async move {
                    if let Err(e) = server.listen::<BenchShutdownRequest, _, _>().await {
                        tracing::error!("BenchShutdown handler error: {:?}", e);
                    }
                },
                "bench_shutdown_handler".to_string(),
            );
        }

        tracing::info!("Benchmark RPC handlers registered successfully");
        Ok(())
    }

    /// Clone the server for use in handler tasks
    /// This creates a shallow clone that shares the worker and handler context
    fn clone_for_handler(&self) -> Self {
        Self {
            worker: self.worker.clone(),
            handler_context: self.handler_context.clone(),
            socket_acceptor: RefCell::new(None),
            connection_mode: self.connection_mode.clone(),
        }
    }
}

/// Socket connection acceptor for incoming client connections
///
/// Handles accept loop in socket connection mode, automatically registering
/// client endpoints with the ClientRegistry for persistent connections.
pub struct SocketAcceptor {
    listener: RefCell<Option<Listener>>,
    client_registry: Rc<ClientRegistry>,
    worker: Rc<Worker>,
    node_id: String,
}

impl SocketAcceptor {
    /// Create a new SocketAcceptor
    ///
    /// # Arguments
    /// * `listener` - UCX Listener for accepting incoming connections
    /// * `client_registry` - Registry for managing client endpoints (LRU cache)
    /// * `worker` - UCX Worker for accepting connections
    /// * `node_id` - Node identifier for logging
    pub fn new(
        listener: Listener,
        client_registry: Rc<ClientRegistry>,
        worker: Rc<Worker>,
        node_id: String,
    ) -> Self {
        Self {
            listener: RefCell::new(Some(listener)),
            client_registry,
            worker,
            node_id,
        }
    }

    /// Spawn accept loop as a background task
    ///
    /// The accept loop runs indefinitely, accepting incoming client connections
    /// and registering them with the ClientRegistry.
    pub fn spawn_accept_loop(self: Rc<Self>) {
        pluvio_runtime::spawn_with_name(
            async move {
                self.run_accept_loop().await;
            },
            "socket_accept_loop".to_string(),
        );
    }

    /// Internal accept loop implementation
    ///
    /// This is the core logic moved from benchfsd_mpi.rs.
    /// Continuously accepts connections and registers client endpoints.
    async fn run_accept_loop(&self) {
        let mut listener = self.listener.borrow_mut().take().unwrap();
        let mut connection_count = 0u64;
        let mut consecutive_errors = 0u32;
        const MAX_CONSECUTIVE_ERRORS: u32 = 10;

        tracing::info!("Accept loop started for node {}", self.node_id);

        loop {
            // Wait for incoming connection
            let conn_request = listener.next().await;
            connection_count += 1;

            tracing::debug!("Received connection request #{}", connection_count);

            // Accept the connection to create an endpoint
            match self.worker.accept(conn_request).await {
                Ok(endpoint) => {
                    consecutive_errors = 0; // Reset error counter on success
                    let endpoint = Rc::new(endpoint);

                    // Generate client_id from connection count (u32)
                    let client_id = connection_count as u32;

                    tracing::info!(
                        "Accepted connection from client {} (total: {})",
                        client_id,
                        connection_count
                    );

                    // CRITICAL: Wait for endpoint to be fully established across all UCX transports
                    // After accept(), the endpoint may not have completed wireup on all lanes
                    // (especially rc_mlx5). Give it time to complete before attempting stream operations.
                    // This delay allows UCX to:
                    // 1. Complete InfiniBand RC endpoint wireup (if rc_mlx5 is in UCX_TLS)
                    // 2. Exchange remote endpoint addresses for all active lanes
                    // 3. Establish ready-to-use bi-directional communication channels
                    //
                    // Without this delay, stream_send() may fail with:
                    // - UCS_ERR_UNREACHABLE: no remote ep address for lane[N]
                    // - UCS_ERR_CONNECTION_RESET: connection closed during incomplete wireup
                    //
                    // Note: Increased to 50ms as 10ms was insufficient in some HPC environments
                    // where InfiniBand RC wireup takes longer due to network latency or load.
                    futures_timer::Delay::new(std::time::Duration::from_millis(50)).await;

                    // Send client_id to client via stream handshake
                    tracing::debug!("About to send client_id {} via stream_send", client_id);
                    let client_id_bytes = client_id.to_ne_bytes();
                    match endpoint.stream_send(&client_id_bytes).await {
                        Ok(_) => {
                            tracing::debug!("Sent client_id {} to client", client_id);
                        }
                        Err(e) => {
                            tracing::error!("Failed to send client_id to client: {:?}", e);
                            consecutive_errors += 1;
                            continue;
                        }
                    }

                    // Register the client endpoint
                    if let Some(evicted_id) = self.client_registry.register(client_id, endpoint) {
                        tracing::warn!("Evicted client {} to make room for {}", evicted_id, client_id);
                    }
                }
                Err(e) => {
                    consecutive_errors += 1;
                    tracing::error!(
                        "Failed to accept connection (consecutive errors: {}/{}): {:?}",
                        consecutive_errors,
                        MAX_CONSECUTIVE_ERRORS,
                        e
                    );

                    if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                        tracing::error!(
                            "Accept loop encountered {} consecutive errors, terminating",
                            MAX_CONSECUTIVE_ERRORS
                        );
                        break;
                    }
                }
            }
        }

        tracing::warn!("Accept loop terminated for node {}", self.node_id);
    }
}
