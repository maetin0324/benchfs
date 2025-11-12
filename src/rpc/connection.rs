//! RPC connection management for distributed operations
//!
//! This module provides socket address-based connection management for RPC operations.
//! Server binds to a socket and publishes hostname:port to shared filesystem.
//! Clients lookup hostname:port and connect using connect_socket().

use std::cell::RefCell;
use std::collections::HashMap;
use std::process::Command;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use crate::rpc::address_registry::WorkerAddressRegistry;
use crate::rpc::stream_client::StreamRpcClient;
use crate::rpc::{RpcClient, RpcError};
use gethostname::gethostname;
use pluvio_ucx::{Context, Worker, listener::Listener};

/// Connection pool for managing RPC client connections to remote nodes
///
/// Uses socket address (hostname:port) exchange via shared filesystem
pub struct ConnectionPool {
    worker: Rc<Worker>,
    context: Arc<Context>,
    registry: WorkerAddressRegistry,
    connections: RefCell<HashMap<String, Rc<RpcClient>>>,
    /// Socket listener for AM RPC (server mode only)
    am_listener: RefCell<Option<Listener>>,
    /// Stream RPC client connections
    stream_connections: RefCell<HashMap<String, Rc<StreamRpcClient>>>,
}

impl ConnectionPool {
    /// Create a new connection pool with socket address registry
    ///
    /// # Arguments
    /// * `worker` - UCX worker for creating connections
    /// * `context` - UCX context for creating Stream connections
    /// * `registry_dir` - Shared filesystem directory for address exchange
    pub fn new<P: AsRef<std::path::Path>>(
        worker: Rc<Worker>,
        context: Arc<Context>,
        registry_dir: P,
    ) -> Result<Self, RpcError> {
        let registry = WorkerAddressRegistry::new(registry_dir)?;

        Ok(Self {
            worker,
            context,
            registry,
            connections: RefCell::new(HashMap::new()),
            am_listener: RefCell::new(None),
            stream_connections: RefCell::new(HashMap::new()),
        })
    }

    /// Bind to a socket and register the address in the shared filesystem
    ///
    /// This method binds to a UCX socket listener and publishes the hostname:port
    /// to the shared filesystem registry for other nodes to discover.
    ///
    /// # Arguments
    /// * `node_id` - Unique identifier for this node
    /// * `listen_addr` - Socket address to bind to (e.g., "0.0.0.0:50051")
    ///
    /// # Returns
    /// The actual socket address that was bound (useful when port 0 was specified)
    pub fn bind_and_register(
        &self,
        node_id: &str,
        listen_addr: std::net::SocketAddr,
    ) -> Result<std::net::SocketAddr, RpcError> {
        // Create socket listener
        let listener = self.worker.create_listener(listen_addr).map_err(|e| {
            RpcError::ConnectionError(format!("Failed to bind socket {}: {:?}", listen_addr, e))
        })?;

        // Get the actual bound address
        let bound_addr = listener.socket_addr().map_err(|e| {
            RpcError::ConnectionError(format!("Failed to get socket address: {:?}", e))
        })?;

        tracing::info!("AM RPC listener bound to {}", bound_addr);

        // Determine hostname/IP for stream registration. Prefer explicit overrides,
        // then interface-based detection, and finally fall back to the system hostname.
        let hostname = Self::determine_stream_hostname();

        // Register WorkerAddress for backward compatibility (needed for registration waiting loop)
        let worker_addr = self.worker.address().map_err(|e| {
            RpcError::ConnectionError(format!("Failed to get worker address: {:?}", e))
        })?;
        let worker_addr_bytes = worker_addr.as_ref();
        self.registry.register(node_id, worker_addr_bytes)?;

        // Register hostname and port separately for socket-based connections
        self.registry.register_stream_hostname(node_id, &hostname)?;
        self.registry
            .register_stream_port(node_id, bound_addr.port())?;

        // Store listener
        *self.am_listener.borrow_mut() = Some(listener);

        tracing::info!(
            "Registered AM RPC address {}:{} for node {}",
            hostname,
            bound_addr.port(),
            node_id
        );

        Ok(bound_addr)
    }

    /// Get the AM RPC listener (for accepting connections in server mode)
    pub fn am_listener(&self) -> bool {
        self.am_listener.borrow().is_some()
    }

    /// Take the listener from the connection pool
    /// This is used to move the listener to the acceptor loop
    pub fn take_listener(&self) -> Option<Listener> {
        self.am_listener.borrow_mut().take()
    }

    /// Get or create a connection to a remote node using socket address
    ///
    /// # Arguments
    /// * `node_id` - Node identifier (must be registered in the registry)
    ///
    /// # Returns
    /// RPC client for the specified node
    pub async fn get_or_connect(&self, node_id: &str) -> Result<Rc<RpcClient>, RpcError> {
        // Check if connection already exists AND is still valid
        {
            let connections = self.connections.borrow();
            if let Some(client) = connections.get(node_id) {
                // Check if the endpoint is still open
                if !client.connection().endpoint().is_closed() {
                    tracing::debug!("Reusing existing connection to {}", node_id);
                    return Ok(client.clone());
                } else {
                    tracing::warn!(
                        "Existing connection to {} is closed, will reconnect",
                        node_id
                    );
                    // Drop the borrow before removing the connection
                }
            }
        }

        // Remove closed connection if it exists
        self.connections.borrow_mut().remove(node_id);

        // Lookup hostname and port from registry
        tracing::info!(
            "Creating new connection to node {} using socket address",
            node_id
        );

        let hostname = self.registry.lookup_stream_hostname(node_id)?;
        let port = self.registry.lookup_stream_port(node_id)?;

        tracing::info!("Connecting to {}:{}", hostname, port);

        // Resolve hostname to SocketAddr using DNS
        use std::net::ToSocketAddrs;
        let addr_string = format!("{}:{}", hostname, port);
        let mut socket_addrs = addr_string.to_socket_addrs().map_err(|e| {
            RpcError::ConnectionError(format!(
                "Failed to resolve hostname {}:{}: {:?}",
                hostname, port, e
            ))
        })?;

        let socket_addr = socket_addrs.next().ok_or_else(|| {
            RpcError::ConnectionError(format!(
                "No IP addresses found for hostname {}:{}",
                hostname, port
            ))
        })?;

        tracing::info!("Resolved {}:{} to {}", hostname, port, socket_addr);

        // Create endpoint using connect_socket() with retries in case the server
        // is still warming up (replaces the eager wait_and_connect() used before).
        const STREAM_CONNECT_MAX_RETRIES: u32 = 10;
        const STREAM_CONNECT_RETRY_DELAY_MS: u64 = 1000;

        let mut last_error: Option<String> = None;
        let mut endpoint_opt = None;

        for attempt in 1..=STREAM_CONNECT_MAX_RETRIES {
            match self.worker.connect_socket(socket_addr).await {
                Ok(endpoint) => {
                    endpoint_opt = Some(endpoint);
                    break;
                }
                Err(e) => {
                    let err_msg = format!("{:?}", e);
                    tracing::warn!(
                        "Failed to connect Stream RPC to {} (node {}) (attempt {}/{}): {:?}",
                        socket_addr,
                        node_id,
                        attempt,
                        STREAM_CONNECT_MAX_RETRIES,
                        e
                    );
                    last_error = Some(err_msg);

                    if attempt == STREAM_CONNECT_MAX_RETRIES {
                        break;
                    }

                    pluvio_timer::sleep(Duration::from_millis(STREAM_CONNECT_RETRY_DELAY_MS)).await;
                }
            }
        }

        let endpoint = endpoint_opt.ok_or_else(|| {
            RpcError::ConnectionError(format!(
                "Failed to connect Stream RPC to {} (node {}) after {} attempts: {}",
                socket_addr,
                node_id,
                STREAM_CONNECT_MAX_RETRIES,
                last_error.unwrap_or_else(|| "unknown error".to_string())
            ))
        })?;

        let conn = crate::rpc::Connection::new(self.worker.clone(), endpoint);
        let client = Rc::new(RpcClient::new(conn));

        // Initialize reply stream
        if let Err(e) = client.init_reply_stream(100) {
            tracing::warn!("Failed to initialize reply stream: {:?}", e);
        }

        // Store in cache
        self.connections
            .borrow_mut()
            .insert(node_id.to_string(), client.clone());

        tracing::info!("Connected to node {} at {}:{}", node_id, hostname, port);

        Ok(client)
    }

    /// Wait for a node to register and then connect
    ///
    /// # Arguments
    /// * `node_id` - Node identifier to wait for
    /// * `timeout_secs` - Maximum time to wait in seconds (0 = no timeout)
    pub async fn wait_and_connect(
        &self,
        node_id: &str,
        timeout_secs: u64,
    ) -> Result<Rc<RpcClient>, RpcError> {
        // Wait for hostname and port to be available
        let _hostname = self
            .registry
            .wait_for_stream_hostname(node_id, timeout_secs)
            .await?;
        let _port = self
            .registry
            .wait_for_stream_port(node_id, timeout_secs)
            .await?;

        // Use get_or_connect() which will lookup and connect
        self.get_or_connect(node_id).await
    }

    /// Get an existing connection without creating a new one
    pub fn get(&self, node_addr: &str) -> Option<Rc<RpcClient>> {
        self.connections.borrow().get(node_addr).cloned()
    }

    /// Remove a connection from the pool
    pub fn disconnect(&self, node_addr: &str) {
        self.connections.borrow_mut().remove(node_addr);
        tracing::info!("Disconnected from {}", node_addr);
    }

    /// Clear all connections
    pub fn clear(&self) {
        let count = self.connections.borrow().len();
        self.connections.borrow_mut().clear();
        tracing::info!("Cleared {} connections", count);
    }

    /// Get the number of active connections
    pub fn connection_count(&self) -> usize {
        self.connections.borrow().len()
    }

    /// Get the registry directory path
    pub fn registry_dir(&self) -> &std::path::Path {
        self.registry.registry_dir()
    }

    /// Get all connected node addresses
    pub fn connected_nodes(&self) -> Vec<String> {
        self.connections.borrow().keys().cloned().collect()
    }

    /// Get the underlying registry
    pub fn registry(&self) -> &WorkerAddressRegistry {
        &self.registry
    }

    /// Get or create a Stream RPC connection to a remote node
    ///
    /// # Arguments
    /// * `node_id` - Node identifier (must be registered in the registry)
    ///
    /// # Returns
    /// Stream RPC client for the specified node
    ///
    /// # Implementation Note
    /// Uses socket-based connection (connect_socket). If Connection Reset errors occur
    /// in HPC environments, the StreamRpcServer implements automatic retry logic to
    /// handle transient connection issues.
    pub async fn get_or_connect_stream(
        &self,
        node_id: &str,
    ) -> Result<Rc<StreamRpcClient>, RpcError> {
        // Check if connection already exists
        {
            let mut needs_remove = false;
            {
                let connections = self.stream_connections.borrow();
                if let Some(client) = connections.get(node_id) {
                    if !client.endpoint().is_closed() {
                        tracing::debug!("Reusing existing Stream RPC connection to {}", node_id);
                        return Ok(client.clone());
                    } else {
                        tracing::warn!(
                            "Existing Stream RPC connection to {} is closed, reconnecting",
                            node_id
                        );
                        needs_remove = true;
                    }
                }
            }
            if needs_remove {
                self.stream_connections.borrow_mut().remove(node_id);
            }
        }

        tracing::info!(
            "Creating new Stream RPC connection to node {} using socket address",
            node_id
        );

        // Lookup hostname and port (same as AM RPC)
        let hostname = self.registry.lookup_stream_hostname(node_id)?;
        let port = self.registry.lookup_stream_port(node_id)?;

        tracing::info!("Connecting Stream RPC to {}:{}", hostname, port);

        // Resolve hostname to SocketAddr using DNS
        use std::net::ToSocketAddrs;
        let addr_string = format!("{}:{}", hostname, port);
        let mut socket_addrs = addr_string.to_socket_addrs().map_err(|e| {
            RpcError::ConnectionError(format!(
                "Failed to resolve Stream RPC hostname {}:{}: {:?}",
                hostname, port, e
            ))
        })?;

        let socket_addr = socket_addrs.next().ok_or_else(|| {
            RpcError::ConnectionError(format!(
                "No IP addresses found for Stream RPC hostname {}:{}",
                hostname, port
            ))
        })?;

        tracing::info!(
            "Resolved Stream RPC {}:{} to {}",
            hostname,
            port,
            socket_addr
        );

        // Retry connecting in case the server-side listener is still warming up.
        const STREAM_CONNECT_MAX_RETRIES: u32 = 5;
        const STREAM_CONNECT_RETRY_DELAY_MS: u64 = 200;

        let mut endpoint_opt = None;
        let mut last_error: Option<String> = None;

        for attempt in 1..=STREAM_CONNECT_MAX_RETRIES {
            match self.worker.connect_socket(socket_addr).await {
                Ok(endpoint) => {
                    endpoint_opt = Some(endpoint);
                    break;
                }
                Err(e) => {
                    let err_string = format!("{:?}", e);
                    tracing::warn!(
                        "Failed to connect Stream RPC to {} (node {}) attempt {}/{}: {:?}",
                        socket_addr,
                        node_id,
                        attempt,
                        STREAM_CONNECT_MAX_RETRIES,
                        e
                    );
                    last_error = Some(err_string);

                    if attempt < STREAM_CONNECT_MAX_RETRIES {
                        pluvio_timer::sleep(Duration::from_millis(STREAM_CONNECT_RETRY_DELAY_MS))
                            .await;
                    }
                }
            }
        }

        let endpoint = endpoint_opt.ok_or_else(|| {
            RpcError::ConnectionError(format!(
                "Failed to connect Stream RPC to {} (node {}) after {} attempts: {}",
                socket_addr,
                node_id,
                STREAM_CONNECT_MAX_RETRIES,
                last_error.unwrap_or_else(|| "unknown error".to_string())
            ))
        })?;

        // Print endpoint debug info
        endpoint.print_to_stderr();
        tracing::info!("Stream RPC client endpoint created for node {}", node_id);

        let client = Rc::new(StreamRpcClient::new(
            endpoint,
            self.worker.clone(),
            self.context.clone(),
        ));

        // Store in cache
        self.stream_connections
            .borrow_mut()
            .insert(node_id.to_string(), client.clone());

        tracing::info!(
            "Stream RPC connection to {} established via socket",
            node_id
        );

        Ok(client)
    }

    /// Wait for a node to register and then create Stream RPC connection
    ///
    /// # Arguments
    /// * `node_id` - Node identifier to wait for
    /// * `timeout_secs` - Maximum time to wait in seconds (0 = no timeout)
    pub async fn wait_and_connect_stream(
        &self,
        node_id: &str,
        timeout_secs: u64,
    ) -> Result<Rc<StreamRpcClient>, RpcError> {
        // Wait for worker address to be available (same as AM RPC)
        let _worker_address_bytes = self.registry.wait_for(node_id, timeout_secs).await?;

        // Create connection using WorkerAddress
        self.get_or_connect_stream(node_id).await
    }
}

impl ConnectionPool {
    fn determine_stream_hostname() -> String {
        if let Ok(explicit) = std::env::var("BENCHFS_STREAM_HOSTNAME") {
            if !explicit.trim().is_empty() {
                return explicit.trim().to_string();
            }
        }

        if let Some(iface_host) = Self::hostname_from_interfaces() {
            return iface_host;
        }

        std::env::var("HOSTNAME").unwrap_or_else(|_| gethostname().to_string_lossy().into_owned())
    }

    fn hostname_from_interfaces() -> Option<String> {
        if let Ok(iface) = std::env::var("BENCHFS_STREAM_INTERFACE") {
            if let Some(ip) = Self::ipv4_for_interface(iface.trim()) {
                tracing::info!(
                    "Using IPv4 {} from BENCHFS_STREAM_INTERFACE={} for Stream RPC",
                    ip,
                    iface
                );
                return Some(ip);
            } else {
                tracing::warn!(
                    "BENCHFS_STREAM_INTERFACE={} set but no IPv4 address was found",
                    iface
                );
            }
        }

        if let Ok(devices) = std::env::var("UCX_NET_DEVICES") {
            for dev in devices.split(',') {
                let dev = dev.trim();
                if dev.is_empty() {
                    continue;
                }

                // UCX syntax like mlx5_0:1 refers to an IB device/port without
                // a direct OS netdev. Ignore those here (mapping them reliably
                // requires additional system introspection). If the value looks
                // like a regular interface name, use it.
                if dev.contains(':') {
                    continue;
                }

                if let Some(ip) = Self::ipv4_for_interface(dev) {
                    tracing::info!(
                        "Using IPv4 {} from UCX_NET_DEVICES entry {} for Stream RPC",
                        ip,
                        dev
                    );
                    return Some(ip);
                }
            }
        }

        None
    }

    fn ipv4_for_interface(interface: &str) -> Option<String> {
        if interface.is_empty() {
            return None;
        }

        let output = Command::new("ip")
            .args(["-o", "-4", "addr", "show", "dev", interface])
            .output()
            .ok()?;

        if !output.status.success() {
            return None;
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        for line in stdout.lines() {
            let mut parts = line.split_whitespace();
            while let Some(token) = parts.next() {
                if token == "inet" {
                    if let Some(addr) = parts.next() {
                        if let Some((ip, _)) = addr.split_once('/') {
                            if !ip.is_empty() {
                                return Some(ip.to_string());
                            }
                        }
                    }
                }
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {

    // Note: These tests require UCX setup and can't run in standard test environment
    // They are here as documentation of the expected behavior

    #[test]
    #[ignore]
    fn test_connection_pool_creation() {
        // This test requires UCX context which is not available in unit tests
        // Use integration tests instead
    }

    #[test]
    fn test_connection_count() {
        // Test basic data structures without UCX
        // (Real tests would need integration test environment)
    }
}
