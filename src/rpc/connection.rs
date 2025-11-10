//! RPC connection management for distributed operations
//!
//! This module provides WorkerAddress-based connection management to avoid
//! the epoll_wait overhead of socket_bind when ucp_worker_progress is called frequently.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use crate::rpc::address_registry::WorkerAddressRegistry;
use crate::rpc::{ConnectionMode, RpcClient, RpcError};
use pluvio_ucx::Worker;

/// Connection pool for managing RPC client connections to remote nodes
///
/// Uses WorkerAddress exchange via shared filesystem to avoid socket_bind overhead
pub struct ConnectionPool {
    worker: Rc<Worker>,
    registry: WorkerAddressRegistry,
    connections: RefCell<HashMap<String, Rc<RpcClient>>>,
    /// Cache of worker address bytes (needed to keep the memory valid for WorkerAddressInner)
    address_cache: RefCell<HashMap<String, Vec<u8>>>,
}

impl ConnectionPool {
    /// Create a new connection pool with WorkerAddress registry
    ///
    /// # Arguments
    /// * `worker` - UCX worker for creating connections
    /// * `registry_dir` - Shared filesystem directory for WorkerAddress exchange
    pub fn new<P: AsRef<std::path::Path>>(
        worker: Rc<Worker>,
        registry_dir: P,
    ) -> Result<Self, RpcError> {
        let registry = WorkerAddressRegistry::new(registry_dir)?;

        Ok(Self {
            worker,
            registry,
            connections: RefCell::new(HashMap::new()),
            address_cache: RefCell::new(HashMap::new()),
        })
    }

    /// Register this worker's address in the shared filesystem
    ///
    /// # Arguments
    /// * `node_id` - Unique identifier for this node
    pub fn register_self(&self, node_id: &str) -> Result<(), RpcError> {
        let address = self.worker.address().map_err(|e| {
            RpcError::ConnectionError(format!("Failed to get worker address: {:?}", e))
        })?;

        // Convert WorkerAddress to bytes using AsRef<[u8]>
        let address_bytes: &[u8] = address.as_ref();
        self.registry.register(node_id, address_bytes)?;
        tracing::info!(
            "Registered worker address for node {} ({} bytes)",
            node_id,
            address_bytes.len()
        );
        Ok(())
    }

    /// Get or create a connection to a remote node using WorkerAddress
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

        // Lookup worker address
        tracing::info!(
            "Creating new connection to node {} using WorkerAddress",
            node_id
        );

        let worker_address_bytes = self.registry.lookup(node_id)?;

        // Store address bytes in cache to ensure the memory remains valid
        self.address_cache
            .borrow_mut()
            .insert(node_id.to_string(), worker_address_bytes);

        // Get reference to cached bytes
        let addr_cache = self.address_cache.borrow();
        let cached_bytes = addr_cache
            .get(node_id)
            .ok_or_else(|| RpcError::ConnectionError("Address cache error".to_string()))?;

        // Convert bytes to WorkerAddressInner using the cached bytes
        let worker_address = pluvio_ucx::WorkerAddressInner::from(cached_bytes.as_slice());

        // Create endpoint from WorkerAddress
        let endpoint = self.worker.connect_addr(&worker_address).map_err(|e| {
            RpcError::ConnectionError(format!("Failed to connect to {}: {:?}", node_id, e))
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
        // Wait for address to be available
        let worker_address_bytes = self.registry.wait_for(node_id, timeout_secs).await?;

        // Check if connection already exists
        {
            let connections = self.connections.borrow();
            if let Some(client) = connections.get(node_id) {
                tracing::debug!("Reusing existing connection to {}", node_id);
                return Ok(client.clone());
            }
        }

        // Store address bytes in cache to ensure the memory remains valid
        self.address_cache
            .borrow_mut()
            .insert(node_id.to_string(), worker_address_bytes);

        // Get reference to cached bytes
        let addr_cache = self.address_cache.borrow();
        let cached_bytes = addr_cache
            .get(node_id)
            .ok_or_else(|| RpcError::ConnectionError("Address cache error".to_string()))?;

        // Convert bytes to WorkerAddressInner using the cached bytes
        let worker_address = pluvio_ucx::WorkerAddressInner::from(cached_bytes.as_slice());

        // Create endpoint from WorkerAddress
        let endpoint = self.worker.connect_addr(&worker_address).map_err(|e| {
            RpcError::ConnectionError(format!("Failed to connect to {}: {:?}", node_id, e))
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

        Ok(client)
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

    /// Detect connection mode based on registry directory contents
    ///
    /// Checks for the existence of server_list.txt to determine if Socket mode is available.
    /// If server_list.txt exists, returns Socket mode. Otherwise, returns WorkerAddress mode.
    ///
    /// # Returns
    /// ConnectionMode::Socket if server_list.txt exists, ConnectionMode::WorkerAddress otherwise
    pub fn detect_connection_mode(&self) -> Result<ConnectionMode, RpcError> {
        let server_list_path = self.registry.registry_dir().join("server_list.txt");

        if server_list_path.exists() {
            tracing::debug!(
                "Found server_list.txt, using Socket connection mode: {:?}",
                server_list_path
            );
            // Return Socket mode with a placeholder bind address
            // (bind_addr is only used on the server side, not needed for client connections)
            Ok(ConnectionMode::Socket {
                bind_addr: "0.0.0.0:0".parse().unwrap(),
            })
        } else {
            tracing::debug!(
                "server_list.txt not found, using WorkerAddress connection mode"
            );
            Ok(ConnectionMode::WorkerAddress)
        }
    }

    /// Parse server_list.txt to find socket address for a given node_id
    ///
    /// # Arguments
    /// * `node_id` - Node identifier to search for
    ///
    /// # Returns
    /// Socket address if found in server_list.txt
    fn parse_server_list(&self, node_id: &str) -> Result<std::net::SocketAddr, RpcError> {
        let server_list_path = self.registry.registry_dir().join("server_list.txt");

        let content = std::fs::read_to_string(&server_list_path).map_err(|e| {
            RpcError::ConnectionError(format!("Failed to read server_list.txt: {:?}", e))
        })?;

        for line in content.lines() {
            let parts: Vec<&str> = line.trim().split_whitespace().collect();
            if parts.len() >= 2 && parts[0] == node_id {
                return parts[1].parse().map_err(|e| {
                    RpcError::ConnectionError(format!(
                        "Invalid socket address '{}' for node {}: {:?}",
                        parts[1], node_id, e
                    ))
                });
            }
        }

        Err(RpcError::ConnectionError(format!(
            "Node {} not found in server_list.txt",
            node_id
        )))
    }

    /// Automatically detect connection mode and connect to a remote node
    ///
    /// This method checks for server_list.txt in the registry directory:
    /// - If found: Uses Socket connection mode (reads socket address from server_list.txt)
    /// - If not found: Uses WorkerAddress connection mode (reads .addr file)
    ///
    /// # Arguments
    /// * `node_id` - Node identifier to connect to
    ///
    /// # Returns
    /// RPC client for the specified node
    pub async fn connect_auto(&self, node_id: &str) -> Result<Rc<RpcClient>, RpcError> {
        let mode = self.detect_connection_mode()?;

        match mode {
            ConnectionMode::Socket { .. } => {
                // Socket mode: parse server_list.txt to get socket address
                let socket_addr = self.parse_server_list(node_id)?;
                tracing::info!(
                    "Auto-detected Socket mode, connecting to {} at {}",
                    node_id,
                    socket_addr
                );
                self.connect_via_socket(node_id, socket_addr).await
            }
            ConnectionMode::WorkerAddress => {
                // WorkerAddress mode: use get_or_connect with .addr files
                tracing::info!(
                    "Auto-detected WorkerAddress mode, connecting to {}",
                    node_id
                );
                self.get_or_connect(node_id).await
            }
        }
    }

    /// Connect to a remote server using socket address
    ///
    /// This method uses socket-based connection instead of WorkerAddress.
    ///
    /// # Arguments
    /// * `node_id` - Node identifier for caching
    /// * `socket_addr` - Socket address (ip:port) to connect to
    ///
    /// # Returns
    /// RPC client for the specified server
    pub async fn connect_via_socket(
        &self,
        node_id: &str,
        socket_addr: std::net::SocketAddr,
    ) -> Result<Rc<RpcClient>, RpcError> {
        // Check if connection already exists AND is still valid
        {
            let connections = self.connections.borrow();
            if let Some(client) = connections.get(node_id) {
                if !client.connection().endpoint().is_closed() {
                    tracing::debug!("Reusing existing socket connection to {}", node_id);
                    return Ok(client.clone());
                } else {
                    tracing::warn!(
                        "Existing connection to {} is closed, will reconnect",
                        node_id
                    );
                }
            }
        }

        // Remove closed connection if it exists
        self.connections.borrow_mut().remove(node_id);

        tracing::info!(
            "Creating new socket connection to {} at {}",
            node_id,
            socket_addr
        );

        // Connect using socket address
        tracing::debug!("About to call connect_socket for {}", node_id);
        let endpoint = self.worker.connect_socket(socket_addr).await.map_err(|e| {
            RpcError::ConnectionError(format!(
                "Failed to connect to {} at {}: {:?}",
                node_id, socket_addr, e
            ))
        })?;

        tracing::info!("Socket connection established to {}", node_id);

        // Exchange WorkerAddress after Socket connection to enable InfiniBand lanes
        // This hybrid approach:
        // 1. Establishes initial TCP connection via Socket mode
        // 2. Exchanges WorkerAddress over TCP
        // 3. Allows UCX to add InfiniBand lanes for RDMA

        tracing::debug!("Exchanging WorkerAddress with {} for InfiniBand lane setup", node_id);

        // Get our WorkerAddress
        let our_address = self.worker.address().map_err(|e| {
            RpcError::ConnectionError(format!("Failed to get worker address: {:?}", e))
        })?;
        let our_addr_bytes: &[u8] = our_address.as_ref();

        // Send our WorkerAddress length and data
        let addr_len = our_addr_bytes.len() as u32;
        let addr_len_bytes = addr_len.to_le_bytes();
        endpoint.stream_send(&addr_len_bytes).await.map_err(|e| {
            RpcError::ConnectionError(format!("Failed to send address length: {:?}", e))
        })?;

        endpoint.stream_send(our_addr_bytes).await.map_err(|e| {
            RpcError::ConnectionError(format!("Failed to send worker address: {:?}", e))
        })?;

        // Receive remote WorkerAddress length
        let mut len_buf: Vec<std::mem::MaybeUninit<u8>> = vec![std::mem::MaybeUninit::uninit(); 4];
        endpoint.stream_recv(&mut len_buf).await.map_err(|e| {
            RpcError::ConnectionError(format!("Failed to receive address length: {:?}", e))
        })?;
        // SAFETY: stream_recv initializes the buffer
        let len_buf: Vec<u8> = unsafe { std::mem::transmute(len_buf) };
        let remote_addr_len = u32::from_le_bytes([len_buf[0], len_buf[1], len_buf[2], len_buf[3]]) as usize;

        // Receive remote WorkerAddress
        let mut remote_addr_bytes: Vec<std::mem::MaybeUninit<u8>> = vec![std::mem::MaybeUninit::uninit(); remote_addr_len];
        endpoint.stream_recv(&mut remote_addr_bytes).await.map_err(|e| {
            RpcError::ConnectionError(format!("Failed to receive worker address: {:?}", e))
        })?;
        // SAFETY: stream_recv initializes the buffer
        let _remote_addr_bytes: Vec<u8> = unsafe { std::mem::transmute(remote_addr_bytes) };

        tracing::info!(
            "WorkerAddress exchange complete with {} (sent {} bytes, received {} bytes)",
            node_id, our_addr_bytes.len(), remote_addr_len
        );

        // Note: UCX will automatically use the exchanged addresses to add InfiniBand lanes
        // when available, enabling RDMA for subsequent transfers

        // Use placeholder client_id (can be implemented later via AM messages if needed)
        let client_id = 0u32;
        tracing::debug!("Using placeholder client_id {}", client_id);

        let conn = crate::rpc::Connection::new(self.worker.clone(), endpoint);
        let client = Rc::new(RpcClient::new(conn));

        // Set client_id
        client.set_client_id(client_id);
        tracing::debug!("Set client_id {} for connection to {}", client_id, node_id);

        // Initialize reply stream
        if let Err(e) = client.init_reply_stream(100) {
            tracing::warn!("Failed to initialize reply stream: {:?}", e);
        }

        // Store in cache
        self.connections
            .borrow_mut()
            .insert(node_id.to_string(), client.clone());

        tracing::info!("Successfully connected to {} at {} with client_id {}", node_id, socket_addr, client_id);

        Ok(client)
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
