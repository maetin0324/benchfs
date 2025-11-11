//! RPC connection management for distributed operations
//!
//! This module provides WorkerAddress-based connection management to avoid
//! the epoll_wait overhead of socket_bind when ucp_worker_progress is called frequently.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use crate::rpc::address_registry::WorkerAddressRegistry;
use crate::rpc::stream_client::StreamRpcClient;
use crate::rpc::{RpcClient, RpcError};
use pluvio_ucx::{Context, Worker};

/// Connection pool for managing RPC client connections to remote nodes
///
/// Uses WorkerAddress exchange via shared filesystem to avoid socket_bind overhead
pub struct ConnectionPool {
    worker: Rc<Worker>,
    context: Arc<Context>,
    registry: WorkerAddressRegistry,
    connections: RefCell<HashMap<String, Rc<RpcClient>>>,
    /// Cache of worker address bytes (needed to keep the memory valid for WorkerAddressInner)
    address_cache: RefCell<HashMap<String, Vec<u8>>>,
    /// Stream RPC client connections
    stream_connections: RefCell<HashMap<String, Rc<StreamRpcClient>>>,
}

impl ConnectionPool {
    /// Create a new connection pool with WorkerAddress registry
    ///
    /// # Arguments
    /// * `worker` - UCX worker for creating connections
    /// * `context` - UCX context for creating Stream connections
    /// * `registry_dir` - Shared filesystem directory for WorkerAddress exchange
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
            address_cache: RefCell::new(HashMap::new()),
            stream_connections: RefCell::new(HashMap::new()),
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
    pub async fn get_or_connect_stream(
        &self,
        node_id: &str,
    ) -> Result<Rc<StreamRpcClient>, RpcError> {
        // Check if connection already exists
        {
            let connections = self.stream_connections.borrow();
            if let Some(client) = connections.get(node_id) {
                tracing::debug!("Reusing existing Stream RPC connection to {}", node_id);
                return Ok(client.clone());
            }
        }

        tracing::info!(
            "Creating new Stream RPC connection to node {}",
            node_id
        );

        // Lookup Stream RPC port
        let stream_port = self.registry.lookup_stream_port(node_id)?;

        // Lookup Stream RPC hostname from registry
        // The actual hostname was registered by the server at startup
        let hostname = self.registry.lookup_stream_hostname(node_id)?;

        // Resolve hostname to SocketAddr using ToSocketAddrs
        use std::net::ToSocketAddrs;
        let addr_str = format!("{}:{}", hostname, stream_port);
        let stream_addr = addr_str
            .to_socket_addrs()
            .map_err(|e| {
                RpcError::ConnectionError(format!(
                    "Failed to resolve hostname {} for {}: {}",
                    hostname, node_id, e
                ))
            })?
            .next()
            .ok_or_else(|| {
                RpcError::ConnectionError(format!(
                    "No addresses found for hostname {} (node {})",
                    hostname, node_id
                ))
            })?;

        tracing::debug!(
            "Connecting to Stream RPC server at {} (resolved from {}:{}, port {})",
            stream_addr,
            hostname,
            stream_port,
            stream_port
        );

        // Create Stream connection
        let endpoint = self
            .worker
            .connect_socket(stream_addr)
            .await
            .map_err(|e| {
                RpcError::ConnectionError(format!(
                    "Failed to connect to {} via Stream RPC: {:?}",
                    node_id, e
                ))
            })?;

        let client = Rc::new(StreamRpcClient::new(
            endpoint,
            self.worker.clone(),
            self.context.clone(),
        ));

        // Store in cache
        self.stream_connections
            .borrow_mut()
            .insert(node_id.to_string(), client.clone());

        tracing::info!("Stream RPC connection to {} established", node_id);

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
        // Wait for Stream RPC port to be available
        let _stream_port = self
            .registry
            .wait_for_stream_port(node_id, timeout_secs)
            .await?;

        // Create connection
        self.get_or_connect_stream(node_id).await
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
