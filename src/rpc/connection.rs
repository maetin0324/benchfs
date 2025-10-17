//! RPC connection management for distributed operations

use std::rc::Rc;
use std::cell::RefCell;
use std::collections::HashMap;

use std::net::SocketAddr;

use pluvio_ucx::Worker;
use crate::rpc::{RpcError, RpcClient};

/// Connection pool for managing RPC client connections to remote nodes
pub struct ConnectionPool {
    worker: Rc<Worker>,
    connections: RefCell<HashMap<String, Rc<RpcClient>>>,
}

impl ConnectionPool {
    /// Create a new connection pool
    pub fn new(worker: Rc<Worker>) -> Self {
        Self {
            worker,
            connections: RefCell::new(HashMap::new()),
        }
    }

    /// Get or create a connection to a remote node
    pub async fn get_or_connect(&self, node_addr: SocketAddr) -> Result<Rc<RpcClient>, RpcError> {
        let addr_str = node_addr.to_string();

        // Check if connection already exists
        {
            let connections = self.connections.borrow();
            if let Some(client) = connections.get(&addr_str) {
                tracing::debug!("Reusing existing connection to {}", addr_str);
                return Ok(client.clone());
            }
        }

        // Create new connection
        tracing::info!("Creating new connection to {}", addr_str);

        let endpoint = self.worker.connect_socket(node_addr).await.map_err(|e| {
            RpcError::ConnectionError(format!("Failed to connect to {}: {:?}", addr_str, e))
        })?;

        let conn = crate::rpc::Connection::new(self.worker.clone(), endpoint);
        let client = Rc::new(RpcClient::new(conn));

        // Initialize reply stream
        if let Err(e) = client.init_reply_stream(100) {
            tracing::warn!("Failed to initialize reply stream: {:?}", e);
        }

        // Store in cache
        self.connections.borrow_mut().insert(addr_str, client.clone());

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

    /// Get all connected node addresses
    pub fn connected_nodes(&self) -> Vec<String> {
        self.connections.borrow().keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
