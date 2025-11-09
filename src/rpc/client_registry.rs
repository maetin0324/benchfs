//! Client endpoint registry with LRU cache
//!
//! This module manages persistent client connections using an LRU cache.
//! When a client connects via socket, it sends its unique client_id,
//! and the server maintains a mapping from client_id to endpoint.
//! When the cache is full, the least recently used entry is evicted.

use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::rc::Rc;
use std::time::Instant;

use pluvio_ucx::endpoint::Endpoint;

/// Information about a registered client
pub struct ClientInfo {
    /// Unique client identifier (u32 for efficient header packing)
    pub client_id: u32,
    /// UCX endpoint for communication
    pub endpoint: Rc<Endpoint>,
    /// Last access time for LRU eviction
    pub last_used: Instant,
}

impl ClientInfo {
    /// Create a new ClientInfo
    pub fn new(client_id: u32, endpoint: Rc<Endpoint>) -> Self {
        Self {
            client_id,
            endpoint,
            last_used: Instant::now(),
        }
    }

    /// Update last_used timestamp
    pub fn touch(&mut self) {
        self.last_used = Instant::now();
    }
}

/// Client registry with LRU eviction policy
///
/// This registry maintains a mapping from client_id to endpoint.
/// When the cache reaches max_size, the least recently used entry is evicted.
pub struct ClientRegistry {
    /// Maximum number of clients to cache
    max_size: usize,
    /// Map from client_id (u32) to ClientInfo
    clients: RefCell<HashMap<u32, ClientInfo>>,
    /// LRU queue (front = most recently used, back = least recently used)
    lru_queue: RefCell<VecDeque<u32>>,
}

impl ClientRegistry {
    /// Create a new client registry
    ///
    /// # Arguments
    /// * `max_size` - Maximum number of clients to cache
    pub fn new(max_size: usize) -> Self {
        Self {
            max_size,
            clients: RefCell::new(HashMap::new()),
            lru_queue: RefCell::new(VecDeque::new()),
        }
    }

    /// Register or update a client
    ///
    /// If the client already exists, update its endpoint and mark as recently used.
    /// If the cache is full, evict the least recently used client.
    ///
    /// # Arguments
    /// * `client_id` - Unique client identifier (u32)
    /// * `endpoint` - UCX endpoint for this client
    ///
    /// # Returns
    /// * `Some(client_id)` - The client_id that was evicted (if any)
    /// * `None` - No eviction occurred
    pub fn register(&self, client_id: u32, endpoint: Rc<Endpoint>) -> Option<u32> {
        let mut clients = self.clients.borrow_mut();
        let mut lru_queue = self.lru_queue.borrow_mut();

        // If client already exists, update it and move to front
        if clients.contains_key(&client_id) {
            // Remove from current position in LRU queue
            lru_queue.retain(|id| *id != client_id);
            // Add to front (most recently used)
            lru_queue.push_front(client_id);
            // Update client info
            if let Some(info) = clients.get_mut(&client_id) {
                info.endpoint = endpoint;
                info.touch();
            }
            tracing::debug!("Updated existing client: {}", client_id);
            return None;
        }

        // Check if we need to evict
        let evicted = if clients.len() >= self.max_size {
            // Evict least recently used (back of queue)
            if let Some(evicted_id) = lru_queue.pop_back() {
                if let Some(evicted_info) = clients.remove(&evicted_id) {
                    tracing::info!(
                        "Evicted client {} (LRU), cache full ({}/{})",
                        evicted_id,
                        clients.len(),
                        self.max_size
                    );
                    // TODO: Send close notification to evicted client
                    drop(evicted_info.endpoint); // Drop endpoint explicitly
                    Some(evicted_id)
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        // Register new client
        let info = ClientInfo::new(client_id, endpoint);
        clients.insert(client_id, info);
        lru_queue.push_front(client_id);

        tracing::info!(
            "Registered new client: {} (cache: {}/{})",
            client_id,
            clients.len(),
            self.max_size
        );

        evicted
    }

    /// Get endpoint for a client
    ///
    /// If the client exists, mark it as recently used and return its endpoint.
    ///
    /// # Arguments
    /// * `client_id` - Client identifier (u32)
    ///
    /// # Returns
    /// * `Some(endpoint)` - Endpoint if client is registered
    /// * `None` - Client not found
    pub fn get(&self, client_id: u32) -> Option<Rc<Endpoint>> {
        let mut clients = self.clients.borrow_mut();
        let mut lru_queue = self.lru_queue.borrow_mut();

        if let Some(info) = clients.get_mut(&client_id) {
            // Update last_used
            info.touch();

            // Move to front of LRU queue
            lru_queue.retain(|id| *id != client_id);
            lru_queue.push_front(client_id);

            tracing::debug!("Found client: {}", client_id);
            Some(info.endpoint.clone())
        } else {
            tracing::debug!("Client not found: {}", client_id);
            None
        }
    }

    /// Remove a client from the registry
    ///
    /// # Arguments
    /// * `client_id` - Client identifier (u32)
    ///
    /// # Returns
    /// * `true` - Client was removed
    /// * `false` - Client not found
    pub fn remove(&self, client_id: u32) -> bool {
        let mut clients = self.clients.borrow_mut();
        let mut lru_queue = self.lru_queue.borrow_mut();

        if clients.remove(&client_id).is_some() {
            lru_queue.retain(|id| *id != client_id);
            tracing::info!("Removed client: {}", client_id);
            true
        } else {
            tracing::debug!("Client not found for removal: {}", client_id);
            false
        }
    }

    /// Check if a client is registered
    pub fn contains(&self, client_id: u32) -> bool {
        self.clients.borrow().contains_key(&client_id)
    }

    /// Get the number of registered clients
    pub fn len(&self) -> usize {
        self.clients.borrow().len()
    }

    /// Check if the registry is empty
    pub fn is_empty(&self) -> bool {
        self.clients.borrow().is_empty()
    }

    /// Get all registered client IDs
    pub fn client_ids(&self) -> Vec<u32> {
        self.clients.borrow().keys().copied().collect()
    }

    /// Clear all clients from the registry
    pub fn clear(&self) {
        let mut clients = self.clients.borrow_mut();
        let mut lru_queue = self.lru_queue.borrow_mut();

        let count = clients.len();
        clients.clear();
        lru_queue.clear();

        tracing::info!("Cleared {} clients from registry", count);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: Full tests require UCX setup and are not suitable for unit tests.
    // These tests verify the basic data structure logic.

    #[test]
    fn test_registry_creation() {
        let registry = ClientRegistry::new(10);
        assert_eq!(registry.len(), 0);
        assert!(registry.is_empty());
        assert_eq!(registry.max_size, 10);
    }

    #[test]
    fn test_basic_operations() {
        let registry = ClientRegistry::new(10);

        // Initially empty
        assert_eq!(registry.len(), 0);
        assert!(!registry.contains(1));

        // After operations (can't test without real endpoint)
        assert_eq!(registry.client_ids().len(), 0);
    }

    #[test]
    fn test_cache_size_limit() {
        let registry = ClientRegistry::new(3);
        assert_eq!(registry.max_size, 3);
        // Further tests would require mock endpoints
    }
}
