use crate::rpc::RpcError;
///! WorkerAddress registry using shared filesystem
///!
///! This module provides a mechanism to exchange UCX WorkerAddresses between nodes
///! using a shared filesystem. This approach avoids the epoll_wait overhead of
///! socket_bind-based connections when ucp_worker_progress is called frequently.
use std::fs;
use std::path::{Path, PathBuf};

/// Registry for storing and retrieving UCX WorkerAddresses via shared filesystem
pub struct WorkerAddressRegistry {
    /// Directory for storing worker address files
    registry_dir: PathBuf,
}

impl WorkerAddressRegistry {
    /// Create a new WorkerAddress registry
    ///
    /// # Arguments
    /// * `registry_dir` - Shared filesystem directory for address exchange
    ///
    /// # Example
    /// ```ignore
    /// let registry = WorkerAddressRegistry::new("/tmp/benchfs/worker_addrs")?;
    /// ```
    pub fn new<P: AsRef<Path>>(registry_dir: P) -> Result<Self, RpcError> {
        let registry_dir = registry_dir.as_ref().to_path_buf();

        // Create registry directory if it doesn't exist
        fs::create_dir_all(&registry_dir).map_err(|e| {
            RpcError::ConnectionError(format!(
                "Failed to create registry directory {:?}: {}",
                registry_dir, e
            ))
        })?;

        Ok(Self { registry_dir })
    }

    /// Register this worker's address in the shared filesystem
    ///
    /// # Arguments
    /// * `node_id` - Unique identifier for this node
    /// * `address_bytes` - UCX WorkerAddress as byte slice
    pub fn register(&self, node_id: &str, address_bytes: &[u8]) -> Result<(), RpcError> {
        let file_path = self.address_file_path(node_id);

        // Write to file atomically
        fs::write(&file_path, address_bytes).map_err(|e| {
            RpcError::ConnectionError(format!(
                "Failed to write worker address for {}: {}",
                node_id, e
            ))
        })?;

        tracing::info!(
            "Registered worker address for {} ({} bytes) at {:?}",
            node_id,
            address_bytes.len(),
            file_path
        );

        Ok(())
    }

    /// Lookup a worker address from the shared filesystem
    ///
    /// # Arguments
    /// * `node_id` - Node identifier to lookup
    ///
    /// # Returns
    /// The WorkerAddress bytes for the specified node, or an error if not found
    pub fn lookup(&self, node_id: &str) -> Result<Vec<u8>, RpcError> {
        let file_path = self.address_file_path(node_id);

        // Check if file exists
        if !file_path.exists() {
            return Err(RpcError::ConnectionError(format!(
                "Worker address file not found for node {}: {:?}",
                node_id, file_path
            )));
        }

        // Read address bytes from file
        let addr_bytes = fs::read(&file_path).map_err(|e| {
            RpcError::ConnectionError(format!(
                "Failed to read worker address for {}: {}",
                node_id, e
            ))
        })?;

        tracing::debug!(
            "Looked up worker address for {} ({} bytes)",
            node_id,
            addr_bytes.len()
        );

        Ok(addr_bytes)
    }

    /// Try to lookup a worker address, returning None if not found
    pub fn try_lookup(&self, node_id: &str) -> Option<Vec<u8>> {
        self.lookup(node_id).ok()
    }

    /// List all registered node IDs
    pub fn list_nodes(&self) -> Result<Vec<String>, RpcError> {
        let entries = fs::read_dir(&self.registry_dir).map_err(|e| {
            RpcError::ConnectionError(format!("Failed to read registry directory: {}", e))
        })?;

        let mut nodes = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|e| {
                RpcError::ConnectionError(format!("Failed to read directory entry: {}", e))
            })?;

            let file_name = entry.file_name();
            if let Some(name) = file_name.to_str() {
                if name.ends_with(".addr") {
                    // Remove .addr extension
                    let node_id = name.trim_end_matches(".addr");
                    nodes.push(node_id.to_string());
                }
            }
        }

        Ok(nodes)
    }

    /// Wait for a worker address to become available
    ///
    /// This will poll the filesystem until the address file appears or timeout occurs.
    ///
    /// # Arguments
    /// * `node_id` - Node identifier to wait for
    /// * `timeout_secs` - Maximum time to wait in seconds (0 = no timeout)
    pub async fn wait_for(&self, node_id: &str, timeout_secs: u64) -> Result<Vec<u8>, RpcError> {
        use std::time::{Duration, Instant};

        let start = Instant::now();
        let timeout = if timeout_secs > 0 {
            Some(Duration::from_secs(timeout_secs))
        } else {
            None
        };

        let mut check_count = 0;
        loop {
            // Try to lookup address
            if let Ok(address) = self.lookup(node_id) {
                return Ok(address);
            }

            // Check timeout
            if let Some(timeout_duration) = timeout {
                if start.elapsed() > timeout_duration {
                    return Err(RpcError::Timeout);
                }
            }

            // Use short blocking sleep
            // Note: We use std::thread::sleep instead of async Delay because:
            // 1. This function is called within UCX connection operations that block the runtime
            // 2. Async delays cannot be polled when the runtime event loop is blocked
            // 3. Short sleep intervals (10ms) minimize impact on UCX progress
            std::thread::sleep(Duration::from_millis(10));
            check_count += 1;

            // Log every 100 checks (= 1 second at 10ms intervals)
            if check_count % 100 == 0 {
                tracing::debug!(
                    "Still waiting for worker address for {} (elapsed: {:?})",
                    node_id,
                    start.elapsed()
                );
            }
        }
    }

    /// Remove a worker address registration
    pub fn unregister(&self, node_id: &str) -> Result<(), RpcError> {
        let file_path = self.address_file_path(node_id);

        if file_path.exists() {
            fs::remove_file(&file_path).map_err(|e| {
                RpcError::ConnectionError(format!(
                    "Failed to remove worker address for {}: {}",
                    node_id, e
                ))
            })?;

            tracing::info!("Unregistered worker address for {}", node_id);
        }

        Ok(())
    }

    /// Clear all worker address registrations
    pub fn clear_all(&self) -> Result<(), RpcError> {
        let entries = fs::read_dir(&self.registry_dir).map_err(|e| {
            RpcError::ConnectionError(format!("Failed to read registry directory: {}", e))
        })?;

        for entry in entries {
            let entry = entry.map_err(|e| {
                RpcError::ConnectionError(format!("Failed to read directory entry: {}", e))
            })?;

            if entry.path().extension().and_then(|s| s.to_str()) == Some("addr") {
                fs::remove_file(entry.path()).map_err(|e| {
                    RpcError::ConnectionError(format!(
                        "Failed to remove file {:?}: {}",
                        entry.path(),
                        e
                    ))
                })?;
            }
        }

        tracing::info!("Cleared all worker address registrations");
        Ok(())
    }

    /// Register Stream RPC port for a node
    ///
    /// # Arguments
    /// * `node_id` - Unique identifier for this node
    /// * `port` - Stream RPC port number
    pub fn register_stream_port(&self, node_id: &str, port: u16) -> Result<(), RpcError> {
        let file_path = self.stream_port_file_path(node_id);

        // Write port as string to file
        fs::write(&file_path, port.to_string()).map_err(|e| {
            RpcError::ConnectionError(format!(
                "Failed to write stream port for {}: {}",
                node_id, e
            ))
        })?;

        tracing::info!(
            "Registered stream RPC port {} for {} at {:?}",
            port,
            node_id,
            file_path
        );

        Ok(())
    }

    /// Lookup Stream RPC port for a node
    ///
    /// # Arguments
    /// * `node_id` - Node identifier to lookup
    ///
    /// # Returns
    /// The Stream RPC port for the specified node, or an error if not found
    pub fn lookup_stream_port(&self, node_id: &str) -> Result<u16, RpcError> {
        let file_path = self.stream_port_file_path(node_id);

        // Check if file exists
        if !file_path.exists() {
            return Err(RpcError::ConnectionError(format!(
                "Stream port file not found for node {}: {:?}",
                node_id, file_path
            )));
        }

        // Read port from file
        let port_str = fs::read_to_string(&file_path).map_err(|e| {
            RpcError::ConnectionError(format!("Failed to read stream port for {}: {}", node_id, e))
        })?;

        let port = port_str.trim().parse::<u16>().map_err(|e| {
            RpcError::ConnectionError(format!(
                "Failed to parse stream port for {}: {}",
                node_id, e
            ))
        })?;

        tracing::debug!("Looked up stream RPC port {} for {}", port, node_id);

        Ok(port)
    }

    /// Wait for Stream RPC port to become available
    ///
    /// # Arguments
    /// * `node_id` - Node identifier to wait for
    /// * `timeout_secs` - Maximum time to wait in seconds (0 = no timeout)
    pub async fn wait_for_stream_port(
        &self,
        node_id: &str,
        timeout_secs: u64,
    ) -> Result<u16, RpcError> {
        use std::time::{Duration, Instant};

        let start = Instant::now();
        let timeout = if timeout_secs > 0 {
            Some(Duration::from_secs(timeout_secs))
        } else {
            None
        };

        let mut check_count = 0;
        loop {
            // Try to lookup port
            if let Ok(port) = self.lookup_stream_port(node_id) {
                return Ok(port);
            }

            // Check timeout
            if let Some(timeout_duration) = timeout {
                if start.elapsed() > timeout_duration {
                    return Err(RpcError::Timeout);
                }
            }

            // Use short blocking sleep
            std::thread::sleep(Duration::from_millis(10));
            check_count += 1;

            // Log every 100 checks (= 1 second at 10ms intervals)
            if check_count % 100 == 0 {
                tracing::debug!(
                    "Still waiting for stream port for {} (elapsed: {:?})",
                    node_id,
                    start.elapsed()
                );
            }
        }
    }

    /// Get the file path for a node's worker address
    fn address_file_path(&self, node_id: &str) -> PathBuf {
        self.registry_dir.join(format!("{}.addr", node_id))
    }

    /// Get the file path for a node's Stream RPC port
    fn stream_port_file_path(&self, node_id: &str) -> PathBuf {
        self.registry_dir.join(format!("{}.stream_port", node_id))
    }

    /// Register Stream RPC hostname for a node
    ///
    /// # Arguments
    /// * `node_id` - Unique identifier for this node
    /// * `hostname` - Hostname or IP address that can be resolved to connect to this node
    pub fn register_stream_hostname(&self, node_id: &str, hostname: &str) -> Result<(), RpcError> {
        let file_path = self.stream_hostname_file_path(node_id);

        // Write hostname as string to file
        fs::write(&file_path, hostname).map_err(|e| {
            RpcError::ConnectionError(format!(
                "Failed to write stream hostname for {}: {}",
                node_id, e
            ))
        })?;

        tracing::info!(
            "Registered stream RPC hostname {} for {} at {:?}",
            hostname,
            node_id,
            file_path
        );

        Ok(())
    }

    /// Lookup Stream RPC hostname for a node
    ///
    /// # Arguments
    /// * `node_id` - Node identifier to lookup
    ///
    /// # Returns
    /// The Stream RPC hostname for the specified node, or an error if not found
    pub fn lookup_stream_hostname(&self, node_id: &str) -> Result<String, RpcError> {
        let file_path = self.stream_hostname_file_path(node_id);

        // Check if file exists
        if !file_path.exists() {
            return Err(RpcError::ConnectionError(format!(
                "Stream hostname file not found for node {}: {:?}",
                node_id, file_path
            )));
        }

        // Read hostname from file
        let hostname = fs::read_to_string(&file_path).map_err(|e| {
            RpcError::ConnectionError(format!(
                "Failed to read stream hostname for {}: {}",
                node_id, e
            ))
        })?;

        let hostname = hostname.trim().to_string();
        tracing::debug!("Looked up stream RPC hostname {} for {}", hostname, node_id);

        Ok(hostname)
    }

    /// Wait for Stream RPC hostname to become available
    ///
    /// # Arguments
    /// * `node_id` - Node identifier to wait for
    /// * `timeout_secs` - Maximum time to wait in seconds (0 = no timeout)
    pub async fn wait_for_stream_hostname(
        &self,
        node_id: &str,
        timeout_secs: u64,
    ) -> Result<String, RpcError> {
        use std::time::{Duration, Instant};

        let start = Instant::now();
        let timeout = if timeout_secs > 0 {
            Some(Duration::from_secs(timeout_secs))
        } else {
            None
        };

        let mut check_count = 0;
        loop {
            // Try to lookup hostname
            if let Ok(hostname) = self.lookup_stream_hostname(node_id) {
                return Ok(hostname);
            }

            // Check timeout
            if let Some(timeout_duration) = timeout {
                if start.elapsed() > timeout_duration {
                    return Err(RpcError::Timeout);
                }
            }

            // Use short blocking sleep
            std::thread::sleep(Duration::from_millis(10));
            check_count += 1;

            // Log every 100 checks (= 1 second at 10ms intervals)
            if check_count % 100 == 0 {
                tracing::debug!(
                    "Still waiting for stream hostname for {} (elapsed: {:?})",
                    node_id,
                    start.elapsed()
                );
            }
        }
    }

    /// Get the file path for a node's Stream RPC hostname
    fn stream_hostname_file_path(&self, node_id: &str) -> PathBuf {
        self.registry_dir
            .join(format!("{}.stream_hostname", node_id))
    }

    /// Register AM RPC hostname for a node
    ///
    /// # Arguments
    /// * `node_id` - Unique identifier for this node
    /// * `hostname` - Hostname or IP address that can be resolved to connect to this node
    pub fn register_am_hostname(&self, node_id: &str, hostname: &str) -> Result<(), RpcError> {
        let file_path = self.am_hostname_file_path(node_id);

        // Write hostname as string to file
        fs::write(&file_path, hostname).map_err(|e| {
            RpcError::ConnectionError(format!(
                "Failed to write AM hostname for {}: {}",
                node_id, e
            ))
        })?;

        tracing::info!(
            "Registered AM RPC hostname {} for {} at {:?}",
            hostname,
            node_id,
            file_path
        );

        Ok(())
    }

    /// Register AM RPC port for a node
    ///
    /// # Arguments
    /// * `node_id` - Unique identifier for this node
    /// * `port` - AM RPC port number
    pub fn register_am_port(&self, node_id: &str, port: u16) -> Result<(), RpcError> {
        let file_path = self.am_port_file_path(node_id);

        // Write port as string to file
        fs::write(&file_path, port.to_string()).map_err(|e| {
            RpcError::ConnectionError(format!(
                "Failed to write AM port for {}: {}",
                node_id, e
            ))
        })?;

        tracing::info!(
            "Registered AM RPC port {} for {} at {:?}",
            port,
            node_id,
            file_path
        );

        Ok(())
    }

    /// Lookup AM RPC hostname for a node
    ///
    /// # Arguments
    /// * `node_id` - Node identifier to lookup
    ///
    /// # Returns
    /// The AM RPC hostname for the specified node, or an error if not found
    pub fn lookup_am_hostname(&self, node_id: &str) -> Result<String, RpcError> {
        let file_path = self.am_hostname_file_path(node_id);

        // Check if file exists
        if !file_path.exists() {
            return Err(RpcError::ConnectionError(format!(
                "AM hostname file not found for node {}: {:?}",
                node_id, file_path
            )));
        }

        // Read hostname from file
        let hostname = fs::read_to_string(&file_path).map_err(|e| {
            RpcError::ConnectionError(format!(
                "Failed to read AM hostname for {}: {}",
                node_id, e
            ))
        })?;

        let hostname = hostname.trim().to_string();
        tracing::debug!("Looked up AM RPC hostname {} for {}", hostname, node_id);

        Ok(hostname)
    }

    /// Lookup AM RPC port for a node
    ///
    /// # Arguments
    /// * `node_id` - Node identifier to lookup
    ///
    /// # Returns
    /// The AM RPC port for the specified node, or an error if not found
    pub fn lookup_am_port(&self, node_id: &str) -> Result<u16, RpcError> {
        let file_path = self.am_port_file_path(node_id);

        // Check if file exists
        if !file_path.exists() {
            return Err(RpcError::ConnectionError(format!(
                "AM port file not found for node {}: {:?}",
                node_id, file_path
            )));
        }

        // Read port from file
        let port_str = fs::read_to_string(&file_path).map_err(|e| {
            RpcError::ConnectionError(format!("Failed to read AM port for {}: {}", node_id, e))
        })?;

        let port = port_str.trim().parse::<u16>().map_err(|e| {
            RpcError::ConnectionError(format!(
                "Failed to parse AM port for {}: {}",
                node_id, e
            ))
        })?;

        tracing::debug!("Looked up AM RPC port {} for {}", port, node_id);

        Ok(port)
    }

    /// Wait for AM RPC hostname to become available
    ///
    /// # Arguments
    /// * `node_id` - Node identifier to wait for
    /// * `timeout_secs` - Maximum time to wait in seconds (0 = no timeout)
    pub async fn wait_for_am_hostname(
        &self,
        node_id: &str,
        timeout_secs: u64,
    ) -> Result<String, RpcError> {
        use std::time::{Duration, Instant};

        let start = Instant::now();
        let timeout = if timeout_secs > 0 {
            Some(Duration::from_secs(timeout_secs))
        } else {
            None
        };

        let mut check_count = 0;
        loop {
            // Try to lookup hostname
            if let Ok(hostname) = self.lookup_am_hostname(node_id) {
                return Ok(hostname);
            }

            // Check timeout
            if let Some(timeout_duration) = timeout {
                if start.elapsed() > timeout_duration {
                    return Err(RpcError::Timeout);
                }
            }

            // Use short blocking sleep
            std::thread::sleep(Duration::from_millis(10));
            check_count += 1;

            // Log every 100 checks (= 1 second at 10ms intervals)
            if check_count % 100 == 0 {
                tracing::debug!(
                    "Still waiting for AM hostname for {} (elapsed: {:?})",
                    node_id,
                    start.elapsed()
                );
            }
        }
    }

    /// Wait for AM RPC port to become available
    ///
    /// # Arguments
    /// * `node_id` - Node identifier to wait for
    /// * `timeout_secs` - Maximum time to wait in seconds (0 = no timeout)
    pub async fn wait_for_am_port(
        &self,
        node_id: &str,
        timeout_secs: u64,
    ) -> Result<u16, RpcError> {
        use std::time::{Duration, Instant};

        let start = Instant::now();
        let timeout = if timeout_secs > 0 {
            Some(Duration::from_secs(timeout_secs))
        } else {
            None
        };

        let mut check_count = 0;
        loop {
            // Try to lookup port
            if let Ok(port) = self.lookup_am_port(node_id) {
                return Ok(port);
            }

            // Check timeout
            if let Some(timeout_duration) = timeout {
                if start.elapsed() > timeout_duration {
                    return Err(RpcError::Timeout);
                }
            }

            // Use short blocking sleep
            std::thread::sleep(Duration::from_millis(10));
            check_count += 1;

            // Log every 100 checks (= 1 second at 10ms intervals)
            if check_count % 100 == 0 {
                tracing::debug!(
                    "Still waiting for AM port for {} (elapsed: {:?})",
                    node_id,
                    start.elapsed()
                );
            }
        }
    }

    /// Get the file path for a node's AM RPC hostname
    fn am_hostname_file_path(&self, node_id: &str) -> PathBuf {
        self.registry_dir
            .join(format!("{}.am_hostname", node_id))
    }

    /// Get the file path for a node's AM RPC port
    fn am_port_file_path(&self, node_id: &str) -> PathBuf {
        self.registry_dir.join(format!("{}.am_port", node_id))
    }

    /// Get the registry directory path
    pub fn registry_dir(&self) -> &Path {
        &self.registry_dir
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_registry_creation() {
        let temp_dir = TempDir::new().unwrap();
        let registry = WorkerAddressRegistry::new(temp_dir.path()).unwrap();
        assert_eq!(registry.registry_dir(), temp_dir.path());
    }

    #[test]
    fn test_list_nodes_empty() {
        let temp_dir = TempDir::new().unwrap();
        let registry = WorkerAddressRegistry::new(temp_dir.path()).unwrap();
        let nodes = registry.list_nodes().unwrap();
        assert_eq!(nodes.len(), 0);
    }

    #[test]
    fn test_address_file_path() {
        let temp_dir = TempDir::new().unwrap();
        let registry = WorkerAddressRegistry::new(temp_dir.path()).unwrap();
        let path = registry.address_file_path("node1");
        assert_eq!(path, temp_dir.path().join("node1.addr"));
    }

    // Note: Full integration tests with actual WorkerAddress require UCX context
    // which is not available in unit tests. Use integration tests instead.
}
