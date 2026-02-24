//! BenchFS server configuration

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Default configuration constants
///
/// This module centralizes all default values used throughout BenchFS.
/// By collecting these constants in one place, we ensure consistency
/// and make it easier to adjust defaults for different deployment scenarios.
pub mod defaults {

    // Storage defaults
    /// Default chunk size: 4MB
    /// Larger than CHFS's 64KB for better RDMA performance with large sequential I/O
    pub const CHUNK_SIZE: usize = 4 * 1024 * 1024; // 4 MB

    /// Alternative chunk size for CHFS compatibility: 64KB
    pub const CHUNK_SIZE_CHFS_COMPAT: usize = 64 * 1024; // 64 KB

    /// Enable io_uring by default for optimal I/O performance
    pub const USE_IOURING: bool = true;

    /// Maximum storage size in GB (0 = unlimited)
    pub const MAX_STORAGE_GB: usize = 0;

    // Network defaults
    /// Connection timeout: 30 seconds
    pub const TIMEOUT_SECS: u64 = 30;

    /// RDMA threshold: 32KB (same as CHFS)
    /// Transfers larger than this use RDMA, smaller use regular RPC
    pub const RDMA_THRESHOLD_BYTES: usize = 32 * 1024; // 32 KB

    /// Default registry directory for worker address management
    pub const fn default_registry_dir() -> &'static str {
        "/tmp/benchfs/worker_addrs"
    }

    // Cache defaults
    /// Metadata cache entries: 1000
    pub const METADATA_CACHE_ENTRIES: usize = 1000;

    /// Chunk cache size: 100MB
    pub const CHUNK_CACHE_MB: usize = 100;

    /// Cache TTL in seconds (0 = no TTL/expiration)
    pub const CACHE_TTL_SECS: u64 = 0;

    // Log level
    /// Default log level
    pub const fn default_log_level() -> &'static str {
        "info"
    }

    // Client-side RPC limits
    /// Maximum concurrent chunk RPCs per client operation
    /// This limits how many chunk read/write RPCs can be in-flight simultaneously
    /// to prevent overwhelming the server when transfer_size > chunk_size.
    /// Default: 64 (NVMe drives thrive on high queue depth; increased from 16)
    pub const MAX_CONCURRENT_CHUNK_RPCS: usize = 64;

    // Stats/diagnostics
    /// Enable detailed timing statistics collection (default: false)
    /// When enabled, collects std::time::Instant measurements for performance analysis
    /// Disable in production benchmarks to avoid measurement overhead
    pub const ENABLE_STATS: bool = false;
}

/// BenchFS server configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// Server node configuration
    pub node: NodeConfig,

    /// Storage configuration
    pub storage: StorageConfig,

    /// Network configuration
    pub network: NetworkConfig,

    /// Cache configuration
    pub cache: CacheConfig,
}

/// Node configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    /// Node ID (unique identifier)
    pub node_id: String,

    /// Data directory
    pub data_dir: PathBuf,

    /// Log level (trace, debug, info, warn, error)
    #[serde(default = "default_log_level")]
    pub log_level: String,

    /// Enable detailed timing statistics collection
    /// When enabled, collects std::time::Instant measurements for performance analysis
    /// Disable in production benchmarks to avoid measurement overhead
    #[serde(default = "default_enable_stats")]
    pub enable_stats: bool,
}

fn default_enable_stats() -> bool {
    defaults::ENABLE_STATS
}

fn default_log_level() -> String {
    defaults::default_log_level().to_string()
}

/// Storage configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Chunk size in bytes (default: 4MB)
    ///
    /// BenchFS uses 4MB chunks by default for optimal RDMA transfer performance.
    /// This is larger than CHFS's default of 64KB, chosen because:
    /// - Better for large sequential I/O workloads
    /// - More efficient RDMA utilization
    /// - Reduced metadata overhead
    ///
    /// Can be configured to 64KB (65536) for CHFS-compatible behavior.
    #[serde(default = "default_chunk_size")]
    pub chunk_size: usize,

    /// Use io_uring for file I/O
    #[serde(default = "default_use_iouring")]
    pub use_iouring: bool,

    /// Maximum storage size in GB (0 = unlimited)
    #[serde(default)]
    pub max_storage_gb: usize,
}

fn default_chunk_size() -> usize {
    defaults::CHUNK_SIZE
}

fn default_use_iouring() -> bool {
    defaults::USE_IOURING
}

/// Network configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkConfig {
    /// Server address to bind (IP:port)
    pub bind_addr: String,

    /// Known peer nodes (for cluster setup)
    #[serde(default)]
    pub peers: Vec<String>,

    /// Connection timeout in seconds
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,

    /// RDMA transfer threshold in bytes (default: 32KB)
    /// Transfers larger than this use RDMA, smaller use regular RPC
    /// Based on CHFS's proven threshold of 32KB
    #[serde(default = "default_rdma_threshold")]
    pub rdma_threshold_bytes: usize,

    /// WorkerAddress registry directory for connection management
    /// This should be a shared filesystem path accessible by all nodes
    #[serde(default = "default_registry_dir")]
    pub registry_dir: PathBuf,
}

fn default_timeout() -> u64 {
    defaults::TIMEOUT_SECS
}

fn default_rdma_threshold() -> usize {
    defaults::RDMA_THRESHOLD_BYTES
}

fn default_registry_dir() -> PathBuf {
    PathBuf::from(defaults::default_registry_dir())
}

/// Cache configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    /// Metadata cache entries (default: 1000)
    #[serde(default = "default_metadata_cache_entries")]
    pub metadata_cache_entries: usize,

    /// Chunk cache size in MB (default: 100MB)
    #[serde(default = "default_chunk_cache_mb")]
    pub chunk_cache_mb: usize,

    /// Cache TTL in seconds (0 = no TTL)
    #[serde(default)]
    pub cache_ttl_secs: u64,
}

fn default_metadata_cache_entries() -> usize {
    defaults::METADATA_CACHE_ENTRIES
}

fn default_chunk_cache_mb() -> usize {
    defaults::CHUNK_CACHE_MB
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            node: NodeConfig {
                node_id: "node1".to_string(),
                data_dir: PathBuf::from("/tmp/benchfs"),
                log_level: default_log_level(),
                enable_stats: default_enable_stats(),
            },
            storage: StorageConfig {
                chunk_size: default_chunk_size(),
                use_iouring: default_use_iouring(),
                max_storage_gb: 0,
            },
            network: NetworkConfig {
                bind_addr: "0.0.0.0:50051".to_string(),
                peers: vec![],
                timeout_secs: default_timeout(),
                rdma_threshold_bytes: default_rdma_threshold(),
                registry_dir: default_registry_dir(),
            },
            cache: CacheConfig {
                metadata_cache_entries: default_metadata_cache_entries(),
                chunk_cache_mb: default_chunk_cache_mb(),
                cache_ttl_secs: 0,
            },
        }
    }
}

impl ServerConfig {
    /// Load configuration from TOML file
    pub fn from_file(path: &str) -> Result<Self, ConfigError> {
        let contents = std::fs::read_to_string(path)
            .map_err(|e| ConfigError::ReadError(format!("Failed to read config file: {}", e)))?;

        let config: ServerConfig = toml::from_str(&contents)
            .map_err(|e| ConfigError::ParseError(format!("Failed to parse config: {}", e)))?;

        config.validate()?;

        Ok(config)
    }

    /// Save configuration to TOML file
    pub fn to_file(&self, path: &str) -> Result<(), ConfigError> {
        let contents = toml::to_string_pretty(self).map_err(|e| {
            ConfigError::SerializeError(format!("Failed to serialize config: {}", e))
        })?;

        std::fs::write(path, contents)
            .map_err(|e| ConfigError::WriteError(format!("Failed to write config file: {}", e)))?;

        Ok(())
    }

    /// Validate configuration
    fn validate(&self) -> Result<(), ConfigError> {
        // Validate node ID
        if self.node.node_id.is_empty() {
            return Err(ConfigError::ValidationError(
                "Node ID cannot be empty".to_string(),
            ));
        }

        // Validate chunk size (must be > 0 and <= 128MB)
        if self.storage.chunk_size == 0 || self.storage.chunk_size > 128 * 1024 * 1024 {
            return Err(ConfigError::ValidationError(
                "Chunk size must be between 1 and 128MB".to_string(),
            ));
        }

        // Validate bind address
        if self.network.bind_addr.is_empty() {
            return Err(ConfigError::ValidationError(
                "Bind address cannot be empty".to_string(),
            ));
        }

        // Validate log level
        match self.node.log_level.as_str() {
            "trace" | "debug" | "info" | "warn" | "error" => {}
            _ => {
                return Err(ConfigError::ValidationError(format!(
                    "Invalid log level: {}",
                    self.node.log_level
                )));
            }
        }

        Ok(())
    }
}

/// Configuration error types
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("Failed to read config: {0}")]
    ReadError(String),

    #[error("Failed to parse config: {0}")]
    ParseError(String),

    #[error("Failed to serialize config: {0}")]
    SerializeError(String),

    #[error("Failed to write config: {0}")]
    WriteError(String),

    #[error("Configuration validation error: {0}")]
    ValidationError(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ServerConfig::default();
        assert_eq!(config.node.node_id, "node1");
        assert_eq!(config.storage.chunk_size, 4 * 1024 * 1024);
        assert!(config.storage.use_iouring);
    }

    #[test]
    fn test_config_validation() {
        let mut config = ServerConfig::default();

        // Valid config
        assert!(config.validate().is_ok());

        // Empty node ID
        config.node.node_id = "".to_string();
        assert!(config.validate().is_err());

        config.node.node_id = "node1".to_string();

        // Invalid chunk size
        config.storage.chunk_size = 0;
        assert!(config.validate().is_err());

        config.storage.chunk_size = 200 * 1024 * 1024;
        assert!(config.validate().is_err());

        config.storage.chunk_size = 4 * 1024 * 1024;

        // Invalid log level
        config.node.log_level = "invalid".to_string();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_serialization() {
        let config = ServerConfig::default();
        let toml_str = toml::to_string(&config).unwrap();
        let deserialized: ServerConfig = toml::from_str(&toml_str).unwrap();

        assert_eq!(config.node.node_id, deserialized.node.node_id);
        assert_eq!(config.storage.chunk_size, deserialized.storage.chunk_size);
    }
}
