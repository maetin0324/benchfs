// メタデータ管理モジュール
pub mod types;
pub mod consistent_hash;
pub mod manager;
pub mod cache;

pub use types::{FileMetadata, DirectoryMetadata, InodeType, FilePermissions, NodeId};
pub use consistent_hash::ConsistentHashRing;
pub use manager::{MetadataManager, MetadataError, MetadataResult};
pub use cache::{MetadataCache, MetadataCacheEntry, CacheStats};

// Consistent Hashing用定数
// 150 virtual nodes per physical node (same as CHFS)
// This value provides good load balancing across nodes
pub const VIRTUAL_NODES_PER_NODE: usize = 150;

// xxHash seed for consistent hashing (same as CHFS)
pub const XXHASH_SEED: u64 = 0;

// チャンクサイズ: 4MB
// BenchFS uses 4MB chunks (larger than CHFS's 64KB) for:
// - Better RDMA transfer efficiency
// - Reduced metadata overhead
// - Optimized for large sequential I/O workloads
pub const CHUNK_SIZE: usize = 4 * 1024 * 1024;
