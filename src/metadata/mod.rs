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
pub const VIRTUAL_NODES_PER_NODE: usize = 150;
pub const XXHASH_SEED: u64 = 0;

// チャンクサイズ: 4MB
pub const CHUNK_SIZE: usize = 4 * 1024 * 1024;
