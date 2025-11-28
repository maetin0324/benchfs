// メタデータ管理モジュール
pub mod cache;
pub mod consistent_hash;
pub mod id_generator;
pub mod manager;
pub mod types;

pub use cache::{CacheStats, MetadataCache, MetadataCacheEntry};
pub use consistent_hash::{koyama_hash, ConsistentHashRing};
pub use id_generator::{IdGenerator, IdGeneratorError, IdGeneratorResult};
pub use manager::{MetadataError, MetadataManager, MetadataResult};
pub use types::{DirectoryMetadata, FileMetadata, FilePermissions, InodeType, NodeId};

// チャンクサイズ: 4MB
// BenchFS uses 4MB chunks (larger than CHFS's 64KB) for:
// - Better RDMA transfer efficiency
// - Reduced metadata overhead
// - Optimized for large sequential I/O workloads
pub const CHUNK_SIZE: usize = 4 * 1024 * 1024;
