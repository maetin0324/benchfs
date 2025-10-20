// データ管理モジュール
pub mod chunking;
pub mod placement;

pub use chunking::{ChunkInfo, ChunkManager, ChunkingError, ChunkingResult};
pub use placement::{PlacementStrategy, RoundRobinPlacement, ConsistentHashPlacement};
