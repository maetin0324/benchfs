use crate::metadata::{FileMetadata, NodeId, CHUNK_SIZE};

/// チャンク情報
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkInfo {
    /// チャンクインデックス
    pub index: u64,

    /// チャンクのオフセット (バイト)
    pub offset: u64,

    /// チャンクサイズ (バイト)
    pub size: u64,

    /// チャンクが配置されているノードID
    pub node_id: Option<NodeId>,
}

impl ChunkInfo {
    /// 新しいチャンク情報を作成
    pub fn new(index: u64, offset: u64, size: u64) -> Self {
        Self {
            index,
            offset,
            size,
            node_id: None,
        }
    }

    /// ノードIDを設定
    pub fn with_node(mut self, node_id: NodeId) -> Self {
        self.node_id = Some(node_id);
        self
    }

    /// チャンクの終端オフセット
    pub fn end_offset(&self) -> u64 {
        self.offset + self.size
    }
}

/// チャンキングエラー
#[derive(Debug, thiserror::Error)]
pub enum ChunkingError {
    #[error("Invalid chunk index: {0}")]
    InvalidChunkIndex(u64),

    #[error("Invalid offset: {0}")]
    InvalidOffset(u64),

    #[error("Invalid file size: {0}")]
    InvalidFileSize(u64),

    #[error("Chunk size mismatch: expected {expected}, got {actual}")]
    ChunkSizeMismatch { expected: u64, actual: u64 },
}

pub type ChunkingResult<T> = Result<T, ChunkingError>;

/// チャンクマネージャー
///
/// ファイルのチャンク分割とチャンク情報の管理を行う
pub struct ChunkManager {
    /// チャンクサイズ (バイト)
    chunk_size: usize,
}

impl ChunkManager {
    /// 新しいチャンクマネージャーを作成 (デフォルトチャンクサイズ)
    pub fn new() -> Self {
        Self::with_chunk_size(CHUNK_SIZE)
    }

    /// 指定されたチャンクサイズでチャンクマネージャーを作成
    pub fn with_chunk_size(chunk_size: usize) -> Self {
        Self { chunk_size }
    }

    /// チャンクサイズを取得
    pub fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    /// ファイルサイズからチャンク数を計算
    pub fn calculate_chunk_count(&self, file_size: u64) -> u64 {
        if file_size == 0 {
            0
        } else {
            (file_size + self.chunk_size as u64 - 1) / self.chunk_size as u64
        }
    }

    /// 指定されたチャンクのオフセットを計算
    pub fn chunk_offset(&self, chunk_index: u64) -> u64 {
        chunk_index * self.chunk_size as u64
    }

    /// 指定されたチャンクのサイズを計算 (最終チャンクは小さい可能性がある)
    pub fn chunk_size_at(&self, chunk_index: u64, file_size: u64) -> ChunkingResult<u64> {
        let offset = self.chunk_offset(chunk_index);
        if offset >= file_size {
            return Err(ChunkingError::InvalidChunkIndex(chunk_index));
        }

        let remaining = file_size - offset;
        Ok(remaining.min(self.chunk_size as u64))
    }

    /// 指定されたオフセットが含まれるチャンクインデックスを計算
    pub fn offset_to_chunk_index(&self, offset: u64) -> u64 {
        offset / self.chunk_size as u64
    }

    /// 指定されたチャンクインデックスのチャンク情報を取得
    pub fn get_chunk_info(&self, chunk_index: u64, file_size: u64) -> ChunkingResult<ChunkInfo> {
        let offset = self.chunk_offset(chunk_index);
        let size = self.chunk_size_at(chunk_index, file_size)?;

        Ok(ChunkInfo::new(chunk_index, offset, size))
    }

    /// ファイル全体のチャンク情報リストを取得
    pub fn get_all_chunks(&self, file_size: u64) -> Vec<ChunkInfo> {
        let chunk_count = self.calculate_chunk_count(file_size);
        (0..chunk_count)
            .map(|index| {
                let offset = self.chunk_offset(index);
                let size = self
                    .chunk_size_at(index, file_size)
                    .expect("Valid chunk index");
                ChunkInfo::new(index, offset, size)
            })
            .collect()
    }

    /// 指定された範囲 (offset, length) に含まれるチャンクリストを取得
    pub fn get_chunks_in_range(
        &self,
        offset: u64,
        length: u64,
        file_size: u64,
    ) -> ChunkingResult<Vec<ChunkInfo>> {
        if offset >= file_size {
            return Err(ChunkingError::InvalidOffset(offset));
        }

        let end_offset = (offset + length).min(file_size);
        let start_chunk = self.offset_to_chunk_index(offset);
        let end_chunk = self.offset_to_chunk_index(end_offset.saturating_sub(1));

        let mut chunks = Vec::new();
        for chunk_index in start_chunk..=end_chunk {
            let chunk_info = self.get_chunk_info(chunk_index, file_size)?;
            chunks.push(chunk_info);
        }

        Ok(chunks)
    }

    /// FileMetadataからチャンク情報リストを作成 (ノードID付き)
    pub fn chunks_from_metadata(&self, metadata: &FileMetadata) -> Vec<ChunkInfo> {
        let chunk_count = self.calculate_chunk_count(metadata.size);
        (0..chunk_count)
            .map(|index| {
                let offset = self.chunk_offset(index);
                let size = self
                    .chunk_size_at(index, metadata.size)
                    .expect("Valid chunk index");

                let mut chunk_info = ChunkInfo::new(index, offset, size);

                // メタデータからノードIDを取得
                if let Some(node_id) = metadata.chunk_locations.get(index as usize) {
                    chunk_info.node_id = Some(node_id.clone());
                }

                chunk_info
            })
            .collect()
    }

    /// 範囲読み込み用のチャンク情報リストを作成 (部分読み込み対応)
    ///
    /// # Arguments
    /// * `offset` - 読み込み開始オフセット
    /// * `length` - 読み込みサイズ
    /// * `file_size` - ファイルサイズ
    ///
    /// # Returns
    /// チャンクごとの読み込み情報 (chunk_index, chunk_offset, read_size)
    pub fn calculate_read_chunks(
        &self,
        offset: u64,
        length: u64,
        file_size: u64,
    ) -> ChunkingResult<Vec<(u64, u64, u64)>> {
        if offset >= file_size {
            return Err(ChunkingError::InvalidOffset(offset));
        }

        let end_offset = (offset + length).min(file_size);
        let start_chunk = self.offset_to_chunk_index(offset);
        let end_chunk = self.offset_to_chunk_index(end_offset.saturating_sub(1));

        let mut read_chunks = Vec::new();

        for chunk_index in start_chunk..=end_chunk {
            let chunk_start = self.chunk_offset(chunk_index);
            let chunk_end = chunk_start + self.chunk_size_at(chunk_index, file_size)?;

            // 読み込み範囲との重複部分を計算
            let read_start = offset.max(chunk_start);
            let read_end = end_offset.min(chunk_end);
            let chunk_offset = read_start - chunk_start;
            let read_size = read_end - read_start;

            read_chunks.push((chunk_index, chunk_offset, read_size));
        }

        Ok(read_chunks)
    }
}

impl Default for ChunkManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunk_manager_creation() {
        let manager = ChunkManager::new();
        assert_eq!(manager.chunk_size(), CHUNK_SIZE);

        let manager = ChunkManager::with_chunk_size(1024 * 1024);
        assert_eq!(manager.chunk_size(), 1024 * 1024);
    }

    #[test]
    fn test_calculate_chunk_count() {
        let manager = ChunkManager::new();

        assert_eq!(manager.calculate_chunk_count(0), 0);
        assert_eq!(manager.calculate_chunk_count(1024), 1);
        assert_eq!(manager.calculate_chunk_count(CHUNK_SIZE as u64), 1);
        assert_eq!(manager.calculate_chunk_count(CHUNK_SIZE as u64 + 1), 2);
        assert_eq!(manager.calculate_chunk_count(10 * CHUNK_SIZE as u64), 10);
    }

    #[test]
    fn test_chunk_offset() {
        let manager = ChunkManager::new();

        assert_eq!(manager.chunk_offset(0), 0);
        assert_eq!(manager.chunk_offset(1), CHUNK_SIZE as u64);
        assert_eq!(manager.chunk_offset(2), 2 * CHUNK_SIZE as u64);
    }

    #[test]
    fn test_chunk_size_at() {
        let manager = ChunkManager::new();
        let file_size = 10 * 1024 * 1024 + 1024; // 10MB + 1KB

        // 最初のチャンク
        assert_eq!(
            manager.chunk_size_at(0, file_size).unwrap(),
            CHUNK_SIZE as u64
        );

        // 2番目のチャンク
        assert_eq!(
            manager.chunk_size_at(1, file_size).unwrap(),
            CHUNK_SIZE as u64
        );

        // 最終チャンク (2MB + 1KB)
        assert_eq!(
            manager.chunk_size_at(2, file_size).unwrap(),
            2 * 1024 * 1024 + 1024
        );

        // 範囲外
        assert!(manager.chunk_size_at(3, file_size).is_err());
    }

    #[test]
    fn test_offset_to_chunk_index() {
        let manager = ChunkManager::new();

        assert_eq!(manager.offset_to_chunk_index(0), 0);
        assert_eq!(manager.offset_to_chunk_index(1024), 0);
        assert_eq!(manager.offset_to_chunk_index(CHUNK_SIZE as u64 - 1), 0);
        assert_eq!(manager.offset_to_chunk_index(CHUNK_SIZE as u64), 1);
        assert_eq!(manager.offset_to_chunk_index(CHUNK_SIZE as u64 + 1), 1);
        assert_eq!(manager.offset_to_chunk_index(2 * CHUNK_SIZE as u64), 2);
    }

    #[test]
    fn test_get_chunk_info() {
        let manager = ChunkManager::new();
        let file_size = 10 * 1024 * 1024; // 10MB

        let chunk = manager.get_chunk_info(0, file_size).unwrap();
        assert_eq!(chunk.index, 0);
        assert_eq!(chunk.offset, 0);
        assert_eq!(chunk.size, CHUNK_SIZE as u64);
        assert_eq!(chunk.end_offset(), CHUNK_SIZE as u64);

        let chunk = manager.get_chunk_info(1, file_size).unwrap();
        assert_eq!(chunk.index, 1);
        assert_eq!(chunk.offset, CHUNK_SIZE as u64);
        assert_eq!(chunk.size, CHUNK_SIZE as u64);
    }

    #[test]
    fn test_get_all_chunks() {
        let manager = ChunkManager::new();
        let file_size = 10 * 1024 * 1024 + 1024; // 10MB + 1KB

        let chunks = manager.get_all_chunks(file_size);
        assert_eq!(chunks.len(), 3); // 3チャンク

        assert_eq!(chunks[0].index, 0);
        assert_eq!(chunks[0].size, CHUNK_SIZE as u64);

        assert_eq!(chunks[1].index, 1);
        assert_eq!(chunks[1].size, CHUNK_SIZE as u64);

        assert_eq!(chunks[2].index, 2);
        assert_eq!(chunks[2].size, 2 * 1024 * 1024 + 1024); // 最終チャンク
    }

    #[test]
    fn test_get_chunks_in_range() {
        let manager = ChunkManager::new();
        let file_size = 10 * 1024 * 1024; // 10MB

        // 最初のチャンクのみ
        let chunks = manager.get_chunks_in_range(0, 1024, file_size).unwrap();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].index, 0);

        // 複数チャンクにまたがる
        let chunks = manager
            .get_chunks_in_range(0, 2 * CHUNK_SIZE as u64, file_size)
            .unwrap();
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].index, 0);
        assert_eq!(chunks[1].index, 1);

        // チャンクの中間から開始
        let chunks = manager
            .get_chunks_in_range(CHUNK_SIZE as u64 / 2, CHUNK_SIZE as u64 * 2, file_size)
            .unwrap();
        assert_eq!(chunks.len(), 3); // chunk 0, 1, 2
        assert_eq!(chunks[0].index, 0);
        assert_eq!(chunks[1].index, 1);
        assert_eq!(chunks[2].index, 2);
    }

    #[test]
    fn test_calculate_read_chunks() {
        let manager = ChunkManager::new();
        let file_size = 10 * 1024 * 1024; // 10MB

        // 単一チャンク内の読み込み
        let reads = manager.calculate_read_chunks(0, 1024, file_size).unwrap();
        assert_eq!(reads.len(), 1);
        assert_eq!(reads[0], (0, 0, 1024)); // (chunk_index, chunk_offset, read_size)

        // チャンクの中間から読み込み
        let reads = manager.calculate_read_chunks(1024, 2048, file_size).unwrap();
        assert_eq!(reads.len(), 1);
        assert_eq!(reads[0], (0, 1024, 2048));

        // 複数チャンクにまたがる読み込み
        let chunk_size = CHUNK_SIZE as u64;
        let reads = manager
            .calculate_read_chunks(chunk_size - 512, 1024, file_size)
            .unwrap();
        assert_eq!(reads.len(), 2);
        assert_eq!(reads[0], (0, chunk_size - 512, 512)); // chunk 0の最後512バイト
        assert_eq!(reads[1], (1, 0, 512)); // chunk 1の最初512バイト
    }

    #[test]
    fn test_chunk_info_with_node() {
        let chunk = ChunkInfo::new(0, 0, 1024).with_node("node1".to_string());
        assert_eq!(chunk.node_id, Some("node1".to_string()));
    }

    #[test]
    fn test_chunks_from_metadata() {
        let manager = ChunkManager::new();
        let metadata = FileMetadata::new(1, "/test.txt".to_string(), 8 * 1024 * 1024); // 8MB

        let chunks = manager.chunks_from_metadata(&metadata);
        assert_eq!(chunks.len(), 2); // 2チャンク
        assert_eq!(chunks[0].size, CHUNK_SIZE as u64);
        assert_eq!(chunks[1].size, CHUNK_SIZE as u64);
    }
}
