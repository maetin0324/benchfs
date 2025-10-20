use super::types::InodeId;
use std::cell::RefCell;
use std::time::{SystemTime, UNIX_EPOCH};

/// Snowflake-like分散ID生成器
///
/// 64ビットのID構成:
/// - 42ビット: タイムスタンプ（ミリ秒単位、カスタムエポックからの経過時間）
/// - 10ビット: ノードID（最大1024ノード: 0-1023）
/// - 12ビット: シーケンス番号（同一ミリ秒内で最大4096個のID生成: 0-4095）
///
/// この設計により:
/// - 約139年分のタイムスタンプ範囲（2^42ミリ秒）
/// - 最大1024ノードの分散環境をサポート
/// - 1ミリ秒あたり4096個のID生成が可能
#[derive(Debug)]
pub struct IdGenerator {
    /// ノードID（0-1023）
    node_id: u64,
    /// カスタムエポック（2024-01-01 00:00:00 UTC）のUNIXタイムスタンプ（ミリ秒）
    epoch: u64,
    /// 最後にIDを生成したタイムスタンプ
    last_timestamp: RefCell<u64>,
    /// 同一ミリ秒内のシーケンス番号
    sequence: RefCell<u64>,
}

/// ID生成エラー
#[derive(Debug, thiserror::Error)]
pub enum IdGeneratorError {
    #[error("Invalid node ID: {0} (must be 0-1023)")]
    InvalidNodeId(u64),

    #[error("Clock moved backwards: last={0}, current={1}")]
    ClockMovedBackwards(u64, u64),

    #[error("Sequence overflow in the same millisecond")]
    SequenceOverflow,
}

pub type IdGeneratorResult<T> = Result<T, IdGeneratorError>;

// ビット構成の定数
const TIMESTAMP_BITS: u64 = 42;
const NODE_ID_BITS: u64 = 10;
const SEQUENCE_BITS: u64 = 12;

// 最大値
const MAX_NODE_ID: u64 = (1 << NODE_ID_BITS) - 1; // 1023
const MAX_SEQUENCE: u64 = (1 << SEQUENCE_BITS) - 1; // 4095

// ビットシフト量
const NODE_ID_SHIFT: u64 = SEQUENCE_BITS;
const TIMESTAMP_SHIFT: u64 = NODE_ID_BITS + SEQUENCE_BITS;

// カスタムエポック: 2024-01-01 00:00:00 UTC のUNIXタイムスタンプ（ミリ秒）
const CUSTOM_EPOCH_MS: u64 = 1704067200000;

impl IdGenerator {
    /// 新しいID生成器を作成
    ///
    /// # Arguments
    /// * `node_id` - ノードID（0-1023）
    ///
    /// # Returns
    /// ID生成器のインスタンス
    ///
    /// # Errors
    /// ノードIDが範囲外の場合はエラー
    pub fn new(node_id: u64) -> IdGeneratorResult<Self> {
        if node_id > MAX_NODE_ID {
            return Err(IdGeneratorError::InvalidNodeId(node_id));
        }

        Ok(Self {
            node_id,
            epoch: CUSTOM_EPOCH_MS,
            last_timestamp: RefCell::new(0),
            sequence: RefCell::new(0),
        })
    }

    /// ノードIDからID生成器を作成（文字列ハッシュベース）
    ///
    /// # Arguments
    /// * `node_id_str` - ノードIDの文字列（例: "server", "client_1"）
    ///
    /// # Returns
    /// ID生成器のインスタンス
    ///
    /// # Safety
    /// This function is safe because the hash is always modulo (MAX_NODE_ID + 1),
    /// ensuring the node_id is always in the valid range [0, 1023].
    /// Therefore, Self::new() will never return an error.
    pub fn from_node_string(node_id_str: &str) -> Self {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        node_id_str.hash(&mut hasher);
        let hash = hasher.finish();

        // ハッシュ値を0-1023の範囲に収める
        let node_id = hash % (MAX_NODE_ID + 1);

        // SAFETY: node_id is guaranteed to be in range [0, MAX_NODE_ID]
        Self::new(node_id).unwrap()
    }

    /// 次のユニークなID（inode番号）を生成
    ///
    /// # Returns
    /// 新しく生成されたinode番号
    ///
    /// # Errors
    /// - 時計が巻き戻った場合
    /// - シーケンス番号がオーバーフローした場合（同一ミリ秒内で4096個以上生成）
    pub fn next_id(&self) -> IdGeneratorResult<InodeId> {
        let mut current_timestamp = self.current_timestamp_ms()?;
        let mut last_timestamp = self.last_timestamp.borrow_mut();
        let mut sequence = self.sequence.borrow_mut();

        if current_timestamp < *last_timestamp {
            return Err(IdGeneratorError::ClockMovedBackwards(
                *last_timestamp,
                current_timestamp,
            ));
        }

        if current_timestamp == *last_timestamp {
            // 同一ミリ秒内: シーケンス番号をインクリメント
            *sequence = (*sequence + 1) & MAX_SEQUENCE;

            if *sequence == 0 {
                // シーケンスがオーバーフロー: 次のミリ秒まで待機
                current_timestamp = self.wait_next_millis(*last_timestamp)?;
            }
        } else {
            // 新しいミリ秒: シーケンスをリセット
            *sequence = 0;
        }

        *last_timestamp = current_timestamp;

        // IDを構築
        let timestamp_part = (current_timestamp - self.epoch) << TIMESTAMP_SHIFT;
        let node_id_part = self.node_id << NODE_ID_SHIFT;
        let sequence_part = *sequence;

        Ok(timestamp_part | node_id_part | sequence_part)
    }

    /// 現在のタイムスタンプ（ミリ秒）を取得
    fn current_timestamp_ms(&self) -> IdGeneratorResult<u64> {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .map_err(|_| IdGeneratorError::ClockMovedBackwards(0, 0))
    }

    /// 次のミリ秒まで待機
    fn wait_next_millis(&self, last_timestamp: u64) -> IdGeneratorResult<u64> {
        let mut timestamp = self.current_timestamp_ms()?;

        while timestamp <= last_timestamp {
            // 短いスピンウェイト
            std::hint::spin_loop();
            timestamp = self.current_timestamp_ms()?;
        }

        Ok(timestamp)
    }

    /// ノードIDを取得
    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    /// IDから各コンポーネントを抽出（デバッグ用）
    pub fn extract_components(id: InodeId) -> (u64, u64, u64) {
        let timestamp = (id >> TIMESTAMP_SHIFT) & ((1 << TIMESTAMP_BITS) - 1);
        let node_id = (id >> NODE_ID_SHIFT) & ((1 << NODE_ID_BITS) - 1);
        let sequence = id & ((1 << SEQUENCE_BITS) - 1);

        (timestamp, node_id, sequence)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_id_generator_creation() {
        let generator = IdGenerator::new(123).unwrap();
        assert_eq!(generator.node_id(), 123);
    }

    #[test]
    fn test_invalid_node_id() {
        let result = IdGenerator::new(1024); // MAX_NODE_ID + 1
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            IdGeneratorError::InvalidNodeId(1024)
        ));
    }

    #[test]
    fn test_from_node_string() {
        let generator1 = IdGenerator::from_node_string("server");
        let generator2 = IdGenerator::from_node_string("server");
        let generator3 = IdGenerator::from_node_string("client");

        // 同じ文字列からは同じノードIDが生成される
        assert_eq!(generator1.node_id(), generator2.node_id());

        // 異なる文字列からは（高確率で）異なるノードIDが生成される
        assert_ne!(generator1.node_id(), generator3.node_id());

        // ノードIDは範囲内
        assert!(generator1.node_id() <= MAX_NODE_ID);
        assert!(generator3.node_id() <= MAX_NODE_ID);
    }

    #[test]
    fn test_generate_unique_ids() {
        let generator = IdGenerator::new(42).unwrap();

        let id1 = generator.next_id().unwrap();
        let id2 = generator.next_id().unwrap();
        let id3 = generator.next_id().unwrap();

        // すべてのIDがユニーク
        assert_ne!(id1, id2);
        assert_ne!(id2, id3);
        assert_ne!(id1, id3);

        // IDは単調増加
        assert!(id2 > id1);
        assert!(id3 > id2);
    }

    #[test]
    fn test_extract_components() {
        let generator = IdGenerator::new(123).unwrap();
        let id = generator.next_id().unwrap();

        let (timestamp, node_id, sequence) = IdGenerator::extract_components(id);

        // ノードIDが正しく埋め込まれている
        assert_eq!(node_id, 123);

        // タイムスタンプが妥当な範囲
        assert!(timestamp > 0);

        // シーケンスが範囲内
        assert!(sequence <= MAX_SEQUENCE);
    }

    #[test]
    fn test_multiple_ids_same_timestamp() {
        let generator = IdGenerator::new(5).unwrap();

        // 同一ミリ秒内で複数のIDを生成
        let mut ids = Vec::new();
        for _ in 0..100 {
            ids.push(generator.next_id().unwrap());
        }

        // すべてのIDがユニーク
        let mut unique_ids = ids.clone();
        unique_ids.sort_unstable();
        unique_ids.dedup();
        assert_eq!(ids.len(), unique_ids.len());

        // ノードIDがすべて同じ
        for id in &ids {
            let (_, node_id, _) = IdGenerator::extract_components(*id);
            assert_eq!(node_id, 5);
        }
    }

    #[test]
    fn test_different_nodes_different_ids() {
        let generator1 = IdGenerator::new(1).unwrap();
        let generator2 = IdGenerator::new(2).unwrap();

        let id1 = generator1.next_id().unwrap();
        let id2 = generator2.next_id().unwrap();

        // 異なるノードからのIDは異なる
        assert_ne!(id1, id2);

        let (_, node_id1, _) = IdGenerator::extract_components(id1);
        let (_, node_id2, _) = IdGenerator::extract_components(id2);

        assert_eq!(node_id1, 1);
        assert_eq!(node_id2, 2);
    }

    #[test]
    fn test_id_monotonicity() {
        let generator = IdGenerator::new(10).unwrap();

        let mut prev_id = 0;
        for _ in 0..1000 {
            let id = generator.next_id().unwrap();
            assert!(id > prev_id, "ID should be monotonically increasing");
            prev_id = id;
        }
    }
}
