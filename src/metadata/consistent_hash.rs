use crate::metadata::types::NodeId;

/// koyama_hash: ファイル名ベースの単純ハッシュ関数
///
/// 文字列を走査し、数字の連続は数値として解釈して加算、
/// それ以外の文字はASCII値として加算する。
///
/// # Arguments
/// * `data` - ハッシュ対象のバイト列
///
/// # Returns
/// 32ビットのハッシュ値
pub fn koyama_hash(data: &[u8]) -> u32 {
    let mut digest: u32 = 0;
    let mut i = 0;

    while i < data.len() {
        let b = data[i];
        if b.is_ascii_digit() {
            // 数字の連続を数値として解釈
            let mut num: u32 = 0;
            while i < data.len() && data[i].is_ascii_digit() {
                num = num.wrapping_mul(10).wrapping_add((data[i] - b'0') as u32);
                i += 1;
            }
            digest = digest.wrapping_add(num);
        } else {
            // 数字以外はASCII値を加算
            digest = digest.wrapping_add(b as u32);
            i += 1;
        }
    }

    digest
}

/// ファイル配置用ハッシュリング
///
/// koyama_hashを使用してファイル名をハッシュ化し、
/// モジュロ演算で対応するノードを決定する。
pub struct ConsistentHashRing {
    /// 物理ノードIDのリスト (追加順 = ノード番号順)
    nodes: Vec<NodeId>,
}

impl ConsistentHashRing {
    /// 新しい空のハッシュリングを作成
    pub fn new() -> Self {
        Self { nodes: Vec::new() }
    }

    /// 仮想ノード数を指定してハッシュリングを作成 (互換性のため残す)
    pub fn with_virtual_nodes(_virtual_nodes_per_node: usize) -> Self {
        Self::new()
    }

    /// ノードをリングに追加
    ///
    /// # Arguments
    /// * `node_id` - 追加するノードID
    pub fn add_node(&mut self, node_id: NodeId) {
        // 既に存在する場合はスキップ
        if self.nodes.contains(&node_id) {
            tracing::warn!("Node {} already exists in the ring", node_id);
            return;
        }

        self.nodes.push(node_id.clone());

        tracing::debug!(
            "Added node {} (total nodes: {})",
            node_id,
            self.nodes.len()
        );
    }

    /// ノードをリングから削除
    ///
    /// # Arguments
    /// * `node_id` - 削除するノードID
    ///
    /// # Returns
    /// ノードが存在して削除された場合は `true`、存在しなかった場合は `false`
    pub fn remove_node(&mut self, node_id: &NodeId) -> bool {
        if let Some(pos) = self.nodes.iter().position(|id| id == node_id) {
            self.nodes.remove(pos);
            tracing::debug!("Removed node {} from the ring", node_id);
            true
        } else {
            tracing::warn!("Node {} not found in the ring", node_id);
            false
        }
    }

    /// 指定されたキーに対応するノードを取得
    ///
    /// ファイル名をkoyama_hashでハッシュ化し、黄金比を使った混合関数でノードを選択する。
    /// 単純なモジュロ演算と異なり、連続したチャンクインデックスが異なるノードに
    /// 分散されるため、block_sizeがノード数の倍数でも負荷が偏らない。
    ///
    /// # Arguments
    /// * `key` - 検索するキー (通常はファイルパス)
    ///
    /// # Returns
    /// 対応するノードID。リングが空の場合は `None`
    pub fn get_node(&self, key: &str) -> Option<NodeId> {
        if self.nodes.is_empty() {
            return None;
        }

        let hash = koyama_hash(key.as_bytes());

        // Use golden ratio-based multiplicative hashing for better distribution
        // This breaks the linear relationship between consecutive chunk indices
        // and their assigned nodes, preventing load imbalance when block_size
        // is a multiple of the node count.
        //
        // Golden ratio constant: (sqrt(5) - 1) / 2 * 2^32 ≈ 0x9e3779b9
        const PHI: u64 = 0x9e3779b9;
        let mixed = ((hash as u64).wrapping_mul(PHI)) >> 32;
        let index = (mixed as usize) % self.nodes.len();

        Some(self.nodes[index].clone())
    }

    /// 指定されたキーに対応する複数のノードを取得 (レプリケーション用)
    ///
    /// # Arguments
    /// * `key` - 検索するキー
    /// * `count` - 取得するノード数
    ///
    /// # Returns
    /// 対応するノードIDのベクター (重複なし)
    pub fn get_nodes(&self, key: &str, count: usize) -> Vec<NodeId> {
        if self.nodes.is_empty() || count == 0 {
            return Vec::new();
        }

        let hash = koyama_hash(key.as_bytes());
        let start_index = (hash as usize) % self.nodes.len();
        let actual_count = count.min(self.nodes.len());

        let mut result = Vec::with_capacity(actual_count);
        for i in 0..actual_count {
            let index = (start_index + i) % self.nodes.len();
            result.push(self.nodes[index].clone());
        }

        result
    }

    /// リング内のノード数を取得
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// リング内の全ノードIDを取得
    pub fn nodes(&self) -> &[NodeId] {
        &self.nodes
    }

    /// 仮想ノード数を取得 (互換性のため残す、実際はノード数と同じ)
    pub fn virtual_node_count(&self) -> usize {
        self.nodes.len()
    }
}

impl Default for ConsistentHashRing {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_koyama_hash() {
        // Cコードのテストケースと同じ結果を確認
        // test("a", 1) => 'a' = 97
        assert_eq!(koyama_hash(b"a"), 97);

        // test("abc", 3) => 'a' + 'b' + 'c' = 97 + 98 + 99 = 294
        assert_eq!(koyama_hash(b"abc"), 294);

        // test("abc123", 6) => 97 + 98 + 99 + 123 = 417
        assert_eq!(koyama_hash(b"abc123"), 417);

        // test("abc12300", 8) => 97 + 98 + 99 + 12300 = 12594
        assert_eq!(koyama_hash(b"abc12300"), 12594);

        // test("abc12300a10", 11) => 97 + 98 + 99 + 12300 + 97 + 10 = 12701
        assert_eq!(koyama_hash(b"abc12300a10"), 12701);
    }

    #[test]
    fn test_add_remove_node() {
        let mut ring = ConsistentHashRing::new();

        ring.add_node("node1".to_string());
        assert_eq!(ring.node_count(), 1);

        ring.add_node("node2".to_string());
        assert_eq!(ring.node_count(), 2);

        assert!(ring.remove_node(&"node1".to_string()));
        assert_eq!(ring.node_count(), 1);

        assert!(!ring.remove_node(&"node999".to_string())); // 存在しないノード
    }

    #[test]
    fn test_get_node() {
        let mut ring = ConsistentHashRing::new();

        // 空のリング
        assert_eq!(ring.get_node("/foo/bar"), None);

        // ノード追加
        ring.add_node("node1".to_string());
        ring.add_node("node2".to_string());
        ring.add_node("node3".to_string());

        // 同じキーは同じノードにマッピングされる
        let node1 = ring.get_node("/foo/bar");
        let node2 = ring.get_node("/foo/bar");
        assert_eq!(node1, node2);

        // 異なるキーは異なるノードにマッピングされる可能性がある
        let node_a = ring.get_node("/path/a");
        let node_b = ring.get_node("/path/b");
        assert!(node_a.is_some());
        assert!(node_b.is_some());
    }

    #[test]
    fn test_get_nodes_for_replication() {
        let mut ring = ConsistentHashRing::new();

        ring.add_node("node1".to_string());
        ring.add_node("node2".to_string());
        ring.add_node("node3".to_string());

        // 3つのノードを取得
        let nodes = ring.get_nodes("/foo/bar", 3);
        assert_eq!(nodes.len(), 3);

        // 重複がないことを確認
        let unique_count = nodes.iter().collect::<std::collections::HashSet<_>>().len();
        assert_eq!(unique_count, 3);

        // 要求数がノード数より多い場合
        let nodes = ring.get_nodes("/foo/bar", 10);
        assert_eq!(nodes.len(), 3); // 最大で3つまで
    }

    #[test]
    fn test_golden_ratio_distribution() {
        let mut ring = ConsistentHashRing::new();

        ring.add_node("node_0".to_string());
        ring.add_node("node_1".to_string());
        ring.add_node("node_2".to_string());

        // 黄金比ハッシュにより、連続したチャンクインデックスが異なるノードに分散される
        // 同じキーは常に同じノードにマッピングされることを確認
        let node0 = ring.get_node("/file/0").unwrap();
        let node1 = ring.get_node("/file/1").unwrap();
        let node2 = ring.get_node("/file/2").unwrap();

        // 全てのノードが有効なノードにマッピングされる
        assert!(["node_0", "node_1", "node_2"].contains(&node0.as_str()));
        assert!(["node_0", "node_1", "node_2"].contains(&node1.as_str()));
        assert!(["node_0", "node_1", "node_2"].contains(&node2.as_str()));

        // 同じキーは常に同じノードにマッピングされる（決定論的）
        assert_eq!(ring.get_node("/file/0").unwrap(), node0);
        assert_eq!(ring.get_node("/file/1").unwrap(), node1);
        assert_eq!(ring.get_node("/file/2").unwrap(), node2);

        // 黄金比ハッシュにより、16ノードでの連続チャンク分散を確認
        let mut ring16 = ConsistentHashRing::new();
        for i in 0..16 {
            ring16.add_node(format!("node_{}", i));
        }

        // 連続した16チャンクが全ノードに分散されることを確認
        let mut node_counts = std::collections::HashMap::new();
        for i in 0..16 {
            let key = format!("/scr/testfile/{}", i);
            let node = ring16.get_node(&key).unwrap();
            *node_counts.entry(node).or_insert(0) += 1;
        }

        // 理想的には16チャンクが16ノードに1つずつ分散される
        // 黄金比ハッシュにより、少なくとも半分以上のノードが使用される
        assert!(
            node_counts.len() >= 8,
            "Expected at least 8 different nodes, got {}",
            node_counts.len()
        );
    }

    #[test]
    fn test_node_distribution() {
        let mut ring = ConsistentHashRing::new();

        ring.add_node("node1".to_string());
        ring.add_node("node2".to_string());
        ring.add_node("node3".to_string());

        // 多数のキーを生成して、分散を確認
        let mut distribution = std::collections::HashMap::new();
        for i in 0..1000 {
            let key = format!("/file/{}", i);
            if let Some(node_id) = ring.get_node(&key) {
                *distribution.entry(node_id).or_insert(0) += 1;
            }
        }

        // koyama_hashのモジュロ分散では、連続する数字が連続するノードに配置される
        // そのため、ほぼ均等に分散される
        for node_id in ring.nodes() {
            let count = distribution.get(node_id).unwrap_or(&0);
            assert!(
                *count > 200,
                "Node {} has only {} keys (expected > 200)",
                node_id,
                count
            );
        }
    }

    #[test]
    fn test_node_removal_redistribution() {
        let mut ring = ConsistentHashRing::new();

        ring.add_node("node1".to_string());
        ring.add_node("node2".to_string());
        ring.add_node("node3".to_string());

        // ノード削除前のマッピング
        let key = "/foo/bar";
        let node_before = ring.get_node(key);

        // ノード1を削除
        ring.remove_node(&"node1".to_string());

        // ノード削除後のマッピング
        let node_after = ring.get_node(key);

        // ノード1がマッピング先だった場合、他のノードに再マッピングされる
        assert!(node_after.is_some());
        if node_before == Some("node1".to_string()) {
            assert_ne!(node_after, Some("node1".to_string()));
        }
    }
}
