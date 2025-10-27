use super::{VIRTUAL_NODES_PER_NODE, XXHASH_SEED};
use crate::metadata::types::NodeId;
use std::collections::BTreeMap;
use xxhash_rust::xxh64::xxh64;

/// ハッシュリング上の位置
type RingPosition = u64;

/// Consistent Hashingリング
///
/// 仮想ノードを使用してノード間でデータを均等に分散する。
/// xxHash64を使用してファイルパスをハッシュ化し、対応するノードを決定する。
pub struct ConsistentHashRing {
    /// リング上の位置 -> ノードID のマッピング
    /// BTreeMapを使用して、範囲検索を効率的に行う
    ring: BTreeMap<RingPosition, NodeId>,

    /// 物理ノードIDのリスト (追加順)
    nodes: Vec<NodeId>,

    /// ノードあたりの仮想ノード数
    virtual_nodes_per_node: usize,
}

impl ConsistentHashRing {
    /// 新しい空のハッシュリングを作成
    pub fn new() -> Self {
        Self::with_virtual_nodes(VIRTUAL_NODES_PER_NODE)
    }

    /// 仮想ノード数を指定してハッシュリングを作成
    pub fn with_virtual_nodes(virtual_nodes_per_node: usize) -> Self {
        Self {
            ring: BTreeMap::new(),
            nodes: Vec::new(),
            virtual_nodes_per_node,
        }
    }

    /// ノードをリングに追加
    ///
    /// 指定されたノードIDに対して、仮想ノードを作成しリングに配置する。
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

        // 仮想ノードを作成してリングに配置
        for i in 0..self.virtual_nodes_per_node {
            let virtual_node_key = format!("{}:{}", node_id, i);
            let position = xxh64(virtual_node_key.as_bytes(), XXHASH_SEED);
            self.ring.insert(position, node_id.clone());
        }

        tracing::debug!(
            "Added node {} with {} virtual nodes",
            node_id,
            self.virtual_nodes_per_node
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
        // ノードリストから削除
        if let Some(pos) = self.nodes.iter().position(|id| id == node_id) {
            self.nodes.remove(pos);

            // 仮想ノードをリングから削除
            for i in 0..self.virtual_nodes_per_node {
                let virtual_node_key = format!("{}:{}", node_id, i);
                let position = xxh64(virtual_node_key.as_bytes(), XXHASH_SEED);
                self.ring.remove(&position);
            }

            tracing::debug!("Removed node {} from the ring", node_id);
            true
        } else {
            tracing::warn!("Node {} not found in the ring", node_id);
            false
        }
    }

    /// 指定されたキーに対応するノードを取得
    ///
    /// キーをハッシュ化し、リング上で時計回りに最も近い仮想ノードを見つける。
    ///
    /// # Arguments
    /// * `key` - 検索するキー (通常はファイルパス)
    ///
    /// # Returns
    /// 対応するノードID。リングが空の場合は `None`
    pub fn get_node(&self, key: &str) -> Option<NodeId> {
        if self.ring.is_empty() {
            return None;
        }

        let hash = xxh64(key.as_bytes(), XXHASH_SEED);

        // hash以上の最小のキーを探す
        if let Some((&_position, node_id)) = self.ring.range(hash..).next() {
            return Some(node_id.clone());
        }

        // 見つからない場合は、リングの先頭 (最小値) に戻る
        self.ring
            .iter()
            .next()
            .map(|(_pos, node_id)| node_id.clone())
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
        if self.ring.is_empty() || count == 0 {
            return Vec::new();
        }

        let hash = xxh64(key.as_bytes(), XXHASH_SEED);
        let mut result = Vec::with_capacity(count);
        let mut seen = std::collections::HashSet::new();

        // hash以降のノードを収集
        for (_position, node_id) in self.ring.range(hash..) {
            if !seen.contains(node_id) {
                seen.insert(node_id.clone());
                result.push(node_id.clone());
                if result.len() >= count {
                    return result;
                }
            }
        }

        // 足りない場合は、リングの先頭から追加
        for (_position, node_id) in self.ring.iter() {
            if !seen.contains(node_id) {
                seen.insert(node_id.clone());
                result.push(node_id.clone());
                if result.len() >= count {
                    return result;
                }
            }
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

    /// 仮想ノード数を取得
    pub fn virtual_node_count(&self) -> usize {
        self.ring.len()
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
    fn test_add_remove_node() {
        let mut ring = ConsistentHashRing::new();

        ring.add_node("node1".to_string());
        assert_eq!(ring.node_count(), 1);
        assert_eq!(ring.virtual_node_count(), VIRTUAL_NODES_PER_NODE);

        ring.add_node("node2".to_string());
        assert_eq!(ring.node_count(), 2);
        assert_eq!(ring.virtual_node_count(), VIRTUAL_NODES_PER_NODE * 2);

        assert!(ring.remove_node(&"node1".to_string()));
        assert_eq!(ring.node_count(), 1);
        assert_eq!(ring.virtual_node_count(), VIRTUAL_NODES_PER_NODE);

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

        // 各ノードが少なくとも100個以上のキーを担当していることを確認
        // (完全に均等ではないが、合理的な分散)
        for node_id in ring.nodes() {
            let count = distribution.get(node_id).unwrap_or(&0);
            assert!(
                *count > 100,
                "Node {} has only {} keys (expected > 100)",
                node_id,
                count
            );
        }

        tracing::info!("Distribution: {:?}", distribution);
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
