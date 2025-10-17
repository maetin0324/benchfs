use crate::metadata::{ConsistentHashRing, NodeId};
use std::path::Path;

/// チャンク配置戦略
///
/// ファイルのチャンクをどのノードに配置するかを決定する戦略
pub trait PlacementStrategy {
    /// 指定されたチャンクを配置するノードを決定
    ///
    /// # Arguments
    /// * `path` - ファイルパス
    /// * `chunk_index` - チャンクインデックス
    ///
    /// # Returns
    /// 配置先のノードID
    fn place_chunk(&self, path: &Path, chunk_index: u64) -> Option<NodeId>;

    /// 複数のノードを取得 (レプリケーション用)
    ///
    /// # Arguments
    /// * `path` - ファイルパス
    /// * `chunk_index` - チャンクインデックス
    /// * `count` - 取得するノード数
    ///
    /// # Returns
    /// 配置先のノードIDリスト
    fn place_chunk_replicas(
        &self,
        path: &Path,
        chunk_index: u64,
        count: usize,
    ) -> Vec<NodeId>;
}

/// ラウンドロビン配置戦略
///
/// チャンクを順番にノードに配置する
pub struct RoundRobinPlacement {
    /// ノードIDリスト
    nodes: Vec<NodeId>,
}

impl RoundRobinPlacement {
    /// 新しいラウンドロビン配置戦略を作成
    pub fn new(nodes: Vec<NodeId>) -> Self {
        Self { nodes }
    }

    /// ノード数を取得
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }
}

impl PlacementStrategy for RoundRobinPlacement {
    fn place_chunk(&self, _path: &Path, chunk_index: u64) -> Option<NodeId> {
        if self.nodes.is_empty() {
            return None;
        }

        let node_index = (chunk_index as usize) % self.nodes.len();
        Some(self.nodes[node_index].clone())
    }

    fn place_chunk_replicas(
        &self,
        _path: &Path,
        chunk_index: u64,
        count: usize,
    ) -> Vec<NodeId> {
        if self.nodes.is_empty() {
            return Vec::new();
        }

        let mut replicas = Vec::with_capacity(count.min(self.nodes.len()));
        let start_index = (chunk_index as usize) % self.nodes.len();

        for i in 0..count.min(self.nodes.len()) {
            let node_index = (start_index + i) % self.nodes.len();
            replicas.push(self.nodes[node_index].clone());
        }

        replicas
    }
}

/// Consistent Hashing配置戦略
///
/// Consistent Hashingを使用してチャンクを配置する
pub struct ConsistentHashPlacement {
    /// Consistent Hashingリング
    ring: ConsistentHashRing,
}

impl ConsistentHashPlacement {
    /// 新しいConsistent Hash配置戦略を作成
    pub fn new(ring: ConsistentHashRing) -> Self {
        Self { ring }
    }

    /// リングを取得
    pub fn ring(&self) -> &ConsistentHashRing {
        &self.ring
    }

    /// ノード数を取得
    pub fn node_count(&self) -> usize {
        self.ring.node_count()
    }

    /// チャンクキーを生成
    fn chunk_key(path: &Path, chunk_index: u64) -> String {
        format!("{}:chunk:{}", path.display(), chunk_index)
    }
}

impl PlacementStrategy for ConsistentHashPlacement {
    fn place_chunk(&self, path: &Path, chunk_index: u64) -> Option<NodeId> {
        let key = Self::chunk_key(path, chunk_index);
        self.ring.get_node(&key)
    }

    fn place_chunk_replicas(
        &self,
        path: &Path,
        chunk_index: u64,
        count: usize,
    ) -> Vec<NodeId> {
        let key = Self::chunk_key(path, chunk_index);
        self.ring.get_nodes(&key, count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_round_robin_placement() {
        let nodes = vec!["node1".to_string(), "node2".to_string(), "node3".to_string()];
        let strategy = RoundRobinPlacement::new(nodes.clone());

        assert_eq!(strategy.node_count(), 3);

        let path = Path::new("/test/file.txt");

        // チャンク0 -> ノード1
        assert_eq!(strategy.place_chunk(path, 0), Some("node1".to_string()));

        // チャンク1 -> ノード2
        assert_eq!(strategy.place_chunk(path, 1), Some("node2".to_string()));

        // チャンク2 -> ノード3
        assert_eq!(strategy.place_chunk(path, 2), Some("node3".to_string()));

        // チャンク3 -> ノード1 (ラウンドロビン)
        assert_eq!(strategy.place_chunk(path, 3), Some("node1".to_string()));

        // チャンク4 -> ノード2
        assert_eq!(strategy.place_chunk(path, 4), Some("node2".to_string()));
    }

    #[test]
    fn test_round_robin_empty_nodes() {
        let strategy = RoundRobinPlacement::new(vec![]);
        let path = Path::new("/test/file.txt");

        assert_eq!(strategy.place_chunk(path, 0), None);
    }

    #[test]
    fn test_round_robin_replicas() {
        let nodes = vec!["node1".to_string(), "node2".to_string(), "node3".to_string(), "node4".to_string()];
        let strategy = RoundRobinPlacement::new(nodes);

        let path = Path::new("/test/file.txt");

        // チャンク0の2つのレプリカ: ノード1, 2
        let replicas = strategy.place_chunk_replicas(path, 0, 2);
        assert_eq!(replicas.len(), 2);
        assert_eq!(replicas[0], "node1");
        assert_eq!(replicas[1], "node2");

        // チャンク1の3つのレプリカ: ノード2, 3, 4
        let replicas = strategy.place_chunk_replicas(path, 1, 3);
        assert_eq!(replicas.len(), 3);
        assert_eq!(replicas[0], "node2");
        assert_eq!(replicas[1], "node3");
        assert_eq!(replicas[2], "node4");

        // チャンク3の2つのレプリカ: ノード4, 1 (ラップアラウンド)
        let replicas = strategy.place_chunk_replicas(path, 3, 2);
        assert_eq!(replicas.len(), 2);
        assert_eq!(replicas[0], "node4");
        assert_eq!(replicas[1], "node1");
    }

    #[test]
    fn test_round_robin_replicas_exceed_nodes() {
        let nodes = vec!["node1".to_string(), "node2".to_string(), "node3".to_string()];
        let strategy = RoundRobinPlacement::new(nodes);

        let path = Path::new("/test/file.txt");

        // ノード数より多いレプリカを要求 -> 最大でノード数まで
        let replicas = strategy.place_chunk_replicas(path, 0, 10);
        assert_eq!(replicas.len(), 3);
        assert_eq!(replicas[0], "node1");
        assert_eq!(replicas[1], "node2");
        assert_eq!(replicas[2], "node3");
    }

    #[test]
    fn test_consistent_hash_placement() {
        let mut ring = ConsistentHashRing::new();
        ring.add_node("node1".to_string());
        ring.add_node("node2".to_string());
        ring.add_node("node3".to_string());

        let strategy = ConsistentHashPlacement::new(ring);
        assert_eq!(strategy.node_count(), 3);

        let path = Path::new("/test/file.txt");

        // 同じパス+チャンクインデックスは常に同じノードにマッピング
        let node1 = strategy.place_chunk(path, 0);
        let node2 = strategy.place_chunk(path, 0);
        assert_eq!(node1, node2);

        // 異なるチャンクは異なるノードにマッピングされる可能性がある
        let node_chunk0 = strategy.place_chunk(path, 0);
        let node_chunk1 = strategy.place_chunk(path, 1);
        assert!(node_chunk0.is_some());
        assert!(node_chunk1.is_some());
    }

    #[test]
    fn test_consistent_hash_replicas() {
        let mut ring = ConsistentHashRing::new();
        ring.add_node("node1".to_string());
        ring.add_node("node2".to_string());
        ring.add_node("node3".to_string());

        let strategy = ConsistentHashPlacement::new(ring);
        let path = Path::new("/test/file.txt");

        // レプリカ取得
        let replicas = strategy.place_chunk_replicas(path, 0, 2);
        assert_eq!(replicas.len(), 2);

        // 重複がないことを確認
        assert_ne!(replicas[0], replicas[1]);
    }

    #[test]
    fn test_consistent_hash_different_paths() {
        let mut ring = ConsistentHashRing::new();
        ring.add_node("node1".to_string());
        ring.add_node("node2".to_string());
        ring.add_node("node3".to_string());

        let strategy = ConsistentHashPlacement::new(ring);

        let path1 = Path::new("/test/file1.txt");
        let path2 = Path::new("/test/file2.txt");

        // 異なるパスの同じチャンクは異なるノードにマッピングされる可能性がある
        let node1 = strategy.place_chunk(path1, 0);
        let node2 = strategy.place_chunk(path2, 0);

        assert!(node1.is_some());
        assert!(node2.is_some());
        // 必ずしも異なるとは限らないが、両方とも有効なノードID
    }

    #[test]
    fn test_chunk_key_generation() {
        let path = PathBuf::from("/foo/bar.txt");
        let key = ConsistentHashPlacement::chunk_key(&path, 5);
        assert_eq!(key, "/foo/bar.txt:chunk:5");
    }

    #[test]
    fn test_placement_distribution() {
        // ラウンドロビンの分散をテスト
        let nodes = vec!["node1".to_string(), "node2".to_string(), "node3".to_string()];
        let strategy = RoundRobinPlacement::new(nodes);

        let path = Path::new("/test/file.txt");
        let mut distribution = std::collections::HashMap::new();

        for i in 0..30 {
            if let Some(node_id) = strategy.place_chunk(path, i) {
                *distribution.entry(node_id).or_insert(0) += 1;
            }
        }

        // 各ノードが10回ずつ選ばれることを確認
        assert_eq!(distribution.get("node1"), Some(&10));
        assert_eq!(distribution.get("node2"), Some(&10));
        assert_eq!(distribution.get("node3"), Some(&10));
    }

    #[test]
    fn test_consistent_hash_chunk_distribution() {
        let mut ring = ConsistentHashRing::new();
        ring.add_node("node1".to_string());
        ring.add_node("node2".to_string());
        ring.add_node("node3".to_string());

        let strategy = ConsistentHashPlacement::new(ring);
        let path = Path::new("/test/large_file.bin");

        let mut distribution = std::collections::HashMap::new();

        // 100チャンクを配置
        for i in 0..100 {
            if let Some(node_id) = strategy.place_chunk(path, i) {
                *distribution.entry(node_id).or_insert(0) += 1;
            }
        }

        // 各ノードが少なくとも10チャンク以上担当していることを確認
        for node_id in &["node1".to_string(), "node2".to_string(), "node3".to_string()] {
            let count = distribution.get(node_id).unwrap_or(&0);
            assert!(
                *count >= 10,
                "Node {} has only {} chunks (expected >= 10)",
                node_id,
                count
            );
        }
    }
}
