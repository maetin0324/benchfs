use super::{
    consistent_hash::ConsistentHashRing,
    types::{DirectoryMetadata, FileMetadata, InodeId, NodeId},
};
use crate::cache::{MetadataCache, CachePolicy};
use std::cell::RefCell;
use std::collections::HashMap;
use std::path::Path;

/// メタデータ管理エラー
#[derive(Debug, thiserror::Error)]
pub enum MetadataError {
    #[error("Metadata not found: {0}")]
    NotFound(String),

    #[error("Metadata already exists: {0}")]
    AlreadyExists(String),

    #[error("No nodes available in the hash ring")]
    NoNodesAvailable,

    #[error("Invalid path: {0}")]
    InvalidPath(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

pub type MetadataResult<T> = Result<T, MetadataError>;

/// メタデータマネージャー
///
/// ファイルとディレクトリのメタデータを管理し、Consistent Hashingを使用して
/// 適切なノードにメタデータを分散配置する。
///
/// Note: シングルスレッド設計のため、RefCellを使用
pub struct MetadataManager {
    /// Consistent Hashingリング
    ring: RefCell<ConsistentHashRing>,

    /// ローカルに保存されているファイルメタデータ (path -> metadata)
    local_file_metadata: RefCell<HashMap<String, FileMetadata>>,

    /// ローカルに保存されているディレクトリメタデータ (path -> metadata)
    local_dir_metadata: RefCell<HashMap<String, DirectoryMetadata>>,

    /// メタデータキャッシュ
    cache: MetadataCache,

    /// 自ノードのID
    self_node_id: NodeId,
}

impl MetadataManager {
    /// 新しいメタデータマネージャーを作成
    ///
    /// # Arguments
    /// * `self_node_id` - 自ノードのID
    pub fn new(self_node_id: NodeId) -> Self {
        Self::with_cache_policy(self_node_id, CachePolicy::default())
    }

    /// キャッシュポリシーを指定してメタデータマネージャーを作成
    ///
    /// # Arguments
    /// * `self_node_id` - 自ノードのID
    /// * `cache_policy` - キャッシュポリシー
    pub fn with_cache_policy(self_node_id: NodeId, cache_policy: CachePolicy) -> Self {
        let mut ring = ConsistentHashRing::new();
        ring.add_node(self_node_id.clone());

        Self {
            ring: RefCell::new(ring),
            local_file_metadata: RefCell::new(HashMap::new()),
            local_dir_metadata: RefCell::new(HashMap::new()),
            cache: MetadataCache::new(cache_policy),
            self_node_id,
        }
    }

    /// ノードを追加
    pub fn add_node(&self, node_id: NodeId) {
        self.ring.borrow_mut().add_node(node_id);
    }

    /// ノードを削除
    pub fn remove_node(&self, node_id: &NodeId) -> bool {
        self.ring.borrow_mut().remove_node(node_id)
    }

    /// 指定されたパスを管理するノードを取得
    ///
    /// # Arguments
    /// * `path` - ファイル/ディレクトリのパス
    ///
    /// # Returns
    /// 対応するノードID
    pub fn get_owner_node(&self, path: &Path) -> MetadataResult<NodeId> {
        let path_str = path.to_str().ok_or_else(|| {
            MetadataError::InvalidPath(format!("Invalid UTF-8 in path: {:?}", path))
        })?;

        self.ring
            .borrow()
            .get_node(path_str)
            .ok_or(MetadataError::NoNodesAvailable)
    }

    /// 指定されたパスを管理する複数のノードを取得 (レプリケーション用)
    ///
    /// # Arguments
    /// * `path` - ファイル/ディレクトリのパス
    /// * `count` - 取得するノード数
    pub fn get_owner_nodes(&self, path: &Path, count: usize) -> MetadataResult<Vec<NodeId>> {
        let path_str = path.to_str().ok_or_else(|| {
            MetadataError::InvalidPath(format!("Invalid UTF-8 in path: {:?}", path))
        })?;

        let nodes = self.ring.borrow().get_nodes(path_str, count);
        if nodes.is_empty() {
            return Err(MetadataError::NoNodesAvailable);
        }

        Ok(nodes)
    }

    /// 指定されたパスが自ノードで管理されているか確認
    pub fn is_local_owner(&self, path: &Path) -> MetadataResult<bool> {
        let owner = self.get_owner_node(path)?;
        Ok(owner == self.self_node_id)
    }

    /// ファイルメタデータをローカルに保存
    ///
    /// # Arguments
    /// * `metadata` - 保存するファイルメタデータ
    pub fn store_file_metadata(&self, metadata: FileMetadata) -> MetadataResult<()> {
        let path = metadata.path.clone();

        let mut local_metadata = self.local_file_metadata.borrow_mut();

        if local_metadata.contains_key(&path) {
            return Err(MetadataError::AlreadyExists(path));
        }

        local_metadata.insert(path.clone(), metadata);
        tracing::debug!("Stored file metadata for: {}", path);

        Ok(())
    }

    /// ファイルメタデータをローカルから取得
    ///
    /// # Arguments
    /// * `path` - ファイルパス
    pub fn get_file_metadata(&self, path: &Path) -> MetadataResult<FileMetadata> {
        let path_str = path
            .to_str()
            .ok_or_else(|| MetadataError::InvalidPath(format!("Invalid UTF-8 in path: {:?}", path)))?;

        // Check cache first
        if let Some(cached) = self.cache.get_file(path_str) {
            tracing::trace!("Cache hit for file metadata: {}", path_str);
            return Ok(cached);
        }

        // Cache miss, get from local storage
        let metadata = self.local_file_metadata
            .borrow()
            .get(path_str)
            .cloned()
            .ok_or_else(|| MetadataError::NotFound(path_str.to_string()))?;

        // Cache the result
        self.cache.put_file(path_str.to_string(), metadata.clone());

        Ok(metadata)
    }

    /// ファイルメタデータを更新
    pub fn update_file_metadata(&self, metadata: FileMetadata) -> MetadataResult<()> {
        let path = metadata.path.clone();

        let mut local_metadata = self.local_file_metadata.borrow_mut();

        if !local_metadata.contains_key(&path) {
            return Err(MetadataError::NotFound(path.clone()));
        }

        local_metadata.insert(path.clone(), metadata.clone());

        // Invalidate cache
        self.cache.invalidate_file(&path);

        tracing::debug!("Updated file metadata for: {}", path);

        Ok(())
    }

    /// ファイルメタデータをローカルから削除
    pub fn remove_file_metadata(&self, path: &Path) -> MetadataResult<()> {
        let path_str = path
            .to_str()
            .ok_or_else(|| MetadataError::InvalidPath(format!("Invalid UTF-8 in path: {:?}", path)))?;

        let mut local_metadata = self.local_file_metadata.borrow_mut();

        local_metadata
            .remove(path_str)
            .ok_or_else(|| MetadataError::NotFound(path_str.to_string()))?;

        // Invalidate cache
        self.cache.invalidate_file(path_str);

        tracing::debug!("Removed file metadata for: {}", path_str);

        Ok(())
    }

    /// ディレクトリメタデータをローカルに保存
    pub fn store_dir_metadata(&self, metadata: DirectoryMetadata) -> MetadataResult<()> {
        let path = metadata.path.clone();

        let mut local_metadata = self.local_dir_metadata.borrow_mut();

        if local_metadata.contains_key(&path) {
            return Err(MetadataError::AlreadyExists(path));
        }

        local_metadata.insert(path.clone(), metadata);
        tracing::debug!("Stored directory metadata for: {}", path);

        Ok(())
    }

    /// ディレクトリメタデータをローカルから取得
    pub fn get_dir_metadata(&self, path: &Path) -> MetadataResult<DirectoryMetadata> {
        let path_str = path
            .to_str()
            .ok_or_else(|| MetadataError::InvalidPath(format!("Invalid UTF-8 in path: {:?}", path)))?;

        // Check cache first
        if let Some(cached) = self.cache.get_dir(path_str) {
            tracing::trace!("Cache hit for directory metadata: {}", path_str);
            return Ok(cached);
        }

        // Cache miss, get from local storage
        let metadata = self.local_dir_metadata
            .borrow()
            .get(path_str)
            .cloned()
            .ok_or_else(|| MetadataError::NotFound(path_str.to_string()))?;

        // Cache the result
        self.cache.put_dir(path_str.to_string(), metadata.clone());

        Ok(metadata)
    }

    /// ディレクトリメタデータを更新
    pub fn update_dir_metadata(&self, metadata: DirectoryMetadata) -> MetadataResult<()> {
        let path = metadata.path.clone();

        let mut local_metadata = self.local_dir_metadata.borrow_mut();

        if !local_metadata.contains_key(&path) {
            return Err(MetadataError::NotFound(path.clone()));
        }

        local_metadata.insert(path.clone(), metadata.clone());

        // Invalidate cache
        self.cache.invalidate_dir(&path);

        tracing::debug!("Updated directory metadata for: {}", path);

        Ok(())
    }

    /// ディレクトリメタデータをローカルから削除
    pub fn remove_dir_metadata(&self, path: &Path) -> MetadataResult<()> {
        let path_str = path
            .to_str()
            .ok_or_else(|| MetadataError::InvalidPath(format!("Invalid UTF-8 in path: {:?}", path)))?;

        let mut local_metadata = self.local_dir_metadata.borrow_mut();

        local_metadata
            .remove(path_str)
            .ok_or_else(|| MetadataError::NotFound(path_str.to_string()))?;

        // Invalidate cache
        self.cache.invalidate_dir(path_str);

        tracing::debug!("Removed directory metadata for: {}", path_str);

        Ok(())
    }

    /// ローカルに保存されているファイルメタデータの数
    pub fn local_file_count(&self) -> usize {
        self.local_file_metadata.borrow().len()
    }

    /// ローカルに保存されているディレクトリメタデータの数
    pub fn local_dir_count(&self) -> usize {
        self.local_dir_metadata.borrow().len()
    }

    /// リング内のノード数
    pub fn node_count(&self) -> usize {
        self.ring.borrow().node_count()
    }

    /// 自ノードのID
    pub fn self_node_id(&self) -> NodeId {
        self.self_node_id.clone()
    }

    /// 新しいinode番号を生成
    ///
    /// 簡易実装: 現在時刻のナノ秒をベースにしたinode生成
    /// 本番環境では分散ID生成アルゴリズム（SnowflakeなEd）を使用すべき
    pub fn generate_inode(&self) -> InodeId {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }

    /// キャッシュ統計を取得
    pub fn cache_stats(&self) -> crate::cache::metadata_cache::CacheStats {
        self.cache.stats()
    }

    /// キャッシュをクリア
    pub fn clear_cache(&self) {
        self.cache.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_metadata_manager_creation() {
        let manager = MetadataManager::new("node1".to_string());
        assert_eq!(manager.self_node_id(), "node1");
        assert_eq!(manager.node_count(), 1);
    }

    #[test]
    fn test_add_remove_nodes() {
        let manager = MetadataManager::new("node1".to_string());

        manager.add_node("node2".to_string());
        manager.add_node("node3".to_string());
        assert_eq!(manager.node_count(), 3);

        assert!(manager.remove_node(&"node2".to_string()));
        assert_eq!(manager.node_count(), 2);

        assert!(!manager.remove_node(&"node999".to_string())); // 存在しないノード
    }

    #[test]
    fn test_get_owner_node() {
        let manager = MetadataManager::new("node1".to_string());
        manager.add_node("node2".to_string());
        manager.add_node("node3".to_string());

        let path = Path::new("/foo/bar.txt");
        let owner = manager.get_owner_node(path).unwrap();

        // 同じパスは常に同じオーナーを返す
        let owner2 = manager.get_owner_node(path).unwrap();
        assert_eq!(owner, owner2);

        // オーナーは存在するノードのいずれか
        assert!(owner == "node1" || owner == "node2" || owner == "node3");
    }

    #[test]
    fn test_get_owner_nodes_for_replication() {
        let manager = MetadataManager::new("node1".to_string());
        manager.add_node("node2".to_string());
        manager.add_node("node3".to_string());

        let path = Path::new("/foo/bar.txt");
        let owners = manager.get_owner_nodes(path, 2).unwrap();

        assert_eq!(owners.len(), 2);
        assert_ne!(owners[0], owners[1]); // 異なるノード
    }

    #[test]
    fn test_is_local_owner() {
        let manager = MetadataManager::new("node1".to_string());
        manager.add_node("node2".to_string());
        manager.add_node("node3".to_string());

        // 多数のパスをテストして、少なくとも1つは自ノードがオーナーであることを確認
        let mut has_local = false;
        let mut has_remote = false;

        for i in 0..100 {
            let path = PathBuf::from(format!("/file/{}.txt", i));
            let is_local = manager.is_local_owner(&path).unwrap();
            if is_local {
                has_local = true;
            } else {
                has_remote = true;
            }
        }

        assert!(has_local, "Expected at least one file to be owned locally");
        assert!(
            has_remote,
            "Expected at least one file to be owned remotely"
        );
    }

    #[test]
    fn test_store_and_get_file_metadata() {
        let manager = MetadataManager::new("node1".to_string());

        let path = PathBuf::from("/foo/bar.txt");
        let metadata = FileMetadata::new(123, path.to_str().unwrap().to_string(), 1024);

        manager.store_file_metadata(metadata.clone()).unwrap();
        assert_eq!(manager.local_file_count(), 1);

        let retrieved = manager.get_file_metadata(&path).unwrap();
        assert_eq!(retrieved.inode, 123);
        assert_eq!(retrieved.size, 1024);
    }

    #[test]
    fn test_store_duplicate_file_metadata() {
        let manager = MetadataManager::new("node1".to_string());

        let path = PathBuf::from("/foo/bar.txt");
        let metadata = FileMetadata::new(123, path.to_str().unwrap().to_string(), 1024);

        manager.store_file_metadata(metadata.clone()).unwrap();

        // 重複エラー
        let result = manager.store_file_metadata(metadata.clone());
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), MetadataError::AlreadyExists(_)));
    }

    #[test]
    fn test_update_file_metadata() {
        let manager = MetadataManager::new("node1".to_string());

        let path = PathBuf::from("/foo/bar.txt");
        let mut metadata = FileMetadata::new(123, path.to_str().unwrap().to_string(), 1024);

        manager.store_file_metadata(metadata.clone()).unwrap();

        // サイズ更新
        metadata.size = 2048;
        manager.update_file_metadata(metadata.clone()).unwrap();

        let retrieved = manager.get_file_metadata(&path).unwrap();
        assert_eq!(retrieved.size, 2048);
    }

    #[test]
    fn test_remove_file_metadata() {
        let manager = MetadataManager::new("node1".to_string());

        let path = PathBuf::from("/foo/bar.txt");
        let metadata = FileMetadata::new(123, path.to_str().unwrap().to_string(), 1024);

        manager.store_file_metadata(metadata).unwrap();
        assert_eq!(manager.local_file_count(), 1);

        manager.remove_file_metadata(&path).unwrap();
        assert_eq!(manager.local_file_count(), 0);

        // 再度削除しようとするとエラー
        let result = manager.remove_file_metadata(&path);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), MetadataError::NotFound(_)));
    }

    #[test]
    fn test_store_and_get_dir_metadata() {
        let manager = MetadataManager::new("node1".to_string());

        let path = PathBuf::from("/foo");
        let metadata = DirectoryMetadata::new(456, path.to_str().unwrap().to_string());

        manager.store_dir_metadata(metadata.clone()).unwrap();
        assert_eq!(manager.local_dir_count(), 1);

        let retrieved = manager.get_dir_metadata(&path).unwrap();
        assert_eq!(retrieved.inode, 456);
    }

    #[test]
    fn test_remove_dir_metadata() {
        let manager = MetadataManager::new("node1".to_string());

        let path = PathBuf::from("/foo");
        let metadata = DirectoryMetadata::new(456, path.to_str().unwrap().to_string());

        manager.store_dir_metadata(metadata).unwrap();
        assert_eq!(manager.local_dir_count(), 1);

        manager.remove_dir_metadata(&path).unwrap();
        assert_eq!(manager.local_dir_count(), 0);
    }
}
