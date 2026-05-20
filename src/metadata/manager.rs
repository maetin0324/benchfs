use super::{
    consistent_hash::ConsistentHashRing,
    id_generator::IdGenerator,
    types::{DirectoryMetadata, FileMetadata, InodeId, InodeType, NodeId},
};
use crate::cache::{CachePolicy, MetadataCache};
use std::cell::RefCell;
use std::collections::HashMap;
use std::path::Path;
use std::rc::Rc;
use tracing::instrument;

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

    /// 親パス → 直接の子エントリ (name → type) の索引。
    /// readdir で O(子数) 検索するための補助構造。job 20645 の
    /// find phase が mdtest-easy で 30s timeout していたのは
    /// `list_local_entries` が全 file/dir metadata を線形スキャン
    /// していたため (3M+ エントリで非現実的)。
    /// すべての store_*_metadata / remove_*_metadata で更新する。
    local_dir_children: RefCell<HashMap<String, HashMap<String, InodeType>>>,

    /// `local_dir_children` のソート済みビュー。`list_local_entries` の
    /// paginated readdir では同じ親に対して連続して call が来る (offset を
    /// 進めながら)。job 20666 で 1.28 M children を毎回 O(N log N) で
    /// sort して 520 ms/call ×1700 calls = 884 s の find phase 時間を
    /// 消費していた。書き込み中はキャッシュ invalidate、読み出し phase
    /// では一度だけ sort して再利用 — find 全体を ~12 s 程度に短縮できる。
    sorted_children_cache: RefCell<HashMap<String, Rc<Vec<(String, InodeType)>>>>,

    /// メタデータキャッシュ
    cache: MetadataCache,

    /// 自ノードのID
    self_node_id: NodeId,

    /// 分散ID生成器（inode番号生成用）
    id_generator: IdGenerator,
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

        // ノードIDの文字列からID生成器を作成
        let id_generator = IdGenerator::from_node_string(&self_node_id);

        Self {
            ring: RefCell::new(ring),
            local_file_metadata: RefCell::new(HashMap::new()),
            local_dir_metadata: RefCell::new(HashMap::new()),
            local_dir_children: RefCell::new(HashMap::new()),
            sorted_children_cache: RefCell::new(HashMap::new()),
            cache: MetadataCache::new(cache_policy),
            self_node_id,
            id_generator,
        }
    }

    /// `path` を末尾 segment を分割し、`(parent, child)` を返す。
    /// ルートや末尾スラッシュは正規化済みであることを想定。
    /// 例: `/A/B/C` → `Some(("/A/B", "C"))`、`/X` → `Some(("/", "X"))`、`/` → `None`。
    fn split_parent_child(path: &str) -> Option<(String, String)> {
        let trimmed = path.trim_end_matches('/');
        if trimmed.is_empty() {
            return None; // root
        }
        let idx = trimmed.rfind('/')?;
        let parent = if idx == 0 {
            "/".to_string()
        } else {
            trimmed[..idx].to_string()
        };
        let child = trimmed[idx + 1..].to_string();
        if child.is_empty() {
            return None;
        }
        Some((parent, child))
    }

    /// 親パスの子索引に `(child, ty)` を追加し、さらに祖先方向にも virtual
    /// directory placeholder を伝播する。これにより、明示的に mkdir されて
    /// いない中間ディレクトリ (mdtest-hard の deeply-nested tree など) も
    /// readdir で見える。既存の Directory placeholder は実体
    /// (file/symlink) で上書きされ、その逆は起きない (CHFS の
    /// `fs_add_entry` と同じ規則)。
    /// public: CHFS-style 中央 parent index で他 server からの
    /// DirIndexUpdate RPC handler から呼ばれるため。
    pub fn dir_index_insert(&self, path: &str, ty: InodeType) {
        let mut idx = self.local_dir_children.borrow_mut();
        let mut cache = self.sorted_children_cache.borrow_mut();
        let mut cur = path.to_string();
        let mut leaf_ty = ty;
        loop {
            let Some((parent, child)) = Self::split_parent_child(&cur) else {
                break;
            };
            let entry = idx.entry(parent.clone()).or_default();
            let modified = match entry.get(&child) {
                Some(InodeType::Directory) => {
                    if !matches!(leaf_ty, InodeType::Directory) {
                        entry.insert(child.clone(), leaf_ty);
                        true
                    } else {
                        false
                    }
                }
                Some(_) => false,
                None => {
                    entry.insert(child.clone(), leaf_ty);
                    true
                }
            };
            if modified {
                cache.remove(&parent);
            }
            if parent == "/" {
                break;
            }
            cur = parent;
            leaf_ty = InodeType::Directory;
        }
    }

    /// 親パスの子索引から `child` を削除。エントリが空になった場合は
    /// 親自体も除去し、祖先方向にも空エントリの掃除を伝播する。
    /// 明示的に mkdir されたパスとの整合性を厳密には保たないが
    /// (祖先が空になっても他の兄弟が残っている場合は無触)、readdir の
    /// 動作には影響しない。
    /// public: CHFS-style 中央 parent index で他 server からの
    /// DirIndexUpdate RPC handler から呼ばれるため。
    pub fn dir_index_remove(&self, path: &str) {
        let mut idx = self.local_dir_children.borrow_mut();
        let mut cache = self.sorted_children_cache.borrow_mut();
        let mut cur = path.to_string();
        loop {
            let Some((parent, child)) = Self::split_parent_child(&cur) else {
                break;
            };
            let Some(entry) = idx.get_mut(&parent) else {
                break;
            };
            if entry.remove(&child).is_some() {
                cache.remove(&parent);
            }
            if !entry.is_empty() {
                break;
            }
            idx.remove(&parent);
            cache.remove(&parent);
            if parent == "/" {
                break;
            }
            cur = parent;
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
    #[instrument(level = "trace", name = "metadata_store_file", skip(self, metadata), fields(path = %metadata.path))]
    pub fn store_file_metadata(&self, metadata: FileMetadata) -> MetadataResult<()> {
        let path = metadata.path.clone();

        {
            let mut local_metadata = self.local_file_metadata.borrow_mut();
            if local_metadata.contains_key(&path) {
                return Err(MetadataError::AlreadyExists(path));
            }
            local_metadata.insert(path.clone(), metadata);
        }
        self.dir_index_insert(&path, InodeType::File);
        tracing::debug!("Stored file metadata for: {}", path);

        Ok(())
    }

    /// ファイルメタデータをローカルから取得
    ///
    /// # Arguments
    /// * `path` - ファイルパス
    #[instrument(level = "trace", name = "metadata_get_file", skip(self), fields(path = ?path))]
    pub fn get_file_metadata(&self, path: &Path) -> MetadataResult<FileMetadata> {
        let path_str = path.to_str().ok_or_else(|| {
            MetadataError::InvalidPath(format!("Invalid UTF-8 in path: {:?}", path))
        })?;

        // Check cache first
        if let Some(cached) = self.cache.get_file(path_str) {
            tracing::trace!("Cache hit for file metadata: {}", path_str);
            return Ok(cached);
        }

        // Cache miss, get from local storage
        let metadata = self
            .local_file_metadata
            .borrow()
            .get(path_str)
            .cloned()
            .ok_or_else(|| MetadataError::NotFound(path_str.to_string()))?;

        // Cache the result
        self.cache.put_file(path_str.to_string(), metadata.clone());

        Ok(metadata)
    }

    /// ファイルメタデータを更新
    #[instrument(level = "trace", name = "metadata_update_file", skip(self, metadata), fields(path = %metadata.path))]
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
    #[instrument(level = "trace", name = "metadata_remove_file", skip(self), fields(path = ?path))]
    pub fn remove_file_metadata(&self, path: &Path) -> MetadataResult<()> {
        let path_str = path.to_str().ok_or_else(|| {
            MetadataError::InvalidPath(format!("Invalid UTF-8 in path: {:?}", path))
        })?;

        {
            let mut local_metadata = self.local_file_metadata.borrow_mut();
            local_metadata
                .remove(path_str)
                .ok_or_else(|| MetadataError::NotFound(path_str.to_string()))?;
        }
        self.dir_index_remove(path_str);

        // Invalidate cache
        self.cache.invalidate_file(path_str);

        tracing::debug!("Removed file metadata for: {}", path_str);

        Ok(())
    }

    /// ディレクトリメタデータをローカルに保存
    #[instrument(level = "trace", name = "metadata_store_dir", skip(self, metadata), fields(path = %metadata.path))]
    pub fn store_dir_metadata(&self, metadata: DirectoryMetadata) -> MetadataResult<()> {
        let path = metadata.path.clone();

        {
            let mut local_metadata = self.local_dir_metadata.borrow_mut();
            if local_metadata.contains_key(&path) {
                return Err(MetadataError::AlreadyExists(path));
            }
            local_metadata.insert(path.clone(), metadata);
        }
        self.dir_index_insert(&path, InodeType::Directory);
        tracing::debug!("Stored directory metadata for: {}", path);

        Ok(())
    }

    /// ディレクトリメタデータをローカルから取得
    #[instrument(level = "trace", name = "metadata_get_dir", skip(self), fields(path = ?path))]
    pub fn get_dir_metadata(&self, path: &Path) -> MetadataResult<DirectoryMetadata> {
        let path_str = path.to_str().ok_or_else(|| {
            MetadataError::InvalidPath(format!("Invalid UTF-8 in path: {:?}", path))
        })?;

        // Check cache first
        if let Some(cached) = self.cache.get_dir(path_str) {
            tracing::trace!("Cache hit for directory metadata: {}", path_str);
            return Ok(cached);
        }

        // Cache miss, get from local storage
        let metadata = self
            .local_dir_metadata
            .borrow()
            .get(path_str)
            .cloned()
            .ok_or_else(|| MetadataError::NotFound(path_str.to_string()))?;

        // Cache the result
        self.cache.put_dir(path_str.to_string(), metadata.clone());

        Ok(metadata)
    }

    /// ディレクトリメタデータを更新
    #[instrument(level = "trace", name = "metadata_update_dir", skip(self, metadata), fields(path = %metadata.path))]
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
    #[instrument(level = "trace", name = "metadata_remove_dir", skip(self), fields(path = ?path))]
    pub fn remove_dir_metadata(&self, path: &Path) -> MetadataResult<()> {
        let path_str = path.to_str().ok_or_else(|| {
            MetadataError::InvalidPath(format!("Invalid UTF-8 in path: {:?}", path))
        })?;

        {
            let mut local_metadata = self.local_dir_metadata.borrow_mut();
            local_metadata
                .remove(path_str)
                .ok_or_else(|| MetadataError::NotFound(path_str.to_string()))?;
        }
        self.dir_index_remove(path_str);

        // Invalidate cache
        self.cache.invalidate_dir(path_str);

        tracing::debug!("Removed directory metadata for: {}", path_str);

        Ok(())
    }

    /// 親パス直下のエントリをローカルメタデータから列挙 (CHFS-style scatter readdir)
    ///
    /// CHFS の `fs_readdir_cb` / `fs_add_entry` と同じ規則:
    ///
    /// - パスが `<parent>/` で始まるエントリのみを対象とする
    /// - 残りが '/' を含まない → 直接の子 (File or Directory) として返す
    /// - 残りが '/' を含む → 最初のセグメントを virtual directory として返す
    ///   (deeply nested mdtest tree 用)
    ///
    /// `offset` は dedup 済みエントリのスキップ数、`limit` は返却上限。
    /// 戻り値: `(entries, is_truncated)` — `is_truncated=true` のとき続きがある。
    pub fn list_local_entries(
        &self,
        parent: &str,
        offset: usize,
        limit: usize,
    ) -> (Vec<(String, InodeType)>, bool) {
        // Normalize parent: strip trailing slash except for root.
        let key = if parent.len() > 1 && parent.ends_with('/') {
            &parent[..parent.len() - 1]
        } else {
            parent
        };

        // Cache-first: paginated readdir over a large dir (mdtest_tree.0
        // with 1.28M children, see job 20666 timing) used to re-sort on
        // every call and dominate the find phase at ~520 ms/call. The
        // sorted Vec is rebuilt lazily after any insert/remove via
        // `sorted_children_cache.remove(parent)`.
        let sorted: Rc<Vec<(String, InodeType)>> = {
            if let Some(cached) = self.sorted_children_cache.borrow().get(key) {
                Rc::clone(cached)
            } else {
                let idx = self.local_dir_children.borrow();
                let Some(children) = idx.get(key) else {
                    return (Vec::new(), false);
                };
                let mut entries: Vec<(String, InodeType)> =
                    children.iter().map(|(n, t)| (n.clone(), *t)).collect();
                entries.sort_unstable_by(|a, b| a.0.cmp(&b.0));
                let rc = Rc::new(entries);
                self.sorted_children_cache
                    .borrow_mut()
                    .insert(key.to_string(), Rc::clone(&rc));
                rc
            }
        };

        let total = sorted.len();
        let start = offset.min(total);
        let end = (start + limit).min(total);
        let truncated = end < total;
        let out: Vec<(String, InodeType)> = sorted[start..end].to_vec();
        (out, truncated)
    }

    /// readdirplus 用: list_local_entries と同じ paginated/sorted
    /// children を返すが、各エントリに size を付ける。FileMetadata
    /// が無い場合 (race or directory) は size = 0。
    pub fn list_local_entries_with_size(
        &self,
        parent: &str,
        offset: usize,
        limit: usize,
    ) -> (Vec<(String, InodeType, u64)>, bool) {
        let (entries, truncated) = self.list_local_entries(parent, offset, limit);
        let key = if parent.len() > 1 && parent.ends_with('/') {
            &parent[..parent.len() - 1]
        } else {
            parent
        };
        let files = self.local_file_metadata.borrow();
        let out: Vec<(String, InodeType, u64)> = entries
            .into_iter()
            .map(|(name, ty)| {
                let size = if matches!(ty, InodeType::File) {
                    let full = if key == "/" {
                        format!("/{}", name)
                    } else {
                        format!("{}/{}", key, name)
                    };
                    files.get(&full).map(|m| m.size).unwrap_or(0)
                } else {
                    0
                };
                (name, ty, size)
            })
            .collect();
        (out, truncated)
    }

    /// ローカルに保存されているファイルメタデータの数
    pub fn local_file_count(&self) -> usize {
        self.local_file_metadata.borrow().len()
    }

    /// ローカルに保存されているディレクトリメタデータの数
    pub fn local_dir_count(&self) -> usize {
        self.local_dir_metadata.borrow().len()
    }

    /// Take a snapshot of every local file (path, size). Used by the
    /// `OnFinalize` flush path at shutdown to enumerate everything that
    /// needs to be persisted to disk in one pass. Returns owned Strings
    /// so the caller doesn't hold the RefCell borrow.
    pub fn snapshot_local_files(&self) -> Vec<(String, u64)> {
        self.local_file_metadata
            .borrow()
            .iter()
            .map(|(p, m)| (p.clone(), m.size))
            .collect()
    }

    /// Take a snapshot of every local directory path. Companion to
    /// `snapshot_local_files`.
    pub fn snapshot_local_dirs(&self) -> Vec<String> {
        self.local_dir_metadata.borrow().keys().cloned().collect()
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
    /// Snowflake-likeな分散ID生成アルゴリズムを使用して、
    /// グローバルにユニークなinode番号を生成する。
    ///
    /// 生成されるIDは以下の特性を持つ:
    /// - タイムスタンプベースで単調増加
    /// - ノードID埋め込みで分散環境でのユニーク性保証
    /// - 1ミリ秒あたり最大4096個のID生成が可能
    pub fn generate_inode(&self) -> InodeId {
        self.id_generator
            .next_id()
            .expect("Failed to generate inode")
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
        let metadata = FileMetadata::new(path.to_str().unwrap().to_string(), 1024);

        manager.store_file_metadata(metadata.clone()).unwrap();
        assert_eq!(manager.local_file_count(), 1);

        let retrieved = manager.get_file_metadata(&path).unwrap();
        assert_eq!(retrieved.size, 1024);
        assert_eq!(retrieved.path, path.to_str().unwrap());
    }

    #[test]
    fn test_store_duplicate_file_metadata() {
        let manager = MetadataManager::new("node1".to_string());

        let path = PathBuf::from("/foo/bar.txt");
        let metadata = FileMetadata::new(path.to_str().unwrap().to_string(), 1024);

        manager.store_file_metadata(metadata.clone()).unwrap();

        // 重複エラー
        let result = manager.store_file_metadata(metadata.clone());
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            MetadataError::AlreadyExists(_)
        ));
    }

    #[test]
    fn test_update_file_metadata() {
        let manager = MetadataManager::new("node1".to_string());

        let path = PathBuf::from("/foo/bar.txt");
        let mut metadata = FileMetadata::new(path.to_str().unwrap().to_string(), 1024);

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
        let metadata = FileMetadata::new(path.to_str().unwrap().to_string(), 1024);

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

    #[test]
    fn test_list_local_entries_direct_children() {
        let manager = MetadataManager::new("node1".to_string());

        manager
            .store_file_metadata(FileMetadata::new("/dir/a.txt".to_string(), 1))
            .unwrap();
        manager
            .store_file_metadata(FileMetadata::new("/dir/b.txt".to_string(), 2))
            .unwrap();
        manager
            .store_dir_metadata(DirectoryMetadata::new(10, "/dir/sub".to_string()))
            .unwrap();

        let (entries, truncated) = manager.list_local_entries("/dir", 0, 100);
        assert!(!truncated);
        assert_eq!(entries.len(), 3);
        let names: std::collections::BTreeSet<String> =
            entries.iter().map(|(n, _)| n.clone()).collect();
        assert!(names.contains("a.txt"));
        assert!(names.contains("b.txt"));
        assert!(names.contains("sub"));
    }

    #[test]
    fn test_list_local_entries_virtual_subdir() {
        // mdtest-hard tree pattern: only deeply-nested files exist as
        // metadata; intermediate directories must be synthesized.
        let manager = MetadataManager::new("node1".to_string());

        manager
            .store_file_metadata(FileMetadata::new(
                "/root/00000000/00000000/f0".to_string(),
                1,
            ))
            .unwrap();
        manager
            .store_file_metadata(FileMetadata::new(
                "/root/00000000/00000001/f1".to_string(),
                1,
            ))
            .unwrap();
        manager
            .store_file_metadata(FileMetadata::new(
                "/root/00000001/00000000/f2".to_string(),
                1,
            ))
            .unwrap();

        // readdir("/root") → ["00000000", "00000001"] as virtual dirs.
        let (entries, _) = manager.list_local_entries("/root", 0, 100);
        let names: std::collections::BTreeSet<String> =
            entries.iter().map(|(n, _)| n.clone()).collect();
        assert_eq!(names.len(), 2);
        assert!(names.contains("00000000"));
        assert!(names.contains("00000001"));
        for (_, ty) in &entries {
            assert!(matches!(ty, InodeType::Directory));
        }
    }

    #[test]
    fn test_list_local_entries_root() {
        let manager = MetadataManager::new("node1".to_string());

        manager
            .store_file_metadata(FileMetadata::new("/top.txt".to_string(), 1))
            .unwrap();
        manager
            .store_dir_metadata(DirectoryMetadata::new(20, "/topdir".to_string()))
            .unwrap();

        let (entries, _) = manager.list_local_entries("/", 0, 100);
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn test_list_local_entries_offset_limit() {
        let manager = MetadataManager::new("node1".to_string());
        for i in 0..10 {
            manager
                .store_file_metadata(FileMetadata::new(format!("/x/f{i}"), 1))
                .unwrap();
        }

        let (first, truncated1) = manager.list_local_entries("/x", 0, 3);
        assert_eq!(first.len(), 3);
        assert!(truncated1);

        let (second, _) = manager.list_local_entries("/x", 3, 100);
        assert_eq!(second.len(), 7);
        // No overlap.
        let first_names: std::collections::BTreeSet<String> =
            first.iter().map(|(n, _)| n.clone()).collect();
        for (n, _) in &second {
            assert!(!first_names.contains(n));
        }
    }
}
