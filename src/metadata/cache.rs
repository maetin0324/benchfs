use super::types::{DirectoryMetadata, FileMetadata};
use lru::LruCache;
use std::cell::RefCell;
use std::num::NonZeroUsize;
use std::path::Path;

/// メタデータキャッシュエントリ
#[derive(Debug, Clone)]
pub enum MetadataCacheEntry {
    File(FileMetadata),
    Directory(DirectoryMetadata),
}

/// メタデータキャッシュ
///
/// リモートノードから取得したメタデータをキャッシュし、
/// ネットワークアクセスを削減する。
///
/// LRU (Least Recently Used) アルゴリズムを使用してエビクションを行う。
///
/// Note: シングルスレッド設計のため、RefCellを使用
pub struct MetadataCache {
    /// キャッシュ (path -> entry)
    cache: RefCell<LruCache<String, MetadataCacheEntry>>,
}

impl MetadataCache {
    /// デフォルトキャッシュサイズ (1024エントリ)
    pub const DEFAULT_CAPACITY: usize = 1024;

    /// 新しいメタデータキャッシュを作成 (デフォルトサイズ)
    pub fn new() -> Self {
        Self::with_capacity(Self::DEFAULT_CAPACITY)
    }

    /// 指定されたキャパシティでメタデータキャッシュを作成
    ///
    /// # Arguments
    /// * `capacity` - キャッシュの最大エントリ数
    pub fn with_capacity(capacity: usize) -> Self {
        let capacity = NonZeroUsize::new(capacity).expect("Capacity must be non-zero");
        Self {
            cache: RefCell::new(LruCache::new(capacity)),
        }
    }

    /// ファイルメタデータをキャッシュに追加
    ///
    /// # Arguments
    /// * `metadata` - キャッシュするファイルメタデータ
    pub fn put_file(&self, metadata: FileMetadata) {
        let path = metadata.path.clone();
        let entry = MetadataCacheEntry::File(metadata);
        self.cache.borrow_mut().put(path.clone(), entry);
        tracing::trace!("Cached file metadata: {:?}", path);
    }

    /// ディレクトリメタデータをキャッシュに追加
    ///
    /// # Arguments
    /// * `metadata` - キャッシュするディレクトリメタデータ
    pub fn put_dir(&self, metadata: DirectoryMetadata) {
        let path = metadata.path.clone();
        let entry = MetadataCacheEntry::Directory(metadata);
        self.cache.borrow_mut().put(path.clone(), entry);
        tracing::trace!("Cached directory metadata: {:?}", path);
    }

    /// ファイルメタデータをキャッシュから取得
    ///
    /// # Arguments
    /// * `path` - ファイルパス
    ///
    /// # Returns
    /// キャッシュにヒットした場合は `Some(FileMetadata)`、ミスした場合は `None`
    pub fn get_file(&self, path: &Path) -> Option<FileMetadata> {
        let path_str = path.to_str()?;
        let mut cache = self.cache.borrow_mut();
        if let Some(entry) = cache.get(path_str) {
            if let MetadataCacheEntry::File(metadata) = entry {
                tracing::trace!("Cache hit for file: {:?}", path);
                return Some(metadata.clone());
            }
        }
        tracing::trace!("Cache miss for file: {:?}", path);
        None
    }

    /// ディレクトリメタデータをキャッシュから取得
    ///
    /// # Arguments
    /// * `path` - ディレクトリパス
    ///
    /// # Returns
    /// キャッシュにヒットした場合は `Some(DirectoryMetadata)`、ミスした場合は `None`
    pub fn get_dir(&self, path: &Path) -> Option<DirectoryMetadata> {
        let path_str = path.to_str()?;
        let mut cache = self.cache.borrow_mut();
        if let Some(entry) = cache.get(path_str) {
            if let MetadataCacheEntry::Directory(metadata) = entry {
                tracing::trace!("Cache hit for directory: {:?}", path);
                return Some(metadata.clone());
            }
        }
        tracing::trace!("Cache miss for directory: {:?}", path);
        None
    }

    /// 指定されたパスのメタデータをキャッシュから削除 (invalidation)
    ///
    /// # Arguments
    /// * `path` - 削除するパス
    ///
    /// # Returns
    /// エントリが存在して削除された場合は `true`、存在しなかった場合は `false`
    pub fn invalidate(&self, path: &Path) -> bool {
        let path_str = match path.to_str() {
            Some(s) => s,
            None => return false,
        };
        let removed = self.cache.borrow_mut().pop(path_str).is_some();
        if removed {
            tracing::debug!("Invalidated cache entry: {:?}", path);
        }
        removed
    }

    /// キャッシュを完全にクリア
    pub fn clear(&self) {
        self.cache.borrow_mut().clear();
        tracing::debug!("Cleared metadata cache");
    }

    /// キャッシュ内のエントリ数
    pub fn len(&self) -> usize {
        self.cache.borrow().len()
    }

    /// キャッシュが空かどうか
    pub fn is_empty(&self) -> bool {
        self.cache.borrow().is_empty()
    }

    /// キャッシュの最大容量
    pub fn capacity(&self) -> usize {
        self.cache.borrow().cap().get()
    }

    /// キャッシュヒット率を計算するための統計情報を取得
    pub fn stats(&self) -> CacheStats {
        // Note: LruCacheはヒット/ミス統計を提供しないため、
        // 統計が必要な場合は手動でカウントする必要がある
        CacheStats {
            size: self.len(),
            capacity: self.capacity(),
        }
    }
}

impl Default for MetadataCache {
    fn default() -> Self {
        Self::new()
    }
}

/// キャッシュ統計情報
#[derive(Debug, Clone, Copy)]
pub struct CacheStats {
    pub size: usize,
    pub capacity: usize,
}

impl CacheStats {
    /// キャッシュ使用率 (0.0 ~ 1.0)
    pub fn utilization(&self) -> f64 {
        if self.capacity == 0 {
            0.0
        } else {
            self.size as f64 / self.capacity as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_creation() {
        let cache = MetadataCache::new();
        assert_eq!(cache.len(), 0);
        assert!(cache.is_empty());
        assert_eq!(cache.capacity(), MetadataCache::DEFAULT_CAPACITY);
    }

    #[test]
    fn test_cache_with_capacity() {
        let cache = MetadataCache::with_capacity(100);
        assert_eq!(cache.capacity(), 100);
    }

    #[test]
    fn test_put_and_get_file() {
        let cache = MetadataCache::new();

        let path_str = "/foo/bar.txt".to_string();
        let metadata = FileMetadata::new(123, path_str.clone(), 1024);

        cache.put_file(metadata.clone());
        assert_eq!(cache.len(), 1);

        let path = Path::new(&path_str);
        let retrieved = cache.get_file(path).unwrap();
        assert_eq!(retrieved.inode, 123);
        assert_eq!(retrieved.size, 1024);
    }

    #[test]
    fn test_put_and_get_dir() {
        let cache = MetadataCache::new();

        let path_str = "/foo".to_string();
        let metadata = DirectoryMetadata::new(456, path_str.clone());

        cache.put_dir(metadata.clone());
        assert_eq!(cache.len(), 1);

        let path = Path::new(&path_str);
        let retrieved = cache.get_dir(path).unwrap();
        assert_eq!(retrieved.inode, 456);
    }

    #[test]
    fn test_cache_miss() {
        let cache = MetadataCache::new();

        let path = Path::new("/nonexistent");
        assert!(cache.get_file(path).is_none());
        assert!(cache.get_dir(path).is_none());
    }

    #[test]
    fn test_invalidate() {
        let cache = MetadataCache::new();

        let path_str = "/foo/bar.txt".to_string();
        let metadata = FileMetadata::new(123, path_str.clone(), 1024);

        cache.put_file(metadata.clone());
        assert_eq!(cache.len(), 1);

        let path = Path::new(&path_str);
        assert!(cache.invalidate(path));
        assert_eq!(cache.len(), 0);

        assert!(!cache.invalidate(path)); // 既に削除済み
    }

    #[test]
    fn test_clear() {
        let cache = MetadataCache::new();

        for i in 0..10 {
            let path = format!("/file{}.txt", i);
            let metadata = FileMetadata::new(i, path, 1024);
            cache.put_file(metadata);
        }

        assert_eq!(cache.len(), 10);

        cache.clear();
        assert_eq!(cache.len(), 0);
        assert!(cache.is_empty());
    }

    #[test]
    fn test_lru_eviction() {
        let cache = MetadataCache::with_capacity(3);

        // 3つのエントリを追加
        for i in 0..3 {
            let path = format!("/file{}.txt", i);
            let metadata = FileMetadata::new(i, path, 1024);
            cache.put_file(metadata);
        }

        assert_eq!(cache.len(), 3);

        // 4つ目を追加すると、最も古いエントリ (file0) がエビクトされる
        let path3 = "/file3.txt".to_string();
        let metadata3 = FileMetadata::new(3, path3.clone(), 1024);
        cache.put_file(metadata3);

        assert_eq!(cache.len(), 3);

        // file0 は削除されている
        let path0 = Path::new("/file0.txt");
        assert!(cache.get_file(path0).is_none());

        // file1, file2, file3 は残っている
        assert!(cache.get_file(Path::new("/file1.txt")).is_some());
        assert!(cache.get_file(Path::new("/file2.txt")).is_some());
        assert!(cache.get_file(Path::new(&path3)).is_some());
    }

    #[test]
    fn test_lru_access_order() {
        let cache = MetadataCache::with_capacity(3);

        // 3つのエントリを追加
        for i in 0..3 {
            let path = format!("/file{}.txt", i);
            let metadata = FileMetadata::new(i, path, 1024);
            cache.put_file(metadata);
        }

        // file0 をアクセスして最新にする
        let path0 = Path::new("/file0.txt");
        assert!(cache.get_file(path0).is_some());

        // 4つ目を追加すると、file1 がエビクトされる (file0は最近アクセスされたため残る)
        let path3 = "/file3.txt".to_string();
        let metadata3 = FileMetadata::new(3, path3.clone(), 1024);
        cache.put_file(metadata3);

        assert_eq!(cache.len(), 3);

        // file1 が削除されている
        assert!(cache.get_file(Path::new("/file1.txt")).is_none());

        // file0, file2, file3 は残っている
        assert!(cache.get_file(path0).is_some());
        assert!(cache.get_file(Path::new("/file2.txt")).is_some());
        assert!(cache.get_file(Path::new(&path3)).is_some());
    }

    #[test]
    fn test_cache_stats() {
        let cache = MetadataCache::with_capacity(100);

        for i in 0..50 {
            let path = format!("/file{}.txt", i);
            let metadata = FileMetadata::new(i, path, 1024);
            cache.put_file(metadata);
        }

        let stats = cache.stats();
        assert_eq!(stats.size, 50);
        assert_eq!(stats.capacity, 100);
        assert_eq!(stats.utilization(), 0.5);
    }

    #[test]
    fn test_update_existing_entry() {
        let cache = MetadataCache::new();

        let path_str = "/foo/bar.txt".to_string();
        let metadata1 = FileMetadata::new(123, path_str.clone(), 1024);
        cache.put_file(metadata1);

        // 同じパスで異なるメタデータを追加 (上書き)
        let metadata2 = FileMetadata::new(123, path_str.clone(), 2048);
        cache.put_file(metadata2);

        assert_eq!(cache.len(), 1); // エントリ数は変わらない

        let path = Path::new(&path_str);
        let retrieved = cache.get_file(path).unwrap();
        assert_eq!(retrieved.size, 2048); // 新しい値
    }
}
