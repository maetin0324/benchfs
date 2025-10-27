//! Metadata caching for file and directory metadata

use lru::LruCache;
use std::cell::RefCell;
use std::num::NonZeroUsize;
use std::time::{Duration, Instant};

use crate::cache::policy::CachePolicy;
use crate::metadata::{DirectoryMetadata, FileMetadata};

/// Cache entry with timestamp for TTL support
#[derive(Clone)]
struct CacheEntry<T> {
    data: T,
    timestamp: Instant,
}

impl<T> CacheEntry<T> {
    fn new(data: T) -> Self {
        Self {
            data,
            timestamp: Instant::now(),
        }
    }

    fn is_expired(&self, ttl: Duration) -> bool {
        self.timestamp.elapsed() > ttl
    }
}

/// Metadata cache for files and directories
///
/// Uses LRU eviction policy with optional TTL support.
/// Note: Uses RefCell for interior mutability in single-threaded context.
pub struct MetadataCache {
    /// Cache for file metadata (path -> FileMetadata)
    file_cache: RefCell<LruCache<String, CacheEntry<FileMetadata>>>,

    /// Cache for directory metadata (path -> DirectoryMetadata)
    dir_cache: RefCell<LruCache<String, CacheEntry<DirectoryMetadata>>>,

    /// Cache policy
    policy: CachePolicy,
}

impl MetadataCache {
    /// Create a new metadata cache with the given policy
    pub fn new(policy: CachePolicy) -> Self {
        // Ensure capacity is at least 1 to avoid panic with NonZeroUsize
        let capacity = NonZeroUsize::new(policy.max_entries.max(1))
            .expect("Capacity must be non-zero (ensured by max(1))");

        Self {
            file_cache: RefCell::new(LruCache::new(capacity)),
            dir_cache: RefCell::new(LruCache::new(capacity)),
            policy,
        }
    }

    /// Create a new metadata cache with default policy
    pub fn with_capacity(capacity: usize) -> Self {
        Self::new(CachePolicy::lru(capacity))
    }

    /// Get file metadata from cache
    pub fn get_file(&self, path: &str) -> Option<FileMetadata> {
        let mut cache = self.file_cache.borrow_mut();

        if let Some(entry) = cache.get(path) {
            // Check TTL if configured
            if let Some(ttl) = self.policy.ttl {
                if entry.is_expired(ttl) {
                    // Entry expired, remove it
                    cache.pop(path);
                    return None;
                }
            }

            Some(entry.data.clone())
        } else {
            None
        }
    }

    /// Put file metadata into cache
    pub fn put_file(&self, path: String, metadata: FileMetadata) {
        let mut cache = self.file_cache.borrow_mut();
        cache.put(path, CacheEntry::new(metadata));
    }

    /// Get directory metadata from cache
    pub fn get_dir(&self, path: &str) -> Option<DirectoryMetadata> {
        let mut cache = self.dir_cache.borrow_mut();

        if let Some(entry) = cache.get(path) {
            // Check TTL if configured
            if let Some(ttl) = self.policy.ttl {
                if entry.is_expired(ttl) {
                    // Entry expired, remove it
                    cache.pop(path);
                    return None;
                }
            }

            Some(entry.data.clone())
        } else {
            None
        }
    }

    /// Put directory metadata into cache
    pub fn put_dir(&self, path: String, metadata: DirectoryMetadata) {
        let mut cache = self.dir_cache.borrow_mut();
        cache.put(path, CacheEntry::new(metadata));
    }

    /// Invalidate file metadata in cache
    pub fn invalidate_file(&self, path: &str) {
        let mut cache = self.file_cache.borrow_mut();
        cache.pop(path);
    }

    /// Invalidate directory metadata in cache
    pub fn invalidate_dir(&self, path: &str) {
        let mut cache = self.dir_cache.borrow_mut();
        cache.pop(path);
    }

    /// Clear all cached entries
    pub fn clear(&self) {
        self.file_cache.borrow_mut().clear();
        self.dir_cache.borrow_mut().clear();
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            file_entries: self.file_cache.borrow().len(),
            dir_entries: self.dir_cache.borrow().len(),
            max_entries: self.policy.max_entries,
        }
    }
}

/// Cache statistics
#[derive(Debug, Clone, Copy)]
pub struct CacheStats {
    pub file_entries: usize,
    pub dir_entries: usize,
    pub max_entries: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_creation() {
        let cache = MetadataCache::with_capacity(100);
        let stats = cache.stats();
        assert_eq!(stats.file_entries, 0);
        assert_eq!(stats.dir_entries, 0);
        assert_eq!(stats.max_entries, 100);
    }

    #[test]
    fn test_file_cache_put_get() {
        let cache = MetadataCache::with_capacity(100);
        let metadata = FileMetadata::new("/test.txt".to_string(), 1024);

        cache.put_file("/test.txt".to_string(), metadata.clone());

        let cached = cache.get_file("/test.txt");
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().path, "/test.txt");
    }

    #[test]
    fn test_dir_cache_put_get() {
        let cache = MetadataCache::with_capacity(100);
        let metadata = DirectoryMetadata::new(1, "/testdir".to_string());

        cache.put_dir("/testdir".to_string(), metadata.clone());

        let cached = cache.get_dir("/testdir");
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().path, "/testdir");
    }

    #[test]
    fn test_cache_invalidation() {
        let cache = MetadataCache::with_capacity(100);
        let metadata = FileMetadata::new("/test.txt".to_string(), 1024);

        cache.put_file("/test.txt".to_string(), metadata);
        assert!(cache.get_file("/test.txt").is_some());

        cache.invalidate_file("/test.txt");
        assert!(cache.get_file("/test.txt").is_none());
    }

    #[test]
    fn test_cache_lru_eviction() {
        let cache = MetadataCache::with_capacity(2);

        let meta1 = FileMetadata::new("/file1.txt".to_string(), 100);
        let meta2 = FileMetadata::new("/file2.txt".to_string(), 200);
        let meta3 = FileMetadata::new("/file3.txt".to_string(), 300);

        cache.put_file("/file1.txt".to_string(), meta1);
        cache.put_file("/file2.txt".to_string(), meta2);
        cache.put_file("/file3.txt".to_string(), meta3);

        // file1 should be evicted (LRU)
        assert!(cache.get_file("/file1.txt").is_none());
        assert!(cache.get_file("/file2.txt").is_some());
        assert!(cache.get_file("/file3.txt").is_some());
    }

    #[test]
    fn test_cache_ttl() {
        use std::thread;

        let policy = CachePolicy::lru_with_ttl(100, Duration::from_millis(100));
        let cache = MetadataCache::new(policy);

        let metadata = FileMetadata::new("/test.txt".to_string(), 1024);
        cache.put_file("/test.txt".to_string(), metadata);

        // Should be cached
        assert!(cache.get_file("/test.txt").is_some());

        // Wait for TTL to expire
        thread::sleep(Duration::from_millis(150));

        // Should be expired
        assert!(cache.get_file("/test.txt").is_none());
    }

    #[test]
    fn test_cache_clear() {
        let cache = MetadataCache::with_capacity(100);

        cache.put_file(
            "/file1.txt".to_string(),
            FileMetadata::new("/file1.txt".to_string(), 100),
        );
        cache.put_dir(
            "/dir1".to_string(),
            DirectoryMetadata::new(2, "/dir1".to_string()),
        );

        assert_eq!(cache.stats().file_entries, 1);
        assert_eq!(cache.stats().dir_entries, 1);

        cache.clear();

        assert_eq!(cache.stats().file_entries, 0);
        assert_eq!(cache.stats().dir_entries, 0);
    }
}
