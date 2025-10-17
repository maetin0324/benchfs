//! Chunk data caching

use std::cell::RefCell;
use std::time::{Duration, Instant};
use lru::LruCache;
use std::num::NonZeroUsize;

use crate::metadata::types::InodeId;
use crate::cache::policy::CachePolicy;

/// Chunk identifier (inode, chunk_index)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ChunkId {
    pub inode: InodeId,
    pub chunk_index: u64,
}

impl ChunkId {
    pub fn new(inode: InodeId, chunk_index: u64) -> Self {
        Self { inode, chunk_index }
    }
}

/// Cache entry with timestamp for TTL support
#[derive(Clone)]
struct CacheEntry {
    data: Vec<u8>,
    timestamp: Instant,
}

impl CacheEntry {
    fn new(data: Vec<u8>) -> Self {
        Self {
            data,
            timestamp: Instant::now(),
        }
    }

    fn is_expired(&self, ttl: Duration) -> bool {
        self.timestamp.elapsed() > ttl
    }

    fn size(&self) -> usize {
        self.data.len()
    }
}

/// Chunk cache with memory usage limits
///
/// Uses LRU eviction policy with optional TTL support.
/// Tracks memory usage and evicts entries when limit is exceeded.
pub struct ChunkCache {
    /// Cache for chunk data (ChunkId -> data)
    cache: RefCell<LruCache<ChunkId, CacheEntry>>,

    /// Cache policy
    policy: CachePolicy,

    /// Maximum memory usage in bytes
    max_memory_bytes: usize,

    /// Current memory usage in bytes
    current_memory_bytes: RefCell<usize>,
}

impl ChunkCache {
    /// Create a new chunk cache with the given policy and memory limit
    ///
    /// # Arguments
    /// * `policy` - Cache policy
    /// * `max_memory_mb` - Maximum memory usage in megabytes
    pub fn new(policy: CachePolicy, max_memory_mb: usize) -> Self {
        let capacity = NonZeroUsize::new(policy.max_entries).unwrap();

        Self {
            cache: RefCell::new(LruCache::new(capacity)),
            policy,
            max_memory_bytes: max_memory_mb * 1024 * 1024,
            current_memory_bytes: RefCell::new(0),
        }
    }

    /// Create a new chunk cache with default policy and memory limit
    pub fn with_memory_limit(max_memory_mb: usize) -> Self {
        Self::new(CachePolicy::lru(10000), max_memory_mb)
    }

    /// Get chunk data from cache
    pub fn get(&self, chunk_id: &ChunkId) -> Option<Vec<u8>> {
        let mut cache = self.cache.borrow_mut();

        if let Some(entry) = cache.get(chunk_id) {
            // Check TTL if configured
            if let Some(ttl) = self.policy.ttl {
                if entry.is_expired(ttl) {
                    // Entry expired, remove it
                    if let Some(removed) = cache.pop(chunk_id) {
                        *self.current_memory_bytes.borrow_mut() -= removed.size();
                    }
                    return None;
                }
            }

            Some(entry.data.clone())
        } else {
            None
        }
    }

    /// Put chunk data into cache
    ///
    /// Returns true if the chunk was cached, false if it was too large
    pub fn put(&self, chunk_id: ChunkId, data: Vec<u8>) -> bool {
        let data_size = data.len();

        // Check if data is too large for cache
        if data_size > self.max_memory_bytes {
            return false;
        }

        // Evict entries if needed to make room
        self.evict_to_fit(data_size);

        let mut cache = self.cache.borrow_mut();
        let entry = CacheEntry::new(data);

        // If chunk_id already exists, remove old entry first
        if let Some(old_entry) = cache.pop(&chunk_id) {
            *self.current_memory_bytes.borrow_mut() -= old_entry.size();
        }

        cache.put(chunk_id, entry);
        *self.current_memory_bytes.borrow_mut() += data_size;

        true
    }

    /// Invalidate a chunk in the cache
    pub fn invalidate(&self, chunk_id: &ChunkId) {
        let mut cache = self.cache.borrow_mut();
        if let Some(entry) = cache.pop(chunk_id) {
            *self.current_memory_bytes.borrow_mut() -= entry.size();
        }
    }

    /// Invalidate all chunks for a given inode
    pub fn invalidate_inode(&self, inode: InodeId) {
        let mut cache = self.cache.borrow_mut();
        let keys_to_remove: Vec<ChunkId> = cache
            .iter()
            .filter(|(k, _)| k.inode == inode)
            .map(|(k, _)| *k)
            .collect();

        for key in keys_to_remove {
            if let Some(entry) = cache.pop(&key) {
                *self.current_memory_bytes.borrow_mut() -= entry.size();
            }
        }
    }

    /// Clear all cached entries
    pub fn clear(&self) {
        self.cache.borrow_mut().clear();
        *self.current_memory_bytes.borrow_mut() = 0;
    }

    /// Evict entries to make room for new data
    fn evict_to_fit(&self, needed_bytes: usize) {
        let mut current_bytes = *self.current_memory_bytes.borrow();

        while current_bytes + needed_bytes > self.max_memory_bytes {
            let mut cache = self.cache.borrow_mut();

            // Pop the least recently used entry
            if let Some((_, entry)) = cache.pop_lru() {
                current_bytes -= entry.size();
            } else {
                break;
            }
        }

        *self.current_memory_bytes.borrow_mut() = current_bytes;
    }

    /// Get cache statistics
    pub fn stats(&self) -> ChunkCacheStats {
        ChunkCacheStats {
            entries: self.cache.borrow().len(),
            memory_bytes: *self.current_memory_bytes.borrow(),
            max_memory_bytes: self.max_memory_bytes,
            max_entries: self.policy.max_entries,
        }
    }

    /// Get cache hit ratio (requires tracking hits/misses)
    pub fn memory_usage_percent(&self) -> f64 {
        let current = *self.current_memory_bytes.borrow() as f64;
        let max = self.max_memory_bytes as f64;
        (current / max) * 100.0
    }
}

/// Cache statistics
#[derive(Debug, Clone, Copy)]
pub struct ChunkCacheStats {
    pub entries: usize,
    pub memory_bytes: usize,
    pub max_memory_bytes: usize,
    pub max_entries: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunk_cache_creation() {
        let cache = ChunkCache::with_memory_limit(10); // 10 MB
        let stats = cache.stats();
        assert_eq!(stats.entries, 0);
        assert_eq!(stats.memory_bytes, 0);
        assert_eq!(stats.max_memory_bytes, 10 * 1024 * 1024);
    }

    #[test]
    fn test_chunk_cache_put_get() {
        let cache = ChunkCache::with_memory_limit(10);
        let chunk_id = ChunkId::new(1, 0);
        let data = vec![1, 2, 3, 4, 5];

        assert!(cache.put(chunk_id, data.clone()));

        let cached = cache.get(&chunk_id);
        assert!(cached.is_some());
        assert_eq!(cached.unwrap(), data);
    }

    #[test]
    fn test_chunk_cache_invalidate() {
        let cache = ChunkCache::with_memory_limit(10);
        let chunk_id = ChunkId::new(1, 0);
        let data = vec![1, 2, 3, 4, 5];

        cache.put(chunk_id, data);
        assert!(cache.get(&chunk_id).is_some());

        cache.invalidate(&chunk_id);
        assert!(cache.get(&chunk_id).is_none());

        let stats = cache.stats();
        assert_eq!(stats.memory_bytes, 0);
    }

    #[test]
    fn test_chunk_cache_invalidate_inode() {
        let cache = ChunkCache::with_memory_limit(10);

        let chunk1 = ChunkId::new(1, 0);
        let chunk2 = ChunkId::new(1, 1);
        let chunk3 = ChunkId::new(2, 0);

        cache.put(chunk1, vec![1, 2, 3]);
        cache.put(chunk2, vec![4, 5, 6]);
        cache.put(chunk3, vec![7, 8, 9]);

        cache.invalidate_inode(1);

        assert!(cache.get(&chunk1).is_none());
        assert!(cache.get(&chunk2).is_none());
        assert!(cache.get(&chunk3).is_some());
    }

    #[test]
    fn test_chunk_cache_memory_limit() {
        let cache = ChunkCache::with_memory_limit(1); // 1 MB
        let chunk_id = ChunkId::new(1, 0);

        // Try to cache 2 MB (should fail)
        let large_data = vec![0u8; 2 * 1024 * 1024];
        assert!(!cache.put(chunk_id, large_data));

        // Cache 512 KB (should succeed)
        let small_data = vec![0u8; 512 * 1024];
        assert!(cache.put(chunk_id, small_data));

        let stats = cache.stats();
        assert_eq!(stats.memory_bytes, 512 * 1024);
    }

    #[test]
    fn test_chunk_cache_eviction() {
        let cache = ChunkCache::with_memory_limit(1); // 1 MB

        let chunk1 = ChunkId::new(1, 0);
        let chunk2 = ChunkId::new(1, 1);

        // Cache 700 KB
        let data1 = vec![0u8; 700 * 1024];
        cache.put(chunk1, data1);

        // Cache another 700 KB (should evict chunk1)
        let data2 = vec![0u8; 700 * 1024];
        cache.put(chunk2, data2);

        assert!(cache.get(&chunk1).is_none());
        assert!(cache.get(&chunk2).is_some());
    }

    #[test]
    fn test_chunk_cache_memory_usage() {
        let cache = ChunkCache::with_memory_limit(10);

        let chunk1 = ChunkId::new(1, 0);
        let data = vec![0u8; 1024 * 1024]; // 1 MB

        cache.put(chunk1, data);

        let usage = cache.memory_usage_percent();
        assert!(usage > 9.0 && usage < 11.0); // ~10%
    }

    #[test]
    fn test_chunk_cache_ttl() {
        use std::thread;

        let policy = CachePolicy::lru_with_ttl(1000, Duration::from_millis(100));
        let cache = ChunkCache::new(policy, 10);

        let chunk_id = ChunkId::new(1, 0);
        let data = vec![1, 2, 3];

        cache.put(chunk_id, data);
        assert!(cache.get(&chunk_id).is_some());

        thread::sleep(Duration::from_millis(150));

        assert!(cache.get(&chunk_id).is_none());
    }

    #[test]
    fn test_chunk_cache_clear() {
        let cache = ChunkCache::with_memory_limit(10);

        cache.put(ChunkId::new(1, 0), vec![1, 2, 3]);
        cache.put(ChunkId::new(1, 1), vec![4, 5, 6]);

        let stats = cache.stats();
        assert_eq!(stats.entries, 2);
        assert!(stats.memory_bytes > 0);

        cache.clear();

        let stats = cache.stats();
        assert_eq!(stats.entries, 0);
        assert_eq!(stats.memory_bytes, 0);
    }
}
