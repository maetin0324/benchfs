//! Cache policies and eviction strategies

use std::time::Duration;

/// Cache policy configuration
#[derive(Debug, Clone)]
pub struct CachePolicy {
    /// Maximum number of entries in the cache
    pub max_entries: usize,

    /// Time-to-live for cache entries
    pub ttl: Option<Duration>,

    /// Eviction policy to use
    pub eviction: EvictionPolicy,
}

impl CachePolicy {
    /// Create a new cache policy with LRU eviction
    pub fn lru(max_entries: usize) -> Self {
        Self {
            max_entries,
            ttl: None,
            eviction: EvictionPolicy::Lru,
        }
    }

    /// Create a new cache policy with LRU eviction and TTL
    pub fn lru_with_ttl(max_entries: usize, ttl: Duration) -> Self {
        Self {
            max_entries,
            ttl: Some(ttl),
            eviction: EvictionPolicy::Lru,
        }
    }
}

impl Default for CachePolicy {
    fn default() -> Self {
        Self::lru(1000)
    }
}

/// Eviction policy for cache entries
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictionPolicy {
    /// Least Recently Used
    Lru,

    /// First In First Out
    Fifo,

    /// Least Frequently Used
    Lfu,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_policy() {
        let policy = CachePolicy::default();
        assert_eq!(policy.max_entries, 1000);
        assert_eq!(policy.eviction, EvictionPolicy::Lru);
        assert!(policy.ttl.is_none());
    }

    #[test]
    fn test_lru_policy() {
        let policy = CachePolicy::lru(500);
        assert_eq!(policy.max_entries, 500);
        assert_eq!(policy.eviction, EvictionPolicy::Lru);
    }

    #[test]
    fn test_lru_with_ttl() {
        let ttl = Duration::from_secs(60);
        let policy = CachePolicy::lru_with_ttl(500, ttl);
        assert_eq!(policy.max_entries, 500);
        assert_eq!(policy.ttl, Some(ttl));
    }
}
