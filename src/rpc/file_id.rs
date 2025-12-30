//! FileId - Compact file and chunk identifier for efficient RPC
//!
//! This module provides a compact 64-bit identifier that combines:
//! - Lower 32 bits: unique file identifier (derived from path hash)
//! - Upper 32 bits: chunk identifier (computed from offset / chunk_size)
//!
//! This eliminates the need to send the full file path (256 bytes) in every RPC,
//! reducing header overhead from ~288 bytes to just 32 bytes.
//!
//! # Design Rationale
//!
//! - **Deterministic**: File ID is computed from koyama_hash, so both client and server
//!   can derive the same ID without coordination (same hash used for node distribution)
//! - **Chunk-aware**: Chunk ID is embedded in the upper bits, allowing efficient
//!   chunk addressing without separate fields
//! - **Consistent**: Uses the same koyama_hash as ConsistentHashRing for node distribution
//!
//! # Usage
//!
//! ```ignore
//! use benchfs::rpc::file_id::FileId;
//!
//! // Create FileId from path and chunk index
//! let file_id = FileId::new("/path/to/file.txt", 5);
//!
//! // Or from path and offset (chunk_size = 4MB default)
//! let file_id = FileId::from_offset("/path/to/file.txt", 20_000_000, 4 * 1024 * 1024);
//!
//! // Get components
//! let path_hash = file_id.path_hash();  // Lower 32 bits
//! let chunk_id = file_id.chunk_id();     // Upper 32 bits
//! ```

use std::collections::HashMap;
use std::cell::RefCell;

use crate::metadata::consistent_hash::koyama_hash;

/// Compact file and chunk identifier (64 bits)
///
/// Layout:
/// ```text
/// |<---- chunk_id (32 bits) ---->|<---- path_hash (32 bits) ---->|
/// |  63                       32 |  31                         0 |
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct FileId(u64);

impl FileId {
    /// Create a new FileId from path and chunk index
    ///
    /// # Arguments
    /// * `path` - File path (used to compute 32-bit hash)
    /// * `chunk_index` - Chunk index (used as upper 32 bits)
    #[inline]
    pub fn new(path: &str, chunk_index: u64) -> Self {
        let path_hash = Self::hash_path(path);
        let chunk_id = (chunk_index & 0xFFFF_FFFF) as u32;
        Self(((chunk_id as u64) << 32) | (path_hash as u64))
    }

    /// Create a new FileId from path and file offset
    ///
    /// # Arguments
    /// * `path` - File path
    /// * `offset` - Byte offset in the file
    /// * `chunk_size` - Size of each chunk in bytes
    #[inline]
    pub fn from_offset(path: &str, offset: u64, chunk_size: u64) -> Self {
        let chunk_index = offset / chunk_size;
        Self::new(path, chunk_index)
    }

    /// Create a FileId from raw value
    #[inline]
    pub const fn from_raw(value: u64) -> Self {
        Self(value)
    }

    /// Get the raw 64-bit value
    #[inline]
    pub const fn as_raw(&self) -> u64 {
        self.0
    }

    /// Get the path hash (lower 32 bits)
    #[inline]
    pub const fn path_hash(&self) -> u32 {
        (self.0 & 0xFFFF_FFFF) as u32
    }

    /// Get the chunk ID (upper 32 bits)
    #[inline]
    pub const fn chunk_id(&self) -> u32 {
        (self.0 >> 32) as u32
    }

    /// Create a FileId with the same path hash but different chunk ID
    #[inline]
    pub const fn with_chunk_id(&self, chunk_id: u32) -> Self {
        Self(((chunk_id as u64) << 32) | (self.path_hash() as u64))
    }

    /// Compute 32-bit koyama_hash of the path
    ///
    /// Uses the same hash function as ConsistentHashRing for node distribution,
    /// ensuring that FileId path_hash matches the hash used for routing.
    #[inline]
    fn hash_path(path: &str) -> u32 {
        koyama_hash(path.as_bytes())
    }

    /// Get the path hash for a given path (utility function)
    #[inline]
    pub fn compute_path_hash(path: &str) -> u32 {
        Self::hash_path(path)
    }
}

impl From<u64> for FileId {
    fn from(value: u64) -> Self {
        Self::from_raw(value)
    }
}

impl From<FileId> for u64 {
    fn from(file_id: FileId) -> Self {
        file_id.as_raw()
    }
}

/// Server-side mapping from path_hash to full path
///
/// This registry is used by the server to resolve FileId back to the
/// original file path for chunk storage operations.
///
/// Thread-local for single-threaded async runtime compatibility.
pub struct FileIdRegistry {
    /// path_hash -> path mapping
    hash_to_path: RefCell<HashMap<u32, String>>,
}

impl FileIdRegistry {
    /// Create a new empty registry
    pub fn new() -> Self {
        Self {
            hash_to_path: RefCell::new(HashMap::new()),
        }
    }

    /// Create a registry with pre-allocated capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            hash_to_path: RefCell::new(HashMap::with_capacity(capacity)),
        }
    }

    /// Register a path and return its hash
    ///
    /// If the path is already registered, this is a no-op.
    pub fn register(&self, path: &str) -> u32 {
        let hash = FileId::compute_path_hash(path);
        let mut map = self.hash_to_path.borrow_mut();
        map.entry(hash).or_insert_with(|| path.to_string());
        hash
    }

    /// Look up a path by its hash
    pub fn lookup(&self, hash: u32) -> Option<String> {
        self.hash_to_path.borrow().get(&hash).cloned()
    }

    /// Check if a path hash is registered
    pub fn contains(&self, hash: u32) -> bool {
        self.hash_to_path.borrow().contains_key(&hash)
    }

    /// Remove a path from the registry
    pub fn unregister(&self, path: &str) -> bool {
        let hash = FileId::compute_path_hash(path);
        self.hash_to_path.borrow_mut().remove(&hash).is_some()
    }

    /// Get the number of registered paths
    pub fn len(&self) -> usize {
        self.hash_to_path.borrow().len()
    }

    /// Check if the registry is empty
    pub fn is_empty(&self) -> bool {
        self.hash_to_path.borrow().is_empty()
    }

    /// Clear all registered paths
    pub fn clear(&self) {
        self.hash_to_path.borrow_mut().clear();
    }
}

impl Default for FileIdRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_id_creation() {
        let file_id = FileId::new("/test/file.txt", 5);
        assert_eq!(file_id.chunk_id(), 5);

        // Same path should produce same hash
        let file_id2 = FileId::new("/test/file.txt", 10);
        assert_eq!(file_id.path_hash(), file_id2.path_hash());
        assert_eq!(file_id2.chunk_id(), 10);
    }

    #[test]
    fn test_file_id_from_offset() {
        let chunk_size = 4 * 1024 * 1024; // 4MB

        let file_id = FileId::from_offset("/test/file.txt", 0, chunk_size);
        assert_eq!(file_id.chunk_id(), 0);

        let file_id = FileId::from_offset("/test/file.txt", chunk_size - 1, chunk_size);
        assert_eq!(file_id.chunk_id(), 0);

        let file_id = FileId::from_offset("/test/file.txt", chunk_size, chunk_size);
        assert_eq!(file_id.chunk_id(), 1);

        let file_id = FileId::from_offset("/test/file.txt", 5 * chunk_size + 100, chunk_size);
        assert_eq!(file_id.chunk_id(), 5);
    }

    #[test]
    fn test_file_id_raw_conversion() {
        let file_id = FileId::new("/test/file.txt", 42);
        let raw = file_id.as_raw();
        let restored = FileId::from_raw(raw);

        assert_eq!(file_id, restored);
        assert_eq!(restored.chunk_id(), 42);
    }

    #[test]
    fn test_file_id_with_chunk_id() {
        let file_id = FileId::new("/test/file.txt", 0);
        let new_file_id = file_id.with_chunk_id(100);

        assert_eq!(file_id.path_hash(), new_file_id.path_hash());
        assert_eq!(new_file_id.chunk_id(), 100);
    }

    #[test]
    fn test_path_hash_distribution() {
        // Test that different paths produce different hashes
        let paths = [
            "/test/file1.txt",
            "/test/file2.txt",
            "/other/file.txt",
            "/a/b/c/d/e/f.txt",
            "/short",
            "/very/long/path/to/some/deeply/nested/file.txt",
        ];

        let hashes: Vec<u32> = paths
            .iter()
            .map(|p| FileId::compute_path_hash(p))
            .collect();

        // All hashes should be unique (no collisions for these test paths)
        for i in 0..hashes.len() {
            for j in (i + 1)..hashes.len() {
                assert_ne!(
                    hashes[i], hashes[j],
                    "Hash collision between {} and {}",
                    paths[i], paths[j]
                );
            }
        }
    }

    #[test]
    fn test_file_id_registry() {
        let registry = FileIdRegistry::new();

        // Register paths
        let hash1 = registry.register("/test/file1.txt");
        let hash2 = registry.register("/test/file2.txt");
        assert_ne!(hash1, hash2);

        // Lookup
        assert_eq!(registry.lookup(hash1), Some("/test/file1.txt".to_string()));
        assert_eq!(registry.lookup(hash2), Some("/test/file2.txt".to_string()));
        assert_eq!(registry.lookup(0x12345678), None);

        // Contains
        assert!(registry.contains(hash1));
        assert!(!registry.contains(0x12345678));

        // Unregister
        assert!(registry.unregister("/test/file1.txt"));
        assert!(!registry.contains(hash1));
        assert!(!registry.unregister("/nonexistent"));
    }

    #[test]
    fn test_file_id_registry_re_register() {
        let registry = FileIdRegistry::new();

        // Register same path multiple times
        let hash1 = registry.register("/test/file.txt");
        let hash2 = registry.register("/test/file.txt");

        assert_eq!(hash1, hash2);
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn test_file_id_size() {
        // Ensure FileId is exactly 8 bytes
        assert_eq!(std::mem::size_of::<FileId>(), 8);
    }
}
