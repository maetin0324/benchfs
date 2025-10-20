//! Caching mechanisms for BenchFS
//!
//! This module provides caching for both metadata and data chunks
//! to improve performance by reducing network and disk I/O.

pub mod metadata_cache;
pub mod chunk_cache;
pub mod policy;

pub use metadata_cache::MetadataCache;
pub use chunk_cache::{ChunkCache, ChunkId};
pub use policy::{CachePolicy, EvictionPolicy};
