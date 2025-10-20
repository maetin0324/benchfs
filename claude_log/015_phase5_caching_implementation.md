# Phase 5: キャッシング機構の実装ログ

**Date**: 2025-10-17
**Author**: Claude (Sonnet 4.5)
**Task**: Phase 5 - キャッシング機構の実装

## Overview

Phase 5では、BenchFSにキャッシング機構を実装しました。メタデータとチャンクデータの両方をキャッシュすることで、ネットワークとディスクI/Oを削減し、パフォーマンスを向上させます。

## 実装したコンポーネント

### 1. キャッシュポリシー (`src/cache/policy.rs`)

キャッシュの動作を制御するポリシー設定を実装しました。

```rust
pub struct CachePolicy {
    /// Maximum number of entries in the cache
    pub max_entries: usize,

    /// Time-to-live for cache entries
    pub ttl: Option<Duration>,

    /// Eviction policy to use
    pub eviction: EvictionPolicy,
}

pub enum EvictionPolicy {
    Lru,  // Least Recently Used
    Fifo, // First In First Out
    Lfu,  // Least Frequently Used
}
```

**機能**:
- エントリ数の上限設定
- TTL（Time To Live）サポート
- LRU/FIFO/LFUなどの追い出しポリシー（現在はLRUのみ実装）

**使用例**:
```rust
// デフォルト: LRU、1000エントリ、TTLなし
let policy = CachePolicy::default();

// カスタム: LRU、500エントリ、60秒TTL
let policy = CachePolicy::lru_with_ttl(500, Duration::from_secs(60));
```

### 2. メタデータキャッシュ (`src/cache/metadata_cache.rs`)

ファイルとディレクトリのメタデータをキャッシュするLRU実装です。

```rust
pub struct MetadataCache {
    file_cache: RefCell<LruCache<String, CacheEntry<FileMetadata>>>,
    dir_cache: RefCell<LruCache<String, CacheEntry<DirectoryMetadata>>>,
    policy: CachePolicy,
}
```

**主要メソッド**:

```rust
// ファイルメタデータの取得・設定
pub fn get_file(&self, path: &str) -> Option<FileMetadata>
pub fn put_file(&self, path: String, metadata: FileMetadata)
pub fn invalidate_file(&self, path: &str)

// ディレクトリメタデータの取得・設定
pub fn get_dir(&self, path: &str) -> Option<DirectoryMetadata>
pub fn put_dir(&self, path: String, metadata: DirectoryMetadata)
pub fn invalidate_dir(&self, path: &str)

// キャッシュ管理
pub fn clear(&self)
pub fn stats(&self) -> CacheStats
```

**TTLサポート**:
- エントリごとにタイムスタンプを記録
- 取得時にTTL期限をチェック
- 期限切れの場合は自動削除

**テスト**:
```rust
#[test]
fn test_cache_lru_eviction() {
    let cache = MetadataCache::with_capacity(2);

    cache.put_file("/file1.txt", meta1);
    cache.put_file("/file2.txt", meta2);
    cache.put_file("/file3.txt", meta3);

    // file1がLRUにより追い出される
    assert!(cache.get_file("/file1.txt").is_none());
    assert!(cache.get_file("/file2.txt").is_some());
    assert!(cache.get_file("/file3.txt").is_some());
}
```

### 3. チャンクキャッシュ (`src/cache/chunk_cache.rs`)

チャンクデータをメモリ使用量を制限しながらキャッシュします。

```rust
pub struct ChunkCache {
    cache: RefCell<LruCache<ChunkId, CacheEntry>>,
    policy: CachePolicy,
    max_memory_bytes: usize,
    current_memory_bytes: RefCell<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ChunkId {
    pub inode: InodeId,
    pub chunk_index: u64,
}
```

**主要メソッド**:

```rust
// チャンクデータの取得・設定
pub fn get(&self, chunk_id: &ChunkId) -> Option<Vec<u8>>
pub fn put(&self, chunk_id: ChunkId, data: Vec<u8>) -> bool

// 無効化
pub fn invalidate(&self, chunk_id: &ChunkId)
pub fn invalidate_inode(&self, inode: InodeId)

// キャッシュ管理
pub fn clear(&self)
pub fn stats(&self) -> ChunkCacheStats
pub fn memory_usage_percent(&self) -> f64
```

**メモリ管理**:
- 最大メモリ使用量を設定（MB単位）
- 新しいエントリ追加時に自動的にLRU追い出し
- 現在のメモリ使用量をリアルタイムで追跡

```rust
// 10 MBのメモリ制限でキャッシュ作成
let cache = ChunkCache::with_memory_limit(10);

// 1 MBのチャンクをキャッシュ
let chunk_data = vec![0u8; 1024 * 1024];
cache.put(ChunkId::new(1, 0), chunk_data);

// メモリ使用率を確認
println!("Memory usage: {:.1}%", cache.memory_usage_percent());
```

**自動追い出しロジック**:
```rust
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
```

**テスト**:
```rust
#[test]
fn test_chunk_cache_memory_limit() {
    let cache = ChunkCache::with_memory_limit(1); // 1 MB

    // 2 MBのデータは大きすぎるため拒否される
    let large_data = vec![0u8; 2 * 1024 * 1024];
    assert!(!cache.put(chunk_id, large_data));

    // 512 KBは受け入れられる
    let small_data = vec![0u8; 512 * 1024];
    assert!(cache.put(chunk_id, small_data));
}
```

### 4. MetadataManagerへの統合

MetadataManagerにキャッシュを統合して、透過的なキャッシング層を実現しました。

#### 構造体の変更

```rust
pub struct MetadataManager {
    ring: RefCell<ConsistentHashRing>,
    local_file_metadata: RefCell<HashMap<String, FileMetadata>>,
    local_dir_metadata: RefCell<HashMap<String, DirectoryMetadata>>,
    cache: MetadataCache,  // ← 追加
    self_node_id: NodeId,
}
```

#### コンストラクタの拡張

```rust
impl MetadataManager {
    // デフォルトキャッシュポリシーで作成
    pub fn new(self_node_id: NodeId) -> Self {
        Self::with_cache_policy(self_node_id, CachePolicy::default())
    }

    // カスタムキャッシュポリシーで作成
    pub fn with_cache_policy(self_node_id: NodeId, cache_policy: CachePolicy) -> Self {
        // ...
        cache: MetadataCache::new(cache_policy),
        // ...
    }
}
```

#### get_file_metadata with Caching

```rust
pub fn get_file_metadata(&self, path: &Path) -> MetadataResult<FileMetadata> {
    let path_str = path.to_str().ok_or_else(|| ...)?;

    // 1. キャッシュチェック
    if let Some(cached) = self.cache.get_file(path_str) {
        tracing::trace!("Cache hit for file metadata: {}", path_str);
        return Ok(cached);
    }

    // 2. キャッシュミス → ローカルストレージから取得
    let metadata = self.local_file_metadata
        .borrow()
        .get(path_str)
        .cloned()
        .ok_or_else(|| MetadataError::NotFound(path_str.to_string()))?;

    // 3. 取得したデータをキャッシュ
    self.cache.put_file(path_str.to_string(), metadata.clone());

    Ok(metadata)
}
```

#### update_file_metadata with Cache Invalidation

```rust
pub fn update_file_metadata(&self, metadata: FileMetadata) -> MetadataResult<()> {
    let path = metadata.path.clone();

    let mut local_metadata = self.local_file_metadata.borrow_mut();

    if !local_metadata.contains_key(&path) {
        return Err(MetadataError::NotFound(path.clone()));
    }

    local_metadata.insert(path.clone(), metadata.clone());

    // キャッシュ無効化（一貫性保証）
    self.cache.invalidate_file(&path);

    tracing::debug!("Updated file metadata for: {}", path);

    Ok(())
}
```

#### remove_file_metadata with Cache Invalidation

```rust
pub fn remove_file_metadata(&self, path: &Path) -> MetadataResult<()> {
    let path_str = path.to_str().ok_or_else(|| ...)?;

    let mut local_metadata = self.local_file_metadata.borrow_mut();

    local_metadata
        .remove(path_str)
        .ok_or_else(|| MetadataError::NotFound(path_str.to_string()))?;

    // キャッシュ無効化
    self.cache.invalidate_file(path_str);

    tracing::debug!("Removed file metadata for: {}", path_str);

    Ok(())
}
```

同様の処理を`get_dir_metadata`、`update_dir_metadata`、`remove_dir_metadata`にも適用しました。

#### キャッシュ統計とクリア

```rust
/// キャッシュ統計を取得
pub fn cache_stats(&self) -> crate::cache::metadata_cache::CacheStats {
    self.cache.stats()
}

/// キャッシュをクリア
pub fn clear_cache(&self) {
    self.cache.clear()
}
```

### 5. BenchFSへのチャンクキャッシュ統合

BenchFSファイル操作APIにチャンクキャッシュを統合しました。

#### 構造体の変更

```rust
pub struct BenchFS {
    metadata_manager: Rc<MetadataManager>,
    chunk_store: Rc<InMemoryChunkStore>,
    chunk_cache: ChunkCache,  // ← 追加
    chunk_manager: ChunkManager,
    placement: Rc<dyn PlacementStrategy>,
    // ...
}
```

#### コンストラクタの拡張

```rust
impl BenchFS {
    pub fn new(node_id: String) -> Self {
        let metadata_manager = Rc::new(MetadataManager::new(node_id.clone()));
        let chunk_store = Rc::new(InMemoryChunkStore::new());
        let chunk_cache = ChunkCache::with_memory_limit(100); // 100 MB cache
        let chunk_manager = ChunkManager::new();
        // ...
    }
}
```

#### benchfs_read with Chunk Caching

```rust
pub fn benchfs_read(&self, handle: &FileHandle, buf: &mut [u8]) -> ApiResult<usize> {
    // ... metadata 取得 ...

    let mut bytes_read = 0;
    for (chunk_index, chunk_offset, read_size) in chunks {
        let chunk_id = ChunkId::new(file_meta.inode, chunk_index);

        // キャッシュチェック
        let chunk_data = if let Some(cached_chunk) = self.chunk_cache.get(&chunk_id) {
            // キャッシュヒット
            tracing::trace!("Cache hit for chunk {}", chunk_index);
            Some(cached_chunk)
        } else {
            // キャッシュミス - チャンクストアから読み込み
            tracing::trace!("Cache miss for chunk {}", chunk_index);
            match self.chunk_store.read_chunk(
                file_meta.inode,
                chunk_index,
                0, // Read full chunk for caching
                self.chunk_manager.chunk_size() as u64,
            ) {
                Ok(full_chunk) => {
                    // フルチャンクをキャッシュ
                    self.chunk_cache.put(chunk_id, full_chunk.clone());
                    Some(full_chunk)
                }
                Err(_) => None,
            }
        };

        // 必要な部分を抽出してバッファにコピー
        if let Some(chunk) = chunk_data {
            let buf_offset = bytes_read;
            let chunk_start = chunk_offset as usize;
            let copy_len = read_size as usize;
            buf[buf_offset..buf_offset + copy_len]
                .copy_from_slice(&chunk[chunk_start..chunk_start + copy_len]);
            bytes_read += copy_len;
        }
        // ...
    }

    Ok(bytes_read)
}
```

**キャッシング戦略**:
1. 部分読み込みでもフルチャンクをキャッシュ（再利用性向上）
2. キャッシュヒット時は即座にデータ返却
3. キャッシュミス時はストア→キャッシュ→返却

#### benchfs_write with Cache Invalidation

```rust
pub fn benchfs_write(&self, handle: &FileHandle, data: &[u8]) -> ApiResult<usize> {
    // ... metadata 取得 ...

    let mut bytes_written = 0;
    for (chunk_index, chunk_offset, write_size) in chunks {
        let chunk_id = ChunkId::new(file_meta.inode, chunk_index);

        // キャッシュ無効化（write-through戦略）
        self.chunk_cache.invalidate(&chunk_id);

        // チャンクストアに書き込み
        self.chunk_store
            .write_chunk(file_meta.inode, chunk_index, chunk_offset, chunk_data)
            .map_err(|e| ApiError::IoError(format!("Failed to write chunk: {:?}", e)))?;

        bytes_written += data_len;
    }

    Ok(bytes_written)
}
```

**Write-through戦略**:
- 書き込み前にキャッシュを無効化
- ストレージに直接書き込み
- 次回読み込み時に新しいデータをキャッシュ

#### benchfs_unlink with Inode-level Cache Invalidation

```rust
pub fn benchfs_unlink(&self, path: &str) -> ApiResult<()> {
    // ... metadata 取得 ...

    // 該当inodeの全チャンクを無効化
    self.chunk_cache.invalidate_inode(file_meta.inode);

    // チャンクストアから削除
    self.chunk_store.delete_file_chunks(file_meta.inode);

    // メタデータを削除
    self.metadata_manager
        .remove_file_metadata(path_ref)
        .map_err(|e| ApiError::Internal(format!("Failed to delete metadata: {:?}", e)))?;

    Ok(())
}
```

**ファイル削除時の処理**:
1. inode単位で全チャンクを無効化
2. ストレージから物理削除
3. メタデータを削除

## アーキテクチャ

### キャッシュフロー

```
User Request
     │
     ▼
┌─────────────────┐
│ MetadataManager │
│  or ChunkManager│
└────────┬────────┘
         │
         ▼
    ┌─────────┐  Cache Hit
    │  Cache  │─────────────> Return cached data
    │  Layer  │
    └────┬────┘
         │ Cache Miss
         ▼
┌────────────────┐
│ Local Storage  │
│  or Network    │
└────────┬───────┘
         │
         ▼
    Update Cache
         │
         ▼
    Return data
```

### キャッシュ無効化のタイミング

1. **書き込み時**: `update_*`メソッドで該当エントリを無効化
2. **削除時**: `remove_*`メソッドで該当エントリを無効化
3. **TTL期限**: `get_*`メソッドで期限切れを検出して自動削除
4. **手動**: `clear()`や`invalidate_*()`で明示的に無効化

### メモリ管理戦略

#### メタデータキャッシュ

- **エントリ数制限**: デフォルト1000エントリ
- **追い出しポリシー**: LRU（最近最も使われていないエントリを削除）
- **メモリ使用量**: メタデータは小さいのでエントリ数で制御

#### チャンクキャッシュ

- **メモリ上限**: MB単位で設定（例: 100 MB）
- **追い出しポリシー**: LRU + メモリサイズベース
- **大きなチャンクの扱い**: キャッシュサイズを超える場合は拒否

## テスト結果

```
test result: ok. 111 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

全テストが合格しました！

### 追加されたテスト

**Cache Policy Tests** (cache/policy.rs):
- `test_default_policy`: デフォルトポリシーの検証
- `test_lru_policy`: LRUポリシーの作成
- `test_lru_with_ttl`: TTL付きポリシーの作成

**Metadata Cache Tests** (cache/metadata_cache.rs):
- `test_cache_creation`: キャッシュの作成
- `test_file_cache_put_get`: ファイルメタデータのキャッシュ
- `test_dir_cache_put_get`: ディレクトリメタデータのキャッシュ
- `test_cache_invalidation`: 無効化の動作
- `test_cache_lru_eviction`: LRU追い出しの検証
- `test_cache_ttl`: TTL期限切れの検証
- `test_cache_clear`: クリア機能

**Chunk Cache Tests** (cache/chunk_cache.rs):
- `test_chunk_cache_creation`: キャッシュの作成
- `test_chunk_cache_put_get`: チャンクデータのキャッシュ
- `test_chunk_cache_invalidate`: 単一チャンクの無効化
- `test_chunk_cache_invalidate_inode`: inode単位の無効化
- `test_chunk_cache_memory_limit`: メモリ制限の検証
- `test_chunk_cache_eviction`: メモリベースの追い出し
- `test_chunk_cache_memory_usage`: メモリ使用率の計算
- `test_chunk_cache_ttl`: TTL期限切れの検証
- `test_chunk_cache_clear`: クリア機能

## パフォーマンスへの影響

### メタデータキャッシュの効果

- **キャッシュヒット時**: O(1)のメモリアクセス
- **キャッシュミス時**: O(1)のHashMap検索 + キャッシュ更新
- **ネットワークI/O削減**: リモートノードへのRPC呼び出しを削減（将来実装）

### チャンクキャッシュの効果

- **読み込み性能**: ディスクI/Oやネットワーク転送を回避
- **書き込み性能**: Write-throughキャッシュ（更新時は無効化）
- **メモリ効率**: 使用量を制限しつつ頻繁にアクセスされるデータを保持

## 今後の拡張

### 1. 統計情報の収集

キャッシュヒット率やミス率を追跡する機能を追加できます。

```rust
pub struct CacheMetrics {
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
}

impl CacheMetrics {
    pub fn hit_rate(&self) -> f64 {
        let hits = self.hits.load(Ordering::Relaxed) as f64;
        let total = hits + self.misses.load(Ordering::Relaxed) as f64;
        if total == 0.0 { 0.0 } else { hits / total * 100.0 }
    }
}
```

### 2. Write-backキャッシュ

現在はwrite-through（書き込み時に即座にストレージを更新）ですが、write-back（キャッシュに書き込み、後でフラッシュ）を実装することで書き込み性能を向上できます。

### 3. プリフェッチング

シーケンシャルアクセスのパターンを検出して、次のチャンクを先読みする機能。

```rust
pub fn prefetch_chunks(&self, inode: InodeId, start_chunk: u64, count: usize) {
    for i in 0..count {
        let chunk_index = start_chunk + i as u64;
        // Background prefetch
        tokio::spawn(async move {
            self.read_chunk(inode, chunk_index).await
        });
    }
}
```

### 4. 分散キャッシュの一貫性

複数ノード間でキャッシュの一貫性を保つメカニズム：
- Cache Invalidation Protocol
- Lease-based Caching
- Versioned Caching

## まとめ

Phase 5で実装した機能：

✅ **完了した項目**:
1. キャッシュポリシーフレームワーク（LRU、TTLサポート）
2. メタデータキャッシュ（ファイル＋ディレクトリ）
3. チャンクキャッシュ（メモリ制限付き）
4. MetadataManagerへのキャッシング統合
5. BenchFS APIへのチャンクキャッシング統合
6. 自動キャッシュ無効化（write-through戦略）
7. 包括的なテストスイート（111テスト合格）

**技術的成果**:
- **パフォーマンス向上**: メタデータとチャンクデータのキャッシング
- **メモリ効率**: LRU追い出しとサイズ制限
- **一貫性保証**: Write-through + 無効化
- **拡張性**: ポリシーベースの設計で将来の拡張が容易
- **透過的統合**: 既存のAPIに影響を与えずにキャッシング追加

**実装詳細**:
- src/cache/mod.rs: モジュール定義とエクスポート
- src/cache/policy.rs: キャッシュポリシー（256行）
- src/cache/metadata_cache.rs: メタデータキャッシュ（254行）
- src/cache/chunk_cache.rs: チャンクキャッシュ（359行）
- src/metadata/manager.rs: MetadataManagerへの統合
- src/api/file_ops.rs: BenchFS APIへの統合

**次のステップ**:
- Phase 6: サーバーデーモンと統合テスト
- Phase 7: パフォーマンステストと最適化
- 将来: キャッシュメトリクス、write-back、プリフェッチング

BenchFSは高性能なキャッシング機構を備えた分散ファイルシステムになりました！
