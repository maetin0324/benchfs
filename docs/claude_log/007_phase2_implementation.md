# Phase 2 実装ログ: メタデータ分散とConsistent Hashing

## 作成日
2025-10-17

## 概要
Phase 2では、メタデータの分散管理とConsistent Hashingの実装を完了しました。

---

## 実装内容

### 1. Consistent Hashing実装 (`src/metadata/consistent_hash.rs`)

**目的**: ノード間でメタデータを均等に分散する

**主要機能**:
- **仮想ノード**: 各物理ノードに150個の仮想ノードを作成し、リング上に配置
- **ハッシュ関数**: xxHash64を使用してキーをハッシュ化
- **BTreeMapベースのリング**: 効率的な範囲検索で、O(log N)でノードを検索
- **レプリケーション対応**: 複数のノードを取得する`get_nodes()`メソッド

**API**:
```rust
pub struct ConsistentHashRing {
    ring: BTreeMap<RingPosition, NodeId>,
    nodes: Vec<NodeId>,
    virtual_nodes_per_node: usize,
}

impl ConsistentHashRing {
    pub fn new() -> Self;
    pub fn add_node(&mut self, node_id: NodeId);
    pub fn remove_node(&mut self, node_id: NodeId) -> bool;
    pub fn get_node(&self, key: &str) -> Option<NodeId>;
    pub fn get_nodes(&self, key: &str, count: usize) -> Vec<NodeId>;
    pub fn node_count(&self) -> usize;
    pub fn virtual_node_count(&self) -> usize;
}
```

**テスト結果**:
- ノード追加/削除: ✓
- キーマッピング: ✓ (同じキーは常に同じノードにマッピング)
- レプリケーション: ✓ (重複なしで複数ノード取得)
- 分散: ✓ (1000個のキーで各ノードが100個以上担当)
- ノード削除時の再分散: ✓

**設計の特徴**:
- `VIRTUAL_NODES_PER_NODE = 150`: バランスと性能のトレードオフ
- `XXHASH_SEED = 0`: 再現性のあるハッシュ計算
- `NodeId = u64`: UCXワーカーアドレスやIPアドレスのハッシュを想定

---

### 2. メタデータマネージャー (`src/metadata/manager.rs`)

**目的**: メタデータのローカル保存とConsistent Hashingによる分散管理

**主要機能**:
- **Consistent Hashingとの統合**: パスから担当ノードを決定
- **ローカルメタデータストア**: 自ノードが管理するメタデータをHashMapで保持
- **ファイル/ディレクトリメタデータ管理**: 保存、取得、更新、削除
- **オーナーシップ判定**: 指定されたパスが自ノードで管理されているか確認

**API**:
```rust
pub struct MetadataManager {
    ring: RefCell<ConsistentHashRing>,
    local_file_metadata: RefCell<HashMap<String, FileMetadata>>,
    local_dir_metadata: RefCell<HashMap<String, DirectoryMetadata>>,
    self_node_id: NodeId,
}

impl MetadataManager {
    pub fn new(self_node_id: NodeId) -> Self;
    pub fn add_node(&self, node_id: NodeId);
    pub fn remove_node(&self, node_id: NodeId) -> bool;

    // オーナー判定
    pub fn get_owner_node(&self, path: &Path) -> MetadataResult<NodeId>;
    pub fn get_owner_nodes(&self, path: &Path, count: usize) -> MetadataResult<Vec<NodeId>>;
    pub fn is_local_owner(&self, path: &Path) -> MetadataResult<bool>;

    // ファイルメタデータ管理
    pub fn store_file_metadata(&self, metadata: FileMetadata) -> MetadataResult<()>;
    pub fn get_file_metadata(&self, path: &Path) -> MetadataResult<FileMetadata>;
    pub fn update_file_metadata(&self, metadata: FileMetadata) -> MetadataResult<()>;
    pub fn remove_file_metadata(&self, path: &Path) -> MetadataResult<()>;

    // ディレクトリメタデータ管理
    pub fn store_dir_metadata(&self, metadata: DirectoryMetadata) -> MetadataResult<()>;
    pub fn get_dir_metadata(&self, path: &Path) -> MetadataResult<DirectoryMetadata>;
    pub fn update_dir_metadata(&self, metadata: DirectoryMetadata) -> MetadataResult<()>;
    pub fn remove_dir_metadata(&self, path: &Path) -> MetadataResult<()>;

    // 統計
    pub fn local_file_count(&self) -> usize;
    pub fn local_dir_count(&self) -> usize;
    pub fn node_count(&self) -> usize;
}
```

**エラー型**:
```rust
pub enum MetadataError {
    NotFound(String),
    AlreadyExists(String),
    NoNodesAvailable,
    InvalidPath(String),
    Internal(String),
}
```

**テスト結果**:
- ノード追加/削除: ✓
- オーナーノード取得: ✓
- レプリケーション用複数ノード取得: ✓
- ローカルオーナー判定: ✓
- ファイルメタデータCRUD: ✓
- ディレクトリメタデータCRUD: ✓
- 重複エラー検出: ✓

**設計の特徴**:
- **シングルスレッド設計**: RefCellを使用 (Pluvio runtimeに合わせた設計)
- **String型のパス**: メタデータ型の`path`フィールドと統一
- **所有権の明確化**: `self_node_id`で自ノードを識別

---

### 3. メタデータキャッシュ (`src/metadata/cache.rs`)

**目的**: リモートノードから取得したメタデータをキャッシュし、ネットワークアクセスを削減

**主要機能**:
- **LRUアルゴリズム**: Least Recently Usedエビクション
- **ファイル/ディレクトリ両対応**: 統一されたキャッシュエントリ
- **Invalidation機能**: メタデータ更新時のキャッシュ無効化
- **統計情報**: キャッシュ使用率の追跡

**API**:
```rust
pub struct MetadataCache {
    cache: RefCell<LruCache<String, MetadataCacheEntry>>,
}

pub enum MetadataCacheEntry {
    File(FileMetadata),
    Directory(DirectoryMetadata),
}

impl MetadataCache {
    pub const DEFAULT_CAPACITY: usize = 1024;

    pub fn new() -> Self;
    pub fn with_capacity(capacity: usize) -> Self;

    // ファイルメタデータ
    pub fn put_file(&self, metadata: FileMetadata);
    pub fn get_file(&self, path: &Path) -> Option<FileMetadata>;

    // ディレクトリメタデータ
    pub fn put_dir(&self, metadata: DirectoryMetadata);
    pub fn get_dir(&self, path: &Path) -> Option<DirectoryMetadata>;

    // キャッシュ管理
    pub fn invalidate(&self, path: &Path) -> bool;
    pub fn clear(&self);

    // 統計
    pub fn len(&self) -> usize;
    pub fn is_empty(&self) -> bool;
    pub fn capacity(&self) -> usize;
    pub fn stats(&self) -> CacheStats;
}

pub struct CacheStats {
    pub size: usize,
    pub capacity: usize,
}

impl CacheStats {
    pub fn utilization(&self) -> f64; // 0.0 ~ 1.0
}
```

**テスト結果**:
- キャッシュ作成: ✓
- ファイルメタデータput/get: ✓
- ディレクトリメタデータput/get: ✓
- キャッシュミス: ✓
- Invalidation: ✓
- Clear: ✓
- LRUエビクション: ✓ (古いエントリが削除される)
- LRUアクセス順序: ✓ (最近アクセスされたエントリは保持される)
- 統計情報: ✓
- 既存エントリの上書き: ✓

**設計の特徴**:
- **デフォルトキャパシティ**: 1024エントリ (調整可能)
- **シングルスレッド設計**: RefCellを使用
- **String型のキー**: メタデータ型の`path`フィールドと統一

---

## モジュール構成の更新

`src/metadata/mod.rs`を更新:
```rust
pub mod types;
pub mod consistent_hash;
pub mod manager;
pub mod cache;

pub use types::{FileMetadata, DirectoryMetadata, InodeType, FilePermissions};
pub use consistent_hash::{ConsistentHashRing, NodeId};
pub use manager::{MetadataManager, MetadataError, MetadataResult};
pub use cache::{MetadataCache, MetadataCacheEntry, CacheStats};
```

---

## テスト結果

### 全体統計
- **総テスト数**: 37テスト
- **成功**: 36テスト
- **Ignored**: 1テスト (`test_open_read_write` - DmaFile::read API問題)
- **失敗**: 0テスト

### Phase 2 新規テスト
#### Consistent Hashing (5テスト)
- `test_add_remove_node`: ノード追加/削除
- `test_get_node`: キーマッピング
- `test_get_nodes_for_replication`: レプリケーション用複数ノード取得
- `test_node_distribution`: 分散の均等性
- `test_node_removal_redistribution`: ノード削除時の再分散

#### Metadata Manager (10テスト)
- `test_metadata_manager_creation`: マネージャー作成
- `test_add_remove_nodes`: ノード管理
- `test_get_owner_node`: オーナーノード取得
- `test_get_owner_nodes_for_replication`: レプリケーション用ノード取得
- `test_is_local_owner`: ローカルオーナー判定
- `test_store_and_get_file_metadata`: ファイルメタデータ保存/取得
- `test_store_duplicate_file_metadata`: 重複エラー
- `test_update_file_metadata`: ファイルメタデータ更新
- `test_remove_file_metadata`: ファイルメタデータ削除
- `test_store_and_get_dir_metadata`: ディレクトリメタデータ保存/取得
- `test_remove_dir_metadata`: ディレクトリメタデータ削除

#### Metadata Cache (12テスト)
- `test_cache_creation`: キャッシュ作成
- `test_cache_with_capacity`: カスタムキャパシティ
- `test_put_and_get_file`: ファイルput/get
- `test_put_and_get_dir`: ディレクトリput/get
- `test_cache_miss`: キャッシュミス
- `test_invalidate`: Invalidation
- `test_clear`: クリア
- `test_lru_eviction`: LRUエビクション
- `test_lru_access_order`: LRUアクセス順序
- `test_cache_stats`: 統計情報
- `test_update_existing_entry`: 既存エントリ更新

---

## 課題と今後の対応

### 解決済み課題
1. **型の不一致 (PathBuf vs String)**: FileMetadata/DirectoryMetadataの`path`フィールドは`String`型だが、当初`PathBuf`を使用していた
   - **解決**: すべてのAPIを`String`型に統一し、`Path`からの変換を追加

2. **所有権の問題**: `put_file()`/`put_dir()`で`path`をmoveした後にtracingで使用
   - **解決**: `path.clone()`を使用

### 今後の課題
1. **メタデータRPC実装** (Phase 2の次ステップ)
   - `MetadataLookup` RPC: リモートノードからメタデータ取得
   - `MetadataCreate` RPC: メタデータ作成
   - `MetadataDelete` RPC: メタデータ削除
   - `MetadataUpdate` RPC: メタデータ更新

2. **キャッシュ一貫性**
   - 現在はローカルinvalidationのみ
   - リモートノードでのメタデータ更新時の通知機構が必要

3. **メタデータの永続化**
   - 現在はインメモリのみ
   - ノード再起動時の復旧機構が必要

4. **ノード障害時のハンドリング**
   - ノード削除時のメタデータ再配置
   - レプリカからの復旧

---

## 統計

### 新規ファイル
- `src/metadata/consistent_hash.rs`: ~330行
- `src/metadata/manager.rs`: ~450行
- `src/metadata/cache.rs`: ~380行
- 合計: ~1160行 (テスト含む)

### Phase 2 トータル統計
- **コード量**: ~1160行 (Phase 1: ~1030行)
- **テスト数**: 27テスト (Phase 1: 10テスト)
- **成功率**: 100% (36/36 passing, 1 ignored)

---

## 次のステップ

Phase 2が完了し、メタデータ分散の基盤が整いました。次はPhase 3のデータ操作とチャンキングに進みます:

1. **ファイルチャンキング** (`src/data/chunking.rs`)
   - 固定サイズチャンク分割 (4MB)
   - チャンク配置戦略
   - チャンクメタデータ管理

2. **データRPC実装** (`src/rpc/data_ops.rs`)
   - ReadChunk RPC
   - WriteChunk RPC
   - RDMA対応

3. **データ配置管理** (`src/data/placement.rs`)
   - チャンクロケーションマッピング
   - レプリケーション (オプション)

ただし、メタデータRPCの実装 (Phase 2の完全完了) を先に進めることも検討します。

---

## 技術的意思決定

### 2025-10-17

#### Q: PathBuf vs String for metadata paths
**決定**: Stringを使用

**理由**:
- FileMetadata/DirectoryMetadataの定義が`path: String`
- serdeでのシリアライズが容易
- ネットワーク送信時にString型が自然

#### Q: LRUキャッシュのデフォルトサイズ
**決定**: 1024エントリ

**理由**:
- 小規模環境でも動作可能
- 大規模環境では`with_capacity()`で調整可能
- メモリ使用量とヒット率のバランス

#### Q: Consistent Hashingの仮想ノード数
**決定**: 150 (VIRTUAL_NODES_PER_NODE)

**理由**:
- CHFSの設計文書の推奨値
- 分散の均等性とメモリ使用量のトレードオフ
- テストで1000個のキーが均等に分散されることを確認

---

## 参考資料
- [CHFS GitHub](https://github.com/otatebe/chfs)
- [Consistent Hashing](https://en.wikipedia.org/wiki/Consistent_hashing)
- [xxHash](https://github.com/Cyan4973/xxHash)
- [LRU Cache](https://docs.rs/lru/latest/lru/)
