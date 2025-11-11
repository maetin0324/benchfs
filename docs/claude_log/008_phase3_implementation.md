# Phase 3 実装ログ: データ操作とチャンキング

## 作成日
2025-10-17

## 概要
Phase 3では、ファイルデータのチャンク分割とチャンク配置戦略の実装を完了しました。

---

## 実装内容

### 1. ファイルチャンキング (`src/data/chunking.rs`)

**目的**: ファイルを固定サイズのチャンクに分割し、チャンク情報を管理する

**主要機能**:
- **固定サイズチャンク**: 4MB (CHUNK_SIZE) のチャンクに分割
- **チャンク情報管理**: インデックス、オフセット、サイズ、ノードIDを追跡
- **範囲読み込み**: 指定された範囲に含まれるチャンクを計算
- **部分読み込み**: チャンク内の部分的な読み込み範囲を計算
- **メタデータ統合**: FileMetadataからチャンク情報を生成

**API**:
```rust
/// チャンク情報
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkInfo {
    pub index: u64,
    pub offset: u64,
    pub size: u64,
    pub node_id: Option<NodeId>,
}

impl ChunkInfo {
    pub fn new(index: u64, offset: u64, size: u64) -> Self;
    pub fn with_node(mut self, node_id: NodeId) -> Self;
    pub fn end_offset(&self) -> u64;
}

/// チャンクマネージャー
pub struct ChunkManager {
    chunk_size: usize,
}

impl ChunkManager {
    pub fn new() -> Self;
    pub fn with_chunk_size(chunk_size: usize) -> Self;
    pub fn chunk_size(&self) -> usize;

    // チャンク計算
    pub fn calculate_chunk_count(&self, file_size: u64) -> u64;
    pub fn chunk_offset(&self, chunk_index: u64) -> u64;
    pub fn chunk_size_at(&self, chunk_index: u64, file_size: u64) -> ChunkingResult<u64>;
    pub fn offset_to_chunk_index(&self, offset: u64) -> u64;

    // チャンク情報取得
    pub fn get_chunk_info(&self, chunk_index: u64, file_size: u64) -> ChunkingResult<ChunkInfo>;
    pub fn get_all_chunks(&self, file_size: u64) -> Vec<ChunkInfo>;
    pub fn get_chunks_in_range(&self, offset: u64, length: u64, file_size: u64)
        -> ChunkingResult<Vec<ChunkInfo>>;

    // メタデータ統合
    pub fn chunks_from_metadata(&self, metadata: &FileMetadata) -> Vec<ChunkInfo>;

    // 範囲読み込み計算
    pub fn calculate_read_chunks(&self, offset: u64, length: u64, file_size: u64)
        -> ChunkingResult<Vec<(u64, u64, u64)>>;
}
```

**エラー型**:
```rust
#[derive(Debug, thiserror::Error)]
pub enum ChunkingError {
    InvalidChunkIndex(u64),
    InvalidOffset(u64),
    InvalidFileSize(u64),
    ChunkSizeMismatch { expected: u64, actual: u64 },
}

pub type ChunkingResult<T> = Result<T, ChunkingError>;
```

**テスト結果**:
- チャンクマネージャー作成: ✓
- チャンク数計算: ✓ (0バイト、1MB、4MB、4MB+1バイト、40MB)
- チャンクオフセット計算: ✓
- チャンクサイズ計算: ✓ (最終チャンクが小さい場合を含む)
- オフセット→チャンクインデックス変換: ✓
- チャンク情報取得: ✓
- 全チャンク情報取得: ✓
- 範囲内チャンク取得: ✓ (単一チャンク、複数チャンク、中間開始)
- 範囲読み込み計算: ✓ (単一チャンク内、チャンク境界をまたぐ)
- ノードID付きチャンク情報: ✓
- メタデータからのチャンク生成: ✓

**設計の特徴**:
- **デフォルトチャンクサイズ**: 4MB (CHUNK_SIZE定数)
- **最終チャンクの扱い**: 最終チャンクは4MB未満の可能性がある
- **柔軟なチャンクサイズ**: `with_chunk_size()`でカスタムサイズを設定可能
- **範囲読み込み対応**: `calculate_read_chunks()`でチャンク内の部分読み込みを計算

---

### 2. チャンク配置戦略 (`src/data/placement.rs`)

**目的**: ファイルチャンクをどのノードに配置するかを決定する

**主要機能**:
- **プラガブル戦略**: PlacementStrategyトレイトで戦略を抽象化
- **ラウンドロビン配置**: チャンクを順番にノードに配置
- **Consistent Hash配置**: パスとチャンクインデックスをハッシュ化してノードを決定
- **レプリケーション対応**: 複数のノードにチャンクを配置

**API**:
```rust
/// チャンク配置戦略トレイト
pub trait PlacementStrategy {
    fn place_chunk(&self, path: &Path, chunk_index: u64) -> Option<NodeId>;
    fn place_chunk_replicas(&self, path: &Path, chunk_index: u64, count: usize)
        -> Vec<NodeId>;
}

/// ラウンドロビン配置
pub struct RoundRobinPlacement {
    nodes: Vec<NodeId>,
}

impl RoundRobinPlacement {
    pub fn new(nodes: Vec<NodeId>) -> Self;
    pub fn node_count(&self) -> usize;
}

/// Consistent Hash配置
pub struct ConsistentHashPlacement {
    ring: ConsistentHashRing,
}

impl ConsistentHashPlacement {
    pub fn new(ring: ConsistentHashRing) -> Self;
    pub fn ring(&self) -> &ConsistentHashRing;
    pub fn node_count(&self) -> usize;
}
```

**テスト結果**:

#### ラウンドロビン配置 (5テスト)
- `test_round_robin_placement`: 基本的なラウンドロビン動作
- `test_round_robin_empty_nodes`: 空のノードリストのハンドリング
- `test_round_robin_replicas`: レプリカ配置
- `test_round_robin_replicas_exceed_nodes`: ノード数を超えるレプリカ要求
- `test_placement_distribution`: 分散の均等性 (30チャンク、各ノード10個ずつ)

#### Consistent Hash配置 (3テスト)
- `test_consistent_hash_placement`: 基本的な配置動作
- `test_consistent_hash_replicas`: レプリカ配置 (重複なし)
- `test_consistent_hash_different_paths`: 異なるパスの処理
- `test_chunk_key_generation`: チャンクキー生成
- `test_consistent_hash_chunk_distribution`: 分散の均等性 (100チャンク)

**設計の特徴**:
- **トレイトベース設計**: 戦略を簡単に切り替え可能
- **チャンクキー**: パスとチャンクインデックスを組み合わせて一意のキーを生成 (`path:chunk:index`)
- **レプリケーション対応**: `place_chunk_replicas()`で複数ノードに配置
- **ラウンドロビンの単純性**: テストやデバッグに有用
- **Consistent Hashの拡張性**: ノード追加/削除時のデータ移動を最小化

---

### 3. NodeId型の統一

**問題**: `metadata/consistent_hash.rs`が独自のNodeId型を定義していたため、`metadata/types.rs`のNodeId (String) と競合

**解決策**:
1. `consistent_hash.rs`を`types::NodeId`を使用するように変更
2. String型はCopyトレイトを実装していないため、適切な箇所に`.clone()`を追加:
   - `add_node()`: ノードリストへの追加時
   - `get_node()` / `get_nodes()`: 戻り値の生成時
3. `remove_node()`のシグネチャを`&NodeId`に変更
4. 全テストコードを整数NodeIDからString NodeIDに変更:
   - `ring.add_node(1)` → `ring.add_node("node1".to_string())`
   - `assert_eq!(owner, 1)` → `assert_eq!(owner, "node1")`

**影響を受けたファイル**:
- `src/metadata/consistent_hash.rs`: 型定義とクローン処理
- `src/metadata/manager.rs`: インポート文とクローン処理
- `src/data/placement.rs`: クローン処理
- `src/data/chunking.rs`: テストコード
- テスト: 23個のテストを更新

---

## モジュール構成の更新

### `src/data/mod.rs` (新規作成)
```rust
pub mod chunking;
pub mod placement;

pub use chunking::{ChunkInfo, ChunkManager, ChunkingError, ChunkingResult};
pub use placement::{PlacementStrategy, RoundRobinPlacement, ConsistentHashPlacement};
```

### `src/lib.rs`
```rust
pub mod rpc;
pub mod storage;
pub mod metadata;
pub mod data;  // 追加
```

### `src/metadata/mod.rs` (更新)
```rust
// NodeIdをconsistent_hashからではなくtypesから直接エクスポート
pub use types::{FileMetadata, DirectoryMetadata, InodeType, FilePermissions, NodeId};
pub use consistent_hash::ConsistentHashRing;
pub use manager::{MetadataManager, MetadataError, MetadataResult};
pub use cache::{MetadataCache, MetadataCacheEntry, CacheStats};
```

---

## テスト結果

### 全体統計
- **総テスト数**: 58テスト
- **成功**: 57テスト
- **Ignored**: 1テスト (`storage::iouring::tests::test_open_read_write`)
- **失敗**: 0テスト

### Phase 3 新規テスト

#### Chunking (14テスト)
- `test_chunk_manager_creation`: マネージャー作成
- `test_calculate_chunk_count`: チャンク数計算
- `test_chunk_offset`: オフセット計算
- `test_chunk_size_at`: チャンクサイズ計算
- `test_offset_to_chunk_index`: インデックス計算
- `test_get_chunk_info`: チャンク情報取得
- `test_get_all_chunks`: 全チャンク取得
- `test_get_chunks_in_range`: 範囲内チャンク取得
- `test_calculate_read_chunks`: 読み込みチャンク計算
- `test_chunk_info_with_node`: ノードID付きチャンク
- `test_chunks_from_metadata`: メタデータからのチャンク生成

#### Placement (8テスト)
- `test_round_robin_placement`: ラウンドロビン配置
- `test_round_robin_empty_nodes`: 空ノードリスト
- `test_round_robin_replicas`: レプリカ配置
- `test_round_robin_replicas_exceed_nodes`: ノード数超過
- `test_placement_distribution`: 配置の均等性
- `test_consistent_hash_placement`: Consistent Hash配置
- `test_consistent_hash_replicas`: レプリカ配置
- `test_consistent_hash_different_paths`: 異なるパスの処理
- `test_chunk_key_generation`: チャンクキー生成
- `test_consistent_hash_chunk_distribution`: 分散の均等性

---

## 課題と今後の対応

### 解決済み課題

1. **NodeId型の競合**: `consistent_hash.rs`が独自のNodeId型 (u64) を定義していたため、`types::NodeId` (String) と競合
   - **解決**: `types::NodeId`に統一し、全コードを更新

2. **String型のムーブエラー**: NodeIdがString型のため、Copyトレイトがなく、ムーブエラーが多数発生
   - **解決**: 必要な箇所に`.clone()`を追加

3. **テストコードの型不一致**: すべてのテストが整数NodeIDを使用していた
   - **解決**: 23個のテストをString NodeIDに変更

### 今後の課題

1. **データRPC実装** (Phase 3の次ステップ)
   - `ReadChunk` RPC: リモートノードからチャンクを読み込み
   - `WriteChunk` RPC: リモートノードにチャンクを書き込み
   - RDMA対応 (UCX-based)

2. **チャンクデータの実際の保存**
   - 現在はチャンク情報のみ管理
   - ローカルストレージにチャンクデータを保存する機構が必要

3. **チャンクレプリケーション**
   - 配置戦略はレプリカノードを返せるが、実際のレプリケーション機構は未実装
   - 書き込み時に複数ノードにデータをコピー

4. **チャンクの再配置**
   - ノード追加/削除時のチャンク移動
   - Consistent Hashingで最小化されているが、実装は必要

5. **メタデータRPC実装** (Phase 2の残りタスク)
   - `MetadataLookup` RPC
   - `MetadataCreate` RPC
   - `MetadataDelete` RPC
   - `MetadataUpdate` RPC

---

## 統計

### 新規ファイル
- `src/data/mod.rs`: ~10行
- `src/data/chunking.rs`: ~430行
- `src/data/placement.rs`: ~340行
- 合計: ~780行 (テスト含む)

### Phase 3 トータル統計
- **コード量**: ~780行 (Phase 1: ~1030行, Phase 2: ~1160行)
- **テスト数**: 22テスト (Phase 1: 10テスト, Phase 2: 27テスト)
- **成功率**: 100% (57/57 passing, 1 ignored)

### 全プロジェクト統計 (Phase 1-3)
- **コード量**: ~2970行 (テスト含む)
- **総テスト数**: 59テスト
- **成功率**: 98.3% (57 passed, 1 ignored, 0 failed)

---

## 次のステップ

Phase 3のチャンク管理が完了し、データ分散の基盤が整いました。次の候補:

### オプション1: データRPC実装 (Phase 3の続き)
1. **ReadChunk RPC** (`src/rpc/data_ops.rs`)
   - チャンクの読み込み
   - RDMA対応
   - エラーハンドリング

2. **WriteChunk RPC** (`src/rpc/data_ops.rs`)
   - チャンクの書き込み
   - RDMA対応
   - 部分書き込み対応

3. **チャンクストレージ** (`src/storage/chunk_store.rs`)
   - ローカルストレージへのチャンク保存
   - チャンクの取得
   - io_uringベースの高速I/O

### オプション2: メタデータRPC実装 (Phase 2の完全完了)
1. **MetadataLookup RPC**
   - リモートノードからメタデータ取得
   - キャッシュとの統合

2. **MetadataCreate/Delete/Update RPC**
   - メタデータの作成/削除/更新
   - キャッシュ無効化

### オプション3: 統合とテスト
1. **エンドツーエンドテスト**
   - ファイル作成 → チャンク分割 → 配置 → 保存
   - ファイル読み込み → チャンク取得 → 結合

2. **パフォーマンステスト**
   - ベンチマーク作成
   - io_uringとRDMAの性能測定

**推奨**: オプション1 (データRPC実装) を進めて、Phase 3を完全に完了させる。

---

## 技術的意思決定

### 2025-10-17

#### Q: チャンクサイズをどうするか？
**決定**: 4MB (CHUNK_SIZE)

**理由**:
- CHFSの推奨値
- ネットワーク転送とストレージI/Oのバランス
- RDMA転送に適したサイズ
- 柔軟性: `with_chunk_size()`でカスタマイズ可能

#### Q: 配置戦略はどうするか？
**決定**: トレイトベースで複数戦略を実装

**理由**:
- ラウンドロビン: シンプルでテスト/デバッグに有用
- Consistent Hash: 本番環境で推奨 (ノード追加/削除時の影響を最小化)
- 将来的に他の戦略 (e.g., ローカリティベース) を追加可能

#### Q: NodeId型をu64とStringのどちらにするか？
**決定**: String

**理由**:
- UCXワーカーアドレスは文字列形式
- IPアドレス + ポートの組み合わせも文字列が自然
- serdeでのシリアライズが容易
- 人間が読める形式 (デバッグ時に有用)
- トレードオフ: Copyトレイトがないため、`.clone()`が必要

#### Q: チャンク情報にノードIDを含めるか？
**決定**: Option<NodeId>として含める

**理由**:
- メタデータ (FileMetadata.chunk_locations) との統合が容易
- チャンク配置後にノードIDを設定可能
- Noneの場合は未配置を示す

---

## 参考資料
- [CHFS GitHub](https://github.com/otatebe/chfs)
- [Consistent Hashing](https://en.wikipedia.org/wiki/Consistent_hashing)
- [Data Chunking Techniques](https://en.wikipedia.org/wiki/Chunking_(division))
