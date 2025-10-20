# フェーズ1実装ログ

## 作成日
2025-10-17

## 実装内容

### 完了した項目

#### 1. 依存関係の追加

`Cargo.toml`に以下の依存関係を追加:

```toml
# Hashing (Consistent Hashing)
xxhash-rust = { version = "0.8", features = ["xxh64"] }

# Cache (LRU)
lru = "0.12"

# Configuration
serde = { version = "1.0", features = ["derive"] }
toml = "0.8"

# Logging
tracing-subscriber = { version = "0.3", features = ["env-filter"] }

# Error handling
thiserror = "2.0"

# Async utilities
futures = "0.3"
async-trait = "0.1"

# System
libc = "0.2"

# Async runtime (for tests)
tokio = { version = "1", features = ["full"] }

[dev-dependencies]
tempfile = "3.0"
```

#### 2. メタデータ構造の定義

**実装ファイル**:
- `src/metadata/mod.rs`: モジュールルート、定数定義
- `src/metadata/types.rs`: メタデータ型定義

**主要な型**:

- `FileMetadata`: ファイルメタデータ
  - Inode番号、パス、サイズ
  - チャンク数とチャンク配置情報
  - パーミッション (Unix風)
  - タイムスタンプ (created/modified/accessed)
  - 所有ノードID

- `DirectoryMetadata`: ディレクトリメタデータ
  - Inode番号、パス
  - 子エントリ (DirectoryEntry)
  - パーミッション、タイムスタンプ
  - 所有ノードID

- `InodeType`: ファイル/ディレクトリ/シンボリックリンクの区別

- `FilePermissions`: Unix風パーミッション (mode, uid, gid)

**定数**:
- `CHUNK_SIZE`: 4MB (4 * 1024 * 1024)
- `VIRTUAL_NODES_PER_NODE`: 150 (Consistent Hashing用)
- `XXHASH_SEED`: 0

**実装したメソッド**:
- `FileMetadata::new()`: 新規ファイルメタデータ作成
- `FileMetadata::calculate_chunk_count()`: チャンク数計算
- `FileMetadata::chunk_offset()`: チャンクオフセット計算
- `FileMetadata::chunk_size()`: チャンクサイズ計算 (最終チャンク考慮)
- `DirectoryMetadata::new()`: 新規ディレクトリメタデータ作成
- `DirectoryMetadata::add_child()`: 子エントリ追加
- `DirectoryMetadata::remove_child()`: 子エントリ削除
- `DirectoryMetadata::find_child()`: 子エントリ検索

**テスト**:
- チャンク数計算のテスト
- チャンクサイズ計算のテスト
- ディレクトリ子エントリ操作のテスト

#### 3. ストレージ層の基盤

**実装ファイル**:
- `src/storage/mod.rs`: モジュールルート、トレイト定義
- `src/storage/error.rs`: エラー型定義
- `src/storage/iouring.rs`: IOURING統合層
- `src/storage/local.rs`: ローカルファイルシステム抽象化

**StorageBackend トレイト**:

```rust
#[async_trait::async_trait]
pub trait StorageBackend: Send + Sync {
    async fn open(&self, path: &Path, flags: OpenFlags) -> StorageResult<FileHandle>;
    async fn close(&self, handle: FileHandle) -> StorageResult<()>;
    async fn read(&self, handle: FileHandle, offset: u64, buffer: &mut [u8]) -> StorageResult<usize>;
    async fn write(&self, handle: FileHandle, offset: u64, buffer: &[u8]) -> StorageResult<usize>;
    async fn create(&self, path: &Path, mode: u32) -> StorageResult<FileHandle>;
    async fn unlink(&self, path: &Path) -> StorageResult<()>;
    async fn stat(&self, path: &Path) -> StorageResult<FileStat>;
    async fn mkdir(&self, path: &Path, mode: u32) -> StorageResult<()>;
    async fn rmdir(&self, path: &Path) -> StorageResult<()>;
    async fn fsync(&self, handle: FileHandle) -> StorageResult<()>;
}
```

**OpenFlags**:
- read/write/create/truncate/append/direct フラグ
- `to_linux_flags()`: libc フラグへの変換
- `with_direct()`: Direct I/O有効化

**StorageError**:
- `IoError`: 標準I/Oエラー
- `NotFound`: ファイル/ディレクトリが見つからない
- `PermissionDenied`: 権限エラー
- `AlreadyExists`: 既に存在
- `InvalidHandle`: 無効なファイルハンドル
- `InvalidOffset`: 無効なオフセット
- `BufferTooSmall`: バッファサイズ不足
- `Unsupported`: サポートされていない操作
- `IOUringError`: IO-uring固有のエラー
- `Internal`: 内部エラー

#### 4. IOURING統合層 (`IOUringBackend`)

**実装内容**:
- `pluvio_uring::file::DmaFile` を使用したファイルI/O
- ファイルディスクリプタ管理 (HashMap)
- 非同期 read/write/open/close
- Direct I/O対応
- ファイル作成/削除
- ディレクトリ作成/削除
- stat (ファイル情報取得)

**設計**:
- `DmaFile` を内部で保持 (fd -> DmaFile のマッピング)
- 仮想fd割り当て (100から開始)
- `OpenFlags` から `OpenOptions` への変換
- エラー変換 (std::io::Error -> StorageError)

**テスト**:
- open/read/write テスト
- create/write テスト
- stat テスト

#### 5. ローカルファイルシステム (`LocalFileSystem`)

**実装内容**:
- ファイル/ディレクトリのメタデータ管理
- 仮想パス → 物理パス変換
- Inode割り当て
- パス → Inode マッピング
- Inode → メタデータ マッピング

**主要メソッド**:
- `new()`: ルートディレクトリを指定して初期化
- `open_file()`: ファイルを開く (メタデータ自動登録)
- `create_file()`: ファイルを作成 (メタデータ登録)
- `unlink_file()`: ファイルを削除 (メタデータ削除)
- `create_directory()`: ディレクトリ作成
- `remove_directory()`: ディレクトリ削除
- `list_directory()`: ディレクトリ内容一覧
- `get_file_metadata()`: ファイルメタデータ取得
- `get_dir_metadata()`: ディレクトリメタデータ取得

**設計**:
- `IOUringBackend` をラップ
- ルートディレクトリ (Inode=1) を自動作成
- 仮想パス `/foo/bar` → 物理パス `<root>/foo/bar` 変換
- 親ディレクトリのメタデータ自動更新

**テスト**:
- create/open ファイルテスト
- ディレクトリ作成テスト
- ディレクトリ一覧テスト

---

## 課題と今後の対応

### 1. **DmaFile の Send + Sync 問題**

**問題**:
`pluvio_uring::file::DmaFile` が `Rc<IoUringReactor>` を内部で保持しているため、
`Send + Sync` が実装されていない。このため、`StorageBackend` トレイトの要件を満たせない。

**エラー内容**:
```
error[E0277]: `Rc<IoUringReactor>` cannot be sent between threads safely
```

**原因**:
- `DmaFile` が `Rc<IoUringReactor>` を保持
- `Rc` は `Send` ではない (スレッド間で送信不可)
- `StorageBackend` トレイトは `Send + Sync` を要求

**対処方針** (今後):

#### オプション1: pluvio_uringの修正
- `DmaFile` を `Arc<IoUringReactor>` に変更するようpluvioライブラリを修正
- または、pluvio_uringに `Send + Sync` 対応版のAPIを追加

#### オプション2: シングルスレッド前提で実装
- Pluvio runtimeはシングルスレッドランタイム
- `Send + Sync` を要求しない設計に変更
- `StorageBackend` トレイトから `Send + Sync` を削除
- ただし、これは将来的なマルチスレッド化を困難にする

#### オプション3: 標準のio_uringを直接使用
- `pluvio_uring` を使わず、`io-uring` crateを直接使用
- 独自の非同期ラッパーを実装
- より柔軟性が高いが、実装コストが大きい

**推奨**: オプション1 (pluvio_uringの修正) + オプション2 (短期的な回避策)

### 2. **fsync の未実装**

**問題**:
`DmaFile` には fsync 相当のメソッドがない。

**対処方針**:
- `io_uring::opcode::Fsync` を使用した実装を追加
- `IOUringBackend::fsync()` を完全実装

### 3. **テストの実行環境**

**問題**:
- IOURING はカーネル 5.1+ が必要
- CI環境でテストできない可能性

**対処方針**:
- モックバックエンドの実装
- または、標準ファイルI/Oフォールバック

---

## 統計

### ファイル数
- 新規作成: 6ファイル
  - `src/metadata/mod.rs`
  - `src/metadata/types.rs`
  - `src/storage/mod.rs`
  - `src/storage/error.rs`
  - `src/storage/iouring.rs`
  - `src/storage/local.rs`
- 修正: 2ファイル
  - `Cargo.toml`
  - `src/lib.rs`

### コード量
- メタデータ層: ~280行
- ストレージ層: ~750行
- 合計: ~1030行 (テスト含む)

### テスト
- メタデータ: 3テスト
- IOURING: 3テスト
- ローカルFS: 3テスト
- 合計: 9テスト (ただし、Send + Sync問題によりビルド失敗中)

---

## 次のステップ (フェーズ2)

フェーズ1の課題を解決後、フェーズ2に進む:

1. **Consistent Hashing実装** (`src/metadata/consistent_hash.rs`)
   - xxHash64ベース
   - 仮想ノード (150個/ノード)
   - BTreeMapによる二分探索

2. **メタデータRPC定義** (`src/rpc/metadata_ops.rs`)
   - MetadataLookup RPC
   - MetadataCreate RPC
   - MetadataDelete RPC
   - MetadataUpdate RPC

3. **メタデータキャッシュ** (`src/metadata/cache.rs`)
   - LRUキャッシュ実装
   - Invalidation機構

---

## 意思決定記録

### 2025-10-17

#### Q: DmaFileのSend + Sync問題にどう対処するか?

**決定待ち**: ユーザーに確認が必要

**選択肢**:
1. pluvio_uringを修正 (Arc対応)
2. シングルスレッド前提で進める (Send + Sync削除)
3. 独自のio_uringラッパーを実装

**影響**:
- 選択肢1: 最も理想的だが、外部依存の修正が必要
- 選択肢2: 短期的には最速だが、将来的に制約
- 選択肢3: 実装コストが高いが、最も柔軟

#### Q: メタデータの永続化は必要か?

**決定**: 現時点では不要

**理由**:
- フェーズ1ではインメモリメタデータで十分
- フェーズ5以降で永続化を検討

---

## 参考資料

- pluvio_uring: https://github.com/maetin0324/pluvio
- io_uring: https://kernel.dk/io_uring.pdf
- chfs: https://github.com/otatebe/chfs
