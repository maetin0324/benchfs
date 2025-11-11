# データRPC実装ログ

## 作成日
2025-10-17

## 概要
Phase 3の続きとして、チャンクデータの読み書きを行うRPCと、チャンクデータを保存するストレージバックエンドの実装を完了しました。

---

## 実装内容

### 1. データRPC (`src/rpc/data_ops.rs`)

**目的**: ノード間でチャンクデータを読み書きするRPC操作

**主要機能**:
- **ReadChunk RPC**: リモートノードからチャンクデータを読み込み
- **WriteChunk RPC**: リモートノードにチャンクデータを書き込み
- **RDMA対応**: ゼロコピーデータ転送のサポート
- **部分的なチャンク操作**: チャンク内の任意のオフセット/長さでの読み書き

#### RPC ID定義
```rust
pub const RPC_READ_CHUNK: RpcId = 10;
pub const RPC_WRITE_CHUNK: RpcId = 11;
```

#### ReadChunk RPC

**リクエストヘッダー**:
```rust
#[repr(C)]
#[derive(Debug, Clone, Copy, zerocopy::FromBytes, zerocopy::IntoBytes, zerocopy::KnownLayout, zerocopy::Immutable)]
pub struct ReadChunkRequestHeader {
    pub chunk_index: u64,
    pub offset: u64,
    pub length: u64,
    pub inode: u64,
}
```

**レスポンスヘッダー**:
```rust
#[repr(C)]
#[derive(Debug, Clone, Copy, zerocopy::FromBytes, zerocopy::IntoBytes, zerocopy::KnownLayout, zerocopy::Immutable)]
pub struct ReadChunkResponseHeader {
    pub bytes_read: u64,
    pub status: i32,
    _padding: [u8; 4],
}
```

**リクエスト構造体**:
```rust
pub struct ReadChunkRequest {
    header: ReadChunkRequestHeader,
    response_buffer: UnsafeCell<Vec<u8>>,
    response_ioslice: UnsafeCell<IoSliceMut<'static>>,
}
```

**特徴**:
- RDMAゼロコピー読み込み (AmRpcCallType::Get)
- サーバーが直接クライアントのバッファにデータを書き込む
- `response_buffer()`でIoSliceMutを返し、UCXがRDMA-Writeで書き込み

#### WriteChunk RPC

**リクエストヘッダー**:
```rust
#[repr(C)]
#[derive(Debug, Clone, Copy, zerocopy::FromBytes, zerocopy::IntoBytes, zerocopy::KnownLayout, zerocopy::Immutable)]
pub struct WriteChunkRequestHeader {
    pub chunk_index: u64,
    pub offset: u64,
    pub length: u64,
    pub inode: u64,
}
```

**レスポンスヘッダー**:
```rust
#[repr(C)]
#[derive(Debug, Clone, Copy, zerocopy::FromBytes, zerocopy::IntoBytes, zerocopy::KnownLayout, zerocopy::Immutable)]
pub struct WriteChunkResponseHeader {
    pub bytes_written: u64,
    pub status: i32,
    _padding: [u8; 4],
}
```

**リクエスト構造体**:
```rust
pub struct WriteChunkRequest {
    header: WriteChunkRequestHeader,
    data: Vec<u8>,
    request_ioslice: UnsafeCell<IoSlice<'static>>,
}
```

**特徴**:
- RDMAゼロコピー書き込み (AmRpcCallType::Put)
- クライアントから直接サーバーのバッファにデータを書き込む
- `request_data()`でIoSliceを返し、UCXがRDMA-Readで読み込み

#### メタデータ統合

```rust
pub fn get_chunk_node(
    chunk_index: u64,
    chunk_locations: &[NodeId],
) -> Option<&NodeId>
```

FileMetadataのchunk_locationsから、指定されたチャンクを保存しているノードを取得。

---

### 2. チャンクストレージ (`src/storage/chunk_store.rs`)

**目的**: チャンクデータをローカルストレージに保存・取得

#### ChunkKey

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ChunkKey {
    pub inode: u64,
    pub chunk_index: u64,
}
```

チャンクを一意に識別するキー。

#### InMemoryChunkStore

**目的**: メモリ内でチャンクを保存 (開発・テスト用)

**API**:
```rust
pub struct InMemoryChunkStore {
    chunks: RefCell<HashMap<ChunkKey, Vec<u8>>>,
    max_chunks: usize,
    chunk_size: usize,
}

impl InMemoryChunkStore {
    pub fn new() -> Self;
    pub fn with_capacity(max_chunks: usize, chunk_size: usize) -> Self;

    // 書き込み
    pub fn write_chunk(
        &self,
        inode: u64,
        chunk_index: u64,
        offset: u64,
        data: &[u8],
    ) -> ChunkStoreResult<usize>;

    // 読み込み
    pub fn read_chunk(
        &self,
        inode: u64,
        chunk_index: u64,
        offset: u64,
        length: u64,
    ) -> ChunkStoreResult<Vec<u8>>;

    // 削除
    pub fn delete_chunk(&self, inode: u64, chunk_index: u64) -> ChunkStoreResult<()>;
    pub fn delete_file_chunks(&self, inode: u64) -> ChunkStoreResult<usize>;

    // 状態確認
    pub fn has_chunk(&self, inode: u64, chunk_index: u64) -> bool;
    pub fn chunk_count(&self) -> usize;
    pub fn storage_size(&self) -> usize;
    pub fn clear(&self);
}
```

**特徴**:
- HashMap<ChunkKey, Vec<u8>>でチャンクを保存
- RefCellを使用 (シングルスレッド設計)
- 容量制限 (max_chunks) でストレージフルを防ぐ
- 部分的な書き込み対応 (チャンク内のオフセット指定)

#### FileChunkStore

**目的**: ファイルシステムにチャンクを保存 (本番環境用)

**API**:
```rust
pub struct FileChunkStore {
    base_dir: PathBuf,
    chunk_size: usize,
}

impl FileChunkStore {
    pub fn new<P: AsRef<Path>>(base_dir: P) -> ChunkStoreResult<Self>;

    // InMemoryChunkStoreと同じAPI
    pub fn write_chunk(...) -> ChunkStoreResult<usize>;
    pub fn read_chunk(...) -> ChunkStoreResult<Vec<u8>>;
    pub fn delete_chunk(...) -> ChunkStoreResult<()>;
    pub fn delete_file_chunks(...) -> ChunkStoreResult<usize>;
    pub fn has_chunk(...) -> bool;
}
```

**ディレクトリ構造**:
```
<base_dir>/
  <inode>/
    <chunk_index>
    <chunk_index>
    ...
```

例: `base_dir/1/0`, `base_dir/1/1`, `base_dir/2/0`

**特徴**:
- ファイルシステムに各チャンクを個別ファイルとして保存
- inode単位でディレクトリを作成
- 永続化 (ノード再起動後も保持)
- 将来的にio_uringバックエンドに置き換え可能

#### エラー型

```rust
#[derive(Debug, thiserror::Error)]
pub enum ChunkStoreError {
    ChunkNotFound { inode: u64, chunk_index: u64 },
    InvalidChunkSize { expected: usize, actual: usize },
    InvalidOffset(u64),
    IoError(#[from] std::io::Error),
    StorageFull(String),
}
```

---

## 技術的な設計判断

### UnsafeCellとライフタイムの問題

**問題**: IoSlice/IoSliceMutを返すメソッドがライフタイムエラーを起こす

当初の実装:
```rust
fn response_buffer(&self) -> &[IoSliceMut<'_>] {
    std::slice::from_ref(&mut IoSliceMut::new(buf.as_mut_slice()))
    // エラー: temporary value created here
}
```

**解決策**: UnsafeCellで'staticライフタイムのIoSliceを保持

```rust
pub struct ReadChunkRequest {
    response_buffer: UnsafeCell<Vec<u8>>,
    response_ioslice: UnsafeCell<IoSliceMut<'static>>,
}

impl ReadChunkRequest {
    pub fn new(...) -> Self {
        let mut buffer = vec![0u8; length as usize];
        let ioslice = unsafe {
            let slice: &'static mut [u8] = std::mem::transmute(buffer.as_mut_slice());
            IoSliceMut::new(slice)
        };

        Self {
            response_buffer: UnsafeCell::new(buffer),
            response_ioslice: UnsafeCell::new(ioslice),
        }
    }

    fn response_buffer(&self) -> &[IoSliceMut<'_>] {
        unsafe { std::slice::from_ref(&*self.response_ioslice.get()) }
    }
}
```

**安全性の保証**:
1. バッファはReadChunkRequestと同じライフタイム
2. IoSliceMutはresponse_buffer()経由でのみアクセス
3. RPCクライアントはRPC実行中のみ使用
4. シングルスレッド環境 (Pluvio runtime)

**トレードオフ**:
- unsafeコードが必要
- ライフタイム管理の複雑さ
- ゼロコピーRDMAのパフォーマンス

### インメモリ vs ファイルベースストレージ

**InMemoryChunkStore**:
- **利点**: 高速、テストが簡単、実装がシンプル
- **欠点**: メモリ制約、永続化なし
- **用途**: 開発・テスト、小規模環境

**FileChunkStore**:
- **利点**: 永続化、大容量、ノード再起動対応
- **欠点**: I/Oオーバーヘッド、ファイルシステム依存
- **用途**: 本番環境

**将来の改善**:
- io_uringバックエンドで非同期I/O
- Direct I/O (O_DIRECT) でページキャッシュをバイパス
- 圧縮・重複排除

---

## テスト結果

### 全体統計
- **総テスト数**: 75テスト
- **成功**: 74テスト
- **Ignored**: 1テスト (`storage::iouring::tests::test_open_read_write`)
- **失敗**: 0テスト

### データRPC新規テスト (8テスト)
- `test_read_chunk_request_header`: リクエストヘッダーのシリアライズ/デシリアライズ
- `test_read_chunk_response_header`: レスポンスヘッダー (success/error)
- `test_read_chunk_request`: ReadChunkRequest作成とバッファ
- `test_write_chunk_request_header`: WriteChunkRequestHeader
- `test_write_chunk_response_header`: WriteChunkResponseHeader
- `test_write_chunk_request`: WriteChunkRequest作成とデータ
- `test_get_chunk_node`: chunk_locationsからノード取得
- `test_rpc_ids`: RPC IDとreply stream ID

### チャンクストレージ新規テスト (10テスト)
- `test_chunk_key`: ChunkKeyの等価性
- `test_inmemory_write_read_chunk`: 基本的な書き込み/読み込み
- `test_inmemory_partial_write`: オフセット指定の部分書き込み
- `test_inmemory_delete_chunk`: チャンク削除
- `test_inmemory_delete_file_chunks`: ファイル単位での削除
- `test_inmemory_storage_full`: 容量制限
- `test_inmemory_invalid_offset`: 不正なオフセットエラー
- `test_inmemory_clear`: 全クリア
- `test_inmemory_storage_size`: ストレージサイズ計算

---

## モジュール構成の更新

### `src/rpc/mod.rs`
```rust
pub mod file_ops;
pub mod data_ops;  // 追加
pub mod server;
pub mod client;
```

### `src/storage/mod.rs`
```rust
pub mod error;
pub mod iouring;
pub mod local;
pub mod chunk_store;  // 追加

pub use chunk_store::{
    InMemoryChunkStore, FileChunkStore,
    ChunkStoreError, ChunkStoreResult, ChunkKey
};
```

---

## 統計

### 新規ファイル
- `src/rpc/data_ops.rs`: ~390行
- `src/storage/chunk_store.rs`: ~590行
- 合計: ~980行 (テスト含む)

### データRPC実装トータル統計
- **コード量**: ~980行
- **テスト数**: 18テスト
- **成功率**: 100% (74/74 passing, 1 ignored)

### 全プロジェクト統計 (Phase 1-3 + データRPC)
- **コード量**: ~3950行 (テスト含む)
- **総テスト数**: 75テスト
- **成功率**: 98.7% (74 passed, 1 ignored, 0 failed)

---

## 課題と今後の対応

### 解決済み課題

1. **ライフタイムエラー**: IoSlice/IoSliceMutを返すメソッド
   - **解決**: UnsafeCellで'staticライフタイムのIoSliceを保持

2. **ゼロコピーRDMA**: パフォーマンス重視の設計
   - **解決**: request_data() / response_buffer() でIoSliceを返す

### 今後の課題

1. **サーバーハンドラー実装**
   - 現在、ReadChunk/WriteChunkのサーバーサイドハンドラーは未実装
   - `src/rpc/server.rs`に統合する必要がある
   - ChunkStoreとの接続

2. **io_uringバックエンド**
   - FileChunkStoreは現在std::fsを使用
   - io_uringで非同期I/Oに置き換え
   - Direct I/O (O_DIRECT) 対応

3. **エラーハンドリングの改善**
   - RPC失敗時のリトライ
   - タイムアウト処理
   - ネットワークエラーのハンドリング

4. **メタデータRPC実装** (Phase 2の残りタスク)
   - MetadataLookup RPC
   - MetadataCreate/Delete/Update RPC

5. **統合テスト**
   - エンドツーエンドのデータフロー
   - ReadChunk/WriteChunk RPCの統合
   - マルチノード環境でのテスト

6. **パフォーマンス最適化**
   - RDMA転送のベンチマーク
   - チャンクサイズの最適化
   - キャッシング戦略

---

## 次のステップ

データRPC実装が完了しました。次の候補:

### オプション1: サーバーハンドラー実装
1. **ReadChunk/WriteChunk サーバーハンドラー**
   - `src/rpc/server.rs`に統合
   - ChunkStoreとの接続
   - エラーハンドリング

2. **統合テスト**
   - クライアント → サーバー → ChunkStore
   - エンドツーエンドのデータフロー

### オプション2: メタデータRPC実装 (Phase 2の完全完了)
1. **MetadataLookup RPC**
2. **MetadataCreate/Delete/Update RPC**

### オプション3: パフォーマンステスト
1. **ベンチマークスイート作成**
2. **RDMA vs TCP パフォーマンス比較**
3. **io_uringバックエンドの性能測定**

**推奨**: オプション1 (サーバーハンドラー実装) を進めて、データRPCを完全に動作させる。

---

## 技術的意思決定

### 2025-10-17

#### Q: IoSlice/IoSliceMutのライフタイム問題をどう解決するか?
**決定**: UnsafeCellで'staticライフタイムのIoSliceを保持

**理由**:
- ゼロコピーRDMAに必須
- Pluvioランタイムはシングルスレッド
- バッファのライフタイムは明確 (Request構造体と同じ)
- unsafeは局所化され、文書化されている

**トレードオフ**:
- unsafeコードのリスク → コメントとドキュメントで軽減
- ライフタイム管理の複雑さ → 構造体設計で局所化

#### Q: チャンクストレージをメモリとファイルのどちらにするか?
**決定**: 両方実装 (InMemoryChunkStore + FileChunkStore)

**理由**:
- InMemory: 開発・テスト用、高速
- File: 本番環境用、永続化
- 同じAPI (read_chunk/write_chunk) で切り替え可能
- 将来的にio_uringバックエンドに置き換え可能

#### Q: RPCリクエストにinodeを含めるか?
**決定**: 含める

**理由**:
- チャンクの識別に必須 (chunk_index だけでは不十分)
- サーバーがChunkKeyを作成してChunkStoreにアクセス
- 将来的にパーミッションチェックに使用可能

#### Q: 部分的なチャンク読み書きをサポートするか?
**決定**: サポートする (offset + length)

**理由**:
- ファイルの任意の範囲を読み書き可能
- チャンク境界をまたぐ操作を効率化
- 柔軟性が高い

---

## 参考資料
- [UCX (Unified Communication X)](https://openucx.org/)
- [zerocopy crate](https://docs.rs/zerocopy/latest/zerocopy/)
- [io_uring](https://kernel.dk/io_uring.pdf)
- [RDMA](https://en.wikipedia.org/wiki/Remote_direct_memory_access)
