# Phase 10: IOUringChunkStore Implementation Complete

## 概要

`benchfsd`のChunkStoreをインメモリ実装（InMemoryChunkStore）から、io_uringを用いた実ファイルシステムへの永続化実装（IOUringChunkStore）に変更しました。

## 実装内容

### 1. IOUringChunkStoreの設計と実装

**ファイル**: `src/storage/chunk_store.rs` (lines 415-732)

#### 主要機能

- **非同期ファイルI/O**: IOUringBackendを使用した高性能な非同期ファイル操作
- **ファイルハンドルキャッシング**: 同じチャンクファイルへの繰り返しアクセスを最適化
- **Read-Modify-Write**: 部分チャンク書き込みに対応
- **データ永続化**: すべての書き込み後にfsyncを実行

#### ディレクトリ構造

```
<base_dir>/
  <inode>/
    <chunk_index>
```

例: `/var/lib/benchfs/chunks/123/0`

#### 主要メソッド

```rust
pub struct IOUringChunkStore {
    base_dir: PathBuf,
    chunk_size: usize,
    backend: Rc<IOUringBackend>,
    open_handles: RefCell<HashMap<ChunkKey, FileHandle>>,
}

impl IOUringChunkStore {
    // チャンクの書き込み（部分書き込み対応）
    pub async fn write_chunk(&self, inode: u64, chunk_index: u64,
                             offset: u64, data: &[u8]) -> ChunkStoreResult<usize>

    // チャンクの読み込み
    pub async fn read_chunk(&self, inode: u64, chunk_index: u64,
                           offset: u64, length: u64) -> ChunkStoreResult<Vec<u8>>

    // チャンクの削除
    pub async fn delete_chunk(&self, inode: u64, chunk_index: u64) -> ChunkStoreResult<()>

    // ファイルの全チャンク削除
    pub async fn delete_file_chunks(&self, inode: u64) -> ChunkStoreResult<usize>

    // 全ハンドルクローズ
    pub async fn close_all(&self) -> ChunkStoreResult<()>
}
```

### 2. API統一化: ChunkStoreメソッドの非同期化

すべてのChunkStore実装（InMemory、File、IOUring）で統一されたasync APIを提供するため、既存の実装も含めてすべてのメソッドを`async`に変更:

**変更ファイル**: `src/storage/chunk_store.rs`

```rust
// Before
impl InMemoryChunkStore {
    pub fn write_chunk(...) -> ChunkStoreResult<usize> { ... }
    pub fn read_chunk(...) -> ChunkStoreResult<Vec<u8>> { ... }
}

// After
impl InMemoryChunkStore {
    pub async fn write_chunk(...) -> ChunkStoreResult<usize> { ... }
    pub async fn read_chunk(...) -> ChunkStoreResult<Vec<u8>> { ... }
}
```

### 3. benchfsdの更新

**ファイル**: `src/bin/benchfsd.rs` (lines 93-207)

#### 主要な変更点

1. **io_uringリアクタの初期化とバッファアロケータの取得**:
```rust
let uring_reactor = IoUringReactor::builder()
    .queue_size(2048)
    .buffer_size(1 << 20) // 1 MiB
    .submit_depth(64)
    .build();

let allocator = uring_reactor.allocator.clone();
runtime.register_reactor("io_uring", uring_reactor.clone());
```

2. **IOUringBackendとIOUringChunkStoreの作成**:
```rust
let io_backend = Rc::new(IOUringBackend::new(allocator));

let chunk_store_dir = config.node.data_dir.join("chunks");
let chunk_store = Rc::new(
    IOUringChunkStore::new(&chunk_store_dir, io_backend.clone())
        .expect("Failed to create chunk store")
);
```

3. **RpcHandlerContextへの適用**:
```rust
let handler_context = Rc::new(RpcHandlerContext::new(
    metadata_manager.clone(),
    chunk_store.clone(),
));
```

### 4. RPCハンドラの更新

**ファイル**: `src/rpc/handlers.rs`

#### RpcHandlerContextの型変更

```rust
// Before
pub struct RpcHandlerContext {
    pub metadata_manager: Rc<MetadataManager>,
    pub chunk_store: Rc<InMemoryChunkStore>,
}

// After
pub struct RpcHandlerContext {
    pub metadata_manager: Rc<MetadataManager>,
    pub chunk_store: Rc<IOUringChunkStore>,
}
```

#### ハンドラメソッドへの`.await`追加

```rust
// handle_read_chunk
match ctx.chunk_store
    .read_chunk(header.inode, header.chunk_index, header.offset, header.length)
    .await  // ← 追加
{
    Ok(data) => { /* ... */ }
}

// handle_write_chunk
match ctx.chunk_store
    .write_chunk(header.inode, header.chunk_index, header.offset, &data)
    .await  // ← 追加
{
    Ok(bytes_written) => { /* ... */ }
}
```

### 5. File Operations APIの更新

**ファイル**: `src/api/file_ops.rs`

#### 主要な変更

1. **`chfs_unlink`を非同期化**:
```rust
// Before
pub fn chfs_unlink(&self, path: &str) -> ApiResult<()>

// After
pub async fn chfs_unlink(&self, path: &str) -> ApiResult<()>
```

2. **すべてのchunk操作に`.await`を追加**:
```rust
// chfs_read
match self.chunk_store.read_chunk(
    file_meta.inode,
    chunk_index,
    0,
    self.chunk_manager.chunk_size() as u64,
).await {  // ← 追加
    Ok(full_chunk) => { /* ... */ }
}

// chfs_write
self.chunk_store
    .write_chunk(file_meta.inode, chunk_index, chunk_offset, chunk_data)
    .await  // ← 追加
    .map_err(|e| ApiError::IoError(format!("Failed to write chunk: {:?}", e)))?;

// chfs_unlink
let _ = self.chunk_store.delete_file_chunks(file_meta.inode).await;  // ← 追加
```

### 6. テストの更新

**ファイル**: `src/storage/chunk_store.rs`

すべてのテストを非同期対応に更新:

```rust
fn run_test<F>(test: F)
where
    F: std::future::Future<Output = ()> + 'static,
{
    let runtime = Runtime::new(256);
    runtime.clone().run(test);
}

#[test]
fn test_inmemory_write_read_chunk() {
    run_test(async {
        let store = InMemoryChunkStore::new();
        let data = vec![0xAA; 1024];
        let written = store.write_chunk(1, 0, 0, &data).await.unwrap();
        // ...
    });
}
```

**ファイル**: `src/api/file_ops.rs`

`test_unlink_file`を非同期対応に更新:

```rust
#[test]
fn test_unlink_file() {
    run_test(async {
        let fs = BenchFS::new("node1".to_string());
        // ...
        fs.chfs_unlink("/test.txt").await.unwrap();
        // ...
    });
}
```

## テスト結果

### コンパイル結果

```
$ cargo check
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 6.80s
```

警告のみで、エラーなし。

### テスト結果

```
$ cargo test --lib
running 118 tests
...
test result: ok. 117 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out
```

すべてのテストが成功。

## アーキテクチャの利点

### 1. 高性能I/O

- **io_uring**: カーネルとのコンテキストスイッチを最小化
- **非同期処理**: ブロッキングI/Oによる待機時間を排除
- **バッチ処理**: 複数のI/O操作を効率的に処理

### 2. データ永続性

- **fsync**: すべての書き込み後にデータをディスクに同期
- **ファイルシステムベース**: プロセス再起動後もデータが保持される
- **クラッシュリカバリ**: ファイルシステムの一貫性保証を活用

### 3. スケーラビリティ

- **ファイルハンドルキャッシング**: 頻繁にアクセスされるチャンクのオーバーヘッドを削減
- **分散ファイル配置**: inode別のディレクトリ構造により、ファイルシステムの負荷を分散

### 4. メンテナビリティ

- **統一されたAPI**: すべてのChunkStore実装が同じインターフェースを持つ
- **型安全性**: Rustの型システムによる安全性保証
- **テスタビリティ**: InMemoryChunkStoreを使用した高速なユニットテスト

## 変更ファイル一覧

1. `src/storage/chunk_store.rs` - IOUringChunkStore実装、全メソッド非同期化、テスト更新
2. `src/storage/mod.rs` - IOUringChunkStoreのエクスポート
3. `src/bin/benchfsd.rs` - IOUringChunkStoreの使用、io_uringリアクタ初期化
4. `src/rpc/handlers.rs` - IOUringChunkStore対応、.await追加
5. `src/api/file_ops.rs` - chfs_unlink非同期化、.await追加、テスト更新

## 次のステップ

### 1. パフォーマンス最適化

- [ ] 登録バッファ（Registered Buffers）の活用
- [ ] バッチI/O処理の実装
- [ ] キャッシュ戦略の最適化

### 2. 機能拡張

- [ ] チャンク圧縮の実装
- [ ] チェックサム検証
- [ ] スナップショット機能

### 3. 運用機能

- [ ] メトリクス収集（I/Oレイテンシ、スループット）
- [ ] ヘルスチェック
- [ ] ガベージコレクション（孤立チャンクの削除）

## まとめ

InMemoryChunkStoreからIOUringChunkStoreへの移行により、BenchFSは以下を達成しました:

1. ✅ **データ永続性**: ファイルシステムへの実際の書き込み
2. ✅ **高性能I/O**: io_uringによる非同期I/O
3. ✅ **統一API**: すべてのChunkStore実装で一貫したインターフェース
4. ✅ **テスト完全性**: 117個のテストすべてが成功

これにより、BenchFSは本格的な分散ファイルシステムとして、実用的なデータ永続性と高性能を兼ね備えることができました。
