# Phase 7: 分散ファイルシステムの基盤実装

**Date**: 2025-10-17
**Author**: Claude (Sonnet 4.5)
**Task**: Phase 7 - 分散ファイルシステムの接続管理とRPC基盤

## Overview

Phase 7では、BenchFSを分散ファイルシステムとして動作させるための基盤を実装しました。リモートノードへのRPC接続を管理するConnectionPoolと、分散モードをサポートするBenchFS APIの拡張を完了しました。

## 実装したコンポーネント

### 1. ConnectionPool (`src/rpc/connection.rs`)

リモートノードへのRPC接続を管理するコネクションプール。

```rust
pub struct ConnectionPool {
    worker: Rc<Worker>,
    connections: RefCell<HashMap<String, Rc<RpcClient>>>,
}

impl ConnectionPool {
    pub fn new(worker: Rc<Worker>) -> Self;
    pub async fn get_or_connect(&self, node_addr: SocketAddr) -> Result<Rc<RpcClient>, RpcError>;
    pub fn get(&self, node_addr: &str) -> Option<Rc<RpcClient>>;
    pub fn disconnect(&self, node_addr: &str);
    pub fn clear(&self);
    pub fn connection_count(&self) -> usize;
    pub fn connected_nodes(&self) -> Vec<String>;
}
```

#### 主な機能

1. **接続の再利用**: 同じノードへの接続を自動的にキャッシュし、再利用
2. **自動接続**: `get_or_connect()`で存在しない接続を自動的に作成
3. **非同期接続**: UCXの`connect_socket()`を使用した非同期接続
4. **接続管理**: 接続数の追跡、接続済みノードのリスト取得

#### 実装の詳細

```rust
pub async fn get_or_connect(&self, node_addr: SocketAddr) -> Result<Rc<RpcClient>, RpcError> {
    let addr_str = node_addr.to_string();

    // Check if connection already exists
    {
        let connections = self.connections.borrow();
        if let Some(client) = connections.get(&addr_str) {
            tracing::debug!("Reusing existing connection to {}", addr_str);
            return Ok(client.clone());
        }
    }

    // Create new connection
    tracing::info!("Creating new connection to {}", addr_str);

    let endpoint = self.worker.connect_socket(node_addr).await.map_err(|e| {
        RpcError::ConnectionError(format!("Failed to connect to {}: {:?}", addr_str, e))
    })?;

    let conn = crate::rpc::Connection::new(self.worker.clone(), endpoint);
    let client = Rc::new(RpcClient::new(conn));

    // Initialize reply stream
    if let Err(e) = client.init_reply_stream(100) {
        tracing::warn!("Failed to initialize reply stream: {:?}", e);
    }

    // Store in cache
    self.connections.borrow_mut().insert(addr_str, client.clone());

    Ok(client)
}
```

### 2. BenchFS分散モード対応 (`src/api/file_ops.rs`)

BenchFS APIに分散ファイルシステムをサポートするための機能を追加。

#### 新しいフィールド

```rust
pub struct BenchFS {
    // ... existing fields

    /// Connection pool for remote RPC calls
    connection_pool: Option<Rc<ConnectionPool>>,
}
```

#### 新しいAPI

```rust
impl BenchFS {
    /// Create a new BenchFS client (local only)
    pub fn new(node_id: String) -> Self;

    /// Create a new BenchFS client with connection pool (distributed mode)
    pub fn with_connection_pool(node_id: String, connection_pool: Rc<ConnectionPool>) -> Self;

    /// Check if distributed mode is enabled
    pub fn is_distributed(&self) -> bool;
}
```

#### リモート読み込みの準備

`benchfs_read`にリモートチャンク読み込みの構造を準備:

```rust
// Cache miss - need to fetch chunk
tracing::trace!("Cache miss for chunk {}", chunk_index);

// Try local chunk store first
match self.chunk_store.read_chunk(...) {
    Ok(full_chunk) => {
        // Cache the full chunk for future reads
        self.chunk_cache.put(chunk_id, full_chunk.clone());
        Some(full_chunk)
    }
    Err(_) => {
        // Local read failed - try remote if distributed mode enabled
        // TODO: Implement remote read via RPC
        // For now, treat as sparse (zeros)
        None
    }
}
```

### 3. RPC エラータイプの拡張 (`src/rpc/mod.rs`)

新しいエラータイプ`ConnectionError`を追加:

```rust
pub enum RpcError {
    InvalidHeader,
    TransportError(String),
    HandlerError(String),
    ConnectionError(String),  // NEW
    Timeout,
}
```

## 技術的な課題と解決策

### 課題1: Endpointのインポートパス

**問題**: `pluvio_ucx::Endpoint`が存在しない

**解決**: 正しいパスを使用
```rust
use pluvio_ucx::endpoint::Endpoint;
```

### 課題2: connect_addr() vs connect_socket()

**問題**: `connect_addr()`は`WorkerAddressInner`を要求し、文字列アドレスを直接受け付けない

**解決**: `connect_socket()`を使用してSocketAddrで接続
```rust
// 修正前:
let endpoint = self.worker.connect_addr(node_addr)?;

// 修正後:
let endpoint = self.worker.connect_socket(node_addr).await.map_err(...)?;
```

### 課題3: 同期 vs 非同期 API

**問題**: RPC呼び出しは`async`だが、既存のBenchFS APIは同期的

**対応**:
- 現時点では基盤のみ実装
- 完全な統合には以下が必要:
  1. `benchfs_read`/`benchfs_write`を`async`に変更
  2. すべてのテストを非同期対応に更新
  3. メタデータのチャンク配置情報の完全な統合

## テスト結果

```
test result: ok. 115 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 1.82s
```

すべてのライブラリテストが正常にパスしています。ConnectionPoolとBenchFS APIの拡張は既存の機能を壊すことなく統合されました。

## アーキテクチャ

### 分散読み込みフロー (計画)

```
Client (benchfs_read)
    ↓
1. メタデータ取得
    ├─> FileMetadata.chunk_locations から該当チャンクのノードを特定
    ↓
2. キャッシュチェック
    ├─> Hit: キャッシュから返す
    ├─> Miss: ↓
    ↓
3. ローカルストアチェック
    ├─> 存在: ローカルから読み込み & キャッシュ
    ├─> 不在: ↓
    ↓
4. リモート読み込み
    ├─> ConnectionPool.get_or_connect(node_addr)
    ├─> ReadChunkRequest作成
    ├─> client.execute(&request).await
    ├─> レスポンスをキャッシュ
    └─> データを返す
```

### 分散書き込みフロー (計画)

```
Client (benchfs_write)
    ↓
1. メタデータ取得/更新
    ├─> 新しいチャンクの配置を決定 (PlacementStrategy)
    ├─> FileMetadata.chunk_locations を更新
    ↓
2. キャッシュ無効化
    ├─> 該当チャンクのキャッシュをクリア
    ↓
3. ローカル or リモート書き込み
    ├─> ローカルノードの場合: chunk_store.write_chunk()
    ├─> リモートノードの場合:
    │   ├─> ConnectionPool.get_or_connect(node_addr)
    │   ├─> WriteChunkRequest作成
    │   └─> client.execute(&request).await
    ↓
4. メタデータ更新
    └─> ファイルサイズ、チャンク数を更新
```

## 既存のRPC構造

BenchFSには既に以下のRPC実装が存在:

### データ操作RPC

1. **ReadChunkRequest** (`src/rpc/data_ops.rs`):
   - チャンクの読み込みリクエスト
   - RDMA-writeでサーバーからクライアントへデータ転送
   - Rendezvousプロトコル使用

2. **WriteChunkRequest** (`src/rpc/data_ops.rs`):
   - チャンクの書き込みリクエスト
   - RDMA-readでサーバーがクライアントからデータ取得
   - Rendezvousプロトコル使用

3. **get_chunk_node()**: チャンク配置情報からノードを取得するヘルパー関数

### メタデータ操作RPC

1. **MetadataLookupRequest** (`src/rpc/metadata_ops.rs`):
   - ファイル/ディレクトリメタデータの取得

2. **MetadataCreateFileRequest**:
   - ファイル作成

3. **MetadataCreateDirRequest**:
   - ディレクトリ作成

4. **MetadataDeleteRequest**:
   - ファイル/ディレクトリ削除

## 今後の実装

### Phase 7 完了項目

✅ **完了した項目**:
1. ConnectionPool実装
2. BenchFS分散モード対応の基盤
3. RpcError拡張
4. リモートRPC呼び出しの統合ポイント特定
5. ビルドとテストの成功

### Phase 8 以降の計画

#### 8.1 非同期API対応

```rust
// 現在 (同期)
pub fn benchfs_read(&self, handle: &FileHandle, buf: &mut [u8]) -> ApiResult<usize>

// 将来 (非同期)
pub async fn benchfs_read(&self, handle: &FileHandle, buf: &mut [u8]) -> ApiResult<usize>
```

この変更には以下が必要:
- すべてのBenchFS APIメソッドを`async`に変更
- テストを非同期対応に更新
- エグゼキュータ/ランタイムの統合

#### 8.2 リモートRPC呼び出しの完全統合

`benchfs_read`でのリモート読み込み実装例:

```rust
Err(_) => {
    // Local read failed - try remote if distributed mode enabled
    if let Some(pool) = &self.connection_pool {
        // Get chunk location from metadata
        if let Some(node_addr) = get_chunk_node(chunk_index, &file_meta.chunk_locations) {
            // Parse node address
            if let Ok(socket_addr) = node_addr.parse() {
                // Connect to remote node
                match pool.get_or_connect(socket_addr).await {
                    Ok(client) => {
                        // Create RPC request
                        let request = ReadChunkRequest::new(
                            chunk_index,
                            0,
                            self.chunk_manager.chunk_size() as u64,
                            file_meta.inode,
                        );

                        // Execute RPC
                        match request.call(&client).await {
                            Ok(response) if response.is_success() => {
                                let full_chunk = request.take_data();
                                // Cache for future reads
                                self.chunk_cache.put(chunk_id, full_chunk.clone());
                                Some(full_chunk)
                            }
                            _ => None,
                        }
                    }
                    Err(e) => {
                        tracing::error!("Failed to connect to {}: {:?}", node_addr, e);
                        None
                    }
                }
            } else {
                None
            }
        } else {
            None
        }
    } else {
        None
    }
}
```

#### 8.3 チャンク配置戦略の統合

```rust
impl BenchFS {
    /// Determine which node should store a chunk
    fn determine_chunk_location(&self, inode: u64, chunk_index: u64) -> NodeId {
        self.placement.place_chunk(inode, chunk_index)
    }

    /// Update file metadata with new chunk locations
    fn update_chunk_locations(&self, file_meta: &mut FileMetadata, chunk_index: u64) {
        let node = self.determine_chunk_location(file_meta.inode, chunk_index);

        // Ensure chunk_locations vector is large enough
        while file_meta.chunk_locations.len() <= chunk_index as usize {
            file_meta.chunk_locations.push(String::new());
        }

        file_meta.chunk_locations[chunk_index as usize] = node;
    }
}
```

#### 8.4 タイムアウトとリトライロジック

```rust
pub struct RpcClientWithRetry {
    client: Rc<RpcClient>,
    max_retries: usize,
    timeout: Duration,
}

impl RpcClientWithRetry {
    pub async fn execute_with_retry<T: AmRpc>(&self, request: &T) -> Result<T::ResponseHeader, RpcError> {
        for attempt in 0..self.max_retries {
            // Try with timeout
            let result = tokio::time::timeout(
                self.timeout,
                request.call(&self.client)
            ).await;

            match result {
                Ok(Ok(response)) => return Ok(response),
                Ok(Err(e)) if attempt == self.max_retries - 1 => return Err(e),
                Ok(Err(_)) => {
                    // Retry on error
                    tracing::warn!("RPC attempt {} failed, retrying...", attempt + 1);
                    continue;
                }
                Err(_) => {
                    // Timeout
                    tracing::warn!("RPC attempt {} timed out", attempt + 1);
                    if attempt == self.max_retries - 1 {
                        return Err(RpcError::Timeout);
                    }
                }
            }
        }

        Err(RpcError::Timeout)
    }
}
```

#### 8.5 統合テスト

```rust
#[cfg(test)]
mod distributed_tests {
    use super::*;

    #[test]
    fn test_distributed_read_write() {
        // Setup: 2つのBenchFSノードを作成
        // Node1: クライアント
        // Node2: サーバー (チャンクを保持)

        // Test: Node1からNode2のチャンクを読み込み
    }

    #[test]
    fn test_connection_pool_reuse() {
        // Test: 同じノードへの接続が再利用されることを確認
    }

    #[test]
    fn test_remote_chunk_caching() {
        // Test: リモートから読み込んだチャンクがキャッシュされることを確認
    }
}
```

## まとめ

Phase 7で実装した機能:

✅ **完了した項目**:
1. **ConnectionPool**: リモートノードへのRPC接続管理
2. **BenchFS分散モード**: `with_connection_pool()`コンストラクタと`is_distributed()`メソッド
3. **RpcError拡張**: ConnectionErrorバリアント
4. **ビルドとテスト**: 115個のテストがすべてパス
5. **リモートRPC統合ポイント**: `benchfs_read`/`benchfs_write`のリモート呼び出し場所を特定

**技術的成果**:
- **接続管理**: 効率的なコネクションプールによる接続の再利用
- **モジュール設計**: 既存コードを壊さない拡張
- **非同期準備**: async/awaitに対応した設計
- **エラーハンドリング**: 接続エラーの適切な処理

**実装詳細**:
- src/rpc/connection.rs: ConnectionPool (105行)
- src/api/file_ops.rs: BenchFS分散モード対応 (20行追加)
- src/rpc/mod.rs: RpcError拡張 (1バリアント追加)

**次のステップ**:
- BenchFS APIの非同期化
- リモートRPC呼び出しの完全統合
- タイムアウト/リトライロジック
- 統合テスト

BenchFSは分散ファイルシステムとして動作するための**接続管理の基盤が完成**しました!
