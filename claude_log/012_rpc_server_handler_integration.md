# RPC Server and Handler Integration Implementation Log

**Date**: 2025-10-17
**Author**: Claude (Sonnet 4.5)
**Task**: RPCサーバーとハンドラーの統合

## Overview

RPCサーバー(`RpcServer`)と実際のハンドラー関数(`handle_read_chunk`など)を統合し、RPCリクエストを適切に処理できるようにしました。

## Problem Statement

実装前の状態:
- ✅ `RpcServer`は実装されているが、プレースホルダーの`server_handler`を呼び出すのみ
- ✅ 実際のハンドラー関数(`handle_read_chunk`など)は`src/rpc/handlers.rs`に実装済み
- ❌ ハンドラー関数に必要な`RpcHandlerContext`がサーバーに統合されていない
- ❌ サーバーとハンドラーの接続機構が存在しない

## Implementation Details

### 1. RpcServerへのハンドラーコンテキスト統合

#### Before (src/rpc/server.rs:10-13)
```rust
pub struct RpcServer {
    worker: Rc<Worker>,
}
```

#### After (src/rpc/server.rs:10-13)
```rust
pub struct RpcServer {
    worker: Rc<Worker>,
    handler_context: Rc<RpcHandlerContext>,
}
```

**変更点**:
- `handler_context`フィールドを追加
- コンストラクタで`RpcHandlerContext`を受け取るように変更

### 2. ハンドラー関数のシグネチャ更新

すべてのハンドラー関数を`Rc<RpcHandlerContext>`を受け取るように変更しました。

#### Before
```rust
pub async fn handle_read_chunk(
    ctx: &RpcHandlerContext,
    am_msg: AmMsg,
) -> Result<ReadChunkResponseHeader, RpcError>
```

#### After
```rust
pub async fn handle_read_chunk(
    ctx: Rc<RpcHandlerContext>,
    am_msg: AmMsg,
) -> Result<ReadChunkResponseHeader, RpcError>
```

**理由**:
- 非同期タスク間でコンテキストを共有するため
- `Rc`による参照カウントで安全にクローン可能

**更新されたハンドラー**:
- `handle_read_chunk`
- `handle_write_chunk`
- `handle_metadata_lookup`
- `handle_metadata_create_file`
- `handle_metadata_create_dir`
- `handle_metadata_delete`

### 3. listen_with_handler メソッドの実装

カスタムハンドラー関数を受け取り、RPCリクエストを処理する新しいメソッドを追加しました。

#### src/rpc/server.rs:90-136
```rust
pub async fn listen_with_handler<Rpc, ReqH, ResH, F, Fut>(
    &self,
    _runtime: Rc<Runtime>,
    handler: F,
) -> Result<(), RpcError>
where
    ResH: Serializable + 'static,
    ReqH: Serializable + 'static,
    Rpc: AmRpc<RequestHeader = ReqH, ResponseHeader = ResH> + 'static,
    F: Fn(Rc<RpcHandlerContext>, pluvio_ucx::async_ucx::ucp::AmMsg) -> Fut + 'static,
    Fut: std::future::Future<Output = Result<ResH, RpcError>> + 'static,
{
    let stream = self.worker.am_stream(Rpc::rpc_id())?;
    tracing::info!("RpcServer: Listening on AM stream ID {} with handler", Rpc::rpc_id());

    let ctx = self.handler_context.clone();

    loop {
        let msg = stream.wait_msg().await;
        if msg.is_none() {
            tracing::info!("RpcServer: Stream closed for RPC ID {}", Rpc::rpc_id());
            break;
        }

        let am_msg = msg.ok_or_else(|| RpcError::TransportError("..."))?;
        let ctx_clone = ctx.clone();

        // Call the handler function
        match handler(ctx_clone, am_msg).await {
            Ok(_response_header) => {
                // TODO: Send response back to client
                tracing::debug!("Handler succeeded for RPC ID {}", Rpc::rpc_id());
            }
            Err(e) => {
                tracing::error!("Handler failed for RPC ID {}: {:?}", Rpc::rpc_id(), e);
            }
        }
    }

    Ok(())
}
```

**機能**:
- 任意のハンドラー関数を受け取り、RPCリクエストごとに呼び出し
- ハンドラーコンテキストをクローンして各ハンドラーに渡す
- エラーハンドリングとロギング

**TODO**:
- 返信の送信機能（現在はログのみ）

### 4. register_all_handlers メソッドの実装

すべての標準RPCハンドラーを一括登録する便利メソッドを追加しました。

#### src/rpc/server.rs:157-257
```rust
pub async fn register_all_handlers(&self, runtime: Rc<Runtime>) -> Result<(), RpcError> {
    use crate::rpc::data_ops::{ReadChunkRequest, WriteChunkRequest};
    use crate::rpc::metadata_ops::{...};
    use crate::rpc::handlers::{...};

    tracing::info!("Registering all RPC handlers...");

    // Spawn ReadChunk handler
    {
        let server = self.clone_for_handler();
        let rt = runtime.clone();
        runtime.spawn(async move {
            if let Err(e) = server.listen_with_handler::<ReadChunkRequest, _, _, _, _>(
                rt,
                handle_read_chunk,
            ).await {
                tracing::error!("ReadChunk handler error: {:?}", e);
            }
        });
    }

    // ... 他のハンドラーも同様に登録 ...

    tracing::info!("All RPC handlers registered successfully");
    Ok(())
}
```

**登録されるハンドラー**:
1. **ReadChunk** (RPC ID 10): チャンクデータの読み取り
2. **WriteChunk** (RPC ID 11): チャンクデータの書き込み
3. **MetadataLookup** (RPC ID 20): メタデータ検索
4. **MetadataCreateFile** (RPC ID 21): ファイルメタデータ作成
5. **MetadataCreateDir** (RPC ID 22): ディレクトリメタデータ作成
6. **MetadataDelete** (RPC ID 23): メタデータ削除

**設計**:
- 各ハンドラーは独立した非同期タスクとして実行
- `runtime.spawn()`で各ハンドラーをバックグラウンドタスクとして起動
- エラーは各ハンドラー内でログ出力

### 5. clone_for_handler ヘルパーメソッド

#### src/rpc/server.rs:261-266
```rust
fn clone_for_handler(&self) -> Self {
    Self {
        worker: self.worker.clone(),
        handler_context: self.handler_context.clone(),
    }
}
```

**目的**:
- 各ハンドラータスクに`RpcServer`のクローンを提供
- `Rc`による参照カウントで効率的に共有

## Usage Example

### サーバーの起動方法

```rust
use std::rc::Rc;
use pluvio_runtime::executor::Runtime;
use benchfs::metadata::MetadataManager;
use benchfs::storage::InMemoryChunkStore;
use benchfs::rpc::handlers::RpcHandlerContext;
use benchfs::rpc::server::RpcServer;

// 1. UCXの初期化
let ctx = pluvio_ucx::Context::new()?;
let worker = Rc::new(ctx.create_worker()?);

// 2. ハンドラーコンテキストの作成
let metadata_manager = Rc::new(MetadataManager::new("node1".to_string()));
let chunk_store = Rc::new(InMemoryChunkStore::new());
let handler_ctx = Rc::new(RpcHandlerContext::new(
    metadata_manager,
    chunk_store,
));

// 3. RPCサーバーの作成
let server = RpcServer::new(worker.clone(), handler_ctx);

// 4. すべてのハンドラーを登録
let runtime = Rc::new(Runtime::new());
server.register_all_handlers(runtime.clone()).await?;

// 5. イベントループの実行
runtime.run();
```

### 個別ハンドラーの登録

```rust
use benchfs::rpc::data_ops::ReadChunkRequest;
use benchfs::rpc::handlers::handle_read_chunk;

server.listen_with_handler::<ReadChunkRequest, _, _, _, _>(
    runtime.clone(),
    handle_read_chunk,
).await?;
```

## Architecture

### コンポーネント関係図

```
┌─────────────────┐
│   RpcServer     │
├─────────────────┤
│ - worker        │◄────────── UCX Worker
│ - handler_ctx   │◄────────── RpcHandlerContext
└────────┬────────┘                 │
         │                          │
         │ register_all_handlers()  │
         │                          │
         ▼                          │
┌─────────────────────────┐        │
│  Handler Tasks          │        │
│  (spawned by runtime)   │        │
├─────────────────────────┤        │
│ - ReadChunk handler     │───────►│
│ - WriteChunk handler    │───────►│
│ - Metadata handlers     │───────►│
└─────────────────────────┘        │
                                    │
                    ┌───────────────┴────────────────┐
                    │     RpcHandlerContext          │
                    ├────────────────────────────────┤
                    │ - metadata_manager             │
                    │ - chunk_store                  │
                    └────────────────────────────────┘
```

### データフロー

```
Client Request
     │
     ▼
[UCX Worker] ──► [AM Stream] ──► [listen_with_handler]
                                          │
                                          ▼
                                   [Handler Function]
                                   (handle_read_chunk, etc.)
                                          │
                                          ▼
                                   [RpcHandlerContext]
                                          │
                    ┌─────────────────────┴─────────────────────┐
                    ▼                                            ▼
            [MetadataManager]                            [ChunkStore]
                    │                                            │
                    ▼                                            ▼
              Response Header ◄────────── Process Request ─────►
                    │
                    ▼
            (TODO: Send reply to client)
```

## Testing

すべての既存テストが合格しました：

```bash
$ cargo test --lib
test result: ok. 85 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out
```

### 作成したサンプル

- `examples/rpc_server_example.rs`: RPCサーバーの使用方法を示すサンプルコード

```bash
$ cargo run --example rpc_server_example
RPC Server Example
==================
... (使用方法の説明)
```

## Remaining Work

### 1. 返信送信機能の実装 (Critical)

現在、ハンドラーは処理を行いますが、クライアントに返信を送信していません。

**TODO in src/rpc/server.rs:124-127**:
```rust
Ok(_response_header) => {
    // TODO: Implement reply sending
    tracing::debug!("Handler succeeded for RPC ID {}", Rpc::rpc_id());
}
```

**実装が必要な機能**:
- クライアントのエンドポイント情報の取得
- 返信ストリームへのレスポンス送信
- RDMA転送のサポート（ReadChunk/WriteChunk用）

### 2. エラーハンドリングの改善

- ネットワークエラー時のリトライ
- タイムアウト処理
- クライアントへのエラー返信

### 3. パフォーマンス最適化

- ハンドラータスクのプール化
- バッチ処理
- メモリアロケーションの最適化

## Benefits

1. **モジュラー設計**: ハンドラー関数が独立しており、テスト・拡張が容易
2. **型安全**: Rustの型システムによる安全なハンドラー登録
3. **柔軟性**: 個別ハンドラー登録と一括登録の両方をサポート
4. **スケーラビリティ**: 各ハンドラーが独立したタスクとして動作
5. **保守性**: 明確な責任分離（サーバー ↔ ハンドラー ↔ ストレージ）

## Files Modified

- `src/rpc/server.rs`: ハンドラー統合機能の追加
- `src/rpc/handlers.rs`: ハンドラー関数シグネチャの更新
- `examples/rpc_server_example.rs`: サンプルコードの作成（新規）

## Next Steps

1. **返信送信機能の実装**: クライアントへのレスポンス送信
2. **メタデータRPCハンドラーの完全実装**: プレースホルダーを実装に置き換え
3. **統合テストの作成**: 実際のUCX環境でのエンドツーエンドテスト
4. **ファイルシステムAPI層の実装**: ユーザー向け公開API（Phase 4）

## Conclusion

RPCサーバーとハンドラーの統合により、BenchFSはRPCリクエストを実際に処理できるようになりました。これは分散ファイルシステムとして機能するための重要な基盤です。

次のステップは、クライアントへの返信送信機能の実装と、メタデータRPCハンドラーの完全実装です。
