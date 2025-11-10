# Stream + RMA ベースRPC基盤リファクタリング実装計画

**作成日**: 2025-11-10
**目的**: Active Message (AM) の不安定性を解決し、UCX Stream + RMA を用いた安定したRPC基盤へ移行する

## 目次

1. [現状分析](#1-現状分析)
2. [問題点と動機](#2-問題点と動機)
3. [新設計概要](#3-新設計概要)
4. [4つのRPCパターン詳細設計](#4-4つのrpcパターン詳細設計)
5. [アーキテクチャ設計](#5-アーキテクチャ設計)
6. [影響を受けるモジュール](#6-影響を受けるモジュール)
7. [実装手順](#7-実装手順)
8. [テスト戦略](#8-テスト戦略)
9. [マイグレーション計画](#9-マイグレーション計画)
10. [パフォーマンス考察](#10-パフォーマンス考察)
11. [リスクと対策](#11-リスクと対策)
12. [進捗管理](#12-進捗管理)

---

## 1. 現状分析

### 1.1 現在のAMベースRPC実装

**コアコンポーネント**:
- `src/rpc/mod.rs` - `AmRpc` トレイト定義
- `src/rpc/client.rs` - `RpcClient` (AM送信、reply stream受信)
- `src/rpc/server.rs` - `RpcServer` (AM受信、reply送信)
- `src/rpc/helpers.rs` - `receive_path()`, `send_rpc_response_via_reply()`
- `src/rpc/data_ops.rs` - ReadChunk, WriteChunk RPC実装
- `src/rpc/metadata_ops.rs` - Metadata操作RPC実装

**現在のRPCフロー** (AM + reply_ep方式):
```
Client                                  Server
   |                                       |
   |-- am_send (header + data) ---------->|
   |                                       |--- server_handler
   |                                       |    (parse header, receive data)
   |<-------- reply (header + data) ------|
   |                                       |
```

**使用しているUCX API**:
- `Endpoint::am_send_vectorized()` - リクエスト送信
- `AmMsg::recv_data_vectored()` - データ受信
- `AmMsg::reply()` / `reply_vectorized()` - レスポンス送信
- `Worker::am_stream()` - AM stream作成
- `AmStream::wait_msg()` - メッセージ待機

### 1.2 現在の問題点

1. **MessageTruncated エラー**
   - Eager/Data モードのデータに対して `recv_data_vectored()` を呼ぶと発生
   - `get_data()` で直接取得すべきデータを誤って `recv_data_vectored()` で受信しようとする
   - `helpers.rs` は修正済みだが、`handlers.rs` に古いコードが残存

2. **AM プロトコルの複雑性**
   - Eager/Rndv/Data の3モードを適切に使い分ける必要
   - データサイズに応じて自動切り替わるが、受信側で正しく処理する必要
   - エラーハンドリングが困難

3. **reply_ep メカニズムの不安定性**
   - WorkerAddress モードでの動作が不安定
   - UCX内部でのreply_ep管理の問題の可能性
   - デバッグログが出力されない問題 (バイナリ更新されていない疑い)

4. **デバッグの困難さ**
   - AMの内部状態が見えにくい
   - データがどのように送られているか追跡困難
   - エラー発生時の診断情報不足

---

## 2. 問題点と動機

### 2.1 AMベースRPCの根本的な問題

**UCXのAMは本質的に一方向通信**:
- リクエスト送信時に reply_ep を設定し、サーバー側で `reply()` を呼ぶ
- しかし、reply_ep の生存期間管理が複雑
- WorkerAddress ベースの接続では reply_ep が正しく機能しない可能性

**データ受信の混乱**:
- 小さいデータ: Eager モードで即座に配信 → `get_data()` で取得
- 中程度のデータ: Data モードで配信 → `get_data()` または `recv_data_vectored()`
- 大きいデータ: Rendezvous モード → 必ず `recv_data_vectored()` が必要
- この判別を送信側・受信側の両方で正しく実装する必要があり、エラーの原因に

### 2.2 Stream + RMA方式のメリット

**明確な通信フロー**:
1. Stream で制御メッセージ (ヘッダー) を送受信
2. RMA (put/get) で大きいデータを転送
3. 完了通知を Stream で送信

**利点**:
- ✅ フロー制御が明確 (Stream で順序保証、RMA で帯域効率)
- ✅ エラーハンドリングが簡単 (各ステップで明示的に結果確認)
- ✅ デバッグが容易 (ログで全ステップを追跡可能)
- ✅ パフォーマンスチューニングが柔軟 (RMA を使うタイミングを制御可能)
- ✅ TCP-like な信頼性 (Stream の順序保証)

---

## 3. 新設計概要

### 3.1 基本コンセプト

**Stream = 制御チャネル**:
- リクエスト/レスポンスのヘッダー送受信
- RMA用のrkey, アドレス, サイズの交換
- 完了通知の送信
- エラー通知

**RMA = データチャネル**:
- 大きなデータの高速転送
- Client PUT: クライアント → サーバー (Write RPC)
- Client GET: サーバー → クライアント (Read RPC)

### 3.2 4種類のRPCパターン

| パターン | 説明 | RMA方向 | 例 |
|---------|------|---------|---|
| **Pattern 1: No RMA** | ヘッダーのみの小さいRPC | なし | Ping-Pong, Stat, Mkdir |
| **Pattern 2: Client PUT** | クライアントがデータをサーバーへ送信 | Client→Server | Write, Create |
| **Pattern 3: Client GET** | サーバーがデータをクライアントへ送信 | Server→Client | Read, List |
| **Pattern 4: Client PUT + Server PUT** | 双方向データ転送 | 両方向 | (将来拡張用) |

### 3.3 新しいRPCフロー (Stream + RMA方式)

#### Pattern 1: No RMA (例: Stat, Ping-Pong)
```
Client                                  Server
   |                                       |
   |-- stream_send(req_header) ---------->|
   |                                       |--- handler: 処理実行
   |<------- stream_recv(res_header) -----|
   |                                       |
```

#### Pattern 2: Client PUT (例: Write)
```
Client                                  Server
   |                                       |
   | 1. メモリ領域登録                      |
   |    mem = MemoryHandle::register()     |
   |    rkey = mem.pack()                  |
   |                                       |
   |-- stream_send(req_header + rkey) --->|
   |                                       |--- 2. rkey unpack
   |                                       |    buffer準備
   |<------- stream_recv(addr, size) -----|
   |                                       |
   | 3. PUT実行                            |
   |-- put(data, remote_addr, rkey) ----->|
   |                                       |--- 4. データ書き込み
   | 4. 完了通知                           |
   |-- stream_send(completion) ---------->|
   |                                       |--- 5. 処理完了
   |<------- stream_recv(res_header) -----|
   |                                       |
```

#### Pattern 3: Client GET (例: Read)
```
Client                                  Server
   |                                       |
   | 1. 受信バッファ登録                    |
   |    mem = MemoryHandle::register()     |
   |    rkey = mem.pack()                  |
   |                                       |
   |-- stream_send(req_header + rkey) --->|
   |                                       |--- 2. rkey unpack
   |                                       |    データ読み込み
   |<------- stream_recv(data_size) ------|
   |                                       |
   |                                       |--- 3. PUT実行
   |<-------- put(data, client_addr) -----|    (server→client)
   |                                       |
   |                                       |--- 4. 完了通知
   |<------- stream_recv(completion) -----|
   |                                       |
```

---

## 4. 4つのRPCパターン詳細設計

### 4.1 Pattern 1: No RMA (Header-Only RPC)

**適用対象**: Ping-Pong, Stat, Mkdir, Rmdir, Shutdown

**クライアント側実装**:
```rust
pub struct StreamRpcRequest {
    header: RequestHeader,
}

impl StreamRpc for StreamRpcRequest {
    async fn call(&self, client: &StreamRpcClient) -> Result<ResponseHeader, RpcError> {
        // 1. リクエストヘッダー送信
        client.endpoint.stream_send(self.header.as_bytes()).await?;

        // 2. レスポンスヘッダー受信
        let mut response_buf = [MaybeUninit::<u8>::uninit(); HEADER_SIZE];
        let len = client.endpoint.stream_recv(&mut response_buf).await?;

        // 3. デシリアライズ
        let response = ResponseHeader::from_bytes(&response_buf[..len])?;
        Ok(response)
    }
}
```

**サーバー側実装**:
```rust
pub async fn handle_no_rma_request(
    endpoint: &Endpoint,
    header: RequestHeader,
) -> Result<ResponseHeader, RpcError> {
    // 1. 処理実行 (例: stat)
    let result = ctx.metadata_manager.get_file_metadata(&path)?;

    // 2. レスポンスヘッダー作成
    let response = ResponseHeader::success(result);

    // 3. レスポンス送信
    endpoint.stream_send(response.as_bytes()).await?;

    Ok(response)
}
```

### 4.2 Pattern 2: Client PUT (Write RPC)

**適用対象**: WriteChunk, CreateFile (with data)

**クライアント側実装**:
```rust
pub struct WriteChunkRequest<'a> {
    header: WriteChunkRequestHeader,
    data: &'a [u8],  // 送信するデータ
}

impl StreamRpc for WriteChunkRequest<'_> {
    async fn call(&self, client: &StreamRpcClient) -> Result<ResponseHeader, RpcError> {
        // 1. データ用メモリ登録
        let mut data_copy = self.data.to_vec();
        let mem = MemoryHandle::register(client.context, &mut data_copy);
        let rkey_buf = mem.pack();

        // 2. リクエストヘッダー + rkey + data_addr + data_size を送信
        let req_msg = RequestMessage {
            header: self.header,
            data_addr: data_copy.as_ptr() as u64,
            data_size: data_copy.len() as u64,
            rkey: rkey_buf.as_ref().to_vec(),
        };
        client.endpoint.stream_send(&req_msg.serialize()).await?;

        // 3. サーバーの準備完了待ち (server_buffer_addr受信)
        let mut ready_buf = [MaybeUninit::<u8>::uninit(); 16];
        let len = client.endpoint.stream_recv(&mut ready_buf).await?;
        let server_addr = u64::from_ne_bytes(...);  // サーバーバッファアドレス

        // 4. PUT実行 (client → server)
        let rkey = RKey::unpack(&client.endpoint, &rkey_buf);
        client.endpoint.put(&data_copy, server_addr, &rkey).await?;

        // 5. 完了通知送信
        client.endpoint.stream_send(b"DONE").await?;

        // 6. レスポンスヘッダー受信
        let mut response_buf = [MaybeUninit::<u8>::uninit(); HEADER_SIZE];
        let len = client.endpoint.stream_recv(&mut response_buf).await?;
        let response = ResponseHeader::from_bytes(&response_buf[..len])?;

        Ok(response)
    }
}
```

**サーバー側実装**:
```rust
pub async fn handle_client_put_request(
    endpoint: &Endpoint,
    header: WriteChunkRequestHeader,
    req_msg: RequestMessage,
) -> Result<ResponseHeader, RpcError> {
    // 1. rkey unpack
    let rkey = RKey::unpack(endpoint, &req_msg.rkey);

    // 2. 受信バッファ準備 (registered buffer使用)
    let mut buffer = ctx.allocator.acquire().await;
    let buffer_addr = buffer.as_ptr() as u64;

    // 3. バッファアドレス送信 (準備完了通知)
    endpoint.stream_send(&buffer_addr.to_ne_bytes()).await?;

    // 4. クライアントのPUT完了待ち
    let mut completion_buf = [MaybeUninit::<u8>::uninit(); 4];
    endpoint.stream_recv(&mut completion_buf).await?;  // "DONE"

    // 5. データをストレージに書き込み (zero-copy)
    let bytes_written = ctx.chunk_store.write_chunk_fixed(
        &path,
        header.chunk_index,
        header.offset,
        buffer,
        req_msg.data_size as usize,
    ).await?;

    // 6. レスポンスヘッダー送信
    let response = ResponseHeader::success(bytes_written);
    endpoint.stream_send(response.as_bytes()).await?;

    Ok(response)
}
```

### 4.3 Pattern 3: Client GET (Read RPC)

**適用対象**: ReadChunk, ReadDir

**クライアント側実装**:
```rust
pub struct ReadChunkRequest {
    header: ReadChunkRequestHeader,
    buffer: Vec<u8>,  // 受信バッファ
}

impl StreamRpc for ReadChunkRequest {
    async fn call(&self, client: &StreamRpcClient) -> Result<ResponseHeader, RpcError> {
        // 1. 受信バッファ用メモリ登録
        let mut buffer = vec![0u8; self.header.length as usize];
        let mem = MemoryHandle::register(client.context, &mut buffer);
        let rkey_buf = mem.pack();

        // 2. リクエストヘッダー + rkey + buffer_addr + buffer_size を送信
        let req_msg = RequestMessage {
            header: self.header,
            buffer_addr: buffer.as_ptr() as u64,
            buffer_size: buffer.len() as u64,
            rkey: rkey_buf.as_ref().to_vec(),
        };
        client.endpoint.stream_send(&req_msg.serialize()).await?;

        // 3. サーバーのPUT完了待ち (data_size受信)
        let mut size_buf = [MaybeUninit::<u8>::uninit(); 8];
        let len = client.endpoint.stream_recv(&mut size_buf).await?;
        let data_size = u64::from_ne_bytes(...);

        // 4. 完了通知受信
        let mut completion_buf = [MaybeUninit::<u8>::uninit(); 4];
        client.endpoint.stream_recv(&mut completion_buf).await?;  // "DONE"

        // 5. バッファにデータが書き込まれている
        // レスポンスヘッダー受信は不要 (data_sizeで成功判定)

        // 6. データを返す
        buffer.truncate(data_size as usize);
        Ok(buffer)
    }
}
```

**サーバー側実装**:
```rust
pub async fn handle_client_get_request(
    endpoint: &Endpoint,
    header: ReadChunkRequestHeader,
    req_msg: RequestMessage,
) -> Result<(), RpcError> {
    // 1. データ読み込み
    let data = ctx.chunk_store.read_chunk(
        &path,
        header.chunk_index,
        header.offset,
        header.length,
    ).await?;

    // 2. rkey unpack
    let rkey = RKey::unpack(endpoint, &req_msg.rkey);

    // 3. PUT実行 (server → client)
    endpoint.put(&data, req_msg.buffer_addr, &rkey).await?;

    // 4. データサイズ送信
    endpoint.stream_send(&(data.len() as u64).to_ne_bytes()).await?;

    // 5. 完了通知送信
    endpoint.stream_send(b"DONE").await?;

    Ok(())
}
```

### 4.4 Pattern 4: Client PUT + Server PUT (将来拡張用)

**適用対象**: 検索、変換系RPC (現時点では未使用)

**概要**:
- クライアントがクエリデータを送信 (Client PUT)
- サーバーが処理して結果を返送 (Server PUT)

**実装詳細**: Pattern 2 と Pattern 3 の組み合わせ

---

## 5. アーキテクチャ設計

### 5.1 新しいトレイト定義

```rust
// src/rpc/stream_rpc.rs (新規作成)

/// Stream + RMA ベースのRPCトレイト
pub trait StreamRpc {
    type RequestHeader: Serializable;
    type ResponseHeader: Serializable;

    /// RPC ID
    fn rpc_id() -> RpcId;

    /// RPC パターン種別
    fn pattern() -> RpcPattern {
        RpcPattern::NoRma  // デフォルト
    }

    /// リクエストヘッダー
    fn request_header(&self) -> &Self::RequestHeader;

    /// クライアント側RPC実行
    async fn call(&self, client: &StreamRpcClient) -> Result<Self::ResponseHeader, RpcError>;

    /// サーバー側ハンドラー
    async fn server_handler(
        ctx: Rc<RpcHandlerContext>,
        endpoint: &Endpoint,
        header: Self::RequestHeader,
    ) -> Result<Self::ResponseHeader, RpcError>;
}

/// RPC パターン種別
pub enum RpcPattern {
    /// Pattern 1: Header-only (no data transfer)
    NoRma,
    /// Pattern 2: Client PUT (client → server)
    ClientPut { data_size: u64 },
    /// Pattern 3: Client GET (server → client)
    ClientGet { buffer_size: u64 },
    /// Pattern 4: Bidirectional (both PUT)
    ClientPutServerPut { request_size: u64, response_size: u64 },
}
```

### 5.2 新しいClient実装

```rust
// src/rpc/stream_client.rs (新規作成)

pub struct StreamRpcClient {
    endpoint: Endpoint,
    worker: Rc<Worker>,
    context: Arc<Context>,
}

impl StreamRpcClient {
    pub fn new(endpoint: Endpoint, worker: Rc<Worker>, context: Arc<Context>) -> Self {
        Self { endpoint, worker, context }
    }

    /// Pattern 1: No RMA
    pub async fn execute_no_rma<T: StreamRpc>(&self, request: &T)
        -> Result<T::ResponseHeader, RpcError>
    {
        // Stream only communication
        self.endpoint.stream_send(request.request_header().as_bytes()).await?;
        let response = self.recv_response_header::<T>().await?;
        Ok(response)
    }

    /// Pattern 2: Client PUT
    pub async fn execute_client_put<T: StreamRpc>(&self, request: &T, data: &[u8])
        -> Result<T::ResponseHeader, RpcError>
    {
        // 1. Register memory and send rkey
        let mut data_copy = data.to_vec();
        let mem = MemoryHandle::register(&self.context, &mut data_copy);
        let rkey_buf = mem.pack();

        let req_msg = self.build_put_request_message(request, &data_copy, &rkey_buf)?;
        self.endpoint.stream_send(&req_msg).await?;

        // 2. Wait for server buffer address
        let server_addr = self.recv_buffer_address().await?;

        // 3. PUT data to server
        let rkey = RKey::unpack(&self.endpoint, rkey_buf.as_ref());
        self.endpoint.put(&data_copy, server_addr, &rkey).await?;

        // 4. Send completion notification
        self.endpoint.stream_send(b"DONE").await?;

        // 5. Receive response
        let response = self.recv_response_header::<T>().await?;
        Ok(response)
    }

    /// Pattern 3: Client GET
    pub async fn execute_client_get<T: StreamRpc>(&self, request: &T, buffer: &mut [u8])
        -> Result<T::ResponseHeader, RpcError>
    {
        // 1. Register buffer and send rkey
        let mem = MemoryHandle::register(&self.context, buffer);
        let rkey_buf = mem.pack();

        let req_msg = self.build_get_request_message(request, buffer, &rkey_buf)?;
        self.endpoint.stream_send(&req_msg).await?;

        // 2. Wait for server PUT completion
        let data_size = self.recv_data_size().await?;

        // 3. Receive completion notification
        self.recv_completion().await?;

        // Buffer now contains the data from server
        let response = T::ResponseHeader::success(data_size);
        Ok(response)
    }

    // Helper methods...
}
```

### 5.3 新しいServer実装

```rust
// src/rpc/stream_server.rs (新規作成)

pub struct StreamRpcServer {
    worker: Rc<Worker>,
    handler_context: Rc<RpcHandlerContext>,
}

impl StreamRpcServer {
    pub fn new(worker: Rc<Worker>, handler_context: Rc<RpcHandlerContext>) -> Self {
        Self { worker, handler_context }
    }

    /// RPC リスナー起動 (endpoint ごと)
    pub async fn serve(&self, endpoint: Endpoint) -> Result<(), RpcError> {
        loop {
            // Check shutdown flag
            if self.handler_context.should_shutdown() {
                break;
            }

            // 1. Receive RPC header
            let rpc_id = self.recv_rpc_id(&endpoint).await?;

            // 2. Dispatch to handler based on RPC ID
            match rpc_id {
                RPC_READ_CHUNK => {
                    self.handle_rpc::<ReadChunkRequest>(&endpoint).await?;
                }
                RPC_WRITE_CHUNK => {
                    self.handle_rpc::<WriteChunkRequest>(&endpoint).await?;
                }
                RPC_METADATA_LOOKUP => {
                    self.handle_rpc::<MetadataLookupRequest>(&endpoint).await?;
                }
                // ... その他のRPC
                _ => {
                    tracing::warn!("Unknown RPC ID: {}", rpc_id);
                }
            }
        }

        Ok(())
    }

    async fn handle_rpc<T: StreamRpc>(&self, endpoint: &Endpoint) -> Result<(), RpcError> {
        // Pattern に応じて処理分岐
        match T::pattern() {
            RpcPattern::NoRma => self.handle_no_rma::<T>(endpoint).await,
            RpcPattern::ClientPut { .. } => self.handle_client_put::<T>(endpoint).await,
            RpcPattern::ClientGet { .. } => self.handle_client_get::<T>(endpoint).await,
            RpcPattern::ClientPutServerPut { .. } => self.handle_bidirectional::<T>(endpoint).await,
        }
    }

    async fn handle_no_rma<T: StreamRpc>(&self, endpoint: &Endpoint) -> Result<(), RpcError> {
        // 1. Receive request header
        let header = self.recv_request_header::<T>(endpoint).await?;

        // 2. Execute handler
        let response = T::server_handler(self.handler_context.clone(), endpoint, header).await?;

        // 3. Send response
        endpoint.stream_send(response.as_bytes()).await?;

        Ok(())
    }

    async fn handle_client_put<T: StreamRpc>(&self, endpoint: &Endpoint) -> Result<(), RpcError> {
        // 1. Receive request message (header + rkey + addr + size)
        let req_msg = self.recv_put_request_message::<T>(endpoint).await?;

        // 2. Prepare buffer for PUT
        let mut buffer = self.handler_context.allocator.acquire().await;
        let buffer_addr = buffer.as_ptr() as u64;

        // 3. Send buffer address (ready notification)
        endpoint.stream_send(&buffer_addr.to_ne_bytes()).await?;

        // 4. Wait for client PUT completion
        self.recv_completion(endpoint).await?;

        // 5. Execute handler with received data
        let response = T::server_handler_with_data(
            self.handler_context.clone(),
            endpoint,
            req_msg.header,
            &buffer.as_slice()[..req_msg.data_size],
        ).await?;

        // 6. Send response
        endpoint.stream_send(response.as_bytes()).await?;

        Ok(())
    }

    async fn handle_client_get<T: StreamRpc>(&self, endpoint: &Endpoint) -> Result<(), RpcError> {
        // 1. Receive request message (header + rkey + buffer_addr + buffer_size)
        let req_msg = self.recv_get_request_message::<T>(endpoint).await?;

        // 2. Execute handler to get data
        let data = T::server_handler_get_data(
            self.handler_context.clone(),
            endpoint,
            req_msg.header,
        ).await?;

        // 3. Unpack client rkey
        let rkey = RKey::unpack(endpoint, &req_msg.rkey);

        // 4. PUT data to client
        endpoint.put(&data, req_msg.buffer_addr, &rkey).await?;

        // 5. Send data size
        endpoint.stream_send(&(data.len() as u64).to_ne_bytes()).await?;

        // 6. Send completion notification
        endpoint.stream_send(b"DONE").await?;

        Ok(())
    }

    // Helper methods...
}
```

---

## 6. 影響を受けるモジュール

### 6.1 新規作成が必要なファイル

| ファイルパス | 説明 | 優先度 |
|------------|------|--------|
| `src/rpc/stream_rpc.rs` | StreamRpc トレイト定義 | 高 |
| `src/rpc/stream_client.rs` | StreamRpcClient 実装 | 高 |
| `src/rpc/stream_server.rs` | StreamRpcServer 実装 | 高 |
| `src/rpc/stream_helpers.rs` | Stream用ヘルパー関数 | 中 |
| `src/rpc/stream_data_ops.rs` | Stream版 Read/Write RPC | 高 |
| `src/rpc/stream_metadata_ops.rs` | Stream版 Metadata RPC | 中 |

### 6.2 修正が必要な既存ファイル

| ファイルパス | 修正内容 | 優先度 |
|------------|---------|--------|
| `src/rpc/mod.rs` | StreamRpc 関連のexport追加 | 高 |
| `src/bin/benchfsd_mpi.rs` | Server起動処理を Stream版に切り替え | 高 |
| `lib/pluvio/pluvio_ucx/lib/async-ucx/src/ucp/endpoint/rma.rs` | RMA API確認・必要に応じて拡張 | 中 |
| `lib/pluvio/pluvio_ucx/lib/async-ucx/src/ucp/endpoint/stream.rs` | Stream API確認・必要に応じて拡張 | 中 |

### 6.3 段階的廃止予定のファイル

| ファイルパス | 廃止タイミング | 備考 |
|------------|--------------|------|
| `src/rpc/client.rs` (AmRpc版) | Phase 4 | 移行完了後に削除 |
| `src/rpc/server.rs` (AmRpc版) | Phase 4 | 移行完了後に削除 |
| `src/rpc/helpers.rs` (一部) | Phase 4 | receive_path, send_rpc_response_via_reply 削除 |
| `src/rpc/data_ops.rs` (AmRpc版) | Phase 4 | Stream版に置き換え |
| `src/rpc/metadata_ops.rs` (AmRpc版) | Phase 4 | Stream版に置き換え |

---

## 7. 実装手順

### Phase 0: 準備 (1-2日)

**目標**: 現状の問題を完全に把握し、設計を確定

**タスク**:
1. ✅ この実装計画ドキュメントの作成
2. ⬜ UCX Stream/RMA API の動作確認
   - `lib/pluvio/pluvio_ucx/lib/async-ucx/examples/stream.rs` を実行
   - `lib/pluvio/pluvio_ucx/lib/async-ucx/examples/rma.rs` を実行
   - 動作確認と性能測定
3. ⬜ 簡単なStream+RMAのプロトタイプ作成
   - `examples/stream_rma_proto.rs` を作成
   - Pattern 1 (No RMA) の動作確認
   - Pattern 2 (Client PUT) の動作確認
   - Pattern 3 (Client GET) の動作確認

**完了条件**: Stream+RMA方式の動作原理を理解し、技術的な実現可能性を確認

---

### Phase 1: 基盤構築 (3-5日)

**目標**: Stream+RMA ベースの新RPC基盤を構築

**タスク**:
1. ⬜ `src/rpc/stream_rpc.rs` 作成
   - `StreamRpc` トレイト定義
   - `RpcPattern` enum 定義
   - 共通型定義 (RequestMessage, ResponseMessage)

2. ⬜ `src/rpc/stream_helpers.rs` 作成
   - メモリ登録・rkey pack/unpack ヘルパー
   - Stream送受信ヘルパー
   - エラーハンドリングユーティリティ

3. ⬜ `src/rpc/stream_client.rs` 作成
   - `StreamRpcClient` 構造体実装
   - Pattern 1-3 の execute メソッド実装
   - connection management

4. ⬜ `src/rpc/stream_server.rs` 作成
   - `StreamRpcServer` 構造体実装
   - `serve()` メインループ実装
   - RPC dispatcher 実装
   - Pattern 1-3 のハンドラー実装

5. ⬜ 単体テスト作成
   - `tests/stream_rpc_basic_test.rs`
   - Pattern 1: No RMA のテスト
   - エラーハンドリングのテスト

**完了条件**: Pattern 1 (No RMA) が動作し、単体テストがパス

---

### Phase 2: データRPC移行 (5-7日)

**目標**: ReadChunk, WriteChunk を Stream+RMA版に移行

**タスク**:
1. ⬜ `src/rpc/stream_data_ops.rs` 作成
   - `ReadChunkRequest` (Pattern 3: Client GET) 実装
   - `WriteChunkRequest` (Pattern 2: Client PUT) 実装
   - Path 送信を Stream で実装 (小さいデータなのでRMA不要)

2. ⬜ Read RPC テスト
   - `tests/stream_rpc_read_test.rs` 作成
   - 小サイズ (1KB) テスト
   - 中サイズ (1MB) テスト
   - 大サイズ (10MB) テスト
   - エラーケーステスト

3. ⬜ Write RPC テスト
   - `tests/stream_rpc_write_test.rs` 作成
   - 小サイズ (1KB) テスト
   - 中サイズ (1MB) テスト
   - 大サイズ (10MB) テスト
   - Zero-copy write with registered buffer テスト

4. ⬜ パフォーマンス測定
   - AM版 vs Stream+RMA版のベンチマーク
   - Latency / Throughput 比較
   - CPU使用率比較

**完了条件**: ReadChunk, WriteChunk が動作し、AM版と同等以上の性能

---

### Phase 3: MetadataRPC移行 (3-5日)

**目標**: 全てのMetadata操作RPCをStream版に移行

**タスク**:
1. ⬜ `src/rpc/stream_metadata_ops.rs` 作成
   - `MetadataLookupRequest` (Pattern 1: No RMA)
   - `MetadataCreateFileRequest` (Pattern 1: No RMA)
   - `MetadataCreateDirRequest` (Pattern 1: No RMA)
   - `MetadataDeleteRequest` (Pattern 1: No RMA)
   - `MetadataUpdateRequest` (Pattern 1: No RMA)
   - `ShutdownRequest` (Pattern 1: No RMA)

2. ⬜ 統合テスト
   - `tests/stream_rpc_metadata_test.rs` 作成
   - 全MetadataRPCの動作確認
   - エラーケーステスト

**完了条件**: 全MetadataRPCが動作し、テストがパス

---

### Phase 4: サーバー統合とIOR統合テスト (5-7日)

**目標**: benchfsd_mpi を Stream版RPC基盤で動作させ、IORテストをパス

**タスク**:
1. ⬜ `src/bin/benchfsd_mpi.rs` 修正
   - RpcServer を StreamRpcServer に切り替え
   - AM stream 初期化を削除
   - Endpoint ベースのサーバー起動に変更
   - WorkerAddress 方式との互換性確保

2. ⬜ Connection 管理の修正
   - `src/rpc/connection.rs` を Stream+RMA対応に修正
   - `get_or_connect()` メソッドを更新
   - Endpoint の再利用戦略

3. ⬜ IOR統合テスト
   - `make test-ior` を実行
   - 全テストケースをパス
   - MessageTruncated エラーが発生しないことを確認

4. ⬜ パフォーマンステスト
   - IOR write/read throughput測定
   - AM版との性能比較
   - レイテンシ測定

5. ⬜ ストレステスト
   - 長時間動作テスト (24時間)
   - 大量ファイル作成/削除テスト
   - 並行アクセステスト

**完了条件**:
- IOR統合テストが全てパス
- 安定性が確認される
- パフォーマンスがAM版と同等以上

---

### Phase 5: 旧実装の削除とドキュメント整備 (2-3日)

**目標**: AM版実装を削除し、ドキュメントを更新

**タスク**:
1. ⬜ 旧実装の削除
   - `src/rpc/client.rs` (AmRpc版) 削除
   - `src/rpc/server.rs` (AmRpc版) 削除
   - `src/rpc/data_ops.rs` (AmRpc版) 削除
   - `src/rpc/metadata_ops.rs` (AmRpc版) 削除
   - `src/rpc/helpers.rs` の AM関連関数削除

2. ⬜ コード整理
   - 未使用コードの削除
   - コメント更新
   - clippy warnings 修正

3. ⬜ ドキュメント更新
   - `README.md` 更新 (RPC基盤の説明)
   - `ARCHITECTURE.md` 作成 (Stream+RMA設計の詳細)
   - `MIGRATION_GUIDE.md` 作成 (AM→Stream移行ガイド)
   - API docコメント追加

4. ⬜ 最終テスト
   - 全単体テスト実行
   - 全統合テスト実行
   - IORテスト実行

**完了条件**:
- 旧実装が完全に削除
- ドキュメントが整備
- 全テストがパス

---

## 8. テスト戦略

### 8.1 単体テスト

**Pattern 1 (No RMA) テスト**:
```rust
// tests/stream_rpc_basic_test.rs
#[test]
fn test_pattern1_ping_pong() {
    // Setup: create client and server
    // Execute: send ping, receive pong
    // Assert: response is correct
}
```

**Pattern 2 (Client PUT) テスト**:
```rust
// tests/stream_rpc_write_test.rs
#[test]
fn test_pattern2_write_chunk() {
    // Setup: prepare data buffer
    // Execute: client PUT to server
    // Assert: data written correctly to storage
}
```

**Pattern 3 (Client GET) テスト**:
```rust
// tests/stream_rpc_read_test.rs
#[test]
fn test_pattern3_read_chunk() {
    // Setup: prepare data on server
    // Execute: client GET from server
    // Assert: received data is correct
}
```

### 8.2 統合テスト

**IOR統合テスト**:
```bash
# tests/docker/Makefile
test-ior: build-optimized
    docker-compose up -d
    # IOR write test
    docker exec benchfs_controller ior -w -a POSIX ...
    # IOR read test
    docker exec benchfs_controller ior -r -a POSIX ...
    docker-compose down
```

**ストレステスト**:
```bash
# tests/stress_test.sh
# 1. Long-running test (24 hours)
# 2. Concurrent access test (100 clients)
# 3. Large file test (100GB file)
# 4. Memory leak test
```

### 8.3 パフォーマンステスト

**ベンチマークスイート**:
```rust
// benches/stream_rpc_bench.rs
fn bench_write_throughput_1mb(c: &mut Criterion) {
    // Measure write throughput for 1MB chunks
}

fn bench_read_latency_4kb(c: &mut Criterion) {
    // Measure read latency for 4KB requests
}
```

**比較メトリクス**:
- Throughput (MB/s): Write/Read
- Latency (μs): Small requests (4KB)
- CPU usage (%): Idle/Load
- Memory usage (MB): Baseline/Peak

---

## 9. マイグレーション計画

### 9.1 段階的移行戦略

**フェーズ1: 両方式の共存**
- AM版とStream版を同時に動作させる
- Feature flag で切り替え可能にする
- 並行テストで安定性確認

**フェーズ2: Stream版をデフォルトに**
- Stream版をデフォルトに設定
- AM版はフォールバック用に残す
- 1週間の安定動作確認

**フェーズ3: AM版削除**
- Stream版のみにする
- AM版のコードを削除
- ドキュメント更新

### 9.2 ロールバック計画

**問題発生時の対応**:
1. Feature flag でAM版に即座に切り替え
2. Stream版の問題を修正
3. 修正後、再度Stream版に切り替え

**ロールバック手順**:
```bash
# 1. Feature flag を切り替え
cargo build --release --no-default-features --features am-rpc

# 2. バイナリを再デプロイ
make deploy

# 3. テスト実行
make test-ior
```

---

## 10. パフォーマンス考察

### 10.1 予想される性能

**Stream + RMA のメリット**:
- ✅ Zero-copy data transfer (RMA)
- ✅ オーバーヘッド削減 (AM protocol parsing 不要)
- ✅ パイプライン可能 (Stream と RMA の並行実行)

**Stream + RMA のデメリット**:
- ❌ ラウンドトリップ増加 (ヘッダー送信 → rkey交換 → データ転送 → 完了通知)
- ❌ メモリ登録コスト (MemoryHandle::register)
- ❌ 小さいデータには不向き (オーバーヘッドが大きい)

### 10.2 最適化戦略

**小さいデータ (< 4KB)**:
- Pattern 1 (No RMA) を使用
- Stream で直接データを送信
- RMA のオーバーヘッドを回避

**中程度のデータ (4KB - 1MB)**:
- AM と Stream+RMA を性能比較
- データサイズに応じて自動切り替え
- 閾値を実測ベースで調整

**大きいデータ (> 1MB)**:
- Pattern 2/3 (RMA) を使用
- Registered buffer で zero-copy
- 高スループットを実現

### 10.3 パイプライン化

**複数チャンクの並行転送**:
```
Time:     0ms    10ms   20ms   30ms   40ms
Chunk 0:  [PUT]  [完了]
Chunk 1:         [PUT]  [完了]
Chunk 2:                [PUT]  [完了]
Chunk 3:                       [PUT]  [完了]
```

**実装方針**:
- クライアント側で複数リクエストを並行発行
- サーバー側でハンドラーを並行実行
- `pluvio_runtime::spawn_polling()` で並行処理

---

## 11. リスクと対策

### 11.1 技術的リスク

| リスク | 影響 | 確率 | 対策 |
|--------|------|------|------|
| Stream性能がAMより劣る | 高 | 中 | ベンチマークで事前確認、閾値ベースの切り替え |
| RMAメモリ登録コストが高い | 中 | 高 | Registered buffer pool 導入 |
| WorkerAddress方式との非互換 | 高 | 低 | Endpoint ベース設計で回避 |
| 実装バグによる不安定性 | 高 | 中 | 段階的移行、十分なテスト |

### 11.2 スケジュールリスク

| リスク | 影響 | 確率 | 対策 |
|--------|------|------|------|
| 実装期間が想定より長い | 中 | 中 | Phase 2までを優先、Phase 3以降は後回し可 |
| バグ修正に時間がかかる | 高 | 中 | 十分なテスト期間を確保、ロールバック準備 |
| IOR統合テストで問題発見 | 高 | 低 | Phase 2でデータRPCを十分にテスト |

### 11.3 対策の優先順位

1. **Phase 0 でプロトタイプ作成** → 技術的実現可能性を確認
2. **Phase 1 で十分な単体テスト** → 基盤品質を確保
3. **Phase 2 でパフォーマンステスト** → 性能問題を早期発見
4. **Phase 4 で統合テスト** → IORテストで安定性確認
5. **ロールバック計画を準備** → 問題発生時の影響最小化

---

## 12. 進捗管理

### 12.1 タスク管理

このドキュメントの各Phaseのタスクリストを使用して進捗管理を行う。

**記号の意味**:
- ✅ 完了
- ⬜ 未着手
- 🚧 作業中
- ❌ ブロック中

### 12.2 定期レビュー

**デイリー**:
- 実装進捗確認
- ブロッカー解消
- テスト結果確認

**ウィークリー**:
- Phase完了状況確認
- パフォーマンス測定結果レビュー
- 次週の計画調整

### 12.3 完了基準

**各Phaseの完了基準**:
- 全タスクが ✅ 状態
- 関連する単体テストが全てパス
- コードレビュー完了
- ドキュメント更新完了

**プロジェクト全体の完了基準**:
- IOR統合テストが全てパス
- パフォーマンスがAM版と同等以上
- 24時間安定動作確認
- ドキュメント完備

---

## 付録A: 用語集

| 用語 | 説明 |
|------|------|
| AM (Active Messages) | UCXの一方向メッセージング機能。送信側が受信側のハンドラーを直接呼び出す。 |
| Stream | UCXのストリーム通信機能。TCP-likeな順序保証がある双方向通信。 |
| RMA (Remote Memory Access) | UCXのリモートメモリアクセス機能。PUT/GET操作でゼロコピー転送。 |
| Eager mode | AMの小データ送信モード。データが即座に配信される。 |
| Rendezvous mode | AMの大データ送信モード。送信側と受信側が協調してRDMA転送を行う。 |
| MemoryHandle | UCXで登録されたメモリ領域。RMA操作に必要。 |
| RKey | Remote Key。RMA操作でリモートメモリにアクセスするための鍵。 |
| WorkerAddress | UCXのワーカーアドレス。ノード間接続に使用。 |
| reply_ep | AMのレスポンス送信用エンドポイント。応答先を識別する。 |

---

## 付録B: 参考資料

**UCX ドキュメント**:
- UCX API Reference: https://openucx.readthedocs.io/
- UCX Programming Guide: https://github.com/openucx/ucx/wiki

**pluvio_ucx コード**:
- `lib/pluvio/pluvio_ucx/lib/async-ucx/src/ucp/endpoint/stream.rs`
- `lib/pluvio/pluvio_ucx/lib/async-ucx/src/ucp/endpoint/rma.rs`
- `lib/pluvio/pluvio_ucx/lib/async-ucx/examples/stream.rs`
- `lib/pluvio/pluvio_ucx/lib/async-ucx/examples/rma.rs`

**現在のBenchFS RPC実装**:
- `src/rpc/mod.rs` - AmRpc トレイト
- `src/rpc/client.rs` - RpcClient
- `src/rpc/server.rs` - RpcServer
- `src/rpc/data_ops.rs` - ReadChunk/WriteChunk
- `src/rpc/metadata_ops.rs` - Metadata操作

---

## 変更履歴

| 日付 | 変更内容 | 変更者 |
|------|---------|--------|
| 2025-11-10 | 初版作成 | Claude |

