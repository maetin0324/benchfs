# BenchFS RPC通信方式リファクタリング実装計画書

**作成日**: 2025-11-08
**目的**: reply_ep方式から永続的Endpoint + ソケットベース通信方式への移行
**背景**: reply_epがリクエストごとに毎回作成されることによるパフォーマンス問題の解決

---

## 目次
1. [現状分析](#現状分析)
2. [新アーキテクチャ設計](#新アーキテクチャ設計)
3. [データ構造設計](#データ構造設計)
4. [通信フロー設計](#通信フロー設計)
5. [実装フェーズ](#実装フェーズ)
6. [ファイル変更一覧](#ファイル変更一覧)
7. [エラーハンドリング戦略](#エラーハンドリング戦略)
8. [テスト戦略](#テスト戦略)
9. [リスクと対策](#リスクと対策)

---

## 1. 現状分析

### 1.1 現在のreply_ep方式の問題点

```
[Client]                           [Server]
   |                                   |
   | connect_addr(WorkerAddress)       |
   |---------------------------------->|
   |                                   |
   | am_send(need_reply=true)          |
   |---------------------------------->| AmMsg.reply_ep が毎回生成される
   |                                   |
   | <--reply_stream.wait_msg()------- | reply_ep.send()
   |                                   |
```

**問題点:**
- 各リクエストでreply_epが新規作成される
- Endpointの作成・破棄コストが高い
- UCXのリソース管理オーバーヘッド

### 1.2 WorkerAddress方式の制約

- クライアント → サーバーの一方向接続のみ
- サーバーはクライアントを識別できない
- 永続的な双方向通信路が確立されない

---

## 2. 新アーキテクチャ設計

### 2.1 ソケットベース永続的接続方式

```
[Server起動]
   |
   | 1. Listener作成 (0.0.0.0:0)
   | 2. SocketAddr取得
   | 3. サーバーリストファイルに書き込み
   |    Format: "node_0 192.168.1.1:45678"
   |
   V
[待機状態]
   |
   | <-- accept() --- [Client接続]
   |                     |
   | RegisterClientRequest  |
   | <-------------------- | (クライアントID送信)
   |                        |
   | RegisterClientResponse |
   | --------------------> |
   |                        |
   | ClientRegistry に登録  |
   | (client_id, endpoint)  |
   |                        |
   V                        V
[以降の通信]
   |
   | <-- RPC Request ---- |
   |                       |
   | ClientRegistry から  |
   | endpoint を取得      |
   |                       |
   | -- RPC Response ---> |
   |   (同じendpointで)   |
   |                       |
```

### 2.2 LRUキャッシュによるEndpoint管理

```
ClientRegistry {
    cache: LruCache<ClientId, ClientInfo> (容量: 1000)
}

キャッシュアウト時:
1. cache.pop_lru() → 最も古いエントリ取得
2. ConnectionCloseRequest 送信
3. endpoint.close()
4. クライアントに通知

クライアント側:
1. ConnectionCloseNotification 受信
2. endpoint.close()
3. 次回必要時に再接続
```

---

## 3. データ構造設計

### 3.1 サーバーリストファイル形式

**ファイル名**: `{registry_dir}/server_list.txt`

**フォーマット**:
```
# BenchFS Server List
# Format: node_id ip:port
node_0 192.168.1.10:45678
node_1 192.168.1.11:45679
node_2 192.168.1.12:45680
node_3 192.168.1.13:45681
```

**特徴:**
- 人間可読なテキスト形式
- コメント行対応（#で始まる行）
- node_id と SocketAddr のペア
- 各サーバーが自分の行を追記

### 3.2 ClientInfo構造体

```rust
/// サーバー側で管理するクライアント情報
pub struct ClientInfo {
    /// クライアントID（クライアントが指定）
    client_id: String,

    /// このクライアント専用のEndpoint
    endpoint: Rc<Endpoint>,

    /// クライアントのSocketAddr（デバッグ用）
    socket_addr: SocketAddr,

    /// 接続確立時刻
    connected_at: Instant,

    /// 最終使用時刻（LRUエビクション用）
    last_used: RefCell<Instant>,
}
```

### 3.3 ClientRegistry構造体

```rust
pub struct ClientRegistry {
    /// ClientID → ClientInfo のLRUキャッシュ
    clients: RefCell<LruCache<String, Rc<ClientInfo>>>,

    /// Worker（Endpoint検索用）
    worker: Rc<Worker>,

    /// キャッシュ容量
    capacity: usize,
}

impl ClientRegistry {
    /// 新しいClientRegistryを作成
    pub fn new(worker: Rc<Worker>, capacity: usize) -> Self;

    /// クライアントを登録
    pub fn register(&self, client_id: String, endpoint: Rc<Endpoint>, addr: SocketAddr);

    /// クライアント情報を取得（LRU更新）
    pub fn get(&self, client_id: &str) -> Option<Rc<ClientInfo>>;

    /// クライアントを削除（明示的クローズ）
    pub fn remove(&self, client_id: &str) -> Option<Rc<ClientInfo>>;

    /// LRUエビクション実行（内部使用）
    fn evict_lru(&self);
}
```

### 3.4 新しいRPC定義

#### RegisterClientRequest

```rust
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, AsBytes, FromBytes, FromZeroes)]
pub struct RegisterClientRequestHeader {
    pub client_id_len: u32,  // クライアントID文字列の長さ
    pub _padding: u32,
}

pub struct RegisterClientRequest {
    header: RegisterClientRequestHeader,
    client_id: String,
    client_id_bytes: Vec<u8>,
    // IoSlice管理...
}
```

#### RegisterClientResponse

```rust
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, AsBytes, FromBytes, FromZeroes)]
pub struct RegisterClientResponseHeader {
    pub status: u32,  // 0: Success, 1: Error
    pub error_code: u32,
}
```

#### ConnectionCloseRequest

```rust
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, AsBytes, FromBytes, FromZeroes)]
pub struct ConnectionCloseRequestHeader {
    pub reason: u32,  // 0: Eviction, 1: Shutdown, 2: Error
    pub _padding: u32,
}
```

---

## 4. 通信フロー設計

### 4.1 サーバー起動フロー

```rust
// benchfsd_mpi.rs

async fn main() {
    // 1. UCX Worker作成
    let worker = ucx_context.create_worker()?;

    // 2. Listenerをバインド (ポート0で動的割り当て)
    let listener = worker.create_listener("0.0.0.0:0".parse()?)?;
    let socket_addr = listener.socket_addr()?;

    tracing::info!("Server listening on {}", socket_addr);

    // 3. サーバーリストファイルに書き込み
    let server_list_path = registry_dir.join("server_list.txt");
    append_to_server_list(&server_list_path, &node_id, socket_addr)?;

    // 4. MPI Barrier（全サーバーが書き込み完了を待つ）
    world.barrier();

    // 5. ClientRegistry作成
    let client_registry = Rc::new(ClientRegistry::new(worker.clone(), 1000));

    // 6. RpcServer作成（ClientRegistryを渡す）
    let rpc_server = Rc::new(RpcServer::new(
        worker.clone(),
        handler_context.clone(),
        Some(client_registry.clone()),
    ));

    // 7. Acceptループ開始（非同期タスク）
    spawn_accept_loop(listener, worker, client_registry, rpc_server);

    // 8. メインイベントループ
    runtime.run(...);
}
```

### 4.2 クライアント初期化フロー

```rust
// ffi/init.rs

pub extern "C" fn benchfs_init(
    node_id: *const c_char,
    registry_dir: *const c_char,
    ...
) -> *mut BenchFS {
    // 1. サーバーリスト読み込み
    let server_list = read_server_list(registry_dir)?;
    // server_list: Vec<(String, SocketAddr)>

    // 2. ターゲットサーバー選択（ハッシュベース）
    let target_server = select_server_by_hash(&node_id, &server_list);

    // 3. ソケット接続
    let endpoint = worker.connect_socket(target_server.1).await?;

    // 4. クライアントID登録RPC送信
    let register_req = RegisterClientRequest::new(node_id.clone());
    let response = register_req.call(&client).await?;

    if response.status != 0 {
        return Err("Failed to register client");
    }

    // 5. ConnectionPoolに登録
    connection_pool.register_endpoint(target_server.0, endpoint);

    // 6. BenchFS作成
    ...
}
```

### 4.3 Accept + クライアント登録フロー

```rust
// rpc/server.rs

async fn accept_loop(
    listener: Listener,
    worker: Rc<Worker>,
    client_registry: Rc<ClientRegistry>,
    rpc_server: Rc<RpcServer>,
) {
    loop {
        // 1. 接続リクエスト待機
        let conn_request = listener.next().await;

        // 2. Endpoint作成
        let endpoint = match worker.accept(conn_request).await {
            Ok(ep) => Rc::new(ep),
            Err(e) => {
                tracing::error!("Failed to accept connection: {:?}", e);
                continue;
            }
        };

        // 3. RegisterClientRequestを待機（専用ストリーム ID=50）
        let am_stream = worker.am_stream(50);
        let am_msg = match timeout(Duration::from_secs(10), am_stream.wait_msg()).await {
            Ok(Some(msg)) => msg,
            _ => {
                tracing::error!("Timeout waiting for RegisterClientRequest");
                continue;
            }
        };

        // 4. RegisterClientRequestをパース
        let request = RegisterClientRequest::parse_from_am_msg(am_msg)?;

        // 5. ClientRegistryに登録
        client_registry.register(
            request.client_id().to_string(),
            endpoint.clone(),
            endpoint.peer_addr()?, // UCX APIで取得可能であれば
        );

        // 6. 応答を返す
        let response = RegisterClientResponseHeader { status: 0, error_code: 0 };
        send_rpc_response_via_endpoint(&endpoint, 151, &response, &[]).await?;

        tracing::info!("Client registered: {}", request.client_id());
    }
}
```

### 4.4 RPC応答フロー（新方式）

```rust
// rpc/data_ops.rs - ReadChunkRequest::server_handler()

pub async fn server_handler(
    am_msg: AmMsg,
    context: Rc<RpcHandlerContext>,
    client_registry: Option<Rc<ClientRegistry>>,
) -> Result<(ServerResponse<Self::ResponseHeader>, AmMsg), (RpcError, AmMsg)> {
    // 1. リクエスト処理（既存と同じ）
    let header = Self::parse_header(&am_msg)?;
    let chunk_data = context.chunk_store().read_chunk(...).await?;

    // 2. レスポンスヘッダー作成
    let response_header = ReadChunkResponseHeader {
        status: 0,
        chunk_size: chunk_data.len() as u64,
        ...
    };

    // 3. クライアント情報取得
    let client_id = extract_client_id_from_am_msg(&am_msg)?; // 新機能

    let client_registry = client_registry.ok_or_else(|| {
        RpcError::HandlerError("Client registry not available".to_string())
    })?;

    let client_info = client_registry.get(&client_id).ok_or_else(|| {
        RpcError::HandlerError(format!("Unknown client: {}", client_id))
    })?;

    // 4. Endpoint経由で応答送信（reply_epの代わり）
    send_rpc_response_via_endpoint(
        &client_info.endpoint,
        Self::reply_stream_id(),
        &response_header,
        &[IoSlice::new(&chunk_data)],
    ).await.map_err(|e| (e, am_msg))?;

    // 5. 成功を返す（AmMsgはクローズ不要）
    Ok((ServerResponse::Completed, am_msg))
}
```

---

## 5. 実装フェーズ

### Phase 1: 基盤実装（3日）

#### タスク 1.1: サーバーリストファイル管理
- [ ] `src/rpc/server_list.rs` 作成
  - `append_to_server_list(path, node_id, addr)`
  - `read_server_list(path) -> Vec<(String, SocketAddr)>`
  - ファイルロック機構（複数プロセスからの書き込み）
  - エラーハンドリング

#### タスク 1.2: benchfsd_mpi.rsの変更
- [ ] Listenerの作成
- [ ] SocketAddrの取得
- [ ] サーバーリストへの書き込み
- [ ] MPI Barrierの追加

#### タスク 1.3: init.rsの変更（読み込みのみ）
- [ ] `read_server_list()` 呼び出し
- [ ] ターゲットサーバー選択ロジック
- [ ] 既存のWorkerAddress方式との並行稼働

**成果物**: サーバーリストファイルが正しく生成・読み込みされることを確認

### Phase 2: クライアント管理機構（5日）

#### タスク 2.1: データ構造実装
- [ ] `src/rpc/client_registry.rs` 作成
  - `ClientInfo` 構造体
  - `ClientRegistry` 構造体
  - LRUキャッシュ統合
  - エビクションロジック

#### タスク 2.2: クライアント登録RPC
- [ ] `src/rpc/client_id_protocol.rs` 作成
  - `RegisterClientRequest/Response`
  - `ConnectionCloseRequest`
  - AmRpc trait実装

#### タスク 2.3: Acceptループ実装
- [ ] `src/rpc/server.rs` に `accept_loop()` 追加
- [ ] RegisterClientRequest待機・処理
- [ ] ClientRegistryへの登録

#### タスク 2.4: クライアント側実装
- [ ] `init.rs`: `connect_socket()` 使用
- [ ] RegisterClientRequest送信
- [ ] ConnectionCloseハンドリング

**成果物**: クライアントが接続し、ClientRegistryに登録されることを確認

### Phase 3: RPC送受信の変更（7日）

#### タスク 3.1: ヘルパー関数実装
- [ ] `src/rpc/helpers.rs`
  - `send_rpc_response_via_endpoint()` 追加
  - エラーハンドリング
  - タイムアウト処理

#### タスク 3.2: AmMsgからClientID抽出
- [ ] UCX APIでリモートアドレス取得方法を調査
- [ ] ClientIDマッピング機構実装
- [ ] または、リクエストヘッダーにClientID埋め込み

#### タスク 3.3: DataRPCs移行
- [ ] `ReadChunkRequest::server_handler()` 変更
- [ ] `WriteChunkRequest::server_handler()` 変更
- [ ] 既存のreply_ep方式をフォールバックとして保持

#### タスク 3.4: MetadataRPCs移行
- [ ] 全MetadataRPCsのserver_handler変更
- [ ] 同様にフォールバック保持

#### タスク 3.5: BenchmarkRPCs移行
- [ ] BenchPingRequest変更
- [ ] BenchShutdownRequest変更

**成果物**: 新方式でRPCが正常に動作することを確認

### Phase 4: テスト・最適化（5日）

#### タスク 4.1: 単体テスト
- [ ] ClientRegistry テスト
- [ ] ServerList テスト
- [ ] RegisterClientRequest テスト

#### タスク 4.2: 統合テスト
- [ ] make test-ior 実行
- [ ] RPCベンチマーク実行
- [ ] 既存テストケース全て通過確認

#### タスク 4.3: パフォーマンステスト
- [ ] reply_ep vs 新方式のレイテンシ比較
- [ ] スループット測定
- [ ] キャッシュヒット率測定

#### タスク 4.4: クリーンアップ
- [ ] reply_ep関連コードの削除（フォールバック含む）
- [ ] 未使用コードの削除
- [ ] コメント・ドキュメント更新

**成果物**: 全テスト通過、パフォーマンス改善確認

---

## 6. ファイル変更一覧

### 6.1 新規作成ファイル

| ファイル | 行数（推定） | 説明 |
|---------|-------------|------|
| `src/rpc/server_list.rs` | 150 | サーバーリストファイル管理 |
| `src/rpc/client_registry.rs` | 300 | クライアント情報管理・LRUキャッシュ |
| `src/rpc/client_id_protocol.rs` | 400 | クライアント登録RPC定義 |
| `claude_log/rpc_refactoring_plan.md` | 1000+ | 本ドキュメント |

### 6.2 変更ファイル

| ファイル | 変更規模 | 主な変更内容 |
|---------|---------|-------------|
| `src/bin/benchfsd_mpi.rs` | 中 | Listener作成、サーバーリスト書き込み |
| `src/ffi/init.rs` | 中 | サーバーリスト読み込み、socket接続 |
| `src/rpc/server.rs` | 大 | accept_loop追加、ClientRegistry統合 |
| `src/rpc/client.rs` | 小 | ClientID送信ロジック追加 |
| `src/rpc/helpers.rs` | 中 | send_via_endpoint追加 |
| `src/rpc/data_ops.rs` | 大 | 全server_handlerの変更 |
| `src/rpc/metadata_ops.rs` | 大 | 全server_handlerの変更 |
| `src/rpc/bench_ops.rs` | 中 | server_handlerの変更 |
| `src/rpc/mod.rs` | 小 | 新モジュール追加 |

### 6.3 削除予定ファイル（Phase 4後）

- なし（既存ファイルは変更のみ）

---

## 7. エラーハンドリング戦略

### 7.1 サーバーリストファイル関連

**エラーケース**:
- ファイルが存在しない
- 読み込み権限がない
- フォーマットが不正
- SocketAddrのパースエラー

**対策**:
```rust
pub enum ServerListError {
    FileNotFound(PathBuf),
    PermissionDenied(PathBuf),
    ParseError { line: usize, content: String },
    IoError(std::io::Error),
}

// リトライロジック
pub fn read_server_list_with_retry(
    path: &Path,
    max_retries: usize,
    delay: Duration,
) -> Result<Vec<(String, SocketAddr)>, ServerListError> {
    for attempt in 1..=max_retries {
        match read_server_list(path) {
            Ok(list) => return Ok(list),
            Err(ServerListError::FileNotFound(_)) if attempt < max_retries => {
                std::thread::sleep(delay);
                continue;
            }
            Err(e) => return Err(e),
        }
    }
    Err(ServerListError::FileNotFound(path.to_path_buf()))
}
```

### 7.2 クライアント登録関連

**エラーケース**:
- 接続タイムアウト
- クライアントID重複
- RegisterClientRequest送信失敗

**対策**:
```rust
// タイムアウト付き登録
async fn register_client_with_timeout(
    endpoint: &Endpoint,
    client_id: String,
    timeout: Duration,
) -> Result<(), ClientRegistrationError> {
    tokio::time::timeout(timeout, async {
        let request = RegisterClientRequest::new(client_id);
        request.call(&RpcClient::new(endpoint.clone())).await
    }).await??;
    Ok(())
}

// 重複時の処理
impl ClientRegistry {
    pub fn register(&self, client_id: String, endpoint: Rc<Endpoint>, addr: SocketAddr) {
        let mut cache = self.clients.borrow_mut();

        // 既存エントリがあれば削除
        if let Some(old_info) = cache.pop(&client_id) {
            tracing::warn!("Client {} reconnected, closing old connection", client_id);
            let _ = old_info.endpoint.close();
        }

        cache.put(client_id.clone(), Rc::new(ClientInfo { ... }));
    }
}
```

### 7.3 RPC応答送信エラー

**エラーケース**:
- Endpointがクローズ済み
- ClientIDが見つからない
- 送信タイムアウト

**対策**:
```rust
// フォールバック付き応答送信
async fn send_response_with_fallback(
    client_registry: &ClientRegistry,
    client_id: &str,
    am_msg: &AmMsg,
    response_header: &[u8],
    data: &[IoSlice<'_>],
) -> Result<(), RpcError> {
    // 1. 新方式を試行
    if let Some(client_info) = client_registry.get(client_id) {
        if !client_info.endpoint.is_closed() {
            match send_via_endpoint(&client_info.endpoint, ...).await {
                Ok(()) => return Ok(()),
                Err(e) => tracing::warn!("Failed to send via endpoint: {:?}, falling back", e),
            }
        }
    }

    // 2. フォールバック: reply_ep使用
    tracing::info!("Using reply_ep fallback for client {}", client_id);
    am_msg.reply_vectorized(...).await
}
```

### 7.4 LRUエビクション時のエラー

**エラーケース**:
- ConnectionCloseRequest送信失敗
- Endpointクローズ失敗

**対策**:
```rust
impl ClientRegistry {
    fn evict_lru(&self) {
        let mut cache = self.clients.borrow_mut();

        if let Some((client_id, client_info)) = cache.pop_lru() {
            tracing::info!("Evicting client: {}", client_id);

            // ベストエフォートでクローズ通知
            let endpoint = client_info.endpoint.clone();
            spawn(async move {
                let close_req = ConnectionCloseRequest::new(CloseReason::Eviction);
                if let Err(e) = close_req.call_no_response(&endpoint).await {
                    tracing::debug!("Failed to send close notification: {:?}", e);
                }
                let _ = endpoint.close();
            });
        }
    }
}
```

---

## 8. テスト戦略

### 8.1 単体テスト

#### ServerList テスト
```rust
#[cfg(test)]
mod tests {
    #[test]
    fn test_append_and_read_server_list() {
        let temp_dir = tempfile::tempdir().unwrap();
        let list_path = temp_dir.path().join("server_list.txt");

        // 書き込みテスト
        append_to_server_list(&list_path, "node_0", "127.0.0.1:1234".parse().unwrap()).unwrap();
        append_to_server_list(&list_path, "node_1", "127.0.0.1:1235".parse().unwrap()).unwrap();

        // 読み込みテスト
        let servers = read_server_list(&list_path).unwrap();
        assert_eq!(servers.len(), 2);
        assert_eq!(servers[0].0, "node_0");
        assert_eq!(servers[0].1.port(), 1234);
    }

    #[test]
    fn test_invalid_format() {
        let temp_dir = tempfile::tempdir().unwrap();
        let list_path = temp_dir.path().join("invalid.txt");
        std::fs::write(&list_path, "invalid format").unwrap();

        let result = read_server_list(&list_path);
        assert!(matches!(result, Err(ServerListError::ParseError { .. })));
    }
}
```

#### ClientRegistry テスト
```rust
#[cfg(test)]
mod tests {
    #[test]
    fn test_client_registration() {
        let runtime = Runtime::new(10);
        runtime.run(async {
            let worker = create_test_worker();
            let registry = ClientRegistry::new(worker.clone(), 2);

            let ep1 = create_test_endpoint(&worker);
            let ep2 = create_test_endpoint(&worker);

            // 登録テスト
            registry.register("client_1".to_string(), ep1, "127.0.0.1:1234".parse().unwrap());
            assert!(registry.get("client_1").is_some());

            // 容量超過でエビクション
            registry.register("client_2".to_string(), ep2, "127.0.0.1:1235".parse().unwrap());
            let ep3 = create_test_endpoint(&worker);
            registry.register("client_3".to_string(), ep3, "127.0.0.1:1236".parse().unwrap());

            // client_1がエビクトされているはず
            assert!(registry.get("client_1").is_none());
            assert!(registry.get("client_2").is_some());
            assert!(registry.get("client_3").is_some());
        });
    }
}
```

### 8.2 統合テスト

#### IORテスト
```bash
# Phase 3完了後
cd tests/docker
make test-ior

# 期待結果:
# - 全てのRPCが新方式で処理される
# - エラーなく完了
# - パフォーマンス改善確認
```

#### RPCベンチマーク
```bash
# Phase 3完了後
cd tests/docker
# カスタムRPCベンチマーク実行
docker exec benchfs_controller /workspace/run_rpc_benchmark.sh

# メトリクス収集:
# - Average latency
# - P50, P99 latency
# - Throughput (ops/sec)
# - Endpoint cache hit rate
```

### 8.3 パフォーマンステスト

#### レイテンシ測定
```rust
// tests/rpc_latency_test.rs
#[test]
fn compare_reply_ep_vs_persistent_endpoint() {
    let iterations = 10000;

    // reply_ep方式
    let start = Instant::now();
    for _ in 0..iterations {
        // ReadChunkRequestを送信（旧方式）
    }
    let old_latency = start.elapsed() / iterations;

    // 新方式
    let start = Instant::now();
    for _ in 0..iterations {
        // ReadChunkRequestを送信（新方式）
    }
    let new_latency = start.elapsed() / iterations;

    println!("Old: {:?}, New: {:?}, Improvement: {:.2}%",
        old_latency, new_latency,
        (old_latency.as_nanos() - new_latency.as_nanos()) as f64 / old_latency.as_nanos() as f64 * 100.0
    );

    assert!(new_latency < old_latency, "New method should be faster");
}
```

#### キャッシュヒット率測定
```rust
impl ClientRegistry {
    pub fn cache_stats(&self) -> CacheStats {
        CacheStats {
            hits: self.cache_hits.get(),
            misses: self.cache_misses.get(),
            evictions: self.evictions.get(),
            hit_rate: self.cache_hits.get() as f64 /
                     (self.cache_hits.get() + self.cache_misses.get()) as f64,
        }
    }
}
```

---

## 9. リスクと対策

### 9.1 技術的リスク

| リスク | 影響度 | 発生確率 | 対策 |
|-------|--------|---------|------|
| UCX APIでクライアント識別が困難 | 高 | 中 | リクエストヘッダーにClientID埋め込み方式にフォールバック |
| Endpoint再利用時の状態不整合 | 高 | 低 | `is_closed()`チェック + 自動再接続ロジック |
| LRUキャッシュのサイズ調整が困難 | 中 | 中 | 動的調整機構の実装 or 設定ファイルで調整可能に |
| reply_epとの並行稼働で複雑化 | 中 | 高 | フォールバック実装を簡潔に保つ、テストを充実 |
| パフォーマンス改善が期待以下 | 中 | 低 | ベンチマーク結果次第で方式再検討 |

### 9.2 スケジュールリスク

| リスク | 影響度 | 発生確率 | 対策 |
|-------|--------|---------|------|
| Phase 2が長引く | 中 | 中 | Phase 1で基盤を固め、Phase 2を分割実装 |
| テストで問題発見 | 高 | 高 | 各Phaseで統合テスト実施、問題の早期発見 |
| 既存機能のデグレード | 高 | 中 | フォールバック機構で既存機能を保護 |

### 9.3 運用リスク

| リスク | 影響度 | 発生確率 | 対策 |
|-------|--------|---------|------|
| サーバーリストファイルの破損 | 中 | 低 | バックアップ機構、WorkerAddress方式へのフォールバック |
| ClientRegistry容量不足 | 中 | 低 | 警告ログ、動的拡張、設定での調整 |
| Endpoint leakによるメモリ不足 | 高 | 低 | 定期的なリソース監視、テストでのリーク検出 |

---

## 10. 実装チェックリスト

### Phase 1: 基盤実装
- [ ] `server_list.rs` 実装完了
- [ ] `benchfsd_mpi.rs` Listener追加
- [ ] `init.rs` サーバーリスト読み込み
- [ ] 単体テスト通過
- [ ] サーバーリストファイル生成確認

### Phase 2: クライアント管理
- [ ] `client_registry.rs` 実装完了
- [ ] `client_id_protocol.rs` 実装完了
- [ ] `server.rs` acceptループ追加
- [ ] `init.rs` socket接続実装
- [ ] 単体テスト通過
- [ ] クライアント登録成功確認

### Phase 3: RPC移行
- [ ] `helpers.rs` 新ヘルパー追加
- [ ] `data_ops.rs` 全変更完了
- [ ] `metadata_ops.rs` 全変更完了
- [ ] `bench_ops.rs` 変更完了
- [ ] 既存テスト全通過
- [ ] make test-ior 成功

### Phase 4: 最適化
- [ ] パフォーマンステスト実施
- [ ] レイテンシ改善確認
- [ ] キャッシュヒット率測定
- [ ] ドキュメント更新
- [ ] コードクリーンアップ

---

## 11. 参考情報

### 11.1 関連ドキュメント
- UCX Documentation: https://openucx.readthedocs.io/
- pluvio_ucx API: `lib/pluvio/pluvio_ucx/src/`
- LRU Cache: https://docs.rs/lru/latest/lru/

### 11.2 既存実装参考箇所
- Endpointキャッシュ: `src/rpc/connection.rs:73-135`
- LRU実装例: `src/cache/chunk_cache.rs`
- MPI同期: `src/bin/benchfsd_mpi.rs:99-107`

### 11.3 デバッグTips
```bash
# UCXログ有効化
export UCX_LOG_LEVEL=DEBUG

# BenchFSログ有効化
export RUST_LOG=benchfs=debug,pluvio_ucx=debug

# Endpointリーク検出
export RUST_BACKTRACE=1
```

---

## 改訂履歴

| 版 | 日付 | 変更内容 | 作成者 |
|----|------|---------|--------|
| 1.0 | 2025-11-08 | 初版作成 | Claude |

---

**計画策定完了 - 実装開始準備完了**
