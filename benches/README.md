# BenchFS 2-Node Benchmark

このベンチマークは、実際の2ノード間でUCX通信を行い、分散ファイルシステムの性能を測定します。

## 概要

- **サーバーモード**: データを保存し、RPCリクエストを処理
- **クライアントモード**: サーバーに接続してベンチマークを実行
- **通信方式**: UCX (Unified Communication X) による高速RDMA通信
- **アドレス交換**: 共有ファイルシステムを通じたWorkerAddressの交換

## 使い方

### 1. 共有ディレクトリの準備

2つのノードからアクセス可能な共有ディレクトリを用意します（NFSなど）:

```bash
# 例: NFSマウントポイント
mkdir -p /shared/benchfs_registry
```

### 2. サーバーの起動（ノード1）

```bash
cargo bench --bench small -- \
  --mode server \
  --registry-dir /shared/benchfs_registry \
  --data-dir /tmp/benchfs_server_data
```

出力例:
```
Starting BenchFS server...

  Data directory:     /tmp/benchfs_server_data
  Registry directory: /shared/benchfs_registry

Setting up server node...
Server worker address registered to /shared/benchfs_registry
Server address: WorkerAddress(...)

Registering RPC handlers...
Server is ready and listening for requests.
Press Ctrl+C to stop the server.
```

### 3. クライアントの実行（ノード2）

別のターミナルまたはノードで:

```bash
cargo bench --bench small -- \
  --mode client \
  --registry-dir /shared/benchfs_registry
```

出力例:
```
Starting BenchFS client...

  Registry directory: /shared/benchfs_registry

Setting up client node...
Waiting for server to register (timeout: 30 seconds)...
Successfully connected to server

=== BenchFS 2-Node Benchmark (Distributed Mode) ===

Configuration:
  Iterations: 100
  Warmup:     10
  File sizes: [1024, 4096, 16384] bytes

Running metadata create benchmark...
Running small write benchmark (1024 bytes)...
Running small write benchmark (4096 bytes)...
Running small write benchmark (16384 bytes)...
Running small read benchmark (1024 bytes)...
Running small read benchmark (4096 bytes)...
Running small read benchmark (16384 bytes)...

=== Benchmark Results ===

  Metadata Create (file creation)
    Iterations: 100
    Total:      500ms
    Average:    5ms
    Min:        2ms
    Max:        15ms
    Throughput: 200.00 ops/sec

  Small Write (1024 bytes)
    Iterations: 100
    Total:      800ms
    Average:    8ms
    Min:        4ms
    Max:        20ms
    Throughput: 125.00 ops/sec

  ...
```

## コマンドライン引数

### 必須引数

- `--mode <server|client>`: 実行モード
  - `server`: サーバーとして起動
  - `client`: クライアントとして起動

- `--registry-dir <path>`: WorkerAddressを交換するための共有ディレクトリ
  - 両ノードからアクセス可能である必要があります
  - サーバーが自動的に作成します

### サーバーモード専用

- `--data-dir <path>`: データを保存するディレクトリ
  - チャンクデータが保存されます
  - ローカルディレクトリで構いません

## ベンチマーク項目

### 1. Metadata Create
- ファイル作成操作の性能を測定
- メタデータ操作のオーバーヘッドを評価

### 2. Small Write
- 小さいファイル（1KB, 4KB, 16KB）の書き込み性能を測定
- UCX経由のRPC通信とio_uringの性能を評価

### 3. Small Read
- 小さいファイル（1KB, 4KB, 16KB）の読み込み性能を測定
- キャッシュの効果とネットワーク転送性能を評価

## アーキテクチャ

```
┌─────────────────────┐                    ┌─────────────────────┐
│   Client Node       │                    │   Server Node       │
│                     │                    │                     │
│  ┌──────────────┐   │                    │  ┌──────────────┐   │
│  │   BenchFS    │   │   UCX Connection   │  │  RPC Server  │   │
│  │   Client     │───┼────────────────────┼─>│              │   │
│  └──────────────┘   │                    │  └──────────────┘   │
│         │           │                    │         │           │
│  ┌──────────────┐   │                    │  ┌──────────────┐   │
│  │ Connection   │   │   Shared FS        │  │ Connection   │   │
│  │   Pool       │<──┼────────────────────┼──│   Pool       │   │
│  └──────────────┘   │  (WorkerAddress)   │  └──────────────┘   │
│                     │                    │         │           │
│                     │                    │  ┌──────────────┐   │
│                     │                    │  │ IOUring      │   │
│                     │                    │  │ ChunkStore   │   │
│                     │                    │  └──────────────┘   │
└─────────────────────┘                    └─────────────────────┘
```

## 注意事項

1. **UCXの設定**: UCXが正しくインストールされている必要があります
2. **共有ディレクトリ**: 両ノードから同じパスでアクセス可能である必要があります
3. **起動順序**: サーバーを先に起動してください
4. **タイムアウト**: クライアントは30秒間サーバーの登録を待ちます

## トラブルシューティング

### デバッグログの有効化

問題が発生した場合、まず詳細なログを有効にして実行してください：

```bash
# サーバー側
RUST_LOG=debug cargo bench --bench small -- \
  --mode server \
  --registry-dir /shared/registry \
  --data-dir /tmp/server_data

# クライアント側
RUST_LOG=debug cargo bench --bench small -- \
  --mode client \
  --registry-dir /shared/registry
```

より詳細なトレースログ:
```bash
RUST_LOG=trace cargo bench --bench small -- ...
```

### クライアントがサーバーに接続できない

```
Error: Failed to connect to server: RegistryError(...)
```

**原因と対処法**:
- サーバーが起動していない → サーバーを先に起動
- 共有ディレクトリが異なる → 両ノードで同じパスを指定
- 共有ディレクトリにアクセスできない → マウント状態を確認
- タイムアウト（30秒） → サーバーのログでWorkerAddress登録を確認

**デバッグ手順**:
1. サーバー側で`Server worker address registered to ...`が出力されているか確認
2. 共有ディレクトリに`server.addr`ファイルが作成されているか確認
   ```bash
   ls -la /shared/registry/
   ```

### ベンチマーク実行中にハングする

クライアントが途中で停止する場合：

**原因**:
- サーバー側のRPCハンドラーが動作していない
- UCXReactorがイベントを処理していない
- ネットワーク接続の問題

**対処法**:
1. サーバー側で`RUST_LOG=debug`を有効にして、RPCリクエストを受信しているか確認
2. サーバー側のログで以下を確認:
   ```
   All RPC handlers registered successfully
   Server is ready and listening for requests
   ```
3. クライアント側で以下のログが出ているか確認:
   ```
   Successfully connected to server
   ```

### UCXエラー

```
Failed to create UCX context
```

**対処法**:
- UCXライブラリのインストールを確認
- 環境変数 `UCX_TLS` の設定を確認

### RPCタイムアウト

リクエストがタイムアウトする場合：

```bash
# UCXのログレベルを上げる
UCX_LOG_LEVEL=info RUST_LOG=debug cargo bench --bench small -- ...
```

## 性能チューニング

### ベンチマーク設定の変更

`benches/small.rs` の `BenchConfig::default()` を編集:

```rust
impl Default for BenchConfig {
    fn default() -> Self {
        Self {
            iterations: 1000,  // イテレーション数を増やす
            file_sizes: vec![1024, 4096, 16384, 65536],  // テストサイズを追加
            warmup: 20,  // ウォームアップ回数を増やす
        }
    }
}
```

### io_uringパラメータの調整

`setup_server()` 関数内:

```rust
let uring_reactor = IoUringReactor::builder()
    .queue_size(512)      // キューサイズを拡大
    .buffer_size(4 << 20) // バッファを4MiBに
    .submit_depth(64)     // 投入深度を増やす
    .build();
```
