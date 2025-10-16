# BenchFS - UCX-based Ad-hoc File System RPC Framework

BenchFS は、UCX (Unified Communication X) の ActiveMessage 機能を用いた、アドホックファイルシステム向けの高性能 RPC 基盤です。

## アーキテクチャ

### RPC システム設計

- **固定長ヘッダー**: ActiveMessage の header フィールドに `RpcHeader` 構造体を格納
  - `offset`: ファイルオフセット (u64)
  - `len`: データ長 (u64)
  - `flags`: 操作固有フラグ (u32)
  - `reserved`: 将来の拡張用 (u32)

- **可変長データ**: ActiveMessage の data フィールドに I/O バッファを格納
  - ゼロコピー転送のため `IoSlice` を使用
  - Eager/Rendezvous プロトコルの自動選択

### コンポーネント

1. **RpcHeader**: 固定長メタデータ構造体
   - `zerocopy` traits により効率的なシリアライゼーション
   - `#[repr(C)]` によるメモリレイアウト保証

2. **RpcCall trait**: クライアント側 RPC 実装用トレイト
   - `rpc_id()`: RPC 操作 ID
   - `header()`: RPC ヘッダー
   - `data()`: ペイロードデータ (IoSlice)
   - `call()`: RPC 実行

3. **RpcServer**: サーバー側 RPC ディスパッチャ
   - ActiveMessage stream でリクエストを受信
   - RPC ID に基づいてハンドラーへディスパッチ
   - 必要に応じてレスポンスを返送

4. **RpcClient**: クライアント側 RPC 呼び出し
   - `am_send_vectorized` による効率的な送信
   - ゼロコピー data 転送

## 依存ライブラリ

- **pluvio**: 高性能シングルスレッド非同期ランタイム
  - `pluvio_runtime`: タスク実行とリアクターシステム
  - `pluvio_ucx`: UCX ワーカーとエンドポイント管理
  - `pluvio_uring`: io_uring ベースの高性能ファイル I/O

- **async-ucx**: UCX の async Rust バインディング
  - ActiveMessage サポート
  - RMA (Remote Memory Access) 操作
  - Tag matching 通信

- **zerocopy**: 効率的なシリアライゼーション
  - FromBytes/IntoBytes traits
  - ゼロコピーバイト変換

## 使用例

### サーバー側

```rust
use benchfs::rpc::{RpcServer, RpcHeader, Connection};
use pluvio_ucx::{Context, UCXReactor};

// UCX ワーカーとエンドポイントを作成
let context = Context::new()?;
let worker = context.create_worker()?;
reactor.register_worker(worker.clone());

// RPC サーバーを作成
let server = RpcServer::new(worker);

// ハンドラーを登録 (実装予定)
// server.register_handler(RPC_READ, handler);

// ActiveMessage を受信してディスパッチ
server.listen(10).await?;
```

### クライアント側

```rust
use benchfs::rpc::{RpcClient, RpcHeader, Connection};
use std::io::IoSlice;

// 接続を確立
let endpoint = worker.connect_addr(&server_addr)?;
let conn = Connection::new(worker, endpoint);
let client = RpcClient::new(conn);

// RPC ヘッダーを構築
let header = RpcHeader::new(
    offset: 0,      // ファイルオフセット
    len: 4096,      // データ長
    flags: 0,       // フラグ
);

// データバッファ (オプション)
let data = vec![0u8; 4096];
let iov = [IoSlice::new(&data)];

// RPC を実行
client.call::<Response>(RPC_READ, header, &iov).await?;
```

## ビルドと実行

```bash
# プロジェクトをビルド
cargo build --release

# サンプルを実行 (サーバー)
cargo run --example rpc_example server

# サンプルを実行 (クライアント)
cargo run --example rpc_example client
```

## 今後の実装予定

- [ ] 同期的なレスポンス受信メカニズム
- [ ] タイムアウト処理
- [ ] エラーハンドリングの強化
- [ ] ハンドラー登録の簡略化
- [ ] ベンチマークとパフォーマンステスト
- [ ] 実際のファイルシステム操作の実装
  - read/write/open/close など
- [ ] マルチクライアント対応
- [ ] 接続プーリング

## パフォーマンス特性

- **低レイテンシ**: UCX ActiveMessage による高速メッセージング
- **ゼロコピー**: IoSlice と固定長ヘッダーによる効率的なデータ転送
- **プロトコル自動選択**: Eager (小データ) / Rendezvous (大データ) の自動切り替え
- **シングルスレッド**: CPU affinity による予測可能な性能

## ライセンス

TBD
