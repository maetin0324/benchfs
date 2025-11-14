# BenchFS Mini 実装計画書

## 目的

既存のBenchFSで発生しているStream RPC接続問題をデバッグするため、極めて簡素化した最小実装を作成する。

## 設計方針

1. **単一ファイル実装**: 全てのコードを `src/bin/benchfsd_mini.rs` に収める
2. **依存関係の最小化**: pluvio_runtime, pluvio_ucx, futuresのみを使用
3. **既存コード非依存**: benchfsのlibコードを参照せず、独立した実装
4. **デバッグ重視**: エンドポイント情報を積極的に出力

## アーキテクチャ

### コンポーネント構成

```
benchfsd_mini.rs
├── FFI Layer (C API)
│   ├── benchfs_mini_init()
│   ├── benchfs_mini_start_server()
│   ├── benchfs_mini_connect()
│   ├── benchfs_mini_write()
│   ├── benchfs_mini_read()
│   ├── benchfs_mini_finalize()
│   └── benchfs_mini_progress()
│
├── RPC Protocol
│   ├── RPC_WRITE (ID: 1)
│   └── RPC_READ (ID: 2)
│
├── Server Implementation
│   ├── handle_client() - クライアント接続処理
│   ├── handle_write() - 書き込み要求処理
│   └── handle_read() - 読み込み要求処理
│
├── Client Implementation
│   ├── rpc_write() - 書き込みRPC実行
│   └── rpc_read() - 読み込みRPC実行
│
├── Storage Layer
│   └── SimpleStorage - インメモリHashMap
│
└── Service Discovery
    ├── register_server() - サーバー情報登録
    └── lookup_server() - サーバー情報検索
```

### データフロー

#### サーバー起動時
```
1. MPI初期化 (外部)
2. benchfs_mini_init(registry_dir, is_server=true)
   ↓
3. Runtime作成
   ↓
4. UCX Context/Worker作成
   ↓
5. Listener作成 (0.0.0.0:0)
   ↓
6. hostname:port を registry_dir/server_{rank}.txt に書き込み
   ↓
7. benchfs_mini_start_server()
   ↓
8. run_server() をspawn (バックグラウンド実行)
```

#### クライアント接続時
```
1. benchfs_mini_init(registry_dir, is_server=false)
   ↓
2. Runtime, UCX初期化
   ↓
3. benchfs_mini_connect(server_rank)
   ↓
4. registry_dir/server_{rank}.txt を読み込み
   ↓
5. worker.connect_socket(hostname:port) で接続
   ↓
6. endpoint.print_to_stderr() でデバッグ情報出力
   ↓
7. endpoint を保存
```

#### Write操作
```
Client側:
1. benchfs_mini_write(path, data, len, server_rank)
   ↓
2. rpc_write(endpoint, path, data)
   ↓
3. Send: RPC_WRITE (2 bytes)
   ↓
4. Send: path_len (4 bytes) + data_len (4 bytes)
   ↓
5. Send: path (path_len bytes)
   ↓
6. Send: data (data_len bytes)
   ↓
7. Recv: status (4 bytes)

Server側:
1. stream_recv() でRPC ID受信
   ↓
2. handle_write()
   ↓
3. Recv: path_len + data_len
   ↓
4. Recv: path
   ↓
5. Recv: data
   ↓
6. SimpleStorage.write(path, data)
   ↓
7. Send: status
```

#### Read操作
```
Client側:
1. benchfs_mini_read(path, buffer, len, server_rank)
   ↓
2. rpc_read(endpoint, path, buffer)
   ↓
3. Send: RPC_READ (2 bytes)
   ↓
4. Send: path_len (4 bytes) + buffer_len (4 bytes)
   ↓
5. Send: path (path_len bytes)
   ↓
6. Recv: status (4 bytes)
   ↓
7. Recv: data_len (4 bytes)
   ↓
8. Recv: data (data_len bytes)

Server側:
1. stream_recv() でRPC ID受信
   ↓
2. handle_read()
   ↓
3. Recv: path_len + buffer_len
   ↓
4. Recv: path
   ↓
5. SimpleStorage.read(path, buffer)
   ↓
6. Send: status
   ↓
7. Send: data_len
   ↓
8. Send: data
```

## IOR統合

### IOR Adapter (aior-BENCHFSMINI.c)

```c
// 主要な関数
- AIOR_Create_BENCHFSMINI()    // 初期化
- AIOR_Open_BENCHFSMINI()      // ファイルオープン
- AIOR_Xfer_BENCHFSMINI()      // 読み書き
- AIOR_Close_BENCHFSMINI()     // クローズ
- AIOR_Finalize_BENCHFSMINI()  // 終了処理
```

### IOR実行フロー

```
1. 全rankでAIOR_Create_BENCHFSMINI()
   ↓
2. Server rank (0-3): benchfs_mini_start_server()
   ↓
3. MPI_Barrier() - サーバー起動待ち
   ↓
4. Client rank: benchfs_mini_connect(0..3) で全サーバーに接続
   ↓
5. MPI_Barrier() - 接続完了待ち
   ↓
6. IORテスト実行
   - AIOR_Open_BENCHFSMINI()
   - AIOR_Xfer_BENCHFSMINI(WRITE)
   - AIOR_Xfer_BENCHFSMINI(READ)
   - AIOR_Close_BENCHFSMINI()
   ↓
7. MPI_Barrier() - テスト完了待ち
   ↓
8. AIOR_Finalize_BENCHFSMINI()
```

## デバッグ機能

1. **エンドポイント情報出力**
   - Client: 接続確立時に `endpoint.print_to_stderr()`
   - Server: 接続受付時に `endpoint.print_to_stderr()`

2. **詳細ログ出力**
   - 各RPC操作の開始/完了をeprintln!で出力
   - データサイズ、パス名を記録

3. **プログレス明示**
   - `benchfs_mini_progress()` でWorker::drive()を呼び出し
   - IOR側から定期的に呼び出し可能

## ビルド設定

### Cargo.toml への追加

```toml
[[bin]]
name = "benchfsd_mini"
path = "src/bin/benchfsd_mini.rs"
required-features = ["mpi-support"]
```

### コンパイル

```bash
cargo build --bin benchfsd_mini --features mpi-support --release
```

## テスト手順

1. **ビルド**
```bash
cd /home/rmaeda/workspace/rust/benchfs
cargo build --bin benchfsd_mini --features mpi-support --release
```

2. **IORコンパイル**
```bash
cd /path/to/ior
./configure --with-benchfsmini=/home/rmaeda/workspace/rust/benchfs
make
```

3. **実行**
```bash
mpirun -np 8 ior -a BENCHFSMINI \
  --benchfsmini.registry /shared/registry_mini \
  -t 1m -b 4m -s 4
```

## 期待される動作

1. Rank 0-3: サーバーとして起動、リッスン開始
2. Rank 4-7: クライアントとして起動、サーバーに接続
3. 接続時にエンドポイント情報がstderrに出力される
4. 各Write/Read操作がログに記録される
5. テスト完了後、正常に終了

## トラブルシューティング

### 接続失敗の場合

1. エンドポイント情報を確認
   - Local address
   - Remote address
   - UCX transport情報

2. レジストリファイルを確認
```bash
cat /shared/registry_mini/server_*.txt
```

3. ホスト名解決を確認
```bash
ping <hostname>
```

### タイムアウトの場合

1. サーバーが起動しているか確認
2. MPI_Barrier()で全rankが同期しているか確認
3. UCX環境変数を確認

## 成功基準

1. ✅ 全rankが正常に初期化完了
2. ✅ クライアントが全サーバーに接続成功
3. ✅ エンドポイント情報が出力される
4. ✅ Write/Read操作が成功
5. ✅ IORテストが完了

## 次のステップ

成功した場合:
- エンドポイント情報を既存のBenchFSと比較
- 動作する設定を既存コードに適用

失敗した場合:
- エンドポイント情報から問題箇所を特定
- UCX設定を調整
- さらに簡素化したテストケースを作成
