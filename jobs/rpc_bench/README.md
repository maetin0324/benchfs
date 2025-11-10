# BenchFS RPC Benchmark Job Scripts

スパコン上でRPCベンチマークを実行するためのジョブスクリプト集です。

## 概要

このディレクトリには、IORやio_uringを使わずにRPC通信のみのパフォーマンスを測定するベンチマークジョブスクリプトが含まれています。スパコン上でIORがハングする問題の調査に使用します。

## ファイル構成

- `rpc_bench.sh` - ジョブ投入スクリプト（ローカルから実行）
- `rpc_bench-job.sh` - 実際のジョブスクリプト（qsubで実行される）
- `common.sh` - 共通関数定義
- `README.md` - このファイル

## 使用方法

### 1. 事前準備

```bash
# プロジェクトルートに移動
cd /path/to/benchfs

# リリースビルド（必須）
cargo build --release --bin benchfs_rpc_bench --features mpi-support
```

### 2. ジョブ投入

```bash
# デフォルト設定で実行（4ノード、10000回イテレーション）
cd jobs/rpc_bench
./rpc_bench.sh

# カスタム設定で実行
PING_ITERATIONS=100000 ELAPSTIM_REQ="1:00:00" ./rpc_bench.sh
```

### 3. 設定パラメータ

環境変数で以下のパラメータをカスタマイズできます：

| 変数名 | デフォルト値 | 説明 |
|--------|-------------|------|
| `PING_ITERATIONS` | 10000 | Ping-Pongイテレーション回数 |
| `ELAPSTIM_REQ` | 0:30:00 | ジョブ実行時間制限 |
| `LABEL` | default | 結果ディレクトリのラベル |

### 4. ノード数の変更

`rpc_bench.sh` の `nnodes_list` を編集：

```bash
nnodes_list=(
  4      # 4ノードで実行
  8      # 8ノードで実行
  16     # 16ノードで実行
  # 32   # 32ノードで実行（コメントアウト）
)
```

## 実行結果

結果は以下のディレクトリに保存されます：

```
benchfs/results/rpc_bench/
└── YYYY.MM.DD-HH.MM.SS-{LABEL}/
    └── YYYY.MM.DD-HH.MM.SS-{JOBID}-{NNODES}/
        ├── rpc_bench-job.sh         # 実行されたジョブスクリプト
        ├── PBS_NODEFILE             # 使用ノード一覧
        ├── common.sh                # 共通関数
        ├── env.txt                  # 環境変数
        ├── job_metadata.json        # ジョブメタデータ
        └── results/
            ├── rpc_bench_1.log      # ベンチマーク出力ログ
            └── rpc_bench_1.json     # ベンチマーク結果（JSON）
```

## 出力例

### コンソール出力

```
==========================================
Ping-Pong Benchmark
==========================================
Iterations: 10000
Servers: 3

Testing server: node_1
------------------------------------------
Results for node_1:
  Min latency:  20.098µs
  Avg latency:  20.96µs
  P50 latency:  20.779µs
  P99 latency:  24.771µs
  Max latency:  44.112µs
```

### job_metadata.json

```json
{
  "jobid": "12345",
  "nnodes": 4,
  "ping_iterations": 10000,
  "start_time": "2025.11.02-08.00.00",
  "mpi_version": "4.1.8/gcc11.4.0-cuda12.8.1",
  "binary": "/path/to/benchfs/target/release/benchfs_rpc_bench"
}
```

## トラブルシューティング

### バイナリが見つからない

```bash
# リリースビルドを実行
cd /path/to/benchfs
cargo build --release --bin benchfs_rpc_bench --features mpi-support

# バイナリの存在確認
ls -la target/release/benchfs_rpc_bench
```

### ジョブがqsubされない

- ジョブ投入スクリプトの実行権限を確認：`chmod +x rpc_bench.sh`
- qsubコマンドが利用可能か確認：`which qsub`
- アカウント設定を確認：`rpc_bench.sh` の `-A NBBG` を適切な値に変更

### MPIバージョンの変更

`rpc_bench.sh` の `param_set_list` を編集：

```bash
param_set_list=(
  "
  NQSV_MPI_VER=4.1.8/gcc11.4.0-cuda12.8.1  # 使用するMPIバージョン
  "
)
```

## 関連ファイル

- `../../src/bin/benchfs_rpc_bench.rs` - RPCベンチマーク本体
- `../../src/rpc/bench_ops.rs` - Ping-Pong RPC定義
- `../../scripts/run_rpc_bench_local.sh` - ローカルテスト用スクリプト

## 備考

- このベンチマークはRPC通信のみを測定し、ストレージI/Oは含みません
- IORがハングする問題の原因切り分けに使用します
- MPI rank 0がクライアント、rank 1以降がサーバーとして動作します
- 各サーバーに対して指定回数のPing-Pongを実行し、レイテンシ統計を出力します
