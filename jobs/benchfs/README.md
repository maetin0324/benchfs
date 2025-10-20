# BenchFS MPI Benchmark Job Scripts

スーパーコンピュータ上でBENCHFSのMPIベンチマークを実行するためのジョブスクリプト群です。

## ファイル構成

- **benchfs.sh** - ジョブ投入スクリプト（qsubラッパー）
- **benchfs-job.sh** - PBSジョブスクリプト本体
- **common.sh** - 共通関数ライブラリ
- **README.md** - このファイル

## 前提条件

### 必須

1. **BENCHFSのビルド**
   ```bash
   cd /home/rmaeda/workspace/rust/benchfs
   cargo build --release --features mpi-support --bin benchfsd_mpi
   ```

2. **IORのビルド**
   ```bash
   cd /home/rmaeda/workspace/rust/benchfs/ior_integration/ior
   ./bootstrap
   ./configure
   make
   ```

3. **MPIの利用可能性**
   - OpenMPI 4.1.8以降
   - PBS/NQSVジョブスケジューラ

4. **共有ファイルシステム**
   - バックエンドディレクトリ用（`/local/`など高速なローカルストレージ推奨）
   - 結果出力用

### 推奨

- UCXライブラリ（RDMA通信用）
- InfiniBandまたはRoCE対応NIC

## 使用方法

### 基本的な実行

```bash
cd /home/rmaeda/workspace/rust/benchfs/jobs/benchfs
./benchfs.sh
```

### カスタムパラメータでの実行

環境変数で各種パラメータをカスタマイズできます：

```bash
# 実行時間の設定（デフォルト: 12:00:00）
ELAPSTIM_REQ="6:00:00" ./benchfs.sh

# ラベルの設定（結果ディレクトリ名に付与）
LABEL="test_run" ./benchfs.sh

# 両方を指定
ELAPSTIM_REQ="6:00:00" LABEL="quick_test" ./benchfs.sh
```

## ベンチマークパラメータ

### ノード数

デフォルトでは以下のノード数でベンチマークを実行します：

```bash
nnodes_list=(1 2 4 8)
```

変更する場合は `benchfs.sh` の該当行を編集してください。

### IORパラメータ

`benchfs-job.sh` で以下のパラメータを調整できます：

#### 転送サイズ (transfer_size_list)

```bash
transfer_size_list=(
  1m    # 1 MiB
  4m    # 4 MiB
  16m   # 16 MiB
)
```

#### ブロックサイズ (block_size_list)

```bash
block_size_list=(
  4m    # 4 MiB
  16m   # 16 MiB
  64m   # 64 MiB
)
```

#### プロセス数/ノード (ppn_list)

```bash
ppn_list=(
  1
  2
  4
)
```

#### IORフラグ (ior_flags_list)

```bash
ior_flags_list=(
  "-w -r -F"  # write, read, file-per-process
  "-w -r"     # write, read, shared file
)
```

#### BenchFSチャンクサイズ (benchfs_chunk_size_list)

```bash
benchfs_chunk_size_list=(
  4194304  # 4 MiB (default)
)
```

## 結果の確認

### 出力ディレクトリ構造

```
/home/rmaeda/workspace/rust/benchfs/results/benchfs/YYYY.MM.DD-HH.MM.SS-LABEL/
├── YYYY.MM.DD-HH.MM.SS-JOBID-NNODES/
│   ├── benchfs-job.sh              # 実行されたジョブスクリプト
│   ├── PBS_NODEFILE                # 使用されたノードリスト
│   ├── common.sh                   # 共通関数スクリプト
│   ├── env.txt                     # 環境変数
│   ├── job_params_0.json           # 各runのジョブパラメータ
│   ├── time_0.json                 # 各runの実行時間統計
│   ├── benchfs_0.toml              # 各runのBenchFS設定
│   ├── benchfsd_logs/              # BenchFSサーバーログ
│   │   └── run_0/
│   │       ├── benchfsd_stdout.log
│   │       └── benchfsd_stderr.log
│   └── ior_results/                # IORベンチマーク結果
│       ├── ior_result_0.txt
│       └── ior_stderr_0.txt
```

### 結果の解析

#### IORベンチマーク結果

```bash
# 特定のrunの結果を確認
cat results/benchfs/TIMESTAMP-LABEL/TIMESTAMP-JOBID-NNODES/ior_results/ior_result_0.txt

# スループット情報を抽出
grep "Max Write\|Max Read" results/benchfs/*/*/ior_results/ior_result_*.txt
```

#### ジョブパラメータの確認

```bash
# 各runのパラメータを確認
cat results/benchfs/TIMESTAMP-LABEL/TIMESTAMP-JOBID-NNODES/job_params_0.json
```

出力例：
```json
{
  "nnodes": 4,
  "ppn": 2,
  "np": 8,
  "jobid": "12345",
  "runid": 0,
  "transfer_size": "1m",
  "block_size": "4m",
  "ior_flags": "-w -r -F",
  "benchfs_chunk_size": 4194304
}
```

#### 実行時間統計の確認

```bash
cat results/benchfs/TIMESTAMP-LABEL/TIMESTAMP-JOBID-NNODES/time_0.json
```

## トラブルシューティング

### サーバーが起動しない

**症状**: "ERROR: BenchFS servers failed to start"

**解決方法**:
1. BenchFSバイナリが正しくビルドされているか確認
   ```bash
   ls -la /home/rmaeda/workspace/rust/benchfs/target/release/benchfsd_mpi
   ```

2. サーバーログを確認
   ```bash
   cat results/benchfs/*/*/benchfsd_logs/run_0/benchfsd_stderr.log
   ```

3. レジストリディレクトリがアクセス可能か確認
   ```bash
   ls -la /local/rmaeda/
   ```

### IORベンチマークが失敗する

**症状**: IORが実行されるが結果が0になる

**解決方法**:
1. IORのエラーログを確認
   ```bash
   cat results/benchfs/*/*/ior_results/ior_stderr_0.txt
   ```

2. BenchFSサーバーが正常に動作しているか確認
   ```bash
   grep "ready\|registered" results/benchfs/*/*/benchfsd_logs/run_0/benchfsd_stdout.log
   ```

3. レジストリファイルが作成されているか確認（ジョブ実行中）
   ```bash
   # 実行中のジョブのバックエンドディレクトリで確認
   ls -la /local/rmaeda/benchfs/TIMESTAMP-JOBID-NNODES/registry/
   ```

### ジョブが投入できない

**症状**: qsubコマンドがエラーを返す

**解決方法**:
1. PBS環境が利用可能か確認
   ```bash
   qstat
   ```

2. スクリプトに実行権限があるか確認
   ```bash
   ls -la benchfs.sh benchfs-job.sh
   chmod +x benchfs.sh benchfs-job.sh
   ```

3. プロジェクトコード（-A NBB）が正しいか確認

## カスタマイズ

### ネットワーク設定の変更

`benchfs-job.sh` のMPI設定を編集：

```bash
cmd_mpirun_common=(
  mpirun
  "${nqsii_mpiopts_array[@]}"
  --mca btl "self,tcp"
  --mca btl_tcp_if_include eno1  # ネットワークインターフェース名を変更
  -x "UCX_TLS=self,tcp"          # UCXトランスポートを変更 (例: rc,ud,sm,self)
)
```

### バックエンドディレクトリの変更

`benchfs.sh` のBACKEND_DIRを編集：

```bash
BACKEND_DIR="/path/to/fast/local/storage/benchfs"
```

推奨：
- `/local/` - ノードローカルの高速SSD
- `/tmp/` - 一時ストレージ
- NFS/Lustreは避ける（遅い）

### ログレベルの変更

`benchfs-job.sh` のRUST_LOG環境変数を変更：

```bash
-x "RUST_LOG=debug"  # より詳細なログ
-x "RUST_LOG=warn"   # 警告のみ
```

## パフォーマンスチューニング

### UCXトランスポート最適化

RDMA対応環境の場合：

```bash
-x "UCX_TLS=rc,ud,sm,self"  # RDMAトランスポートを有効化
```

### BenchFSチャンクサイズの最適化

ワークロードに応じてチャンクサイズを調整：

```bash
benchfs_chunk_size_list=(
  4194304   # 4 MiB - 大規模シーケンシャルI/O向け（デフォルト）
  1048576   # 1 MiB - 中規模I/O向け
  65536     # 64 KiB - 小ファイル向け（CHFS互換）
)
```

### プロセスバインディングの調整

`benchfs-job.sh` のMPI設定を変更：

```bash
--bind-to core    # コアにバインド
--bind-to socket  # ソケットにバインド
--bind-to none    # バインドしない（デフォルト）
```

## 参考資料

- [BenchFS MPI使用ガイド](../../MPI_USAGE.md)
- [BenchFS設定ガイド](../../benchfs.toml)
- [IOR統合ドキュメント](../../ior_integration/README.md)
- [参考実装](../../../fjsrpc-eval-0.2/jobs/rmibench/)

## ライセンス

BenchFSプロジェクトに準拠

## 問い合わせ

問題が発生した場合は、ログファイルとジョブパラメータを添えて報告してください。
