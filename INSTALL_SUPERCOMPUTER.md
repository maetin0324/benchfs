# BenchFS Installation Guide for Supercomputers

このガイドでは、スーパーコンピューター環境でBenchFSをインストールし、IORベンチマークと統合する方法を説明します。

## 前提条件

以下のソフトウェアがインストールされている必要があります：

- Rust toolchain (1.70以上)
- MPI実装（OpenMPI 4.0以上、またはMPICH）
- UCX 1.15.0以上
- pkg-config
- autoconf, automake, libtool（IORビルド用）

## インストール手順

### 1. BenchFSのビルドとインストール

```bash
# BenchFSディレクトリに移動
cd /path/to/benchfs

# リリースビルドを実行
cargo build --release --lib

# インストールスクリプトを実行（デフォルト: $HOME/.local）
./install.sh

# または、カスタムインストール先を指定
PREFIX=/your/custom/path ./install.sh
```

インストールスクリプトは以下を実行します：
- `libbenchfs.so`を`$PREFIX/lib`にコピー
- ヘッダーファイルを`$PREFIX/include`にコピー
- `benchfs.pc`（pkg-config設定ファイル）を`$PREFIX/lib/pkgconfig`に生成

### 2. 環境変数の設定

`~/.bashrc`または`.bash_profile`に以下を追加：

```bash
# BenchFSインストール先（install.sh実行時に指定したPREFIX）
export BENCHFS_PREFIX=$HOME/.local

# pkg-configパスの設定
export PKG_CONFIG_PATH="$BENCHFS_PREFIX/lib/pkgconfig:$PKG_CONFIG_PATH"

# ライブラリパスの設定
export LD_LIBRARY_PATH="$BENCHFS_PREFIX/lib:$LD_LIBRARY_PATH"
```

設定を反映：

```bash
source ~/.bashrc
```

### 3. インストールの確認

```bash
# BenchFSがpkg-configから検出できることを確認
pkg-config --modversion benchfs
# 出力: 0.1.0

# コンパイルフラグを確認
pkg-config --cflags benchfs
# 出力: -I/path/to/include

# リンクフラグを確認
pkg-config --libs benchfs
# 出力: -L/path/to/lib -lbenchfs -lpthread -ldl -lm
```

### 4. IORのビルドとインストール

```bash
cd ior_integration/ior

# configureスクリプトを生成
./bootstrap

# BenchFSサポートを有効にしてconfigure
# pkg-configが自動的にBenchFSを検出します
./configure --prefix=$BENCHFS_PREFIX

# ビルドとインストール
make -j$(nproc)
make install
```

### 5. IORの動作確認

```bash
# IORのバージョンとサポートされているバックエンドを確認
$BENCHFS_PREFIX/bin/ior -h | grep -i benchfs

# BenchFSライブラリがリンクされていることを確認
ldd $BENCHFS_PREFIX/bin/ior | grep benchfs
# 出力: libbenchfs.so => /path/to/lib/libbenchfs.so
```

## ジョブスクリプト例

### Slurmの場合

```bash
#!/bin/bash
#SBATCH --job-name=benchfs_ior
#SBATCH --nodes=4
#SBATCH --ntasks-per-node=1
#SBATCH --time=00:30:00

# 環境変数の設定
export BENCHFS_PREFIX=$HOME/.local
export PKG_CONFIG_PATH="$BENCHFS_PREFIX/lib/pkgconfig:$PKG_CONFIG_PATH"
export LD_LIBRARY_PATH="$BENCHFS_PREFIX/lib:$LD_LIBRARY_PATH"

# UCX設定
export UCX_TLS=tcp,sm,self
export UCX_NET_DEVICES=mlx5_0:1

# BenchFS設定ファイル
export BENCHFS_CONFIG=/path/to/benchfs.toml

# 共有ディレクトリ（全ノードからアクセス可能）
SHARED_DIR=/scratch/$USER/benchfs
REGISTRY_DIR=$SHARED_DIR/registry
DATA_DIR=$SHARED_DIR/data

mkdir -p $REGISTRY_DIR $DATA_DIR

# BenchFSサーバーをバックグラウンドで起動
mpirun -np $SLURM_NTASKS \
    benchfsd_mpi $REGISTRY_DIR $BENCHFS_CONFIG &

BENCHFS_PID=$!

# サーバーの起動を待つ
sleep 10

# IORベンチマーク実行
mpirun -np $SLURM_NTASKS \
    $BENCHFS_PREFIX/bin/ior \
    -a BENCHFS \
    -t 1m -b 4m -s 16 \
    -o $DATA_DIR/testfile

# BenchFSサーバーを停止
kill $BENCHFS_PID
wait
```

### PBSの場合

```bash
#!/bin/bash
#PBS -N benchfs_ior
#PBS -l select=4:ncpus=1:mpiprocs=1
#PBS -l walltime=00:30:00

cd $PBS_O_WORKDIR

# 環境変数の設定
export BENCHFS_PREFIX=$HOME/.local
export PKG_CONFIG_PATH="$BENCHFS_PREFIX/lib/pkgconfig:$PKG_CONFIG_PATH"
export LD_LIBRARY_PATH="$BENCHFS_PREFIX/lib:$LD_LIBRARY_PATH"

# UCX設定
export UCX_TLS=tcp,sm,self

# BenchFS設定
export BENCHFS_CONFIG=/path/to/benchfs.toml
SHARED_DIR=/scratch/$USER/benchfs
REGISTRY_DIR=$SHARED_DIR/registry
DATA_DIR=$SHARED_DIR/data

mkdir -p $REGISTRY_DIR $DATA_DIR

# ノード数を取得
NNODES=$(cat $PBS_NODEFILE | uniq | wc -l)

# BenchFSサーバーを起動
mpiexec -np $NNODES \
    benchfsd_mpi $REGISTRY_DIR $BENCHFS_CONFIG &

BENCHFS_PID=$!
sleep 10

# IORベンチマーク実行
mpiexec -np $NNODES \
    $BENCHFS_PREFIX/bin/ior \
    -a BENCHFS \
    -t 1m -b 4m -s 16 \
    -o $DATA_DIR/testfile

# クリーンアップ
kill $BENCHFS_PID
wait
```

## 設定ファイル例

`benchfs.toml`:

```toml
[node]
node_id = "node0"  # MPIランクで自動的に上書きされます
data_dir = "/scratch/$USER/benchfs/data"
log_level = "info"

[storage]
chunk_size = 4194304  # 4 MiB
use_iouring = true    # io_uringを有効化（カーネル5.1以上が必要）
max_storage_gb = 0    # 無制限

[network]
bind_addr = "0.0.0.0:50051"
timeout_secs = 30
rdma_threshold_bytes = 32768  # 32 KB以上のデータはRDMAを使用
registry_dir = "/scratch/$USER/benchfs/registry"

[cache]
metadata_cache_entries = 1000
chunk_cache_mb = 100
cache_ttl_secs = 0  # キャッシュ有効期限なし
```

## トラブルシューティング

### pkg-configがbenchfsを見つけられない

```bash
# PKG_CONFIG_PATHが正しく設定されているか確認
echo $PKG_CONFIG_PATH

# benchfs.pcファイルが存在するか確認
ls -la $BENCHFS_PREFIX/lib/pkgconfig/benchfs.pc
```

### IORがlibbenchfs.soを見つけられない

```bash
# LD_LIBRARY_PATHが正しく設定されているか確認
echo $LD_LIBRARY_PATH

# ライブラリが存在するか確認
ls -la $BENCHFS_PREFIX/lib/libbenchfs.so

# ldconfigでキャッシュを更新（root権限が必要）
sudo ldconfig
```

### IORビルド時にBenchFSバックエンドが見つからない

```bash
# configureログを確認
cat config.log | grep -i benchfs

# pkg-configが動作しているか確認
pkg-config --exists benchfs && echo "OK" || echo "NG"

# 手動でconfigure
./configure --prefix=$BENCHFS_PREFIX \
    CFLAGS="$(pkg-config --cflags benchfs)" \
    LDFLAGS="$(pkg-config --libs benchfs)"
```

### MPI関連のエラー

```bash
# MPIが正しくロードされているか確認
which mpirun
mpirun --version

# UCX設定を確認
ucx_info -d

# UCXトランスポートを確認
export UCX_LOG_LEVEL=info
```

## パフォーマンスチューニング

### io_uringの有効化

io_uringを使用すると、I/Oパフォーマンスが大幅に向上します：

```toml
[storage]
use_iouring = true
```

要件：
- Linux kernel 5.1以上
- io_uringシステムコールが有効

### UCXトランスポートの最適化

InfiniBandがある場合：

```bash
export UCX_TLS=rc_x,ud_x,sm,self
export UCX_NET_DEVICES=mlx5_0:1  # InfiniBandデバイス
```

Ethernet（RoCE）の場合：

```bash
export UCX_TLS=tcp,sm,self
export UCX_NET_DEVICES=eth0  # ネットワークインターフェース
```

### チャンクサイズの調整

大きなシーケンシャルI/Oの場合：

```toml
[storage]
chunk_size = 16777216  # 16 MiB
```

小さなランダムI/Oの場合：

```toml
[storage]
chunk_size = 1048576  # 1 MiB
```

## 参考資料

- [BenchFS README](README.md)
- [IOR User Guide](https://ior.readthedocs.io/)
- [UCX Documentation](https://openucx.readthedocs.io/)
- [pkg-config Guide](https://people.freedesktop.org/~dbn/pkg-config-guide.html)
