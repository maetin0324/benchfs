# IOR with BenchFS Backend - Quick Start Guide

このガイドでは、BenchFSバックエンドを使用してIORを実行する最も簡単な方法を説明します。

## クイックインストール

### 1. BenchFSのインストール

```bash
cd /path/to/benchfs

# ライブラリをビルドしてインストール
./install.sh

# 環境変数を設定（~/.bashrcに追加推奨）
export PKG_CONFIG_PATH="$HOME/.local/lib/pkgconfig:$PKG_CONFIG_PATH"
export LD_LIBRARY_PATH="$HOME/.local/lib:$LD_LIBRARY_PATH"
```

### 2. IORのビルド

```bash
cd ior_integration/ior

# configureスクリプトを生成
./bootstrap

# pkg-configを使用してBenchFSを自動検出
./configure --prefix=$HOME/.local

# ビルド
make -j$(nproc)

# インストール
make install
```

### 3. 動作確認

```bash
# BenchFSバックエンドが有効か確認
$HOME/.local/bin/ior -h | grep -i benchfs

# ライブラリのリンク確認
ldd $HOME/.local/bin/ior | grep benchfs
```

## 使用方法

### シングルノードでのテスト

```bash
# 共有ディレクトリの準備
export BENCHFS_DIR=/tmp/benchfs_test
mkdir -p $BENCHFS_DIR/registry $BENCHFS_DIR/data

# BenchFS設定ファイルの作成
cat > $BENCHFS_DIR/benchfs.toml << 'CONFIG'
[node]
node_id = "node0"
data_dir = "/tmp/benchfs_test/data"
log_level = "info"

[storage]
chunk_size = 4194304
use_iouring = false
max_storage_gb = 0

[network]
bind_addr = "0.0.0.0:50051"
timeout_secs = 30
rdma_threshold_bytes = 32768
registry_dir = "/tmp/benchfs_test/registry"

[cache]
metadata_cache_entries = 1000
chunk_cache_mb = 100
cache_ttl_secs = 0
CONFIG

# BenchFSサーバーを起動（バックグラウンド）
benchfsd_mpi $BENCHFS_DIR/registry $BENCHFS_DIR/benchfs.toml &
BENCHFS_PID=$!

# サーバーの起動を待つ
sleep 3

# IORベンチマーク実行
mpirun -np 1 $HOME/.local/bin/ior \
    -a BENCHFS \
    -t 1m -b 4m -s 4 \
    -o $BENCHFS_DIR/data/testfile

# BenchFSサーバーを停止
kill $BENCHFS_PID
```

### マルチノードでのテスト

```bash
# ノード数を設定
NNODES=4

# BenchFSサーバーを全ノードで起動
mpirun -np $NNODES \
    benchfsd_mpi $BENCHFS_DIR/registry $BENCHFS_DIR/benchfs.toml &

BENCHFS_PID=$!
sleep 5

# IORベンチマーク実行（全ノードから並列アクセス）
mpirun -np $NNODES $HOME/.local/bin/ior \
    -a BENCHFS \
    -t 1m -b 4m -s 16 \
    -o $BENCHFS_DIR/data/testfile

# クリーンアップ
kill $BENCHFS_PID
wait
```

## IORパラメータ解説

- `-a BENCHFS`: BenchFSバックエンドを使用
- `-t 1m`: 転送サイズ 1 MiB
- `-b 4m`: ブロックサイズ 4 MiB
- `-s 16`: セグメント数 16
- `-o <path>`: 出力ファイルパス

## トラブルシューティング

### IORがBenchFSバックエンドを認識しない

```bash
# pkg-configが動作しているか確認
pkg-config --modversion benchfs

# IORを再ビルド
cd ior_integration/ior
make clean
./configure --prefix=$HOME/.local
make -j$(nproc)
make install
```

### BenchFSサーバーが起動しない

```bash
# 共有ディレクトリが存在するか確認
ls -la $BENCHFS_DIR/registry

# 設定ファイルを確認
cat $BENCHFS_DIR/benchfs.toml

# ログを確認
RUST_LOG=debug benchfsd_mpi $BENCHFS_DIR/registry $BENCHFS_DIR/benchfs.toml
```

## 次のステップ

- [詳細なインストールガイド](../INSTALL_SUPERCOMPUTER.md)
- [BenchFS設定リファレンス](../README.md)
- [IOR公式ドキュメント](https://ior.readthedocs.io/)

