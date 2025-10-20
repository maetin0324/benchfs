# IOR Setup Guide for BenchFS

IORは独立したプロジェクトとして管理されており、BenchFSリポジトリには含まれていません。
このガイドでは、IORのセットアップ方法を説明します。

## クイックスタート

```bash
# BenchFSリポジトリのルートディレクトリで実行
cd /home/rmaeda/workspace/rust/benchfs/ior_integration

# IORをクローン
git clone https://github.com/hpc/ior.git

# IORをビルド
cd ior
./bootstrap
./configure
make -j$(nproc)

# 確認
./src/ior --help
```

## スーパーコンピュータでのセットアップ

### 1. リポジトリをクローン

```bash
cd /work/NBB/your-username/
git clone <your-benchfs-repo-url> benchfs
cd benchfs
```

### 2. IORをセットアップ

```bash
cd ior_integration

# IORをクローン
git clone https://github.com/hpc/ior.git
cd ior

# 必要なモジュールをロード（Pegasus の例）
module purge
module load openmpi/4.1.8/gcc11.4.0-cuda12.8.1

# ビルド
./bootstrap
./configure
make -j$(nproc)

# 確認
ls -la src/ior
./src/ior --help
```

### 3. BenchFSをビルド

```bash
cd ../../  # benchfsのルートに戻る
cargo build --release --features mpi-support --bin benchfsd_mpi
```

## 自動セットアップスクリプト

便利なセットアップスクリプトも用意されています：

```bash
cd /home/rmaeda/workspace/rust/benchfs
./ior_integration/scripts/setup_ior.sh
```

## Git Submodule として管理する場合（オプション）

プロジェクトメンテナーが IOR を submodule として管理したい場合：

```bash
# ローカル開発環境で実行
cd /home/rmaeda/workspace/rust/benchfs

# 既存のiorディレクトリを削除
rm -rf ior_integration/ior

# Submoduleとして追加
git submodule add https://github.com/hpc/ior.git ior_integration/ior

# 初期化・更新
git submodule init
git submodule update

# コミット
git add .gitmodules ior_integration/ior
git commit -m "Add IOR as git submodule"
git push
```

スパコン側では：

```bash
# --recursiveオプションでsubmoduleも同時にクローン
git clone --recursive <your-repo-url> benchfs

# または、既にクローン済みの場合
cd benchfs
git submodule init
git submodule update --recursive
```

## トラブルシューティング

### bootstrap が失敗する

Autotoolsがインストールされていない可能性があります：

```bash
# Ubuntu/Debian
sudo apt-get install autoconf automake libtool

# CentOS/RHEL
sudo yum install autoconf automake libtool

# スーパーコンピュータの場合（モジュールで提供されている場合）
module avail autotools
module load autotools
```

### configure が失敗する（MPI not found）

MPIモジュールがロードされていない可能性があります：

```bash
# モジュールを確認
module list

# MPIをロード
module load openmpi  # または mpich, intel-mpi など

# MPIコンパイラを確認
which mpicc
mpicc --version
```

### make が失敗する

ログを確認して問題を特定します：

```bash
./configure 2>&1 | tee configure.log
make 2>&1 | tee make.log

# ログから ERROR や failed を検索
grep -i error make.log
```

## ディレクトリ構造

```
benchfs/
├── ior_integration/
│   ├── README.md
│   ├── benchfs_backend/    # BenchFSバックエンド実装
│   ├── ior/                # IORソース（git cloneで取得）
│   │   ├── src/
│   │   │   └── ior         # IORバイナリ
│   │   └── ...
│   └── scripts/            # ヘルパースクリプト
└── ...
```

## 参考資料

- [IOR GitHub](https://github.com/hpc/ior)
- [IOR Documentation](https://ior.readthedocs.io/)
- [BenchFS README](README.md)
- [BenchFS MPI Usage](MPI_USAGE.md)
