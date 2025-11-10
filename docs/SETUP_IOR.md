# IOR Setup Guide for BenchFS

IORは独立したプロジェクトとして管理されており、BenchFSリポジトリには含まれていません。
このガイドでは、IORのセットアップとBenchFSバックエンドの統合方法を説明します。

**重要**: IORにはBenchFS固有のバックエンド実装が含まれています。バニラのIORをクローンした後、BenchFS用の修正を適用する必要があります。

**注意**: 2025年10月に、pkg-config依存の問題を修正した新しいパッチ(v2)をリリースしました。古いbenchfs_exportを使用している場合は、ローカル環境で`./scripts/export_benchfs_modifications.sh`を再実行して最新版を取得してください。

## クイックスタート（自動セットアップ）

### ローカル開発環境

```bash
# BenchFSリポジトリのルートディレクトリで実行
cd /home/rmaeda/workspace/rust/benchfs/ior_integration

# IORをクローン
git clone https://github.com/hpc/ior.git

# BenchFS修正を適用
./scripts/apply_benchfs_modifications.sh

# IORをビルド
cd ior
./bootstrap
./configure
make -j$(nproc)

# 確認: BENCHFSバックエンドが利用可能か確認
./src/ior -h | grep BENCHFS
```

## スーパーコンピュータでのセットアップ

### 1. ローカル環境でBenchFS修正をエクスポート

```bash
# ローカル開発環境で実行
cd /home/rmaeda/workspace/rust/benchfs/ior_integration

# BenchFS固有の修正をエクスポート
./scripts/export_benchfs_modifications.sh
```

これにより、`benchfs_export/`ディレクトリが作成され、以下のファイルが含まれます：
- `benchfs_ior.patch` - IORソースコードへの修正パッチ
- `benchfs_backend.tar.gz` - BenchFSバックエンド実装
- `apply_benchfs_modifications.sh` - 自動適用スクリプト
- `README.txt` - 詳細な手順

### 2. スパコンにファイルを転送

```bash
# benchfs_exportディレクトリ全体を転送
scp -r benchfs_export/ <your-supercomputer>:/work/NBB/rmaeda/workspace/rust/benchfs/ior_integration/
```

### 3. スパコンでIORをセットアップ

```bash
# スパコンにログイン
ssh <your-supercomputer>

# プロジェクトディレクトリに移動
cd /work/NBB/rmaeda/workspace/rust/benchfs/ior_integration

# IORをクローン
git clone https://github.com/hpc/ior.git

# 必要なモジュールをロード（Pegasus の例）
module purge
module load openmpi/4.1.8/gcc11.4.0-cuda12.8.1

# BenchFS修正を適用
cd benchfs_export
./apply_benchfs_modifications.sh

# IORをビルド
cd ../ior
./bootstrap
./configure
make -j$(nproc)

# 確認: BENCHFSバックエンドが利用可能か確認
./src/ior -h | grep BENCHFS
```

### 4. BenchFSをビルド

```bash
cd ../../  # benchfsのルートに戻る
cargo build --release --features mpi-support --bin benchfsd_mpi
```

## 手動セットアップ（自動スクリプトが使えない場合）

自動スクリプトが使えない場合、以下の手順で手動セットアップできます：

### 1. BenchFSバックエンドファイルを配置

```bash
cd /work/NBB/rmaeda/workspace/rust/benchfs/ior_integration

# benchfs_backend を展開
tar xzf benchfs_export/benchfs_backend.tar.gz

# aiori-BENCHFS.c をIORソースツリーにコピー
cp benchfs_backend/src/aiori-BENCHFS.c ior/src/
```

### 2. IORソースコードを手動で修正

#### configure.ac に追加:

```bash
cd ior

# configure.ac の FINCHFS セクションの後に以下を追加
cat >> configure.ac <<'EOF'

# BENCHFS support
PKG_CHECK_MODULES([BENCHFS], [benchfs],
  [AC_DEFINE([USE_BENCHFS_AIORI], [], [Build BENCHFS backend AIORI])
   BENCHFS_RPATH=$(pkg-config --libs-only-L benchfs | sed 's/-L/-Wl,-rpath=/g')
   AC_SUBST(BENCHFS_RPATH)],
  [with_benchfs=no])
AM_CONDITIONAL([USE_BENCHFS_AIORI], [test x$with_benchfs != xno])
EOF
```

#### src/Makefile.am に追加:

```bash
# FINCHFS セクションの後に以下を追加
cat >> src/Makefile.am <<'EOF'

if USE_BENCHFS_AIORI
extraSOURCES += aiori-BENCHFS.c
extraCPPFLAGS += @BENCHFS_CFLAGS@
extraLDFLAGS += @BENCHFS_RPATH@
extraLDADD += @BENCHFS_LIBS@
endif
EOF
```

#### src/aiori.c を編集:

`available_aiori[]` 配列に以下を追加:

```c
#ifdef USE_BENCHFS_AIORI
	&benchfs_aiori,
#endif
```

#### src/aiori.h を編集:

extern 宣言に以下を追加:

```c
extern ior_aiori_t benchfs_aiori;
```

### 3. ビルド

```bash
./bootstrap
./configure
make -j$(nproc)
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

## ファイル転送の確認

転送がうまくいったかを確認：

```bash
# スパコン側で確認
cd /work/NBB/rmaeda/workspace/rust/benchfs/ior_integration

# エクスポートディレクトリの内容を確認
ls -lh benchfs_export/
# 以下のファイルが存在することを確認:
#   - benchfs_ior.patch
#   - benchfs_backend.tar.gz
#   - apply_benchfs_modifications.sh
#   - README.txt

# パッチの内容を確認
head -20 benchfs_export/benchfs_ior.patch
```

## トラブルシューティング

詳細なトラブルシューティング手順は [TROUBLESHOOTING.md](ior_integration/TROUBLESHOOTING.md) を参照してください。

### よくある問題

#### 問題1: IORでBENCHFSバックエンドが認識されない

**症状:**
```bash
Error invalid argument: --benchfs.registry
Error invalid argument: --benchfs.datadir
```

**原因:** BENCHFSバックエンドがビルドされていない（パッチ未適用またはビルドエラー）

**解決方法:**
```bash
# 確認
./ior/src/ior -h | grep -A 5 "Module BENCHFS"

# 表示されない場合、ior_integration/TROUBLESHOOTING.md を参照
```

#### 問題2: 古いパッチを使用している（pkg-config エラー）

**症状:** configureがBENCHFSを検出せず、ビルドから除外される

**解決方法:** 最新のbenchfs_exportを使用
```bash
# ローカル環境で最新版を再エクスポート
cd /home/rmaeda/workspace/rust/benchfs/ior_integration
./scripts/export_benchfs_modifications.sh

# スパコンに転送
scp -r benchfs_export/ <host>:/work/NBB/rmaeda/workspace/rust/benchfs/ior_integration/

# スパコン側で再適用
cd /work/NBB/rmaeda/workspace/rust/benchfs/ior_integration
rm -rf ior
git clone https://github.com/hpc/ior.git
cd benchfs_export
./apply_benchfs_modifications.sh
```

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
