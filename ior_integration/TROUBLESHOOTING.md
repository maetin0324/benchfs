# IOR BenchFS統合 トラブルシューティングガイド

## IORでBENCHFSバックエンドが認識されない

### 症状

```bash
Error invalid argument: --benchfs.registry
Error invalid argument: --benchfs.datadir
Invalid options
```

`ior -h`を実行しても、`Module BENCHFS`のセクションが表示されない。

### 原因

BENCHFSバックエンドがIORのビルドに含まれていません。以下のいずれかの原因が考えられます：

1. **パッチが正しく適用されていない**
2. **aiori-BENCHFS.cがコピーされていない**
3. **configureがBENCHFSを検出していない** (古いパッチの問題)
4. **ビルドエラーが発生している**

### 解決方法

#### ステップ1: 現在の状態を確認

```bash
cd /work/NBB/rmaeda/workspace/rust/benchfs/ior_integration/ior

# 1. aiori-BENCHFS.c が存在するか確認
ls -la src/aiori-BENCHFS.c

# 2. パッチが適用されているか確認
git diff configure.ac src/Makefile.am src/aiori.c src/aiori.h

# 3. configureの結果を確認
cat config.log | grep -i benchfs
```

#### ステップ2: 最新のパッチを適用

古いパッチにはpkg-config依存の問題がありました。最新版のパッチを使用してください。

```bash
# 古いior を削除
cd /work/NBB/rmaeda/workspace/rust/benchfs/ior_integration
rm -rf ior

# 最新のbenchfs_exportを取得（ローカルから再転送）
# ローカルで: ./scripts/export_benchfs_modifications.sh
# スパコンに転送: scp -r benchfs_export/ <host>:/work/NBB/rmaeda/workspace/rust/benchfs/ior_integration/

# IORを再クローン
git clone https://github.com/hpc/ior.git

# 最新のパッチを適用
cd benchfs_export
./apply_benchfs_modifications.sh
```

#### ステップ3: ビルド

```bash
cd ../ior

# 必要なモジュールをロード
module purge
module load openmpi/4.1.8/gcc11.4.0-cuda12.8.1

# autotools の確認
which autoconf || module load autotools

# ビルド
./bootstrap
./configure
make -j$(nproc)
```

#### ステップ4: 確認

```bash
# BENCHFSバックエンドが利用可能か確認
./src/ior -h | grep -A 10 "Module BENCHFS"
```

期待される出力:
```
Module BENCHFS

Optional arguments
  --benchfs.registry=STRING     Registry directory path
  --benchfs.datadir=STRING      Data directory path (for server)
  --benchfs.use-mpi-rank=1      Use MPI rank for node ID
```

### ビルドエラーが発生する場合

#### エラー: "benchfs_c_api.h: No such file or directory"

```bash
# include pathを確認
ls -la ../../benchfs_backend/include/benchfs_c_api.h

# Makefile.amに include path を追加する必要がある場合
cd src
# benchfs_backend/include への相対パスを設定
export CPPFLAGS="-I../../benchfs_backend/include"
cd ..
make clean
make -j$(nproc)
```

#### エラー: "undefined reference to benchfs_init"

これは正常です。BenchFS C APIはまだスタブ実装のみで、実際のリンクは不要です。
IORのビルドは成功しますが、実行時にエラーが出る可能性があります。

## パッチ適用エラー

### 症状

```bash
error: patch failed: configure.ac:412
error: configure.ac: patch does not apply
```

### 原因

IORのバージョンが異なるため、パッチが適用できません。

### 解決方法

#### 方法1: 手動適用

`benchfs_export/benchfs_ior.patch`の内容を確認し、手動で適用:

1. **configure.ac** に以下を追加（FINCHFS セクションの後):

```bash
# BENCHFS support (always enabled, no pkg-config required)
AC_DEFINE([USE_BENCHFS_AIORI], [], [Build BENCHFS backend AIORI])
AM_CONDITIONAL([USE_BENCHFS_AIORI], [true])
```

2. **src/Makefile.am** に以下を追加（FINCHFS セクションの後):

```makefile
if USE_BENCHFS_AIORI
extraSOURCES += aiori-BENCHFS.c
endif
```

3. **src/aiori.c** の `available_aiori[]` 配列に追加:

```c
#ifdef USE_BENCHFS_AIORI
	&benchfs_aiori,
#endif
```

4. **src/aiori.h** に extern 宣言を追加:

```c
extern ior_aiori_t benchfs_aiori;
```

#### 方法2: 3-way merge

```bash
cd ior
git apply --3way ../benchfs_export/benchfs_ior.patch
# コンフリクトがあれば手動で解決
```

## configure が失敗する

### 症状

```bash
./configure: line xxx: syntax error
```

### 解決方法

```bash
# autotoolsを再実行
./bootstrap

# もしbootstrapが失敗する場合
autoreconf -fi

# 再度configure
./configure
```

## make が失敗する

### ビルドログを確認

```bash
# 詳細なログを確認
make clean
make V=1 2>&1 | tee build.log

# BENCHFSに関連するエラーを検索
grep -i benchfs build.log
grep -i error build.log
```

### よくあるエラー

1. **aiori-BENCHFS.c がない**
   ```bash
   # ファイルをコピー
   cp ../benchfs_backend/src/aiori-BENCHFS.c src/
   ```

2. **ヘッダーファイルが見つからない**
   ```bash
   # include pathを設定
   export CPPFLAGS="-I$(pwd)/../benchfs_backend/include"
   make clean
   make -j$(nproc)
   ```

## IORビルド時のリンクエラー

### 症状

```
undefined reference to `benchfs_init'
undefined reference to `benchfs_write'
...
```

### 原因

BenchFS共有ライブラリ(`libbenchfs.so`)が見つからない、または未ビルドです。

### 解決方法

#### ステップ1: BenchFS共有ライブラリをビルド

```bash
cd /work/NBB/rmaeda/workspace/rust/benchfs

# 共有ライブラリをビルド
cargo build --release

# 生成を確認
ls -lh target/release/libbenchfs.so
```

期待される出力:
```
-rwxr-xr-x 2 user group 1.5M Oct 23 04:50 target/release/libbenchfs.so
```

#### ステップ2: IORを再ビルド

```bash
cd ior_integration/ior
make clean
make -j$(nproc)
```

#### ステップ3: 共有ライブラリのパスを確認

```bash
# lddでライブラリの依存関係を確認
ldd src/ior | grep benchfs
```

期待される出力:
```
    libbenchfs.so => /work/NBB/rmaeda/workspace/rust/benchfs/target/release/libbenchfs.so
```

### よくある問題

#### 問題: `libbenchfs.so: cannot open shared object file`

**原因**: LD_LIBRARY_PATHが設定されていない

**解決方法**: Makefile.amのrpathで解決されるはずですが、失敗する場合:

```bash
export LD_LIBRARY_PATH=/work/NBB/rmaeda/workspace/rust/benchfs/target/release:$LD_LIBRARY_PATH
```

#### 問題: Rustがインストールされていない

```bash
# Rustをインストール
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# または、スパコンのモジュールを確認
module avail rust
module load rust
```

## IOR実行時のエラー

### "BENCHFS not initialized" または segmentation fault

**原因**: BenchFS C FFI実装に問題がある、またはランタイム初期化に失敗

**解決方法**:

1. **ログを確認**:
   ```bash
   # RUST_LOGを有効にして実行
   export RUST_LOG=debug
   mpirun -np 2 ./src/ior -a BENCHFS -t 1m -b 4m -w
   ```

2. **BenchFSバージョンを確認**:
   ```bash
   cd /work/NBB/rmaeda/workspace/rust/benchfs
   git log --oneline -1
   # FFI実装があるか確認
   ls -la src/ffi/
   ```

3. **シンプルなテストを実行**:
   ```bash
   # 最小構成でテスト
   mpirun -np 1 ./src/ior -a BENCHFS -t 1k -b 4k -w -v -v -v
   ```

## パッチのバージョン履歴

### v1 (初期版) - 問題あり

pkg-configを使用していたため、BenchFSが検出されずビルドから除外される問題がありました。

```bash
# 古いパッチの特徴
PKG_CHECK_MODULES([BENCHFS], [benchfs], ...)
```

### v2 (修正版) - 推奨

pkg-configに依存せず、BENCHFSを常に有効化します。

```bash
# 新しいパッチの特徴
AC_DEFINE([USE_BENCHFS_AIORI], [], [Build BENCHFS backend AIORI])
AM_CONDITIONAL([USE_BENCHFS_AIORI], [true])
```

## サポートが必要な場合

1. ビルドログを保存: `make V=1 2>&1 | tee build.log`
2. configure出力を保存: `./configure 2>&1 | tee configure.log`
3. 以下の情報を確認:
   - IORのバージョン: `git log --oneline -1`
   - パッチの内容: `cat benchfs_export/benchfs_ior.patch`
   - エラーメッセージ全文

## 関連ドキュメント

- [IOR Setup Guide](../SETUP_IOR.md)
- [IOR Integration README](README.md)
- [BenchFS Documentation](../../README.md)
