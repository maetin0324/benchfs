# スーパーコンピュータでのBenchFS+IOR クイックスタートガイド

このガイドでは、スーパーコンピュータ上でBenchFSとIORを素早くセットアップして実行する手順を説明します。

## 前提条件

- ローカル開発環境でBenchFSプロジェクトがビルド済み
- スパコンへのSSHアクセス
- スパコン上でMPIとautotoolsが利用可能

## ステップ1: ローカル環境でエクスポート

```bash
# ローカル開発環境で実行
cd /home/rmaeda/workspace/rust/benchfs/ior_integration

# BenchFS修正をエクスポート
./scripts/export_benchfs_modifications.sh
```

出力例:
```
==========================================
Export Complete!
==========================================

Export location: /home/rmaeda/workspace/rust/benchfs/ior_integration/benchfs_export

Files created:
  - apply_benchfs_modifications.sh (4.9K)
  - benchfs_backend.tar.gz         (4.4K)
  - benchfs_ior.patch              (1.6K)
  - README.txt                     (2.0K)
```

## ステップ2: スパコンにファイルを転送

```bash
# ローカル環境で実行
scp -r benchfs_export/ <your-supercomputer>:/work/NBB/rmaeda/workspace/rust/benchfs/ior_integration/
```

## ステップ3: スパコンでIORをセットアップ

```bash
# スパコンにログイン
ssh <your-supercomputer>

# プロジェクトディレクトリに移動
cd /work/NBB/rmaeda/workspace/rust/benchfs/ior_integration

# IORをクローン（まだの場合）
git clone https://github.com/hpc/ior.git

# 必要なモジュールをロード
module purge
module load openmpi/4.1.8/gcc11.4.0-cuda12.8.1

# BenchFS修正を適用
cd benchfs_export
./apply_benchfs_modifications.sh
```

期待される出力:
```
==========================================
Apply BenchFS Modifications to IOR
==========================================

[1/3] Extracting BenchFS backend files...
  ✓ BenchFS backend extracted

[2/3] Copying aiori-BENCHFS.c to IOR source tree...
  ✓ aiori-BENCHFS.c copied

[3/3] Applying IOR modifications patch...
  ✓ Patch applied successfully

==========================================
Modifications Applied Successfully!
==========================================
```

## ステップ4: IORをビルド

```bash
cd ../ior

# Bootstrap（autotools）
./bootstrap

# Configure
./configure

# ビルド
make -j$(nproc)
```

## ステップ5: 確認

```bash
# BENCHFSバックエンドが認識されているか確認
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

## ステップ6: BenchFSをビルド

```bash
cd /work/NBB/rmaeda/workspace/rust/benchfs

# Rustモジュールをロード（必要に応じて）
# module load rust

# BenchFS MPI版をビルド
cargo build --release --features mpi-support --bin benchfsd_mpi
```

## ステップ7: ベンチマークを実行

```bash
cd jobs/benchfs

# ジョブスクリプトを確認・編集
cat benchfs.sh

# ジョブを投入
./benchfs.sh
```

## トラブルシューティング

### IORでBENCHFSが認識されない

```bash
Error invalid argument: --benchfs.registry
```

**解決方法:**
```bash
# ステップ3〜5を再実行
cd /work/NBB/rmaeda/workspace/rust/benchfs/ior_integration
rm -rf ior
git clone https://github.com/hpc/ior.git
cd benchfs_export
./apply_benchfs_modifications.sh
cd ../ior
./bootstrap && ./configure && make -j$(nproc)
```

詳細は [TROUBLESHOOTING.md](TROUBLESHOOTING.md) を参照。

### パッチ適用エラー

```bash
error: patch failed: configure.ac:412
```

**解決方法:**

手動でパッチを適用します。詳細は [TROUBLESHOOTING.md](TROUBLESHOOTING.md) の「パッチ適用エラー」セクションを参照。

### bootstrap が失敗

```bash
./bootstrap: line 2: autoreconf: command not found
```

**解決方法:**
```bash
module load autotools
# または
module avail autotools  # 利用可能なバージョンを確認
```

### make が失敗

```bash
src/aiori-BENCHFS.c:23:10: fatal error: benchfs_c_api.h: No such file or directory
```

**解決方法:**
```bash
# ファイルが存在するか確認
ls -la ../benchfs_backend/include/benchfs_c_api.h

# パッチ適用をやり直す
cd ..
rm -rf ior
git clone https://github.com/hpc/ior.git
cd benchfs_export
./apply_benchfs_modifications.sh
```

## 次のステップ

1. **ジョブスクリプトのカスタマイズ**
   - `jobs/benchfs/benchfs.sh` を編集
   - ノード数、転送サイズ、ブロックサイズなどを調整

2. **結果の確認**
   ```bash
   cd /work/NBB/rmaeda/workspace/rust/benchfs/results/benchfs
   ls -lt  # 最新の結果を確認
   ```

3. **デバッグ**
   - ジョブが失敗した場合: `jobs/benchfs/debug-job.sh` を実行
   - ログを確認: `results/benchfs/<timestamp>/benchfsd_logs/`

## 関連ドキュメント

- [IOR Setup Guide](../../SETUP_IOR.md) - 詳細なセットアップ手順
- [Troubleshooting Guide](TROUBLESHOOTING.md) - トラブルシューティング
- [IOR Integration README](README.md) - IOR統合の技術詳細
- [BenchFS MPI Usage](../../MPI_USAGE.md) - MPI環境での使い方

## チェックリスト

スパコン上でのセットアップ完了を確認：

- [ ] benchfs_exportディレクトリがスパコンに転送されている
- [ ] IORがクローンされている
- [ ] BenchFS修正が適用されている（`git diff`で確認）
- [ ] aiori-BENCHFS.cがコピーされている（`ls ior/src/aiori-BENCHFS.c`）
- [ ] IORがビルドされている（`ls ior/src/ior`）
- [ ] BENCHFSバックエンドが認識されている（`./ior/src/ior -h | grep BENCHFS`）
- [ ] BenchFS MPIバイナリがビルドされている（`ls target/release/benchfsd_mpi`）
- [ ] ジョブスクリプトが準備されている（`ls jobs/benchfs/benchfs.sh`）

すべてチェックが完了したら、`./jobs/benchfs/benchfs.sh` でベンチマークを実行できます！
