# UCX/MPI ベースイメージ最適化ガイド

## 概要

BenchFSのDockerビルドを高速化するため、UCXとOpenMPIを含むベースイメージを作成し、再利用する方式を導入しました。

## 問題

従来のDockerfile（`Dockerfile`）では、ビルドのたびにUCXとOpenMPI（合計10-15分）をビルドしていました。これは非効率的です。

## 解決策

UCX/MPIを含むベースイメージを一度だけビルドし、以降のBenchFSビルドではそれを再利用します。

### ビルド時間の比較

| 方式 | 初回ビルド | 2回目以降 | 節約時間 |
|------|----------|----------|---------|
| 従来方式 | 15-20分 | 15-20分 | なし |
| **最適化方式** | **15-20分** | **2-3分** | **12-17分** |

## ファイル構成

```
tests/docker/
├── Dockerfile                     # 従来のDockerfile（変更なし）
├── Dockerfile.ucx-mpi-base       # UCX/MPIベースイメージ用
├── Dockerfile.optimized          # 最適化版BenchFS Dockerfile
├── Makefile                      # 既存のMakefile
└── Makefile.ucx-mpi             # UCX/MPI管理用Makefile
```

## 使い方

### 1. ベースイメージのビルド（初回のみ）

```bash
cd tests/docker
make -f Makefile.ucx-mpi build-base
```

このコマンドは10-15分かかりますが、**一度だけ実行すれば良い**です。

### 2. BenchFSのビルド（高速）

```bash
make -f Makefile.ucx-mpi build
```

ベースイメージを使用するため、**2-3分で完了**します。

### 3. 両方を一度にビルド

```bash
make -f Makefile.ucx-mpi build-all
```

## 詳細な使い方

### ヘルプの表示

```bash
make -f Makefile.ucx-mpi help
```

### イメージ情報の確認

```bash
make -f Makefile.ucx-mpi info
```

出力例：
```
=== Image Information ===

Base Image:
  Name: [benchfs/ucx-mpi-base:1.0]
  ID: sha256:abc123...
  Created: 2025-10-28T02:00:00Z
  Size: 1234567890 bytes

BenchFS Image:
  Name: [benchfs:latest]
  ID: sha256:def456...
  Created: 2025-10-28T02:10:00Z
  Size: 987654321 bytes

Build time comparison:
  With cached base: ~2-3 minutes (Rust build only)
  Without cache:    ~15-20 minutes (UCX + MPI + Rust)
```

### イメージのクリーンアップ

```bash
make -f Makefile.ucx-mpi clean
```

## レジストリへの共有（オプション）

チームでベースイメージを共有する場合、Docker Registryにプッシュできます。

### 1. ベースイメージをプッシュ

```bash
make -f Makefile.ucx-mpi push-base REGISTRY=docker.io/your-username
```

### 2. 他のマシンでプル

```bash
make -f Makefile.ucx-mpi pull-base REGISTRY=docker.io/your-username
```

### 3. プルしたベースイメージでビルド

```bash
make -f Makefile.ucx-mpi build
```

## docker-compose との統合

既存の`docker-compose.yml`を変更せずに使用できます：

```bash
# 最適化版イメージをビルド
make -f Makefile.ucx-mpi build

# docker-compose.ymlのimage名を更新（オプション）
# image: benchfs:latest

# 通常通り起動
docker-compose up -d
```

## 既存のワークフローとの互換性

### 従来のDockerfileも引き続き使用可能

最適化版を使いたくない場合、従来の`Dockerfile`も引き続き動作します：

```bash
# 従来の方式（UCX/MPIを毎回ビルド）
docker build -f Dockerfile -t benchfs:latest ../..
```

### Makefileターゲットの使い分け

```bash
# 従来の方式
make build

# 最適化版（推奨）
make -f Makefile.ucx-mpi build-base  # 初回のみ
make -f Makefile.ucx-mpi build       # 2回目以降
```

## ベースイメージの更新が必要な場合

以下の場合、ベースイメージを再ビルドする必要があります：

1. **UCXのバージョンを変更**
   - `Dockerfile.ucx-mpi-base`の`UCX_VERSION`を変更
   - `make -f Makefile.ucx-mpi build-base`を再実行

2. **OpenMPIのバージョンを変更**
   - `Dockerfile.ucx-mpi-base`の`OPENMPI_VERSION`を変更
   - `make -f Makefile.ucx-mpi build-base`を再実行

3. **ベースOSを変更**
   - `Dockerfile.ucx-mpi-base`の`FROM ubuntu:22.04`を変更
   - `make -f Makefile.ucx-mpi build-base`を再実行

## トラブルシューティング

### エラー: Base image not found

```
ERROR: Base image not found: benchfs/ucx-mpi-base:1.0
Please build it first: make -f Makefile.ucx-mpi build-base
```

**解決方法**: ベースイメージをビルドしてください：
```bash
make -f Makefile.ucx-mpi build-base
```

### ディスク容量の確認

ベースイメージは約1-2GBのディスク容量を使用します：

```bash
docker images benchfs/ucx-mpi-base
```

不要になった場合：
```bash
make -f Makefile.ucx-mpi clean
```

## まとめ

### メリット

1. **ビルド時間の大幅短縮**: 2回目以降は2-3分で完了
2. **ネットワーク帯域の節約**: UCX/MPIのダウンロードは初回のみ
3. **開発効率の向上**: コードの変更後すぐにテスト可能
4. **チーム共有**: レジストリを使えばベースイメージを共有可能

### 推奨ワークフロー

```bash
# 1. プロジェクトセットアップ時（一度だけ）
cd tests/docker
make -f Makefile.ucx-mpi build-base

# 2. 開発中（何度でも高速）
# コードを修正...
make -f Makefile.ucx-mpi build

# 3. テスト実行
docker-compose up -d
docker exec benchfs_controller /scripts/test-ior.sh basic 4
```

### 従来方式との比較

| 項目 | 従来方式 | 最適化方式 |
|------|---------|----------|
| 初回ビルド | 15-20分 | 15-20分 |
| 2回目以降 | 15-20分 | **2-3分** ⚡ |
| ディスク使用量 | 変わらず | +1-2GB（ベースイメージ） |
| レジストリ共有 | 不可 | 可能 |

---

**推奨**: 開発時は最適化版を使用し、本番環境や初回セットアップ時は従来版を使用するのが良いでしょう。
