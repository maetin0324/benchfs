# BenchFS Docker MPI Tests

Docker Composeを使用したBENCHFSのマルチノード・マルチクライアントテスト環境です。

## 📋 概要

このテスト環境は、Docker Composeを使用して以下を提供します：

- **複数のサーバーノード**: 2ノードまたは4ノードのクラスタ
- **分離されたファイルシステム**: 各サーバーが独立したデータボリュームを持つ
- **共有レジストリ**: UCX WorkerAddressの交換用
- **MPI統合**: OpenMPIによるサーバー起動と管理
- **簡単な実行**: Makefileとスクリプトによる自動化

## 🏗️ アーキテクチャ

### ネットワーク構成

```
172.20.0.0/16 (benchfs_net)
├── 172.20.0.10 - controller (MPI coordinator)
├── 172.20.0.11 - server1 (MPI rank 0)
├── 172.20.0.12 - server2 (MPI rank 1)
├── 172.20.0.13 - server3 (MPI rank 2)
└── 172.20.0.14 - server4 (MPI rank 3)
```

### ボリューム構成

各ノードは以下のボリュームをマウントします：

| ノード | レジストリ | データ | 用途 |
|--------|-----------|--------|------|
| server1 | shared_registry (共有) | server1_data (独立) | BenchFSサーバー |
| server2 | shared_registry (共有) | server2_data (独立) | BenchFSサーバー |
| server3 | shared_registry (共有) | server3_data (独立) | BenchFSサーバー |
| server4 | shared_registry (共有) | server4_data (独立) | BenchFSサーバー |
| controller | shared_registry (共有) | controller_results (独立) | テスト実行 |

**重要**: 各サーバーのデータボリュームは完全に分離されており、別々のファイルシステムとして動作します。

## 📦 前提条件

### 必須

- Docker (20.10以降)
- Docker Compose (v2.0以降)
- Rust (1.70以降)
- GNU Make

### 確認方法

```bash
docker --version
docker-compose --version
cargo --version
make --version
```

## 🚀 クイックスタート

### 1. 基本的なテスト（2ノード）

```bash
cd /home/rmaeda/workspace/rust/benchfs/tests/docker

# すべて自動で実行
./run-tests.sh

# または、Makefileを使用
make quick-test
```

### 2. フルテスト（4ノード）

```bash
# フルテストを実行
./run-tests.sh --test full

# または、Makefileを使用
make full-test
```

## 🔧 詳細な使用方法

### Makefileコマンド

```bash
# ヘルプを表示
make help

# Docker イメージをビルド
make build-simple

# 2ノードクラスタを起動
make up-small

# 4ノードクラスタを起動
make up

# テストを実行
make test-small  # 2ノード
make test        # 4ノード

# ログを確認
make logs
make logs-controller

# コントローラーにシェルで接続
make shell

# クラスタを停止
make down

# すべてクリーンアップ
make clean
```

### 手動でのテスト実行

#### ステップ1: ビルド

```bash
# BenchFS MPI バイナリをビルド
cd /home/rmaeda/workspace/rust/benchfs
cargo build --release --features mpi-support --bin benchfsd_mpi
```

#### ステップ2: Dockerイメージのビルド

```bash
cd tests/docker
docker-compose -f docker-compose.small.yml build
```

#### ステップ3: クラスタの起動

```bash
# 2ノード構成
docker-compose -f docker-compose.small.yml up -d

# または4ノード構成
docker-compose -f docker-compose.yml up -d
```

#### ステップ4: コンテナの確認

```bash
docker-compose ps

# 期待される出力:
# NAME                    STATUS
# benchfs_controller      Up
# benchfs_server1         Up
# benchfs_server2         Up
```

#### ステップ5: テストの実行

```bash
# コントローラーでテストを実行
docker exec benchfs_controller /scripts/test-cluster.sh basic 2

# または、コントローラーに入って手動で実行
docker exec -it benchfs_controller /bin/bash
```

#### ステップ6: クラスタの停止

```bash
docker-compose down
```

## 📊 テストスクリプト

### test-cluster.sh

完全な統合テストを実行します。

```bash
docker exec benchfs_controller /scripts/test-cluster.sh <test_type> <nnodes>
```

パラメータ:
- `test_type`: テストの種類 (`basic`, `stress`)
- `nnodes`: ノード数 (2 または 4)

例:
```bash
# 基本テスト (2ノード)
docker exec benchfs_controller /scripts/test-cluster.sh basic 2

# ストレステスト (4ノード)
docker exec benchfs_controller /scripts/test-cluster.sh stress 4
```

### run-mpi-test.sh

MPIサーバーを起動して動作確認します。

```bash
docker exec benchfs_controller /scripts/run-mpi-test.sh <nnodes>
```

例:
```bash
docker exec benchfs_controller /scripts/run-mpi-test.sh 2
```

## 🔍 デバッグ

### ログの確認

```bash
# すべてのコンテナのログ
docker-compose logs

# 特定のコンテナのログ
docker logs benchfs_server1
docker logs benchfs_controller

# ログをフォロー
docker logs -f benchfs_server1
```

### コンテナ内での確認

```bash
# コントローラーに接続
docker exec -it benchfs_controller /bin/bash

# レジストリの内容を確認
ls -lh /shared/registry/

# ネットワーク接続を確認
ping server1
ping server2

# MPIの動作確認
mpirun --hostfile /tmp/hostfile -np 2 hostname
```

### サーバーノードに接続

```bash
# server1に接続
docker exec -it benchfs_server1 /bin/bash

# データディレクトリを確認
ls -lh /shared/data/
```

## 📁 ディレクトリ構造

```
tests/docker/
├── Dockerfile              # フルビルド用Dockerfile
├── Dockerfile.simple       # 簡易版Dockerfile（推奨）
├── docker-compose.yml      # 4ノード構成
├── docker-compose.small.yml # 2ノード構成
├── Makefile                # ビルド・テストコマンド
├── run-tests.sh            # メインテストスクリプト
├── README.md               # このファイル
├── configs/
│   └── benchfs_test.toml   # BenchFS設定
└── scripts/
    ├── run-mpi-test.sh     # MPIテスト起動スクリプト
    └── test-cluster.sh     # 統合テストスクリプト
```

## 🐛 トラブルシューティング

### サーバーが起動しない

**症状**: "ERROR: BenchFS servers failed to start"

**確認事項**:

1. バイナリが存在するか
   ```bash
   ls -la ../../target/release/benchfsd_mpi
   ```

2. コンテナが起動しているか
   ```bash
   docker-compose ps
   ```

3. サーバーログを確認
   ```bash
   docker logs benchfs_server1
   ```

### SSH接続ができない

**症状**: "Failed to connect to serverX"

**解決方法**:

1. コンテナの再起動
   ```bash
   docker-compose restart
   ```

2. SSHサービスの確認
   ```bash
   docker exec benchfs_server1 service ssh status
   ```

### ノードが登録されない

**症状**: "Only X/Y nodes registered"

**確認事項**:

1. レジストリディレクトリを確認
   ```bash
   docker exec benchfs_controller ls -lh /shared/registry/
   ```

2. UCX設定を確認
   ```bash
   docker exec benchfs_server1 printenv | grep UCX
   ```

3. ネットワーク接続を確認
   ```bash
   docker exec benchfs_controller ping server1
   ```

### ボリュームの問題

**症状**: "Permission denied" や "No such file or directory"

**解決方法**:

```bash
# ボリュームをクリーンアップ
make clean

# または手動で
docker volume ls
docker volume rm benchfs_server1_data
docker volume rm benchfs_server2_data
# ...

# 再構築
make build-simple
make up-small
```

## 🎯 カスタマイズ

### ノード数の変更

新しい構成を作成する場合:

1. `docker-compose.yml`をコピー
   ```bash
   cp docker-compose.yml docker-compose.8nodes.yml
   ```

2. サーバー定義を追加
   ```yaml
   server5:
     build:
       context: ../..
       dockerfile: tests/docker/Dockerfile.simple
     # ... (server1-4と同様の設定)
   ```

3. ボリュームを追加
   ```yaml
   volumes:
     server5_data:
       driver: local
   ```

### 設定のカスタマイズ

`configs/benchfs_test.toml`を編集:

```toml
[storage]
chunk_size = 1048576  # 1 MiB に変更

[cache]
metadata_cache_entries = 10000  # キャッシュを増やす
```

### ネットワーク設定の変更

`docker-compose.yml`のネットワーク設定を編集:

```yaml
networks:
  benchfs_net:
    driver: bridge
    ipam:
      config:
        - subnet: 192.168.100.0/24  # サブネットを変更
```

## 🧪 テストの追加

新しいテストを追加するには、`scripts/test-cluster.sh`を編集:

```bash
case "$TEST_NAME" in
    "basic")
        # 既存のテスト
        ;;

    "mytest")
        echo "Test: My custom test"
        # カスタムテストのロジック
        TEST_RESULT="PASS"
        ;;
esac
```

## 📚 参考資料

- [BenchFS README](../../README.md)
- [MPI使用ガイド](../../MPI_USAGE.md)
- [Docker Compose Documentation](https://docs.docker.com/compose/)
- [OpenMPI Documentation](https://www.open-mpi.org/doc/)

## 🤝 コントリビューション

問題や改善提案がある場合は、issueを作成してください。

## 📝 ライセンス

BENCHFSプロジェクトに準拠
