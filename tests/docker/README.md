# BenchFS Docker Test Environment

Docker環境でBenchFSクラスタをテストするための構成です。

## 機能

- **マルチノードクラスタ**: 2ノードまたは4ノード構成でテスト可能
- **MPI統合**: OpenMPI 4.1.5を使用した分散実行
- **UCX通信**: UCX 1.18.0によるRDMA対応通信
- **io_uring対応**: 高性能非同期I/Oをサポート
- **IORベンチマーク**: BenchFSバックエンド (`-a BENCHFS`) を使用した分散ファイルシステムベンチマーク

## 必要条件

- Docker
- docker-compose v1.29.2以上
- カーネル 5.1以上（io_uring使用時）

## クイックスタート

### 基本テスト

```bash
# 2ノードクラスタでテスト
make test-small

# 4ノードクラスタでテスト
make test
```

### IORベンチマーク

```bash
# 2ノードでIORベンチマーク
make test-ior-small

# 4ノードでIORベンチマーク
make test-ior
```

**重要**: IORは `-a BENCHFS` オプションでBenchFS分散ファイルシステムに直接アクセスします。
これは以下のように動作します：

1. BenchFSサーバーが各ノードで起動（rank 0がメタデータサーバー兼ストレージサーバー、rank 1以降がクライアント）
2. IORプロセスがBenchFS C APIを通じてファイルI/Oを実行
3. 実際のデータは `/shared/data` に格納されますが、IORからは分散ファイルシステムとして透過的にアクセス

## 利用可能なコマンド

| コマンド | 説明 |
|---------|------|
| `make build` | Dockerイメージをビルド（フルビルド） |
| `make build-simple` | シンプルなDockerイメージをビルド（ホストのバイナリを使用） |
| `make up` | 4ノードクラスタを起動 |
| `make up-small` | 2ノードクラスタを起動 |
| `make down` | コンテナを停止・削除 |
| `make test` | 4ノードクラスタでテスト実行 |
| `make test-small` | 2ノードクラスタでテスト実行 |
| `make test-ior` | 4ノードでIORベンチマーク実行 |
| `make test-ior-small` | 2ノードでIORベンチマーク実行 |
| `make test-ior-8gib-ssf` | 4ノードで8GiB SSFベンチマーク実行 |
| `make logs` | コンテナログを表示 |
| `make shell` | コントローラコンテナにシェル接続 |
| `make clean` | すべてのコンテナ、ボリューム、イメージを削除 |
| `make clean-all` | ディープクリーン（Dockerキャッシュも含む） |

## 設定

### io_uringの有効化/無効化

`configs/benchfs_test.toml`で設定：

```toml
[storage]
use_iouring = true   # io_uringを有効化
# use_iouring = false  # io_uringを無効化（Docker環境で権限エラーが発生する場合）
```

**注意**: io_uringを有効にするには、Dockerコンテナに以下の権限が必要です：
- `CAP_SYS_RESOURCE`
- `CAP_SYS_ADMIN`
- `seccomp:unconfined`

これらの権限は`docker-compose.yml`および`docker-compose.small.yml`で既に設定されています。

## パフォーマンス比較

### 2ノード構成

| モード | Write (MiB/s) | Read (MiB/s) |
|--------|---------------|--------------|
| io_uring無効 | 885.85 | 2,406.05 |
| io_uring有効 | 3,500.71 | 12,429.67 |
| **向上率** | **3.95x** | **5.17x** |

### 4ノード構成

| モード | Write (MiB/s) | Read (MiB/s) |
|--------|---------------|--------------|
| io_uring無効 | 7,552.73 | 21,085.86 |
| io_uring有効 | 8,512.58 | 30,717.78 |
| **向上率** | **1.13x** | **1.46x** |

## アーキテクチャ

### ネットワーク構成

```
172.20.0.0/16 (benchfs_net)
├── 172.20.0.10: controller (コントローラノード)
├── 172.20.0.11: server1    (ストレージサーバー1)
├── 172.20.0.12: server2    (ストレージサーバー2)
├── 172.20.0.13: server3    (ストレージサーバー3) ※4ノード構成のみ
└── 172.20.0.14: server4    (ストレージサーバー4) ※4ノード構成のみ
```

### ボリューム構成

- **shared_registry**: サービスディスカバリ用の共有レジストリ（全ノード）
- **server{1-4}_data**: 各サーバーの独立したデータディレクトリ
- **controller_results**: テスト結果の保存先

## トラブルシューティング

### ContainerConfigエラー

古いコンテナイメージが原因の場合があります：

```bash
make clean
docker network prune -f
docker volume prune -f
```

### io_uring権限エラー

Dockerコンテナ内でio_uringが利用できない場合：

1. カーネルバージョンを確認: `uname -r`（5.1以上が必要）
2. 設定ファイルで無効化: `use_iouring = false`
3. または、docker-compose.ymlの権限設定を確認

### MPIエラー

SSHの問題がある場合：

```bash
make check-env  # 全ノードの環境を確認
make debug      # デバッグスクリプトを実行
```

## 開発

### 新しいテストの追加

1. `scripts/`ディレクトリにテストスクリプトを作成
2. スクリプトを実行可能にする: `chmod +x scripts/test-name.sh`
3. `Makefile`に新しいターゲットを追加
4. Docker compose設定にスクリプトをマウント

### IORテストケース

利用可能なIORテストケース：

1. **basic**: 基本的なwrite/readテスト (16MB, 2ノード, FPP)
2. **write**: 書き込み専用テスト (32MB, 4ノード, SSF)
3. **read**: 読み込み専用テスト (32MB, 4ノード, SSF)
4. **large-perf**: 大規模パフォーマンステスト (4GB, 4ノード, FPP)
5. **8gib-ssf**: 8GiB Shared Single Fileテスト (32GB total, 4ノード, SSF)

8GiB SSFテストの特徴：
- **データサイズ**: 各ランクが8GiB、合計32GiB
- **モード**: SSF (Shared Single File) - 全ランクが同じファイルに書き込み/読み込み
- **パラメータ**: transfer size=2MB, block size=64MB, segments=128
- **実行時間**: 数分かかる場合があります
- **計算**: 64MB × 128 segments = 8GiB per rank

実行方法：
```bash
make test-ior-8gib-ssf
```

または、直接実行：
```bash
make up
docker exec benchfs_controller /scripts/test-ior.sh 8gib-ssf 4
```

### IORカスタムテスト

`scripts/test-ior.sh`を編集して、新しいテストケースを追加できます：

```bash
case "$TEST_NAME" in
    "custom")
        echo "Test: Custom IOR test with BenchFS"
        mpirun \
            --hostfile ${HOSTFILE} \
            -np ${NNODES} \
            --mca btl tcp,self \
            --mca btl_tcp_if_include eth0 \
            ${IOR_BIN} \
                -a BENCHFS \
                --benchfs.registry ${REGISTRY_DIR} \
                --benchfs.datadir ${DATA_DIR} \
                [custom IOR parameters] \
                -o /custom_testfile
        ;;
esac
```

実行：

```bash
make up-small
docker exec benchfs_controller /scripts/test-ior.sh custom 2
```

## 参考資料

- [BenchFS Documentation](../../README.md)
- [IOR User Guide](https://ior.readthedocs.io/)
- [OpenMPI Documentation](https://www.open-mpi.org/doc/)
- [UCX Documentation](https://openucx.readthedocs.io/)
- [io_uring Documentation](https://kernel.dk/io_uring.pdf)
