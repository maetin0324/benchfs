# BenchFS C Client - Makefile使用ガイド

BenchFS C ClientのテストをMakeコマンドで簡単に実行できます。

## クイックスタート

### 最も簡単な方法 (自動ビルド)

```bash
cd tests/docker

# テスト実行 (必要なイメージを自動的にビルド)
make test-c-client
```

初回実行時は、必要なDockerイメージを自動的にビルドします:
1. ベースイメージ (`benchfs/ucx-mpi-base:1.0`) - 約10-15分
2. BenchFSサーバーイメージ (`benchfs:latest`) - 約2-3分
3. C Clientイメージ (`benchfs-c-client:latest`) - 約3-5分

**合計: 初回は約15-20分、2回目以降は既存のイメージを使用するため即座に開始**

### 手動ビルド (オプション)

事前にイメージをビルドしたい場合:

```bash
cd tests/docker

# 1. ベースイメージのビルド (初回のみ、10-15分)
make build-base

# 2. C Clientイメージのビルド
make build-c-client

# 3. テスト実行
make test-c-client
```

## 利用可能なMakeターゲット

### ビルドコマンド

```bash
# UCX/MPIベースイメージのビルド (初回のみ必要)
make build-base

# C Clientイメージのビルド
make build-c-client

# BenchFS標準イメージのビルド
make build-optimized
```

### テストコマンド

#### C Clientテスト (推奨)

```bash
# クイックテスト (4MB/rank、約10秒)
make test-c-client-quick

# 基本テスト (16MB/rank、約30秒)
make test-c-client

# 4GiBテスト (4GiB/rank、数分)
make test-c-client-4gib

# 8GiBテスト (8GiB/rank、数分)
make test-c-client-8gib
```

#### 標準IORテスト (比較用)

```bash
# 標準IORテスト (Rust FFI版)
make test-ior

# 8GiB SSFテスト (Rust FFI版)
make test-ior-8gib-ssf
```

### ユーティリティコマンド

```bash
# クラスタの起動/停止
make up-c-client    # C Clientクラスタ起動
make down           # 全クラスタ停止

# ログ確認
make logs

# クリーンアップ
make clean          # コンテナとボリュームの削除
make clean-all      # 完全クリーンアップ (Dockerキャッシュ含む)

# ヘルプ表示
make help
```

## 詳細な使用例

### 1. 初回セットアップ

```bash
cd tests/docker

# ベースイメージをビルド (初回のみ)
make build-base

# C Clientイメージをビルド
make build-c-client
```

### 2. クイックテスト実行

```bash
# 最も速いテスト (動作確認用)
make test-c-client-quick
```

期待される出力:
```
Running quick C client test...
==========================================
BenchFS C Client Test Runner
==========================================
Test: quick
Nodes: 4

✓ Docker images ready
Starting BenchFS cluster...
✓ All containers running
✓ All servers registered: 4/4
Running IOR test with C client...
✓ Test Result: PASS
==========================================
Results saved to: ./results/c-client-YYYYMMDD-HHMMSS
✓ C Client Test PASSED
==========================================
```

### 3. 基本テスト実行

```bash
# 標準的なテスト
make test-c-client
```

### 4. 大規模テスト実行

```bash
# 4GiBテスト (推奨: メモリ32GB以上)
make test-c-client-4gib

# 8GiBテスト (推奨: メモリ64GB以上)
make test-c-client-8gib
```

### 5. Rust FFI版との性能比較

```bash
# C Client版テスト
make test-c-client

# Rust FFI版テスト (比較用)
make test-ior

# 結果を比較
ls -lh results/
```

## トラブルシューティング

### イメージが見つからないエラー

```bash
ERROR: Base image not found: benchfs/ucx-mpi-base:1.0
Please build it first: make build-base
```

**解決方法:**
```bash
make build-base
make build-c-client
```

### テストが失敗する

```bash
# クリーンアップしてやり直し
make clean
make build-c-client
make test-c-client
```

### コンテナが起動しない

```bash
# コンテナ状態確認
docker ps -a

# ログ確認
make logs

# 完全クリーンアップ
make clean-all
make build-base
make build-c-client
```

### メモリ不足

大規模テスト (4GiB/8GiB) を実行する場合:

```bash
# Dockerのメモリ制限を確認
docker info | grep Memory

# 小さいテストから試す
make test-c-client-quick
make test-c-client
```

## テスト結果の確認

テスト結果は `tests/docker/results/c-client-TIMESTAMP/` に保存されます:

```bash
# 最新の結果を確認
ls -lt results/ | head -5

# IOR出力を確認
cat results/c-client-YYYYMMDD-HHMMSS/ior_output.txt

# サーバーログを確認
cat results/c-client-YYYYMMDD-HHMMSS/server_stderr.log
```

主要なメトリクス:
- **write** - 書き込み帯域幅 (MiB/s)
- **read** - 読み込み帯域幅 (MiB/s)

## ワークフロー例

### 開発中のテストワークフロー

```bash
# 1. コードを修正
vim c_client/benchfs_client.c

# 2. イメージを再ビルド
make build-c-client

# 3. クイックテストで確認
make test-c-client-quick

# 4. 基本テストで確認
make test-c-client

# 5. 問題があればクリーンアップ
make clean
```

### CI/CDでの使用例

```bash
#!/bin/bash
set -e

cd tests/docker

# ベースイメージをキャッシュから使用
make build-base || true

# C Clientイメージをビルド
make build-c-client

# テスト実行
make test-c-client-quick
make test-c-client

# クリーンアップ
make clean
```

## その他の情報

- 詳細なドキュメント: `README-C-CLIENT.md`
- クイックリファレンス: `QUICKSTART-C-CLIENT.md`
- C Clientライブラリ: `../../c_client/README.md`

## まとめ

| コマンド | 用途 | 実行時間 |
|---------|------|---------|
| `make build-base` | ベースイメージビルド (初回のみ) | 10-15分 |
| `make build-c-client` | C Clientイメージビルド | 3-5分 |
| `make test-c-client-quick` | クイックテスト | ~10秒 |
| `make test-c-client` | 基本テスト | ~30秒 |
| `make test-c-client-4gib` | 4GiBテスト | 数分 |
| `make test-c-client-8gib` | 8GiBテスト | 数分-10分 |

**推奨フロー:**
1. `make build-base` (初回のみ)
2. `make build-c-client`
3. `make test-c-client-quick` (動作確認)
4. `make test-c-client` (通常テスト)
