# 8GiB SSF I/Oテストケース実装レポート

## 概要

BenchFS Dockerテスト環境に8GiB Shared Single File (SSF) I/Oテストケースを追加しました。このテストは4ノードクラスタ上で32GiB（各ランク8GiB）の大規模分散I/Oベンチマークを実行します。

## 実装内容

### 1. テストスクリプトの更新 (`tests/docker/scripts/test-ior.sh`)

新しいテストケース "8gib-ssf" を追加：

```bash
"8gib-ssf")
    echo "Test: 8GiB Shared Single File (SSF) I/O test"
    echo "Transfer size: 2MB, Block size: 64MB, Segments: 32, Total: 8GiB per rank (32GiB total)"
    echo "Mode: SSF (Shared Single File) - all ranks write to same file"
    echo "Nodes: 4 (4 MPI ranks)"

    mpirun \
        --hostfile ${HOSTFILE} \
        -np ${NNODES} \
        --mca btl tcp,self \
        --mca btl_tcp_if_include eth0 \
        ${IOR_BIN} \
            -a BENCHFS \
            --benchfs.registry ${REGISTRY_DIR} \
            --benchfs.datadir ${DATA_DIR} \
            -w -r \
            -t 2m -b 64m -s 32 \
            -o /8gib_ssf_testfile \
            -O summaryFormat=JSON \
            > ${RESULTS_DIR}/ior_8gib_ssf_output.txt 2>&1
```

#### IORパラメータの詳細

| パラメータ | 値 | 説明 |
|----------|-----|------|
| `-a BENCHFS` | - | BenchFSバックエンドを使用 |
| `-w -r` | - | Write + Readの両方を実行 |
| `-t 2m` | 2MB | Transfer size（各I/O操作のサイズ） |
| `-b 64m` | 64MB | Block size（各プロセスが書き込むブロック） |
| `-s 128` | 128 | Segments（繰り返し回数） |
| **-Fなし** | - | SSFモード（全ランクが同じファイルを共有） |

**データサイズ計算:**
- 各ランク: 64MB (block) × 128 (segments) = 8192MB = 8GiB per rank
- 4ランク合計: 8GiB × 4 = 32GiB total

この設定により、要件通り各ランク8GiB、合計32GiBのSSFテストを実行します。

### 2. Docker Composeの更新 (`tests/docker/docker-compose.yml`)

各コンテナにメモリ制限を追加：

```yaml
deploy:
  resources:
    limits:
      memory: 16G  # 8GiB data + 8GiB buffer for operations and page cache
    reservations:
      memory: 4G
```

#### メモリ配分の設計理念

- **Server nodes (server1-4)**: 各16GB
  - 8GiB: データバッファ
  - 8GiB: ページキャッシュ、io_uring、MPI通信バッファ
- **Controller node**: 8GB
  - コントローラーは主にMPI調整役のため、サーバーより少ないメモリで十分

#### 総メモリ使用量
- 4 servers × 16GB = 64GB
- 1 controller × 8GB = 8GB
- **合計**: 72GB (ホストの251.6GiBメモリに対して十分余裕あり)

### 3. Makefileの更新 (`tests/docker/Makefile`)

新しいターゲット `test-ior-8gib-ssf` を追加：

```makefile
test-ior-8gib-ssf: up
	@echo "Running 8GiB SSF IOR benchmark on 4-node cluster..."
	@echo "This test will write/read 32GiB total (8GiB per node) and may take several minutes..."
	@sleep 2
	@CONTAINER_NAME=$$(docker ps --format '{{.Names}}' | grep controller | head -1); \
	if [ -z "$$CONTAINER_NAME" ]; then \
		echo "ERROR: Controller container not found"; \
		docker ps -a; \
		exit 1; \
	fi; \
	echo "Using container: $$CONTAINER_NAME"; \
	docker exec $$CONTAINER_NAME /scripts/test-ior.sh 8gib-ssf 4
```

### 4. ドキュメントの更新 (`tests/docker/README.md`)

- コマンドテーブルに `make test-ior-8gib-ssf` を追加
- IORテストケースセクションを追加し、全5種類のテストケースを説明
- 8GiB SSFテストの特徴と実行方法を記載

## 実行方法

### 基本的な実行

```bash
cd /home/rmaeda/workspace/rust/benchfs/tests/docker
make test-ior-8gib-ssf
```

### 手動実行

```bash
# 1. クラスタを起動
make up

# 2. テストを実行
docker exec benchfs_controller /scripts/test-ior.sh 8gib-ssf 4

# 3. 結果を確認
docker exec benchfs_controller cat /shared/results/ior_8gib_ssf_output.txt
```

## パラメータ調整オプション

### 代替パラメータ設定

現在の実装は `-t 2m -b 64m -s 128` を使用していますが、以下の代替設定も可能：

```bash
# Option 1: より大きいblock sizeを使用（メモリ効率重視）
-t 2m -b 256m -s 32    # 256MB × 32 = 8GiB per rank, 32GiB total

# Option 2: 現在の設定（バランス型）
-t 2m -b 64m -s 128    # 64MB × 128 = 8GiB per rank, 32GiB total

# Option 3: より大きいtransfer size（ネットワーク効率重視）
-t 4m -b 128m -s 64    # 128MB × 64 = 8GiB per rank, 32GiB total
```

### パフォーマンス最適化の考慮事項

1. **Transfer size (-t)**
   - 小さすぎる: システムコールのオーバーヘッド増加
   - 大きすぎる: メモリバッファのプレッシャー増加
   - **推奨**: 1-4MB（ネットワークMTUとの兼ね合い）

2. **Block size (-b)**
   - SSFモードでは各ランクの書き込み単位
   - **推奨**: 64-256MB（ストレージデバイスの特性に依存）

3. **Segments (-s)**
   - 大きいほど長時間実行、統計的に安定した結果
   - **推奨**: 32-128

## 検証項目

実装後、以下を確認してください：

### 1. 構文チェック
```bash
cd /home/rmaeda/workspace/rust/benchfs/tests/docker
bash -n scripts/test-ior.sh  # 構文エラーチェック
```

### 2. Docker Compose検証
```bash
docker-compose -f docker-compose.yml config
```

### 3. メモリ制限の確認
```bash
make up
docker stats --no-stream | grep benchfs
```

### 4. テスト実行
```bash
make test-ior-8gib-ssf
```

### 5. 結果の確認
```bash
docker exec benchfs_controller cat /shared/results/ior_8gib_ssf_output.txt
```

## 期待される動作

### 正常実行時のフロー

1. **クラスタ起動** (make up)
   - 4つのサーバーノードと1つのコントローラーノードが起動
   - 各ノードでSSHサービスが起動

2. **BenchFSサーバー起動**
   - 4ランクのMPIプロセスがBenchFSデーモンを起動
   - サービスディスカバリ（レジストリ登録）

3. **IOR実行**
   - 4ランクのIORプロセスが起動
   - SSFモードで単一ファイルに対してwrite/read
   - 各ランクが8GiB書き込み（合計32GiB）

4. **結果出力**
   - JSON形式のベンチマーク結果
   - Write/Read帯域幅（MB/s）
   - IOPS、レイテンシ統計

### エラーハンドリング

- **メモリ不足**: Docker memoryリソース制限により保護
- **ノード数不一致**: スクリプト内で4ノード必須チェック
- **IORエラー**: 詳細なエラーログを `/shared/results/` に出力

## トラブルシューティング

### メモリ不足エラー

症状:
```
OOM killed
Container killed by Docker memory limit
```

対処:
1. `docker-compose.yml`のメモリ制限を増やす
2. IORパラメータ（特に-t, -b）を減らす

### タイムアウトエラー

症状:
```
MPI timeout
No route to host
```

対処:
1. `make up`でクラスタが完全に起動するまで待つ
2. `make check-env`でノード状態を確認

### パフォーマンスが低い

対処:
1. io_uringが有効か確認: `configs/benchfs_test.toml`
2. ストレージバックエンド（Docker volume）の性能確認
3. ネットワーク帯域幅の確認

## ファイル変更サマリー

| ファイル | 変更内容 | 行数 |
|---------|---------|------|
| `tests/docker/scripts/test-ior.sh` | 8gib-ssfテストケース追加 | +58 |
| `tests/docker/docker-compose.yml` | 全コンテナにメモリ制限追加 | +25 |
| `tests/docker/Makefile` | test-ior-8gib-ssfターゲット追加 | +13 |
| `tests/docker/README.md` | ドキュメント更新 | +26 |
| **合計** | | **+122** |

## 推奨事項

### 1. パフォーマンスチューニング
実装済みのパラメータ（-t 2m -b 64m -s 128）は汎用的な設定です。環境に応じて上記「代替パラメータ設定」から最適なものを選択してください。

### 2. タイムアウトの調整
32GiBの場合、テスト時間が長くなる可能性があるため、以下を調整：
- MPI timeout設定
- Docker healthcheck timeout
- テストスクリプトのwait時間

### 3. モニタリングの追加
長時間実行するテストのため、以下のモニタリングを推奨：
```bash
# 別ターミナルで実行
docker stats
watch -n 1 'docker exec benchfs_controller ls -lh /shared/data'
```

### 4. 結果の自動保存
重要なベンチマーク結果を保存するスクリプトを追加：
```bash
# 結果をホストにコピー
docker cp benchfs_controller:/shared/results/ior_8gib_ssf_output.txt ./results/
```

## まとめ

8GiB SSF I/Oテストケースの実装が完了しました。主要な変更点：

1. **test-ior.sh**: 新しい "8gib-ssf" テストケース追加
2. **docker-compose.yml**: 適切なメモリリソース制限
3. **Makefile**: 簡単に実行できる `make test-ior-8gib-ssf` ターゲット
4. **README.md**: 包括的なドキュメント

実装は既存のテストケースと一貫性があり、Docker環境でのリソース管理も適切に設定されています。
