# UCX通信パラメータ最適化ガイド

## 問題の要約

IORベンチマークでRPCが返ってこない問題が発生していました。詳細な調査の結果、以下が判明しました：

### 根本原因
- **UCX UDトランスポート**（Unreliable Datagram）が使用されていた
- UDは大規模データ転送に不向きで、パケットロスが発生しやすい
- 輻輳ウィンドウが最小値（cwnd=2）まで低下
- サーバー側は応答を送信しているが、クライアント側で受信できない

### 症状
```
サーバー側: WriteChunk処理 OK → 応答送信 OK (am_send: complete)
クライアント側: wait_msg() 無限待機 → "Runtime may be stuck"
UCXログ: "ca drop@cwnd = 2 in flight: 1" 大量発生
```

---

## 解決方法

### 🔴 最優先：トランスポート層をRCに変更

```bash
# 設定ファイルを読み込む
source benchfs_ucx_optimized.env

# または直接環境変数を設定
export UCX_TLS=rc_mlx5,self  # RCトランスポート使用
```

この変更だけで問題が解決する可能性が高いです。

---

## 使用方法

### 1. 本番環境（InfiniBand使用）

```bash
# 最適化設定を読み込む
source /work/0/NBB/rmaeda/workspace/rust/benchfs/benchfs_ucx_optimized.env

# MPIジョブ実行
mpirun -np 4 \
    -x UCX_TLS \
    -x UCX_NET_DEVICES \
    -x UCX_RC_TIMEOUT \
    -x UCX_AM_MAX_EAGER \
    -x RUST_LOG \
    ./target/release/benchfsd_mpi --config benchfs.toml

# IORベンチマーク
mpirun -np 4 \
    -x UCX_TLS \
    -x UCX_NET_DEVICES \
    ./ior -a BENCHFS -t 1m -b 16m -s 4
```

### 2. デバッグ環境（TCP使用）

```bash
# デバッグ設定を読み込む
source /work/0/NBB/rmaeda/workspace/rust/benchfs/benchfs_ucx_debug.env

# ローカルテスト実行
mpirun -np 2 \
    -x UCX_TLS \
    -x UCX_LOG_LEVEL \
    -x RUST_LOG \
    ./target/debug/benchfsd_mpi --config benchfs.toml
```

---

## 設定ファイル一覧

| ファイル | 用途 | トランスポート |
|---------|------|--------------|
| `benchfs_ucx_optimized.env` | 本番環境 | RC (InfiniBand) |
| `benchfs_ucx_debug.env` | デバッグ | TCP（IB不要） |

---

## 段階的な最適化

問題が解決しない場合、以下の順で設定を追加してください：

### Phase 1: トランスポート層変更（必須）
```bash
export UCX_TLS=rc_mlx5,self
```

### Phase 2: タイムアウト延長
```bash
export UCX_RC_TIMEOUT=2.0s
export UCX_RC_RETRY_COUNT=16
```

### Phase 3: AMバッファ増加
```bash
export UCX_AM_MAX_EAGER=8192
export UCX_RNDV_THRESH=16384
```

### Phase 4: RDMA最適化
```bash
export UCX_ZCOPY_THRESH=0
export UCX_RNDV_SCHEME=get_zcopy
```

---

## トラブルシューティング

### Q1: InfiniBandデバイスが見つからない

**エラー例:**
```
Failed to create AM stream: Transport not available
```

**解決方法:**
```bash
# デバイス確認
ibv_devices

# 利用可能なトランスポート確認
ucx_info -d

# TCPにフォールバック
export UCX_TLS=tcp,self
```

### Q2: 依然としてタイムアウトが発生する

**確認事項:**
```bash
# UCX設定が正しく読み込まれているか確認
echo $UCX_TLS
echo $UCX_RC_TIMEOUT

# ログレベルを上げて詳細確認
export UCX_LOG_LEVEL=debug
export RUST_LOG=benchfs=trace
```

### Q3: 性能が低い

**最適化案:**
```bash
# RDMAゼロコピー有効化
export UCX_ZCOPY_THRESH=0

# 受信キュー増加
export UCX_RC_MLX5_RX_QUEUE_LEN=4096

# Rendezvous閾値調整（大きいデータの場合）
export UCX_RNDV_THRESH=32768  # 32KB
```

---

## 詳細な原因分析

詳細な調査結果は、以下に記載されています：

### タイムライン分析
```
15:47:21.401 - サーバー: WriteChunk処理開始
15:47:21.406 - サーバー: データ受信完了 (UCS_OK)
15:47:21.410 - サーバー: IO処理完了
15:47:21.417 - サーバー: 応答送信完了 (am_send: complete)
15:48:31-42  - クライアント: "Runtime may be stuck" × 70万回
```

### UCX統計
```
全エンドポイント: cwnd=2（最小値）
トランスポート: UD (Unreliable Datagram)
パケットロス率: 非常に高い（ca drop 大量発生）
```

---

## UCXパラメータリファレンス

### トランスポート層
- `UCX_TLS`: 使用するトランスポート層
  - `rc_mlx5`: RC over Mellanox HCA（推奨）
  - `rc_verbs`: RC over IB Verbs
  - `dc`: Dynamically Connected（スケーラブル）
  - `ud`: Unreliable Datagram（**非推奨**）
  - `tcp`: TCP/IP（フォールバック）

### タイムアウト
- `UCX_RC_TIMEOUT`: ACK待機時間（例: 2.0s）
- `UCX_RC_RETRY_COUNT`: リトライ回数（デフォルト: 7）
- `UCX_RC_TIMEOUT_MULTIPLIER`: タイムアウト乗数（デフォルト: 2.0）

### Active Message
- `UCX_AM_MAX_SHORT`: Short AMサイズ（デフォルト: 128B）
- `UCX_AM_MAX_EAGER`: Eager AMサイズ（デフォルト: 8KB）
- `UCX_RNDV_THRESH`: Rendezvous閾値（デフォルト: 8KB）

### RDMA
- `UCX_ZCOPY_THRESH`: ゼロコピー閾値（0 = 常時有効）
- `UCX_RNDV_SCHEME`: Rendezvous方式
  - `get_zcopy`: GET with zero-copy（推奨）
  - `put_zcopy`: PUT with zero-copy

---

## 参考資料

- UCX公式ドキュメント: https://openucx.readthedocs.io/
- UCX GitHub: https://github.com/openucx/ucx
- UCX環境変数一覧: `ucx_info -f`

---

## お問い合わせ

問題が解決しない場合は、以下の情報を添えてご連絡ください：

```bash
# 環境情報収集
ucx_info -v
ibv_devinfo
cat benchfs.toml
env | grep UCX
env | grep RUST_LOG

# ログ収集（最初の100行と最後の100行）
head -100 benchfsd_stdout.log > debug_start.log
tail -100 benchfsd_stdout.log > debug_end.log
```
