# BenchFS マルチRPC性能問題の調査結果

## 調査実施日: 2026-01-24

---

## 現状の症状

### 設定
- transfer_size: 2 MiB
- chunk_size: 1 MiB
- ノード数: 16
- クライアント数: 736 (46 プロセス × 16 ノード)
- **1回のtransferで2つのRPCが異なるノードに送信される**

### 観測された問題
| 指標 | 観測値 | 期待値 |
|------|--------|--------|
| WRITE スループット | 48.28 MiB/s | ~88 GiB/s |
| READ スループット | 49.16 MiB/s | ~88 GiB/s |
| RPC WRITE 平均レイテンシ | **15.3 秒** | < 100 ms |
| RPC READ 平均レイテンシ | **13.7 秒** | < 100 ms |
| RPC P90 レイテンシ | **~30 秒** (タイムアウト閾値) | < 500 ms |
| リトライ率 | 34.55% | < 1% |

---

## Phase 1: データ収集と可視化 ✅完了

### 結果ディレクトリ
- 異常時: `results/benchfs/2026.01.24-22.49.26-debug_large/2026.01.24-22.56.47-541890.nqsv-16/`
- 正常時: `results/benchfs/2026.01.24-16.21.10-debug_large/2026.01.24-16.21.23-541415.nqsv-16/`

### 発見事項

#### 1.1 RPCレイテンシ時系列パターン
- **初期 (time_bucket 50370-50371)**: avg_elapsed_us = 1,300,000-1,400,000 (1.3-1.4秒)
- **後半 (time_bucket 50398+)**: avg_elapsed_us = 15,000,000+ (15秒以上)
- **max_elapsed_us**: 30,100,000前後 (30秒 = タイムアウト閾値)

#### 1.2 リトライ統計
- 異常時: 各クライアント22リクエスト、7-8リトライ (31-36%)
- 正常時: 各クライアント~8000リクエスト、リトライ0回 (0.00%)

#### 1.3 io_depth分析
- 40.4%がio_depth=1 (unbatched)
- 一部ノードはmax 553まで到達
- 効率: 23.5% (理想batch_size=64に対して)

---

## Phase 2: 比較分析 ✅完了

### 設定比較
| 設定 | 正常 (4m/4m) | 異常 (2m/1m) |
|------|-------------|--------------|
| transfer_size | 4 MiB | 2 MiB |
| chunk_size | 4 MiB | 1 MiB |
| 比率 | 1:1 | 2:1 |
| RPC数/transfer | 1 | 2 |

### 性能比較
| 指標 | 正常 (4m/4m) | 異常 (2m/1m) |
|------|-------------|--------------|
| クライアント毎総RPC数 | ~8,000 | 22 |
| **リトライ率** | **0.00%** | **34.55%** |
| RPCレイテンシ | 1.3-1.4秒 | 15-30秒 |
| スループット | 正常 (~数GiB/s) | 48 MiB/s |

### 結論
- transfer_size = chunk_size の場合: 正常動作
- transfer_size > chunk_size の場合: 性能崩壊

---

## Phase 3: コードパス分析 ✅完了

### 3.1 クライアント側 (`src/api/file_ops.rs`)

```rust
// 行851-854: チャンク分割
let chunks = self.chunk_manager.calculate_read_chunks(offset, length, new_size)?;

// 行863-1001: 並列RPC発行
let chunk_write_futures: Vec<_> = chunks.iter().enumerate().map(...).collect();

// 行1003-1004: 全チャンク同時待機
let write_results_handles = join_all(chunk_write_futures).await;
```

**問題**: `join_all`で全チャンクのRPCを同時発行。transfer_size=2MiB, chunk_size=1MiBの場合、
736クライアント × 2チャンク = **最大1472の同時RPC**がサーバーに殺到。

### 3.2 サーバー側 (`src/rpc/server.rs`)

```rust
// 行221-239: リクエスト毎に新タスクspawn
pluvio_runtime::spawn(async move {
    let _guard = RpcRequestGuard::new(rpc_id);
    match Rpc::server_handler(ctx_clone, am_msg).await {
        ...
    }
});
```

**問題**: 同時実行タスク数に**制限がない**。1000+リクエストが同時到着すると1000+タスクが生成される。

### 3.3 RPCクライアント (`src/rpc/client.rs`)

```rust
const DEFAULT_RPC_TIMEOUT_SECS: u64 = 30;  // 30秒タイムアウト
const DEFAULT_RPC_RETRY_COUNT: u32 = 3;     // 3回リトライ
const DEFAULT_RPC_RETRY_DELAY_MS: u64 = 100; // 初期遅延100ms
const DEFAULT_RPC_RETRY_BACKOFF: f64 = 2.0;  // 指数バックオフ
```

### 3.4 バッファアロケーター (`lib/pluvio/pluvio_uring/src/allocator.rs`)

```rust
// バッファ枯渇時はPendingを返して待機キューに追加
if let Some(buffer) = this.allocator.acquire_inner() {
    Poll::Ready(buffer)
} else {
    // 待機キューに追加
    this.allocator.acquire_queue.borrow_mut().push_back(state_clone);
    Poll::Pending
}
```

**問題**: バッファプールが枯渇すると後続のリクエストは待機状態になる。

---

## Phase 4: 仮説検証 🔄進行中

### 仮説A: クライアント側同時RPC過多 ✅確認済み
- **検証方法**: コード分析
- **結果**: `join_all`で全チャンクを同時発行。transfer/chunk比率が2:1の場合、RPC数が2倍になる。
- **根本原因として確認**

### 仮説B: サーバー側リソース競合 ✅確認済み
- **検証方法**: server.rsのコード分析
- **結果**: タスクspawnに制限がない。大量のリクエストで過負荷状態になる。
- **寄与要因として確認**

### 仮説C: ネットワーク輻輳 ❓未検証
- RPCレイテンシの増加パターンから、ネットワーク自体の問題ではなくサーバー側の処理遅延と推定

### 仮説D: io_uringキュー飽和 ❓未検証
- queue_size=16384, submit_depth=512の設定
- バッファプール枯渇と関連する可能性あり

### 仮説E: バッファプール枯渇 ✅確認済み
- **検証方法**: allocator.rsのコード分析
- **結果**: バッファ枯渇時は待機キューでブロック。大量の同時リクエストでバッファが不足すると、
  後続リクエストの処理が遅延し、タイムアウトの連鎖を引き起こす。
- **寄与要因として確認**

---

## 問題のメカニズム（確定）

```
[クライアント側]
  736クライアント × 2チャンク/transfer = 1472 同時RPC
                    ↓
[サーバー側]
  1472タスクが同時spawn → バッファプール枯渇
                    ↓
  後続リクエストがバッファ待ち → 処理遅延累積
                    ↓
  30秒タイムアウト発生 → リトライ発生
                    ↓
  リトライで負荷増加 → さらにタイムアウト増加
                    ↓
  **タイムアウト/リトライのカスケード** (34%リトライ率)
```

---

## Phase 5: 改善案 📋準備中

### 考えられる改善方向

1. **クライアント側のRPC同時発行数制限** (最優先)
   - Semaphoreで同時発行RPC数を制限
   - transfer_size > chunk_sizeの場合でも安定動作

2. **サーバー側のリクエストキューイング**
   - 同時処理タスク数の上限設定
   - バックプレッシャー機構の導入

3. **タイムアウト値の調整**
   - クライアント側タイムアウトを短縮してリトライを早期化
   - または、タイムアウトを延長してリトライ頻度を下げる

4. **バッファプールサイズの動的調整**
   - 負荷に応じてバッファプールを拡張
   - メモリ使用量とのトレードオフ

### 推奨アプローチ
**クライアント側のRPC同時発行数制限**が最も効果的。
サーバー側の変更なしで問題を解決できる可能性が高い。

---

## 参照ファイル

| ファイル | 内容 |
|---------|------|
| `src/api/file_ops.rs` | クライアント側チャンク分割・RPC発行ロジック |
| `src/rpc/server.rs` | サーバーRPC処理ロジック |
| `src/rpc/client.rs` | RPCクライアント（タイムアウト・リトライ） |
| `lib/pluvio/pluvio_uring/src/allocator.rs` | バッファプールアロケーター |
| `src/bin/benchfsd_mpi.rs` | サーバー起動・設定 |
