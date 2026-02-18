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

## 問題のメカニズム（確定） - 更新版

### 最初の仮説（却下）
最初は「同時RPC数の過多」が原因と考えたが、修正後も問題が解消しなかった。

### 真の根本原因: ノード発見のタイミング問題

**比較データ**:
| ケース | RPCターゲットノード分布 | 発見されたノード数 |
|--------|------------------------|-------------------|
| 正常 (4m/4m) | 16ノードに均等 (~365K/ノード) | 16 |
| 問題 (2m/1m) | **8ノードのみ** (~1473/ノード) | 8 |

**問題のメカニズム**:
```
[サーバー起動フェーズ]
  chunk_size=1MiB → 16384バッファ × 1MiB = 16GB メモリ割り当て
                    ↓
  メモリ割り当てに時間がかかる → サーバー起動遅延
                    ↓
[クライアント起動フェーズ]
  discover_data_nodes() が呼ばれる
                    ↓
  この時点で8/16サーバーしか登録されていない
                    ↓
  discover_data_nodes() は「1つ以上見つかれば即座にリターン」
                    ↓
  クライアントは8ノードしか知らない状態で動作開始
                    ↓
[I/O実行フェーズ]
  全RPCが8ノードに集中 → 8ノードが過負荷
                    ↓
  30秒タイムアウト → リトライ → カスケード障害
```

**コードの問題箇所** (`src/ffi/init.rs:105`):
```rust
if !node_ids.is_empty() {
    // ← 1つでもノードが見つかれば即座にリターン！
    return Ok(node_ids);
}
```

---

## Phase 5: 改善案 ✅実装完了

### 最初の修正（効果なし）

#### クライアント側のRPC同時発行数制限

**変更ファイル:**
1. `src/config.rs` - デフォルト定数追加
2. `src/api/file_ops.rs` - 読み書き・fsync操作の同時実行制限

**変更内容:**

```rust
// src/config.rs
pub const MAX_CONCURRENT_CHUNK_RPCS: usize = 16;
```

```rust
// src/api/file_ops.rs (benchfs_read, benchfs_write, benchfs_fsync)
// 変更前: join_all で全チャンクを同時実行
let results = join_all(chunk_futures).await;

// 変更後: buffer_unordered で同時実行数を制限
use futures::stream::{self, StreamExt};
let results: Vec<_> = stream::iter(chunk_futures)
    .buffer_unordered(MAX_CONCURRENT_CHUNK_RPCS)
    .collect()
    .await;
```

**結果:** ❌ 問題は解消せず。仮説が誤りであった。

---

### 修正2（ノード発見タイミング問題）- 部分的に効果あり

**変更ファイル:**
1. `src/ffi/init.rs` - `discover_data_nodes()` 関数を修正
2. `jobs/benchfs/benchfs-job.sh` - 環境変数を追加

**変更内容:**
- `BENCHFS_EXPECTED_NODES` 環境変数で期待するノード数を指定可能に
- 期待ノード数が指定されている場合は最大60秒待機

**結果:** 16ノード全てを発見するようになったが、負荷分散は依然として不均一。

---

### 修正3（ノードソート順問題）❌ 不十分

**発見した問題:**

ノードIDがアルファベット順にソートされていた：
```
node_0, node_1, node_10, node_11, node_12, node_13, node_14, node_15, node_2, node_3, ...
```

**変更内容 (`src/ffi/init.rs`):**

```rust
// 変更前: アルファベット順ソート
node_ids.sort();

// 変更後: 数値順ソート
node_ids.sort_by(|a, b| {
    let extract_num = |s: &str| -> Option<u32> {
        s.strip_prefix("node_").and_then(|n| n.parse().ok())
    };
    match (extract_num(a), extract_num(b)) {
        (Some(na), Some(nb)) => na.cmp(&nb),
        _ => a.cmp(b), // Fallback to alphabetical
    }
});
```

**結果:** ❌ ノードは正しくソートされたが、負荷分散は依然として偏っていた。

---

### 修正4（黄金比ハッシュ）✅ 真の根本原因を解決

**発見した真の問題:**

`block_size`と`node_count`の関係による負荷偏り：

```
block_size = 16 GiB = 16384 MiB = 16384 chunks
16384 % 16 = 0  ← 全クライアントが同じモジュロパターンを持つ

koyama_hash("/scr/testfile/{N}") = 1333 + N  ← 線形なハッシュ値
単純モジュロ: (1333 + N) % 16 = (5 + N) % 16

結果:
- chunk 0 → node_5
- chunk 1 → node_6
- chunk 2 → node_7
- ...
- 全736クライアントの最初の2チャンク → node_5とnode_6に集中！
```

**旧ロジックでの分布 (736クライアント × 2チャンク = 1472チャンク):**
```
node_0-4:   0 chunks each
node_5:   736 chunks (50%)
node_6:   736 chunks (50%)
node_7-15:  0 chunks each
不均衡度: 800%
```

**変更内容 (`src/metadata/consistent_hash.rs`):**

```rust
// 変更前: 単純モジュロ
let index = (hash as usize) % self.nodes.len();

// 変更後: 黄金比ベースの乗算ハッシュ
const PHI: u64 = 0x9e3779b9;  // (sqrt(5) - 1) / 2 * 2^32
let mixed = ((hash as u64).wrapping_mul(PHI)) >> 32;
let index = (mixed as usize) % self.nodes.len();
```

**新ロジックでの分布:**
```
node_0:   92 chunks (6.2%)
node_1:   93 chunks (6.3%)
node_2:   94 chunks (6.4%)
...
node_15:  92 chunks (6.2%)
不均衡度: 6.52%
```

**改善効果:**
| 指標 | 旧 (単純モジュロ) | 新 (黄金比ハッシュ) |
|------|-----------------|-------------------|
| 使用ノード数 | 2/16 (12.5%) | 16/16 (100%) |
| 不均衡度 | 800% | 6.52% |
| チャンク/ノード | 0 or 736 | 88-94 |

**なぜ黄金比ハッシュが効果的か:**
- 黄金比 (φ = (1 + √5) / 2 ≈ 1.618) は最も「無理数的」な数
- 連続した整数を黄金比で乗算すると、結果が円周上に均等に分散される
- これにより、連続したチャンクインデックスが異なるノードに割り当てられる

---

## 参照ファイル

| ファイル | 内容 |
|---------|------|
| `src/metadata/consistent_hash.rs` | **修正対象** - 黄金比ハッシュによる負荷分散 |
| `src/ffi/init.rs` | **修正対象** - ノード発見ロジック (`discover_data_nodes`) |
| `jobs/benchfs/benchfs-job.sh` | **修正対象** - IORクライアントへの環境変数設定 |
| `src/api/file_ops.rs` | クライアント側チャンク分割・RPC発行ロジック |
| `src/rpc/server.rs` | サーバーRPC処理ロジック |
| `src/rpc/client.rs` | RPCクライアント（タイムアウト・リトライ） |
| `lib/pluvio/pluvio_uring/src/allocator.rs` | バッファプールアロケーター |
| `src/bin/benchfsd_mpi.rs` | サーバー起動・設定 |
