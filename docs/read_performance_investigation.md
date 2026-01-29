# READ Performance Investigation Progress

## 問題の概要

BenchFSにおいてREAD性能がWRITE性能に比べて異常に低い。

### 観測された事実

| 指標 | WRITE | READ | 比率 |
|------|-------|------|------|
| IORスループット | 40,841 MiB/s | 9,771 MiB/s | 4.18x |
| RPCレイテンシ (avg) | 17.7 ms | 75.0 ms | 4.24x |
| iostat rkb_s (per node) | 1,177-1,205 MB/s | 143-292 MB/s | ~5x |
| iostat r_await (問題ノード) | 0.7-1.1 ms | 最大122 ms | ~100x |

比較対象 (CHFS): READ 172 GiB/s > WRITE 116 GiB/s (READの方が速い)

---

## 検証項目の進捗

### 1. サーバー側READ処理のブレークダウン ✅ 実装完了

**実装内容**: `src/rpc/data_ops.rs` に詳細なタイミングログを追加

追加した計測ポイント:
- [x] RPC受信からハンドラ開始までの時間 (parse_us)
- [x] バッファ取得時間 (buffer_acquire_us)
- [x] io_uring READ操作の時間 (io_read_us)
- [x] レスポンス構築時間 (response_construct_us)
- [x] RDMA/UCXレスポンス送信時間 (reply_us)

**ログフォーマット**:
```
RPC_TIMING_READ rpc_type="READ" chunk_id=123 total_us=1234 parse_us=10
  buffer_acquire_us=50 io_read_us=800 response_construct_us=20 reply_us=300 bytes=1048576
```

### 2. io_uring READ vs WRITE の処理差異 ✅ 実装完了

**実装内容**: `src/storage/chunk_store.rs` にチャンクI/Oのタイミングログを追加

追加した計測ポイント:
- [x] ファイルopen時間 (open_us)
- [x] io_uring READ/WRITE操作時間 (read_us / write_us)
- [x] ファイルclose時間 (close_us)

**ログフォーマット**:
```
CHUNK_IO_TIMING op_type="READ_CHUNK_FIXED" chunk_index=123 bytes_read=1048576
  open_us=100 read_us=500 close_us=50 total_us=650

CHUNK_FILE_OPEN op_type="READ" chunk_index=123 ensure_dir_us=0
  open_syscall_us=100 total_open_us=100
```

### 3. FixedBufferAllocator のバッファ枯渇

**既存のログ**: `lib/pluvio/pluvio_uring/src/allocator.rs` に既にログあり
- バッファプール枯渇時に `Buffer pool exhausted, task waiting for buffer` ログ出力
- バッファ取得時に utilization_pct を出力

**次のアクション**: 既存ログで確認可能

### 4. 特定ノードでのr_await高騰

**分析方法**:
1. `extract_iostat_csv.sh` で時系列データを抽出
2. `benchfs_analysis.ipynb` で問題ノード特定

**次のアクション**: 新しいベンチマーク実行後に分析

### 5. チャンクファイルのopen/close オーバーヘッド ✅ 実装完了

**実装内容**: `src/storage/chunk_store.rs` の `open_chunk_file()` にタイミングログを追加

### 6. RPCレスポンス送信のボトルネック ✅ 実装完了

**実装内容**: `src/rpc/data_ops.rs` の reply_vectorized 呼び出しにタイミング計測を追加

---

## 追加したExtractスクリプト

### extract_server_rpc_timing.sh ✅ 作成完了

**出力ファイル**:
- `server_rpc_timing_raw.csv`: 生のRPCタイミングデータ
- `server_rpc_timing_aggregated.csv`: 集約されたタイミングデータ
- `server_rpc_timing_summary.txt`: サマリー統計
- `chunk_io_timing_raw.csv`: チャンクI/Oタイミングデータ

---

## 次のステップ

### Step 1: テスト実行

```bash
ELAPSTIM_REQ=00:10:00 RUST_LOG_S="debug" RUST_LOG_C="debug" ENABLE_NODE_DIAGNOSTICS=1 PARAM_FILE=./jobs/params/debug_large.conf ./jobs/benchfs/benchfs.sh
```

### Step 2: データ抽出

```bash
./jobs/benchfs/extract.sh <結果ディレクトリ>
```

### Step 3: 分析

`plot/benchfs_analysis.ipynb` で以下を確認:
1. RPC_TIMING_READ vs RPC_TIMING_WRITE の比較
2. io_read_us vs io_write_us の比較
3. open/close オーバーヘッドの比較
4. バッファ枯渇の有無

---

## O_DIRECT設定について

### 現在の設定

`src/storage/chunk_store.rs` の `open_chunk_file()`:
- READ: O_DIRECT有効
- WRITE: O_DIRECT有効

**注**: 過去の調査でbuffered I/OとO_DIRECTで性能差がなかったため、両方ともO_DIRECTを使用している（意図的な設計）。

コメントは古い設計を記述しているため、更新が必要。

---

## 仮説

### 仮説1: O_DIRECT + READ の相性問題 → 除外

過去の調査でbuffered/directで性能差なしを確認済み。O_DIRECTは原因ではない。

### 仮説2: ファイルopen/closeオーバーヘッド

各READ/WRITE操作でファイルをopen/closeしている。READでは特に問題になる可能性:
- WRITEは新規ファイル作成 (O_CREAT)
- READは既存ファイルを毎回open

### 仮説3: io_uringのREAD処理とWRITE処理の非対称性

io_uring側でREADとWRITEの処理に差がある可能性。

### 仮説4: RDMA応答送信のボトルネック

READはレスポンスにデータを含むため、RDMA送信に時間がかかる可能性。

---

## 変更したファイル

| ファイル | 変更内容 |
|---------|---------|
| `src/rpc/data_ops.rs` | READ/WRITEハンドラにステップ別タイミングログ追加 |
| `src/storage/chunk_store.rs` | open_chunk_file, read_chunk_fixed, write_chunk_fixed にタイミングログ追加 |
| `jobs/benchfs/extract_server_rpc_timing.sh` | 新規作成 - サーバーRPCタイミング抽出 |
| `jobs/benchfs/extract.sh` | extract_server_rpc_timing.sh を追加 |
| `docs/read_performance_investigation.md` | 本ドキュメント |

---

## 更新履歴

- 2026-01-27: 初期計画作成、タイミングログ実装完了
