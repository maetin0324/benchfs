# io_uring Read Performance Analysis

## 概要

BenchFSにおけるRead性能がWrite性能に比べて極端に低い（4-8倍遅い）問題の原因を調査した結果をまとめる。

**結論**: io_uringの問題ではなく、**特定ノードのハードウェア/構成問題**が根本原因であることが判明した。

---

## 1. 問題の発見

### IORベンチマーク結果

| Run | Tasks | Write (MiB/s) | Read (MiB/s) | Write/Read比 |
|-----|-------|---------------|--------------|--------------|
| 0   | 16    | 68,868        | 14,616       | 4.7x         |
| 4   | 128   | 45,617        | 10,131       | 4.5x         |
| 8   | 512   | 78,392        | 10,064       | 7.8x         |

### 初期観察

- Read性能がタスク数に関わらず約10 GiB/sで頭打ち
- Write性能は68-78 GiB/sでスケール
- Readレイテンシが512タスク時に16倍悪化（0.0044s → 0.0733s）

---

## 2. 初期仮説: io_uringのバッチ効率

### 計測の実装

`src/storage/iouring.rs` と `lib/pluvio/pluvio_uring/src/reactor.rs` に計測コードを追加。

### 分析スクリプト

- `plot/analyze_io_timing.py` - I/Oタイミング分析
- `plot/analyze_io_depth.py` - io_depth（バッチサイズ）分析

### 計測結果（wait_submit_timeout=1µs）

#### io_depth統計

| 指標 | 値 |
|------|-----|
| Total submits | 3,717,256 |
| Mean io_depth | 1.01 |
| io_depth=1の割合 | 98.8% |
| Max io_depth | 18 |

**問題点**: 98.8%のsubmitがio_depth=1で、バッチ処理が全く行われていなかった。

#### I/Oタイミング統計

| 指標 | 値 |
|------|-----|
| Total entries | 754,458 |
| Median time | 654µs |
| Mean time | 294ms |
| P95 time | 1.85s |
| P99 time | 7.13s |

**観察**: Medianは高速だが、Meanが大きく乖離 → 二峰性分布の存在を示唆。

#### 時間分布

| 範囲 | 割合 |
|------|------|
| 0-100µs | 0% |
| 100-500µs | 1.7% |
| 500µs-1ms | 60.4% |
| 1-5ms | 0.4% |
| 5-10ms | 4.2% |
| 10-50ms | 20.1% |
| 50-100ms | 2.0% |
| >100ms | **11.2%** |

---

## 3. 実験: wait_submit_timeout の調整

### 変更内容

`wait_submit_timeout` を 1µs → 10ms に変更してバッチ効率向上を試みた。

### 結果比較

| 指標 | Before (1µs) | After (10ms) | 変化 |
|------|--------------|--------------|------|
| IOR Read | 10,064 MiB/s | 9,851 MiB/s | -2% |
| IOR Write | 78,392 MiB/s | 81,214 MiB/s | +4% |
| Mean io_depth | 1.01 | 4.61 | +356% |
| io_depth=1の割合 | 98.8% | 48.3% | 改善 |
| I/O Median Time | 654µs | 20.8ms | **31倍悪化** |
| I/O Median BW | 6,113 MiB/s | 192 MiB/s | **32倍悪化** |

### 結論

io_depthは改善されたが、タイムアウト待ちによるレイテンシ増加で相殺され、性能は改善しなかった。

**io_uringのバッチ効率は根本原因ではない。**

---

## 4. 根本原因の特定

### 遅いI/O操作の詳細分析

100ms以上かかる操作を詳細に分析した。

#### ノード別統計

| ノード | Total Ops | Slow Ops | Slow率 | Mean Time |
|--------|-----------|----------|--------|-----------|
| **bnode045** | 47,789 | 47,771 | **100.0%** | **4,340ms** |
| **bnode004** | 47,046 | 26,305 | **55.9%** | 188ms |
| **bnode013** | 47,081 | 10,662 | **22.6%** | 68.6ms |
| bnode046 | 47,082 | 0 | 0.0% | 0.65ms |
| bnode044 | 47,787 | 0 | 0.0% | 0.69ms |
| bnode006 | 47,053 | 0 | 0.0% | 0.65ms |
| bnode010 | 47,082 | 0 | 0.0% | 0.65ms |
| bnode011 | 47,081 | 0 | 0.0% | 0.65ms |
| bnode014 | 47,064 | 0 | 0.0% | 0.65ms |
| bnode015 | 47,064 | 0 | 0.0% | 12.3ms |
| bnode017 | 47,062 | 0 | 0.0% | 19.4ms |
| bnode018 | 47,057 | 0 | 0.0% | 16.9ms |
| bnode032 | 47,055 | 0 | 0.0% | 0.65ms |
| bnode040 | 47,052 | 0 | 0.0% | 0.65ms |
| bnode041 | 47,052 | 0 | 0.0% | 0.65ms |
| bnode042 | 47,051 | 0 | 0.0% | 0.65ms |

#### 時系列分析（bnode045）

ベンチマーク全期間（304秒）を通じて一貫して100%の操作が遅い：

| 時間 (s) | Count | Slow | Slow% | Mean (ms) |
|----------|-------|------|-------|-----------|
| 0-10 | 1,570 | 1,554 | 99.0% | 3,154 |
| 10-20 | 1,574 | 1,574 | 100.0% | 4,549 |
| 20-30 | 1,570 | 1,570 | 100.0% | 3,838 |
| ... | ... | ... | 100.0% | ~4,000 |
| 190-200 | 1,555 | 1,555 | 100.0% | 4,387 |

ウォームアップの問題ではなく、**恒常的な問題**。

#### 同時刻での他ノードとの比較

同じ時間帯（2026-01-16 09:53:50 〜 09:58:54）での比較：

| ノード | Operations | Mean Time | Slow率 |
|--------|------------|-----------|--------|
| bnode045 | 47,787 | 4,340ms | 100% |
| bnode046 | 47,082 | 0.65ms | 0% |
| bnode044 | 47,768 | 0.69ms | 0% |
| bnode006 | 47,053 | 0.65ms | 0% |

**bnode045は他ノードより約6,000倍遅い** - これはソフトウェアの問題ではない。

---

## 5. 追加検証: 問題ノード除外後のベンチマーク

### 概要

前回特定した問題ノード（bnode045, bnode004, bnode013）を除外して再ベンチマークを実施。

### ベンチマーク環境

- **日時**: 2026-01-16 23:06:12
- **ノード**: bnode008, 012, 014-026, 028（16ノード）
- **設定**: wait_submit_timeout=10ms

### IOR結果

| Operation | BW (MiB/s) | Latency (s) | Time (s) |
|-----------|------------|-------------|----------|
| Write     | 78,271     | 0.0320      | 154.06   |
| Read      | 9,813      | 0.0738      | 302.32   |

**Write/Read比: 8.0x** - 問題ノード除外後も性能比は改善せず。

### ノード別READ性能分析

| ノード | READ Ops | Slow Ops (>100ms) | Slow率 | Mean Time | READ/WRITE比 |
|--------|----------|-------------------|--------|-----------|--------------|
| **bnode025** | 47,167 | 47,167 | **100.0%** | **2,684ms** | **83.3x** |
| **bnode026** | 47,164 | 41,355 | **87.7%** | 894ms | 25.3x |
| **bnode019** | 47,168 | 37,661 | **79.8%** | 596ms | 27.9x |
| bnode020 | 46,894 | 0 | 0.0% | 12.9ms | 0.58x |
| bnode021 | 46,896 | 0 | 0.0% | 12.4ms | 0.56x |
| bnode008 | 46,885 | 0 | 0.0% | 12.9ms | **0.52x** |
| bnode014 | 46,877 | 0 | 0.0% | 14.9ms | **0.48x** |
| bnode028 | 46,872 | 0 | 0.0% | 11.2ms | **0.40x** |

### 重要な発見

1. **新たな問題ノードの発見**: bnode025, bnode026, bnode019が極端に遅い
2. **READ/WRITE非対称性**: 問題ノードはWriteは正常だがReadが極端に遅い
3. **一部ノードはReadがWriteより速い**: bnode008, bnode014, bnode028はREAD/WRITE比が1未満

### 時間分布（遅いノードのREAD）

| 範囲 | 操作数 | 割合 |
|------|--------|------|
| 0-100ms | 14,778 | 10.6% |
| 100-500ms | 17,491 | 12.5% |
| 500-1000ms | 52,539 | 37.5% |
| 1000-2000ms | 25,477 | 18.2% |
| 2000-4000ms | 15,427 | 11.0% |
| 5000-10000ms | 6,192 | 4.4% |

### File-per-proc モードでの検証

Shared-file モードが原因の可能性を排除するため、file-per-proc モードでもテストを実施。

**結論**: file-per-proc モードでも同様の性能特性を示す。Shared-file モードのメタデータ競合は根本原因ではない。

---

## 6. 結論

### 根本原因

**特定ノードのNVMe READ性能の問題**

- 問題は**特定ノードのREAD操作のみ**に発生
- 同じノードのWRITE操作は正常に動作
- io_uringやBenchFSのソフトウェア実装の問題ではない
- アクセスパターン（shared-file vs file-per-proc）も原因ではない

### 証拠

1. 正常なノードのREAD: ~12ms/操作
2. 問題ノード（bnode025）のREAD: 2,684ms/操作（224倍遅い）
3. **同じ問題ノードのWRITEは正常**: bnode025のWRITE=32.2ms（READより83倍速い）
4. ベンチマーク全期間で一貫して問題が発生
5. 問題ノードは実行ごとに異なる可能性がある

### 問題ノードの一覧（複数回の検証から）

| ノード | 検出日時 | Slow率 | Mean Time | 状態 |
|--------|----------|--------|-----------|------|
| bnode045 | 01/16 09:53 | 100% | 4,340ms | 確認済み |
| bnode004 | 01/16 09:53 | 56% | 188ms | 確認済み |
| bnode013 | 01/16 09:53 | 23% | 68.6ms | 確認済み |
| bnode025 | 01/16 23:07 | 100% | 2,684ms | **新規** |
| bnode026 | 01/16 23:07 | 88% | 894ms | **新規** |
| bnode019 | 01/16 23:07 | 80% | 596ms | **新規** |

### 考えられるハードウェア原因

1. **NVMe SSDのREAD性能劣化**（WRITEは正常なためREAD特有の問題）
2. **NANDフラッシュのread disturb**による劣化
3. **SSDのファームウェア問題**（READ処理パスのバグ）
4. **サーマルスロットリング**（特にREAD負荷時）
5. **PCIeリンク品質**（特定の転送パターンで問題発生）

### 推奨アクション

```bash
# 問題ノードのNVMe状態確認
ssh bnode025 'sudo nvme smart-log /dev/nvme0n1'
ssh bnode025 'sudo dmesg | grep -i nvme'

# ディスクI/O状態確認（READ/WRITE別）
ssh bnode025 'fio --name=read_test --rw=read --bs=4M --size=1G --direct=1 --runtime=30'
ssh bnode025 'fio --name=write_test --rw=write --bs=4M --size=1G --direct=1 --runtime=30'

# NVMe SMART情報詳細
ssh bnode025 'sudo nvme smart-log /dev/nvme0n1 -H'

# ファイルシステム状態確認
ssh bnode025 'sudo xfs_info /scr'
ssh bnode025 'df -h /scr'
```

### 性能改善への道筋

1. **短期**: 問題ノード（bnode025, bnode026, bnode019等）をベンチマークから除外
2. **短期**: 各ノードのREAD/WRITE性能を事前にプロファイリングしてから選択
3. **中期**: ハードウェア診断と修復/交換
4. **長期**: ノード健全性監視の自動化導入

---

## 付録: 分析ツールの使用方法

### I/Oタイミング分析

```bash
cd plot && source .venv/bin/activate
python analyze_io_timing.py <log_file> [--output csv] [--by-node]
```

### io_depth分析

```bash
cd plot && source .venv/bin/activate
python analyze_io_depth.py <log_file> [--output csv] [--by-node]
```

### ログ出力の有効化

```bash
RUST_LOG_S="info,benchfs::storage=debug,pluvio_uring::reactor=debug" ./benchfs.sh
```

---

## 7. 追加検証（01/16 23:44）

### 重要な発見: bnode025の回復

前回（01/16 23:06）で100% slow operationsを示していたbnode025が、約40分後の再実行では正常に動作した。

| 実行時刻 | bnode025 READ Mean | Slow% | READ/WRITE比 |
|----------|-------------------|-------|--------------|
| 01/16 23:06 | 2,684ms | 100% | **83.3x** |
| 01/16 23:44 | 22.8ms | 0% | **0.77x** |

**結論**: 問題は恒久的なハードウェア障害ではなく、**一時的/環境的要因**である可能性が高い。

### 今回の遅いノード（01/16 23:44）

| ノード | READ Mean | Slow% | READ/WRITE比 |
|--------|-----------|-------|--------------|
| bnode079 | 3,161ms | 99.97% | 73.3x |
| bnode081 | 842ms | 91.88% | 57.9x |
| bnode026 | 179ms | 56.29% | 12.3x |
| bnode074 | 181ms | 30.54% | 8.9x |
| bnode080 | 125ms | 33.97% | 8.7x |

### 結論の更新

これまでの仮説「特定ノードのハードウェア問題」は再検討が必要：

1. **一時的な問題**: bnode025の回復は、問題が恒久的ではないことを示す
2. **問題ノードの変動**: 実行ごとに異なるノードが遅くなる傾向
3. **考えられる原因**:
   - 他ジョブとの競合（I/O負荷、メモリ競合）
   - カーネル/ファイルシステムキャッシュの状態
   - NVMe SSDの内部GC（ガベージコレクション）
   - サーマルスロットリング
   - PCIeバス競合

### 次のステップ: リアルタイム診断

問題の原因を特定するため、ベンチマーク実行時に以下の診断を行う必要がある：

1. **事前診断**: fioによる各ノードのREAD/WRITE性能測定
2. **実行中監視**: iostatによるI/O統計収集
3. **事後分析**: fio結果とBenchFS結果の比較

詳細な診断手順については、`jobs/benchfs/analyze_diagnostics.sh`を参照。

---

## 参考資料

- [Comparing sequential I/O performance between io_uring and read(2)/write()](https://radiki.dev/posts/compare-sync-uring/)
- [EAGAINs impacting the performance of io_uring - liburing #1175](https://github.com/axboe/liburing/issues/1175)
- [Efficient IO with io_uring (kernel.dk)](https://kernel.dk/io_uring.pdf)
- [Missing Manuals - io_uring worker pool (Cloudflare)](https://blog.cloudflare.com/missing-manuals-io_uring-worker-pool/)
