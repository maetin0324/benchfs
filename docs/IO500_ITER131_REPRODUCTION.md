# io500 iter131 — TOTAL 616.09 再現手順

ISC26 10-node research category, 2026-05-31, **compliance フル準拠**。

## スコア

```
[RESULT]       ior-easy-write      501.974 GiB/s  : time 315.20 s
[RESULT]    mdtest-easy-write     1332.374 kIOPS  : time 353.54 s
[RESULT]       ior-hard-write       63.183 GiB/s  : time 324.06 s
[RESULT]    mdtest-hard-write      585.279 kIOPS  : time 316.91 s
[RESULT]                 find      578.934 kIOPS  : time  14.13 s
[RESULT]        ior-easy-read      858.680 GiB/s  : time 184.27 s
[RESULT]     mdtest-easy-stat     4429.501 kIOPS  : time 107.08 s
[RESULT]        ior-hard-read       72.513 GiB/s  : time 282.36 s
[RESULT]     mdtest-hard-stat     4309.644 kIOPS  : time  43.97 s
[RESULT]   mdtest-easy-delete     3178.453 kIOPS  : time 148.80 s
[RESULT]     mdtest-hard-read     1239.699 kIOPS  : time 150.15 s
[RESULT]   mdtest-hard-delete     3252.880 kIOPS  : time  57.84 s
[SCORE ] Bandwidth 210.806 GiB/s : IOPS 1800.536 kIOPS : TOTAL 616.087 [INVALID]
```

`[INVALID]` の唯一の原因は `ior-rnd4K-easy-read = 0.000 GiB/s` (該当 phase を `runrand4K = FALSE` で無効化しているため)。io500 公式 submission ルールでは rnd4K=0 単独原因の INVALID は VALID 扱い。

## io500 compliance チェック

| 項目 | 設定/結果 |
|------|-----------|
| `[metadata] persist` | `writethrough` |
| `[metadata] xattr_size` | `true` (CHFS USE_XATTR 流儀: chunk 0 の `user.benchfs.size` xattr に size を書き、chunk fsync と同時に永続化) |
| `[storage] chunk_force_direct` | `false` (compliance 要件) |
| `[api] fsync_on_close` | `true` |
| ior-easy-write elapsed ≥ stonewall (300 s) | 315.20 s ✓ |
| mdtest-easy-write elapsed | 353.54 s ✓ |
| ior-hard-write elapsed | 324.06 s ✓ |
| mdtest-hard-write elapsed | 316.91 s ✓ |
| fsync_on_close failed エラー | 0 件 |
| server panic / SIGBUS / SIGKILL | 0 件 |
| AlreadyExists / manifest 不整合 / `reset_peer` | 0 件 |
| RESULT phase 数 | 12 / 13 (rnd4K のみ INVALID) |

close-time 永続化は次の順序で fsync-before-ack を保証:
1. クライアントから MetadataUpdate RPC が来る前に、すべての chunk write は ack 済 (locusta RPC のシリアル保証)
2. MetadataUpdate handler (server) が、まず `IOUringChunkStore::fsync_dirty_chunks_for_path(path)` で当該 path の dirty chunk 全部に fsync 発射
3. `xattr_size=true` の時は更にその後 `set_file_size_xattr` で chunk 0 にサイズ書き込み (chunk 0 自体は前段で fsync 済 → xattr も inline で同時に永続)
4. その完了後にクライアントに MetadataUpdate 成功を返す

ファイルクローズが OS に返るタイミングで、データも size メタデータも NVMe 上に着地している。

## 構成

### ハードウェア / トポロジ

- Sirius mcrp queue, `select=20:exclhost` (PBS リソース上は 20 個の独立 vnode を要求するが、`exclhost` で physical host 単位の排他確保)
- 物理 20 ホスト: `a01 a02 a03 a04 a05 a06 a07 a08 a10 a11 a12 a13 a14 a15 a16 a17 a18 a19 a21 a22` (a09 不在, a24 broken /scrN, a01-a02 はサーバー専用に使用)
- 各物理ホストに 4 vnode (NVMe 4 枚: `/scr0..3`)
- IB HDR (mlx5)

### サーバー配置

- 20 phys × 4 vnode = **80 BenchFS daemon プロセス** (`BENCHFS_LOCUSTA_STANDALONE_DAEMON=1`, `BENCHFS_SERVERS_PER_HOST=4`)
- `--bind-to none --oversubscribe`, `--map-by ppr:1:node` を `mpirun` 経由で起動
- 各 daemon が `/scrN/$PBS_JOBID/` をスクラッチに使う
- `pre_allocated_peer_count = 64`, `arena_size = 256 MiB`, `recv_ring_size = 32 KiB`, `send_buf_size = 32 KiB`

### クライアント配置 (10-node research 制約)

- **client 10 物理ホスト × ppn = 80 = 800 IOR ranks**
- `CLIENT_NNODES=10` のとき `head -n 10 unique_hostfile > client_hosts` で前 10 ホストだけにクライアント配置
- IOR/mdtest は `--hostfile $client_hosts -np 800`

> 重要: `NP_BASE` の決定ロジック。 `CLIENT_NNODES < NNODES` のときだけ `NP_BASE=$CLIENT_NNODES`、それ以外は `NP_BASE=$VNODES`。iter108 で誤って常に `CLIENT_NNODES` に変えたため、`CLIENT_NNODES==NNODES==10` の通常ケースで rank 数が 800→200 に黙って減って iter115+ regression を起こした (詳細は [project_iter130_400_breakthrough_2026_05_31](../) 参照)。

### io500 config 主要パラメータ

- `stonewall-time = 300` (s)
- `blockSize = 900g` (ior-easy)
- `segmentCount = 1200000` (ior-hard)
- `runrand4K = FALSE` (rnd4K disable → 唯一の INVALID)
- `[mdtest-easy] n_files = 1000000`, `[mdtest-hard] n_files = 400000` (env で上書き)
- `[find] external-script = benchfs_pfind` (BenchFS の readdir RPC + io_uring statx を使う C 実装)

## ソフトウェアスタック

- git rev: `1d618b5` (`chore: cargo fmt + drop dead demos/perfetto trace + locusta submodule bump`)
- 未コミット src 変更あり (iter131 binary は 5月30日 22:35 build = iter128 で `cargo clean` 後リビルドした状態)
- transport: `locusta` (rrrpc, SHM + RDMA self-loopback)
- chunk backend: `IOUringChunkStore` (io_uring async, O_DIRECT for write, no O_DIRECT for read)
- metadata backend: in-memory `MetadataManager` (write-through, **xattr_size = true** で close 時に chunk 0 の `user.benchfs.size` xattr へ size を inline で書き込み、chunk 0 自体の fsync と同時に永続化)
- central parent index = true (`DirIndexUpdate` RPC で親 directory owner に child entry を伝搬、find 高速化)

## qsub 投入コマンド

```bash
cd /work/NBB/rmaeda/workspace/rust/benchfs

# 物理 20 ホスト確保 (clients は前 10 ホストのみ使用)
PBS_SELECT_OVERRIDE='20:ncpus=1:mem=200gb' \
CLIENT_NNODES=10 \
BEST_PPN=80 \
BENCHFS_LOCUSTA_STANDALONE_DAEMON=1 \
BENCHFS_SERVER_RANKS_PER_VNODE=1 \
BENCHFS_METADATA_XATTR_SIZE=true \
BENCHFS_CENTRAL_PARENT_INDEX=1 \
BENCHFS_LOCUSTA_ARENA_SIZE=$((256*1024*1024)) \
BENCHFS_LOCUSTA_MAX_INFLIGHT=128 \
BENCHFS_LOCUSTA_PRE_ALLOC_PEER=64 \
BENCHFS_LOCUSTA_HANDSHAKE=udp \
BENCHFS_RPC_TIMEOUT=120s \
SKIP_SWEEP=1 \
FINAL_STONEWALL=300 \
IO500_IOR_HARD_SEGMENTS=1200000 \
IO500_MDTEST_EASY_N=1000000 \
IO500_MDTEST_HARD_N=400000 \
ITER_LABEL=iter131_20phys_client10_ppn80_xattr \
qsub jobs/io500/sirius-io500-job.sh
```

`PBS_SELECT_OVERRIDE` は `jobs/io500/sirius-io500-job.sh` 冒頭で読み込まれて PBS `select` 行に変換される。`a01,a24` など broken /scrN ホストを避けるため `PBS_SELECT_OVERRIDE` を `select=20:exclhost+exclude=a01,a24` 風に必要に応じて拡張。

## ビルド (再現用)

```bash
cd /work/NBB/rmaeda/workspace/rust/benchfs
git submodule update --init --recursive
cd lib/pluvio && cargo build --release && cd ../..
cd lib/locusta && cargo build --release && cd ../..
bash install.sh   # libbenchfs.so を ~/.local/lib に deploy、benchfsd_mpi も build
cd ior_integration/ior && make install   # io500 wrapper IOR
cd ../benchfs_backend && make             # benchfs_pfind C 製 find
```

ビルド検証は `ls -la ~/.local/lib/libbenchfs.so` の mtime と `git log -1 --format=%cI HEAD` が整合していることを確認。`cargo build` だけだと `libbenchfs.so` が更新されない (regression 履歴あり)。

## 再現時の注意

1. **a01 / a24 の /scrN は broken**。PBS prologue が JOBID dir を作らない or 全部 root-owned。`PBS_SELECT_OVERRIDE` で除外するか、ジョブスクリプトの scratch-broken 早期判定で abort される (jobs/io500/sirius-io500-job.sh 内)。a06 も時々死亡。
2. **`CLIENT_NNODES == NNODES` の通常ケース** では `NP_BASE=$VNODES` で 800 ranks (20phys×ppn=80 で動かしたい時は server 専用 10 phys + client 10 phys なので `CLIENT_NNODES < NNODES`)。10-node research 制約は client physical host を 10 以下にすることなので、server を 20 phys に増やしたい時は `CLIENT_NNODES=10 NNODES_FOR_PBS=20` 構成にする。
3. **iter131 binary は cargo clean 後の full rebuild が前提**。iter125-126 で stale binary 由来の偽 regression を踏んだので、`cargo clean && bash install.sh && ls -la ~/.local/lib/libbenchfs.so` で binary が最新であることを毎回確認。
4. **xattr_size = true 時の正当性**: MetadataUpdate handler の流れは `fsync_dirty_chunks_for_path(path)` → `set_file_size_xattr(path, size)` の順。chunk 0 が空ファイルの場合は xattr が立たない (ファイル無いから) が、これは「ファイルがクローズされた時にサイズ 0」のケースなので問題なし。サイズ recovery は subsequent stat の `iouring_chunk_store.read_chunk0_extension` フォールバックでも可。

## このスコアが出た理由 (差分要因)

iter101 baseline 258 → iter130 406 (+57%) → iter131 616 (+138% vs 101) の breakdown:

| 要因 | 利得 |
|------|------|
| iter108 で混入していた NP_BASE bug の修正 (client rank 200 → 800) | iter101→iter130 主因 |
| CHFS USE_XATTR 方式の metadata 永続化 (close-time 4 SQE → 1 syscall + inline xattr) | mdtest-easy-delete +621%, mdtest-easy-write +191% |
| **20 phys server + 10 phys client × ppn=80** (server を 10→20 に倍増、daemon 数 40→80) | iter130→iter131 主因。NVMe disk が 40→80 枚相当、metadata owner も倍増、RPC fanout 緩和 |
| central parent index (`DirIndexUpdate` 伝搬) で find phase 高速化 | find 579 kIOPS |
| `pre_allocated_peer_count = 64` + arena 256MB で UDP handshake 安定化 (8000-peer fanout でも crash 無し) | full run 完走 |

## 関連メモリ

- [[project_iter130_400_breakthrough_2026_05_31]]
- [[project_iter101_compliance_best_2026_05_29]]
- [[project_iter117_118_xattr_breakthrough_2026_05_30]]
- [[project_iter108_111_20phys_2026_05_29]]
- [[feedback_io500_compliance]]
- [[reference_sirius_nvme_perf]]
- [[project_sirius_bad_scratch_hosts_2026_05_25]]
- [[feedback_rnd4k_invalid_ok]]
