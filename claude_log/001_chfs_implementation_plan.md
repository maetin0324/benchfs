# CHFS実装計画

## 作成日
2025-10-17

## 目的
BenchFSプロジェクトにchfs (並列キャッシングファイルシステム) のロジックを、Rust、UCX、IOURINGを用いて実装する。

---

## 1. 現状分析

### 1.1 実装済みコンポーネント
- **非同期ランタイム**: Pluvioベースのシングルスレッド非同期ランタイム
  - タスクスケジューリング機能
  - リアクター登録機能
  - CPU affinity対応

- **RPC層**: UCX ActiveMessageベースの完全実装
  - `AmRpc` トレイト: RPC操作の汎用インターフェース
  - `RpcClient`: クライアント側実装 (execute/execute_no_reply)
  - `RpcServer`: サーバー側実装 (listen/dispatch)
  - ゼロコピーデータ転送 (zerocopyライブラリ)
  - Eager/Rendezvousプロトコル対応

- **UCX統合**:
  - Context/Worker/Endpoint管理
  - ActiveMessageストリーム
  - 非同期送受信

### 1.2 未実装コンポーネント
- IOURING統合 (依存関係のみ存在)
- ファイルシステム操作 (file_ops.rsが空)
- メタデータ管理
- データ分散ロジック
- キャッシング機構

---

## 2. CHFS アーキテクチャ分析

### 2.1 CHFS の主要コンポーネント

1. **サーバー層 (chfsd)**
   - 各計算ノードで動作するデーモン
   - ローカルストレージリソースを管理
   - クライアントリクエストをRPCで処理

2. **クライアントライブラリ**
   - ネイティブCHFS API
   - POSIX互換インターフェース

3. **通信プロトコル**
   - Mochi-margoベースのRPC (元実装)
   - RDMA転送 (大きいデータ用)
   - ソケット/TCP/UDP対応

4. **ファイルシステム操作**
   - 標準POSIX呼び出し (read/write/create/unlink)
   - 特殊操作 (stagein/sync)
   - 階層的ディレクトリ構造

5. **メタデータ管理**
   - Consistent Hashingによる分散
   - ディレクトリ/ファイルメタデータの分散配置

6. **データ分散戦略**
   - ファイルのチャンク分割
   - ノード間分散
   - I/O認識型フラッシング

---

## 3. 実装戦略

### 3.1 実装フェーズ

#### フェーズ1: ファイルシステム基盤 (Week 1-2)
**目標**: ローカルファイル操作とIOURING統合

**タスク**:
1. IOURING統合の実装
   - `pluvio_uring`を使った非同期ファイルI/O
   - open/close/read/write操作

2. ローカルファイルシステム抽象化
   - ファイルハンドル管理
   - パス解決
   - 権限チェック

3. メタデータ構造の定義
   - FileMetadata構造体
   - DirectoryMetadata構造体
   - Inode相当の情報

**成果物**:
- `src/storage/mod.rs`: ストレージ層の定義
- `src/storage/local.rs`: ローカルファイルシステム操作
- `src/storage/iouring.rs`: IOURING統合
- `src/metadata/mod.rs`: メタデータ構造

#### フェーズ2: 分散メタデータ管理 (Week 3-4)
**目標**: Consistent Hashingによるメタデータ分散

**タスク**:
1. Consistent Hashing実装
   - ノードリング構造
   - ハッシュ関数 (murmur3/xxhashなど)
   - 仮想ノード対応

2. メタデータRPC定義
   - MetadataLookup RPC
   - MetadataCreate RPC
   - MetadataDelete RPC
   - MetadataUpdate RPC

3. メタデータキャッシュ
   - ローカルキャッシュ機構
   - 一貫性保証 (invalidation)

**成果物**:
- `src/metadata/consistent_hash.rs`: Consistent Hashing実装
- `src/rpc/metadata_ops.rs`: メタデータRPC定義
- `src/metadata/cache.rs`: メタデータキャッシュ

#### フェーズ3: データ操作とチャンキング (Week 5-6)
**目標**: ファイルデータの分散読み書き

**タスク**:
1. ファイルチャンキング
   - 固定サイズチャンク分割
   - チャンク配置戦略
   - チャンクメタデータ管理

2. データRPC実装
   - ReadChunk RPC (既存のReadRPCを拡張)
   - WriteChunk RPC (既存のWriteRPCを拡張)
   - RDMA対応 (大きいチャンク用)

3. データ配置管理
   - チャンクロケーションマッピング
   - レプリケーション (オプション)

**成果物**:
- `src/data/chunking.rs`: チャンキングロジック
- `src/data/placement.rs`: データ配置戦略
- `src/rpc/data_ops.rs`: データRPC (既存を拡張)

#### フェーズ4: ファイルシステムAPI (Week 7-8)
**目標**: POSIX風APIの実装

**タスク**:
1. ファイル操作API
   - chfs_open()
   - chfs_read()
   - chfs_write()
   - chfs_close()
   - chfs_unlink()

2. ディレクトリ操作API
   - chfs_mkdir()
   - chfs_rmdir()
   - chfs_readdir()

3. 特殊操作
   - chfs_stagein() (明示的キャッシング)
   - chfs_sync() (一貫性保証)

**成果物**:
- `src/api/mod.rs`: 公開API定義
- `src/api/file_ops.rs`: ファイル操作実装
- `src/api/dir_ops.rs`: ディレクトリ操作実装

#### フェーズ5: キャッシング機構 (Week 9-10)
**目標**: ノードローカルキャッシングとフラッシング

**タスク**:
1. キャッシュマネージャ
   - LRU/LFUキャッシュポリシー
   - キャッシュサイズ制限
   - エビクション戦略

2. I/O認識型フラッシング
   - ダーティデータトラッキング
   - 非同期フラッシング
   - バッチ書き込み最適化

3. キャッシュ一貫性
   - Write-through/Write-back選択
   - Invalidationプロトコル

**成果物**:
- `src/cache/mod.rs`: キャッシュマネージャ
- `src/cache/policy.rs`: キャッシュポリシー
- `src/cache/flush.rs`: フラッシングロジック

#### フェーズ6: 統合とテスト (Week 11-12)
**目標**: システム統合とベンチマーク

**タスク**:
1. サーバーデーモン実装
   - chfsd相当のバイナリ
   - 設定ファイル読み込み
   - 起動/シャットダウン処理

2. クライアントライブラリ
   - C FFI対応 (オプション)
   - エラーハンドリング統一

3. ベンチマークとテスト
   - ユニットテスト
   - 統合テスト
   - 性能ベンチマーク

**成果物**:
- `src/bin/chfsd.rs`: サーバーデーモン
- `tests/`: テストスイート
- `benches/`: ベンチマーク

---

## 4. 技術的な意思決定事項

### 4.1 判断が必要な項目

1. **チャンクサイズ**
   - デフォルトのチャンクサイズ (候補: 1MB, 4MB, 8MB)
   - 可変チャンクサイズ対応の是非

2. **Consistent Hashingパラメータ**
   - 仮想ノード数 (候補: 100, 150, 200)
   - ハッシュ関数選択 (murmur3, xxhash, blake3)

3. **キャッシュポリシー**
   - デフォルトポリシー (LRU vs LFU)
   - キャッシュサイズの決定方法 (固定 vs メモリ比率)

4. **レプリケーション**
   - データレプリケーションの実装タイミング (初期 or 後期)
   - レプリカ数 (1, 2, 3)

5. **永続化バックエンド**
   - NVMe SSD想定
   - 永続メモリ (pmem) 対応の是非

### 4.2 暫定決定事項 (要確認)

- **チャンクサイズ**: 4MB (RDMA転送との相性を考慮)
- **仮想ノード数**: 150 (バランス重視)
- **ハッシュ関数**: xxhash (速度重視)
- **キャッシュポリシー**: LRU (実装シンプル)
- **初期実装スコープ**: レプリケーション無し (Phase 1実装)

---

## 5. プロジェクト構造 (予定)

```
benchfs/
├── src/
│   ├── main.rs                    # サーバーバイナリエントリポイント
│   ├── lib.rs                     # ライブラリルート
│   ├── api/                       # 公開API
│   │   ├── mod.rs
│   │   ├── file_ops.rs
│   │   └── dir_ops.rs
│   ├── rpc/                       # RPC層 (既存)
│   │   ├── mod.rs
│   │   ├── client.rs
│   │   ├── server.rs
│   │   ├── metadata_ops.rs        # NEW
│   │   └── data_ops.rs            # NEW (既存file_ops.rsを拡張)
│   ├── storage/                   # ストレージ層
│   │   ├── mod.rs
│   │   ├── local.rs
│   │   └── iouring.rs
│   ├── metadata/                  # メタデータ管理
│   │   ├── mod.rs
│   │   ├── consistent_hash.rs
│   │   └── cache.rs
│   ├── data/                      # データ管理
│   │   ├── mod.rs
│   │   ├── chunking.rs
│   │   └── placement.rs
│   ├── cache/                     # キャッシング
│   │   ├── mod.rs
│   │   ├── policy.rs
│   │   └── flush.rs
│   └── bin/
│       └── chfsd.rs               # サーバーデーモン
├── examples/
│   ├── rpc_example.rs             # 既存
│   └── chfs_example.rs            # NEW
├── tests/
│   └── integration_tests.rs
├── benches/
│   └── file_ops_bench.rs
└── claude_log/
    └── 001_chfs_implementation_plan.md  # このファイル
```

---

## 6. 依存関係追加 (予定)

```toml
[dependencies]
# 既存の依存関係は維持

# NEW: Hashing
xxhash-rust = "0.8"  # Consistent Hashing用

# NEW: キャッシュ
lru = "0.12"  # LRUキャッシュ実装

# NEW: 設定ファイル
serde = { version = "1.0", features = ["derive"] }
toml = "0.8"

# NEW: ロギング強化
tracing-subscriber = "0.3"
```

---

## 7. リスクと課題

### 7.1 技術的リスク

1. **IOURING統合の複雑性**
   - リスク: `pluvio_uring`のAPIが不完全な可能性
   - 対策: 早期フェーズでプロトタイプ実装

2. **分散メタデータ一貫性**
   - リスク: ノード障害時の不整合
   - 対策: フェーズ2でトランザクション機構検討

3. **RDMA性能最適化**
   - リスク: UCX ActiveMessageの性能特性が未知
   - 対策: ベンチマーク主導開発

### 7.2 スケジュールリスク

- 各フェーズ2週間は楽観的見積もり
- バッファとして+20%を想定 (全体14週間 → 17週間)

---

## 8. 次のステップ

1. **このドキュメントのレビュー**: 実装方針の承認
2. **技術的決定事項の確認**: 上記4.1の項目について判断
3. **フェーズ1の詳細設計**: IOURING統合とローカルファイルシステム
4. **プロトタイプ実装開始**: `src/storage`モジュールから着手

---

## 9. 参考資料

- [CHFS GitHub](https://github.com/otatebe/chfs)
- [UCX Documentation](https://openucx.readthedocs.io/)
- [io_uring Documentation](https://kernel.dk/io_uring.pdf)
- [Consistent Hashing](https://en.wikipedia.org/wiki/Consistent_hashing)
