# 技術的決定事項

## 作成日
2025-10-17

## 決定事項サマリー

以下の技術的決定事項が確定されました:

### 1. チャンクサイズ: 4MB

**理由**:
- RDMA転送との相性が良い
- UCX Rendezvousプロトコルの閾値 (8KB) を大幅に超える
- 小さすぎるとメタデータオーバーヘッドが増加
- 大きすぎるとキャッシュミス時のペナルティが大きい

**実装詳細**:
```rust
pub const CHUNK_SIZE: usize = 4 * 1024 * 1024; // 4MB
```

---

### 2. Consistent Hashing: xxHash64 + chfs設計参考

**ベースアルゴリズム**: xxHash64 (xxhash-rust crate使用)

**chfsからの学び** (詳細: `003_koyama_hash_analysis.md`):
- chfsはkoyama_hash (単純な合算ハッシュ) をデフォルトで使用
- koyama_hashは軽量だが衝突率が高く均等分散性が低い
- **BenchFSではxxHash64を採用** (高速 + 均等分散性)
- chfsの二分探索アルゴリズム (O(log n)) を参考
- **仮想ノードはchfsには無いが、BenchFSでは実装する**

**パラメータ**:
- 仮想ノード数: 150 (物理ノードあたり)
- ハッシュ関数: xxHash64
- データ構造: BTreeMap (ソート済み + O(log n) 検索)

**実装方針**:
```rust
// src/metadata/consistent_hash.rs
use std::collections::BTreeMap;
use xxhash_rust::xxh64::xxh64;

pub type NodeId = String;
pub type VirtualNodeId = u64;

pub struct ConsistentHash {
    /// ハッシュ値 -> 物理ノードID
    ring: BTreeMap<VirtualNodeId, NodeId>,
    /// 物理ノードあたりの仮想ノード数
    virtual_nodes_per_node: usize,
    /// 登録済み物理ノード
    nodes: Vec<NodeId>,
}

impl ConsistentHash {
    // 仮想ノード生成: "node_id:index" をハッシュ化
    pub fn add_node(&mut self, node_id: NodeId) {
        for i in 0..self.virtual_nodes_per_node {
            let vnode_key = format!("{}:{}", node_id, i);
            let hash = xxh64(vnode_key.as_bytes(), 0);
            self.ring.insert(hash, node_id.clone());
        }
    }

    // BTreeMapのrange()で二分探索 (O(log n))
    pub fn lookup(&self, key: &str) -> Option<&NodeId> {
        let hash = xxh64(key.as_bytes(), 0);
        self.ring.range(hash..)
            .next()
            .or_else(|| self.ring.iter().next())
            .map(|(_, node_id)| node_id)
    }
}
```

---

### 3. キャッシュポリシー: LRU

**理由**:
- 実装がシンプル
- 時間的局所性に強い
- `lru` crateで既製実装を利用可能

**実装詳細**:
```rust
// Cargo.toml
lru = "0.12"

// src/cache/policy.rs
use lru::LruCache;

pub struct CacheManager {
    cache: LruCache<FileId, CachedData>,
    max_size: usize,
}
```

---

### 4. レプリケーション: 実装しない

**理由**:
- 初期実装の複雑性を抑える
- 単一コピーで性能特性を評価
- 必要に応じて後期フェーズで追加可能

**影響**:
- ノード障害時のデータロス可能性あり
- 本番環境では注意が必要
- ベンチマーク/研究用途では許容範囲

---

### 5. 永続化バックエンド: NVMe SSD

**想定環境**:
- Linux io_uring対応カーネル (5.1+)
- NVMe SSD搭載ノード
- 直接I/O (Direct I/O) 使用

**IOURINGパラメータ**:
```rust
// src/storage/iouring.rs
pub const QUEUE_DEPTH: u32 = 256;
pub const IO_FLAGS: i32 = libc::O_DIRECT | libc::O_SYNC;
```

**最適化方針**:
- バッチI/O (複数操作をまとめる)
- SQポーリング (可能な場合)
- 非同期フラッシング

---

## 実装への影響

### モジュール設計への影響

1. **src/data/chunking.rs**
   - `CHUNK_SIZE = 4MB` を定数として定義
   - ファイルをチャンク単位で分割

2. **src/metadata/consistent_hash.rs**
   - xxHash64を使用
   - koyama_hashのアルゴリズムを参考実装
   - 仮想ノード数150で初期化

3. **src/cache/mod.rs**
   - `lru` crateを依存関係に追加
   - LruCache<FileId, CachedData>を使用

4. **src/storage/iouring.rs**
   - NVMe SSD向けフラグ設定
   - Direct I/Oを有効化

### 依存関係の更新

```toml
[dependencies]
# ... 既存の依存関係 ...

# Hashing
xxhash-rust = { version = "0.8", features = ["xxh64"] }

# Cache
lru = "0.12"

# Configuration
serde = { version = "1.0", features = ["derive"] }
toml = "0.8"

# Logging
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
```

---

## 次のステップ

1. koyama_hashの詳細調査完了後、実装に着手
2. フェーズ1: IOURING統合とローカルファイルシステムの実装開始
3. プロトタイプによる性能検証

---

## 変更履歴

- 2025-10-17: 初版作成、全決定事項を確定
