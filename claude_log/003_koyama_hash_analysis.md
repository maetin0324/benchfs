# koyama_hash 分析レポート

## 作成日
2025-10-17

## 概要

chfsの`koyama_hash`実装と、それを用いたConsistent Hashing (ring_list) の実装を詳細に調査しました。

---

## 1. koyama_hash アルゴリズム

### 1.1 実装コード

ソースファイル: `/tmp/chfs-reference/lib/koyama_hash.c`

```c
void
koyama_hash(const void *buf, size_t size, unsigned int *digest)
{
    const char *b = buf, *be = (char *)buf + size;
    unsigned int d = 0, l;

    while (b < be) {
        if (isdigit(*b)) {
            l = 0;
            while (b < be && isdigit(*b))
                l = l * 10 + *b++ - '0';
        } else
            l = *b++;
        d += l;
    }
    *digest = d;
}
```

### 1.2 アルゴリズムの特徴

**基本原理**:
- 入力バッファを1バイトずつ走査
- 数字の連続を検出した場合、それを10進数として解釈
- 数字以外の文字はASCII値をそのまま使用
- すべての値を合算してハッシュ値を生成

**例**:
- `"abc"` → `'a'(97) + 'b'(98) + 'c'(99) = 294`
- `"abc123"` → `'a'(97) + 'b'(98) + 'c'(99) + 123 = 417`
- `"abc12300"` → `'a'(97) + 'b'(98) + 'c'(99) + 12300 = 12594`

**設計思想**:
1. **シンプルさ**: 非常に軽量で高速な実装
2. **数値認識**: パス名に含まれる数値を意味的に扱う (例: "file1" と "file10" の違い)
3. **衝突許容**: セキュリティ用途ではなく、分散ハッシュ用途
4. **決定性**: 同じ入力に対して常に同じ出力

**問題点**:
- 暗号学的に安全ではない
- 衝突率が高い可能性がある
- 均等分散性は保証されない

**使用用途**:
- ファイル名/パス名からノードへのマッピング
- メタデータ分散の基本ハッシュとして使用

---

## 2. Consistent Hashing 実装 (ring_list)

### 2.1 データ構造

```c
struct ring_node {
    char *address;    // ノードアドレス (例: "192.168.1.100:1234")
    char *name;       // ノード名 (例: "node0")
    HASH_T hash;      // ノード名のハッシュ値
};

static struct ring_list {
    int n;                    // ノード数
    struct ring_node *nodes;  // ソート済みノード配列
} ring_list;
```

### 2.2 ハッシュ関数の選択

chfsは3種類のハッシュ関数をサポート (コンパイル時選択):

```c
#ifdef USE_DIGEST_MD5
    // MD5ハッシュ (128bit)
#elif defined(USE_DIGEST_MURMUR3)
    // MurmurHash3 (32bit)
#else
    // koyama_hash (32bit) - デフォルト
#endif
```

**デフォルト**: koyama_hash (32bit unsigned int)

### 2.3 ノード登録と初期化

**ring_list_update() の処理フロー**:

1. **ノード情報のコピー**:
   ```c
   for (i = 0; i < src->n; ++i) {
       ring_list.nodes[i].address = strdup(src->s[i].address);
       ring_list.nodes[i].name = strdup(src->s[i].name);
       // ノード名をハッシュ化
       HASH((unsigned char *)ring_list.nodes[i].name,
            strlen(ring_list.nodes[i].name),
            ring_list.nodes[i].hash);
   }
   ```

2. **ハッシュリングのソート**:
   ```c
   qsort(ring_list.nodes, ring_list.n, sizeof(ring_list.nodes[0]),
         ring_list_cmp);
   ```

**重要**: 仮想ノードは実装されていない
- 各物理ノードは1つのハッシュ値のみを持つ
- ノード追加/削除時のデータ移動量が多くなる可能性

### 2.4 ルックアップアルゴリズム

chfsは2つのアプローチを実装:

#### A. モジュラーハッシング (`USE_MODULAR_HASHING` 定義時)

```c
static char *
ring_list_lookup_modulo(const char *key, int key_size)
{
    HASH_T hash;
    unsigned int i, index = 0;
    int klen = strlen(key) + 1;
    char *r;

    if (klen < key_size)
        index = atoi(key + klen);
    HASH((const unsigned char *)key, klen, hash);
    ABT_mutex_lock(ring_list_mutex);
    i = HASH_MODULO(hash, ring_list.n);  // hash % n
    if (index > 0)
        i = (i + index) % ring_list.n;
    r = strdup(ring_list.nodes[i].address);
    ABT_mutex_unlock(ring_list_mutex);
    return (r);
}
```

**特徴**:
- 単純なモジュロ演算 `hash % n`
- ノード追加/削除時に大規模な再ハッシュが必要
- 均等分散が保証される
- **Consistent Hashingではない** (従来型)

#### B. Consistent Hashing (デフォルト)

**線形探索版** (ノード数 < 7):
```c
static char *
ring_list_lookup_linear(const char *key, int key_size)
{
    HASH_T hash;
    char *r;
    int i;

    HASH((const unsigned char *)key, key_size, hash);
    ABT_mutex_lock(ring_list_mutex);
    for (i = 0; i < ring_list.n; ++i)
        if (HASH_CMP(ring_list.nodes[i].hash, hash) >= 0)
            break;
    if (i == ring_list.n)
        i = 0;  // ラップアラウンド
    r = strdup(ring_list.nodes[i].address);
    ABT_mutex_unlock(ring_list_mutex);
    return (r);
}
```

**二分探索版** (ノード数 >= 7):
```c
static char *
ring_list_lookup_binary(const char *key, int key_size)
{
    HASH_T hash;
    char *r;

    HASH((const unsigned char *)key, key_size, hash);
    ABT_mutex_lock(ring_list_mutex);
    if (HASH_CMP(ring_list.nodes[0].hash, hash) >= 0 ||
        HASH_CMP(ring_list.nodes[ring_list.n - 1].hash, hash) < 0) {
        r = strdup(ring_list.nodes[0].address);
    } else
        r = ring_list_lookup_internal(hash, 0, ring_list.n - 1);
    ABT_mutex_unlock(ring_list_mutex);
    return (r);
}

// 再帰的二分探索
static char *
ring_list_lookup_internal(HASH_T hash, int low, int hi)
{
    int mid = (low + hi) / 2;

    if (hi - low == 1)
        return (strdup(ring_list.nodes[hi].address));
    if (HASH_CMP(ring_list.nodes[mid].hash, hash) < 0)
        return (ring_list_lookup_internal(hash, mid, hi));
    else
        return (ring_list_lookup_internal(hash, low, mid));
}
```

**アルゴリズム**:
1. キーをハッシュ化
2. ソート済みリングから、`hash値 >= キーのハッシュ値` となる最小のノードを探索
3. 見つからない場合はring[0]にラップアラウンド

**計算量**:
- 線形探索: O(n)
- 二分探索: O(log n)

### 2.5 リレーグループ最適化

chfsには「リレーグループ」という最適化機構があります:

```c
static char *
ring_list_lookup_relay(const char *key, int key_size)
{
    HASH_T hash;
    unsigned int n, g, i, index = 0, self_g, target_g, target_l, nodes_g;
    int klen = strlen(key) + 1;
    char *r;

    if (klen < key_size)
        index = atoi(key + klen);
    HASH((const unsigned char *)key, klen, hash);
    ABT_mutex_lock(ring_list_mutex);
    n = ring_list.n;
    g = ring_list_get_lookup_relay_group();
    self_g = ring_list_self_index % g;
    i = HASH_MODULO(hash, n);
    if (index > 0)
        i = (i + index) % n;
    target_g = i % g;
    target_l = i / g;
    nodes_g = n / g + (self_g < n % g);
    if (target_g == self_g)
        /* i */;
    else if (target_l < nodes_g)
        i = self_g + target_l * g;
    else
        i = self_g + i % nodes_g * g;
    r = strdup(ring_list.nodes[i].address);
    ABT_mutex_unlock(ring_list_mutex);
    return (r);
}
```

**目的**:
- ノード間のホップ数を削減
- グループ内の代表ノードのみがメタデータを保持
- グループサイズ = `ceil(sqrt(n))`

**自動設定**:
```c
if (ring_list_does_lookup_relay_group_auto())
    ring_list_set_lookup_relay_group(ceilsqrt(src->n));

// ceil(sqrt(n)) の実装
int ceilsqrt(int n)
{
    int i, j, ij;
    for (i = 1; i < n; ) {
        for (j = 1; j < n; j *= 2) {
            ij = i + 2 * j;
            if (ij * ij >= n)
                break;
        }
        i += j;
        if (i * i >= n && (i - 1) * (i - 1) < n)
            break;
    }
    return (i);
}
```

### 2.6 ノード責任範囲の判定

```c
int
ring_list_is_in_charge(const char *key, int key_size)
{
    HASH_T hash;
    int r = 1;

    HASH((const unsigned char *)key, key_size, hash);
    ABT_mutex_lock(ring_list_mutex);
    if (ring_list_self_index > 0)
        r = (HASH_CMP(ring_list.nodes[ring_list_self_index - 1].hash,
                hash) < 0 && HASH_CMP(hash,
                ring_list.nodes[ring_list_self_index].hash)
                    <= 0);
    else if (ring_list_self_index == 0)
        r = (HASH_CMP(ring_list.nodes[ring_list.n - 1].hash, hash)
            < 0 || HASH_CMP(hash, ring_list.nodes[0].hash) <= 0);
    ABT_mutex_unlock(ring_list_mutex);
    return (r);
}
```

**ロジック**:
- ノードiの責任範囲: `(node[i-1].hash, node[i].hash]`
- ノード0の責任範囲: `(node[n-1].hash, node[0].hash]` (ラップアラウンド)

---

## 3. BenchFSへの実装方針

### 3.1 採用する設計

**ハッシュ関数**:
- **koyama_hashは採用しない** (衝突率が高く、均等分散性が低い)
- **xxHash64を使用** (高速で均等分散性が高い)

**Consistent Hashingアプローチ**:
- chfsの二分探索アルゴリズムを採用
- ソート済み配列 + 二分探索: O(log n)

**仮想ノード**:
- chfsは仮想ノード未実装だが、**BenchFSでは実装する**
- 各物理ノードあたり150個の仮想ノード
- ノード追加/削除時のデータ移動量を削減

**リレーグループ**:
- **初期実装では省略** (複雑性が高い)
- 後期フェーズで必要に応じて追加

### 3.2 Rust実装の構造

```rust
// src/metadata/consistent_hash.rs

use std::collections::BTreeMap;
use xxhash_rust::xxh64::xxh64;

pub type NodeId = String;
pub type VirtualNodeId = u64;

#[derive(Debug, Clone)]
pub struct ConsistentHash {
    /// 仮想ノードリング (ハッシュ値 -> 物理ノードID)
    ring: BTreeMap<VirtualNodeId, NodeId>,

    /// 物理ノードあたりの仮想ノード数
    virtual_nodes_per_node: usize,

    /// 登録済み物理ノード
    nodes: Vec<NodeId>,
}

impl ConsistentHash {
    /// 新しいConsistent Hashリングを作成
    pub fn new(virtual_nodes_per_node: usize) -> Self {
        Self {
            ring: BTreeMap::new(),
            virtual_nodes_per_node,
            nodes: Vec::new(),
        }
    }

    /// ノードを追加
    pub fn add_node(&mut self, node_id: NodeId) {
        for i in 0..self.virtual_nodes_per_node {
            let vnode_key = format!("{}:{}", node_id, i);
            let hash = xxh64(vnode_key.as_bytes(), 0);
            self.ring.insert(hash, node_id.clone());
        }
        self.nodes.push(node_id);
    }

    /// ノードを削除
    pub fn remove_node(&mut self, node_id: &str) {
        for i in 0..self.virtual_nodes_per_node {
            let vnode_key = format!("{}:{}", node_id, i);
            let hash = xxh64(vnode_key.as_bytes(), 0);
            self.ring.remove(&hash);
        }
        self.nodes.retain(|n| n != node_id);
    }

    /// キーに対応するノードを検索 (二分探索)
    pub fn lookup(&self, key: &str) -> Option<&NodeId> {
        if self.ring.is_empty() {
            return None;
        }

        let hash = xxh64(key.as_bytes(), 0);

        // BTreeMapのrange()で hash以上の最小値を取得
        self.ring
            .range(hash..)
            .next()
            .or_else(|| self.ring.iter().next())
            .map(|(_, node_id)| node_id)
    }

    /// このノードがキーを担当しているか判定
    pub fn is_responsible(&self, key: &str, self_node: &str) -> bool {
        self.lookup(key)
            .map(|node| node == self_node)
            .unwrap_or(false)
    }

    /// 登録済みノード数を取得
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }
}
```

### 3.3 パラメータ設定

```rust
// src/metadata/mod.rs

pub const VIRTUAL_NODES_PER_NODE: usize = 150;
pub const XXHASH_SEED: u64 = 0;
```

### 3.4 テスト方針

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_lookup() {
        let mut ch = ConsistentHash::new(150);
        ch.add_node("node0".to_string());
        ch.add_node("node1".to_string());
        ch.add_node("node2".to_string());

        let node = ch.lookup("/path/to/file").unwrap();
        assert!(ch.is_responsible("/path/to/file", node));
    }

    #[test]
    fn test_node_removal() {
        let mut ch = ConsistentHash::new(150);
        ch.add_node("node0".to_string());
        ch.add_node("node1".to_string());

        let before = ch.lookup("/test").unwrap().clone();
        ch.remove_node("node0");
        let after = ch.lookup("/test").unwrap();

        // ノード削除後も一貫性を保つ
        if before != "node0" {
            assert_eq!(before, *after);
        }
    }

    #[test]
    fn test_distribution() {
        let mut ch = ConsistentHash::new(150);
        for i in 0..10 {
            ch.add_node(format!("node{}", i));
        }

        let mut counts = std::collections::HashMap::new();
        for i in 0..10000 {
            let key = format!("/file{}", i);
            let node = ch.lookup(&key).unwrap();
            *counts.entry(node.clone()).or_insert(0) += 1;
        }

        // 各ノードに約1000件ずつ分散されることを確認
        for (_, count) in counts {
            assert!(count > 800 && count < 1200); // ±20%の範囲
        }
    }
}
```

---

## 4. まとめ

### 4.1 採用する設計

| 項目 | CHFS | BenchFS |
|------|------|---------|
| ハッシュ関数 | koyama_hash (デフォルト) | xxHash64 |
| 仮想ノード | なし (物理ノードのみ) | あり (150個/ノード) |
| 検索アルゴリズム | 二分探索 (O(log n)) | BTreeMap range (O(log n)) |
| リレーグループ | あり (オプション) | なし (初期) |
| データ構造 | ソート済み配列 | BTreeMap |

### 4.2 実装優先度

1. **Phase 1**: 基本的なConsistent Hash実装
   - xxHash64統合
   - 仮想ノード実装
   - add_node/remove_node/lookup

2. **Phase 2**: 最適化
   - キャッシュ追加
   - 並行アクセス対応 (RwLock)

3. **Phase 3** (オプション): 拡張機能
   - リレーグループ相当の最適化
   - レプリケーション対応

---

## 5. 依存関係

```toml
[dependencies]
xxhash-rust = { version = "0.8", features = ["xxh64"] }
```

---

## 参考資料

- chfs `lib/koyama_hash.c`
- chfs `lib/ring_list.c`
- [xxHash](https://github.com/Cyan4973/xxHash)
