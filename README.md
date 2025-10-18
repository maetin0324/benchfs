# BenchFS

**高性能分散ファイルシステムのベンチマークフレームワーク**

BenchFSは、Rustで実装された分散ファイルシステムで、io_uringとUCX (Unified Communication X)を活用した高性能I/O、RDMA通信、包括的なメトリクス収集機能を提供します。HPCクラスタでの分散ファイルシステムの性能評価とベンチマークを主な目的として設計されています。

[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-TBD-blue.svg)](LICENSE)

## 🎯 プロジェクト概要

BenchFSは、[CHFS (Cached Hierarchical File System)](https://github.com/otatebe/chfs)から着想を得た分散ファイルシステムで、以下の特徴を持ちます：

- **高性能I/O**: io_uringによる非同期ファイル操作
- **RDMA通信**: UCX ActiveMessageによる低レイテンシ通信
- **スケーラブル**: Consistent Hashingによる分散メタデータ管理
- **柔軟性**: プラガブルストレージバックエンド
- **観測可能性**: 包括的なメトリクス収集とトレーシング
- **型安全**: Rustによるメモリ安全性とスレッド安全性

### CHFSとの違い

| 観点 | BenchFS | CHFS |
|------|---------|------|
| **目的** | ベンチマーク・性能分析 | 本番環境HPC向けFS |
| **言語** | Rust | C |
| **実行モデル** | シングルスレッド/プロセス | マルチスレッド |
| **ストレージ** | プラガブル（複数選択可） | 2層固定（pmemkv + POSIX） |
| **最適化目標** | 柔軟性と観測性 | レイテンシとスループット |

詳細は [docs/ARCHITECTURE_COMPARISON.md](docs/ARCHITECTURE_COMPARISON.md) を参照してください。

## ✨ 主要機能

### 分散ファイルシステム

- **Consistent Hashing**: 150個の仮想ノードによる均等な負荷分散
- **チャンキング**: 4MBデフォルトチャンクサイズ（設定可能）
- **メタデータ管理**: 分散メタデータストアとLRUキャッシング
- **レプリケーション**: 設定可能なレプリカ数

### 高性能I/O

- **io_uring統合**: 登録済みバッファによるゼロコピーI/O
- **RDMA最適化**: 32KB閾値での自動RDMA/RPC切り替え（CHFSと同じ）
- **非同期処理**: Pluvioランタイムによる効率的な非同期タスク管理

### RPC通信システム

- **UCX ActiveMessage**: 低レイテンシメッセージング
- **ゼロコピー転送**: zerocopyクレートによる効率的なシリアライゼーション
- **プロトコル自動選択**: Eager/Rendezvousの自動切り替え
- **6種類のRPC操作**:
  - ReadChunk / WriteChunk
  - MetadataLookup / MetadataCreateFile / MetadataCreateDir / MetadataDelete

### ストレージバックエンド

- **InMemoryChunkStore**: 高速メモリベースストレージ
- **FileChunkStore**: ファイルベース永続化ストレージ
- **IOUringBackend**: io_uring統合の高性能バックエンド
- **プラガブル設計**: StorageBackendトレイトで拡張可能

## 🏗️ アーキテクチャ

```
┌─────────────────────────────────────────────────────────────────┐
│                        BenchFS サーバーノード                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │         Pluvio Runtime（非同期エグゼキュータ）              │  │
│  └────────────────────────────────────────────────────────────┘  │
│           │                        │                    │         │
│           ▼                        ▼                    ▼         │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐│
│  │  RPCサーバー     │  │ メタデータ管理   │  │  チャンクストア  ││
│  │  (6 streams)    │  │ (Consistent Hash)│  │  (Pluggable)    ││
│  └──────────────────┘  └──────────────────┘  └──────────────────┘│
│           │                                           │           │
│           ▼                                           ▼           │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │    ストレージバックエンド（io_uring + 登録済みバッファ）   │  │
│  └────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

## 📋 前提条件

### 必須

- **Rust**: 1.70以降
- **Linux**: kernel 5.10以降（io_uringサポート必須）
- **UCX**: 1.12以降（RDMA/共有メモリ通信用）

### 推奨

- **InfiniBand**: RDMA通信用（またはRoCE対応NIC）
- **NVMe SSD**: 高速ストレージ用
- **CPU**: NUMA対応プロセッサ

## 🚀 インストール

### 1. UCXのインストール

```bash
# Ubuntu/Debian
sudo apt-get install libucx-dev

# または、ソースからビルド
git clone https://github.com/openucx/ucx.git
cd ucx
./autogen.sh
./configure --prefix=/usr/local
make -j$(nproc)
sudo make install
```

### 2. BenchFSのビルド

```bash
# リポジトリをクローン
git clone <repository-url>
cd benchfs

# 依存関係を含めてビルド
cargo build --release

# テスト実行
cargo test

# チェック
cargo check
```

## 🎬 クイックスタート

### 1. 設定ファイルの作成

```bash
cp benchfs.toml.example benchfs.toml
```

`benchfs.toml`を編集：

```toml
[node]
node_id = "node1"
data_dir = "/tmp/benchfs"
log_level = "info"

[storage]
chunk_size = 4194304        # 4MB
use_iouring = true

[network]
bind_addr = "0.0.0.0:50051"
rdma_threshold_bytes = 32768  # 32KB

[cache]
metadata_cache_entries = 1000
chunk_cache_mb = 100
```

### 2. サーバーの起動

```bash
# サーバーを起動
cargo run --release --bin benchfsd -- --config benchfs.toml

# または、デフォルト設定で起動
cargo run --release --bin benchfsd
```

### 3. ログ確認

```bash
# サーバーログを確認
tail -f /tmp/benchfs/benchfs.log
```

## ⚙️ 設定

### チャンクサイズの調整

```toml
[storage]
# 大規模シーケンシャルI/O向け（デフォルト）
chunk_size = 4194304  # 4MB

# または、CHFS互換の小ファイル向け
chunk_size = 65536    # 64KB
```

### RDMA閾値の調整

```toml
[network]
# 32KB以上のデータ転送でRDMA使用（CHFSと同じ）
rdma_threshold_bytes = 32768

# より大きなデータでRDMA使用
rdma_threshold_bytes = 131072  # 128KB
```

### キャッシュ設定

```toml
[cache]
# メタデータキャッシュエントリ数
metadata_cache_entries = 1000

# チャンクキャッシュサイズ（MB）
chunk_cache_mb = 100

# キャッシュTTL（秒、0=無期限）
cache_ttl_secs = 0
```

詳細は `benchfs.toml.example` を参照してください。

## 📖 使用例

### Rust APIの使用

```rust
use benchfs::api::{BenchFS, OpenFlags, ApiError};
use benchfs::config::ServerConfig;

#[pluvio_runtime::main]
async fn main() -> Result<(), ApiError> {
    // 設定を読み込み
    let config = ServerConfig::from_file("benchfs.toml")?;

    // BenchFSインスタンスを作成
    let fs = BenchFS::new(config).await?;

    // ファイルを作成
    let fd = fs.create("/test.txt", OpenFlags::CREATE | OpenFlags::WRITE).await?;

    // データを書き込み
    let data = b"Hello, BenchFS!";
    let written = fs.write(fd, data).await?;
    println!("Wrote {} bytes", written);

    // ファイルを閉じる
    fs.close(fd).await?;

    // ファイルを読み取り
    let fd = fs.open("/test.txt", OpenFlags::READ).await?;
    let mut buffer = vec![0u8; 1024];
    let read = fs.read(fd, &mut buffer).await?;
    println!("Read {} bytes: {:?}", read, &buffer[..read]);

    fs.close(fd).await?;

    Ok(())
}
```

### メタデータ操作

```rust
use benchfs::metadata::{MetadataManager, FileMetadata};

// メタデータマネージャーを作成
let manager = MetadataManager::new(cache_policy);
manager.add_node("node1".to_string());

// ファイルメタデータを保存
let metadata = FileMetadata::new(1, "/data/file.txt".to_string(), 1024);
manager.store_file_metadata(metadata).await?;

// メタデータを取得
let retrieved = manager.get_file_metadata("/data/file.txt").await?;
println!("File size: {} bytes", retrieved.size);
```

### ストレージバックエンドの使用

```rust
use benchfs::storage::{InMemoryChunkStore, ChunkStore};

// インメモリストアを作成
let mut store = InMemoryChunkStore::new(1024 * 1024 * 100); // 100MB

// チャンクを書き込み
let data = vec![42u8; 4096];
store.write_chunk(1, 0, 0, &data).await?;

// チャンクを読み取り
let read_data = store.read_chunk(1, 0, 0, 4096).await?;
assert_eq!(data, read_data);
```

## 🔧 開発ガイド

### プロジェクト構造

```
benchfs/
├── src/
│   ├── api/              # ユーザー向けAPI
│   ├── rpc/              # RPC通信システム
│   ├── metadata/         # メタデータ管理
│   ├── storage/          # ストレージバックエンド
│   ├── data/             # データ分散・チャンキング
│   ├── cache/            # キャッシング層
│   ├── config.rs         # 設定管理
│   └── bin/
│       └── benchfsd.rs   # サーバーデーモン
├── docs/                 # ドキュメント
│   ├── ARCHITECTURE_COMPARISON.md
│   └── IMPLEMENTATION_CHANGELOG.md
├── benchfs.toml.example  # 設定ファイル例
└── README.md             # このファイル
```

### テストの実行

```bash
# 全テストを実行
cargo test

# 特定のモジュールのテスト
cargo test metadata::

# 統合テスト
cargo test --test integration_tests

# カバレッジ付きテスト（tarpaulin必須）
cargo tarpaulin --out Html
```

### ベンチマークの実行

```bash
# ベンチマークを実行
cargo bench

# 特定のベンチマーク
cargo bench --bench file_ops_bench
```

### コードフォーマットとLint

```bash
# フォーマット
cargo fmt

# Lint
cargo clippy -- -D warnings

# ドキュメント生成
cargo doc --no-deps --open
```

## 📊 性能特性

### レイテンシ目標（CHFSベンチマーク）

| 操作 | 目標レイテンシ |
|------|----------------|
| 小読み取り（< 32 KB） | < 1 ms |
| 大読み取り（> 32 KB、RDMA） | < 100 μs |
| 小書き込み（< 32 KB） | < 1 ms |
| 大書き込み（> 32 KB、RDMA） | < 1 ms |
| メタデータ操作 | < 10 ms |

### スケーラビリティ目標

- **ノード数**: 1024ノード以上対応
- **メタデータルックアップ**: O(log n)
- **Consistent Hashing**: 150仮想ノード/物理ノード
- **負荷分散**: 各ノードが均等にキーを担当

## 📚 ドキュメント

### アーキテクチャドキュメント

- **[アーキテクチャ比較](docs/ARCHITECTURE_COMPARISON.md)**: BenchFS vs CHFS の詳細比較（日本語）
- **[実装変更ログ](docs/IMPLEMENTATION_CHANGELOG.md)**: 最新の実装変更内容

### 設計ノート

- **[CHFS実装計画](claude_log/001_benchfs_implementation_plan.md)**: 当初の実装計画
- **[CHFS詳細分析](claude_log/022_CHFS_architecture_analysis.md)**: CHFSアーキテクチャの詳細（英語）
- **[CHFS比較分析](claude_log/023_CHFS_BENCHFS_COMPARISON.md)**: 比較分析（英語）

### API ドキュメント

```bash
# APIドキュメントを生成して開く
cargo doc --no-deps --open
```

## 🛣️ ロードマップ

### 実装済み ✓

- [x] RPC通信システム（UCX ActiveMessage）
- [x] Consistent Hashingメタデータ管理
- [x] プラガブルストレージバックエンド
- [x] io_uring統合
- [x] メタデータキャッシング（LRU）
- [x] チャンキングとデータ分散
- [x] RDMA閾値設定

### 短期（1-2週間）

- [ ] RDMA閾値による自動RPC/RDMA切り替え実装
- [ ] チャンクサイズ違いでのベンチマーク
- [ ] 設定バリデーション強化
- [ ] クライアントライブラリの充実

### 中期（1-2ヶ月）

- [ ] 2層ストレージバックエンド（InMemory + File自動階層化）
- [ ] クライアント側バッファリング
- [ ] メタデータキャッシュTTL完全実装
- [ ] FUSE統合（chfuse相当）

### 長期（2-3ヶ月）

- [ ] I/O認識型フラッシング機構
- [ ] リング選択アルゴリズム（フォールトトレランス）
- [ ] ノード参加/離脱プロトコル
- [ ] CHFS vs BenchFS 包括的ベンチマーク

## 🤝 貢献

貢献を歓迎します！以下の手順で貢献してください：

1. このリポジトリをフォーク
2. フィーチャーブランチを作成（`git checkout -b feature/amazing-feature`）
3. 変更をコミット（`git commit -m 'Add amazing feature'`）
4. ブランチにプッシュ（`git push origin feature/amazing-feature`）
5. プルリクエストを作成

### コーディング規約

- Rustの標準スタイル（`cargo fmt`）に従う
- `cargo clippy`の警告をすべて解決
- 新機能には必ずテストを追加
- パブリックAPIにはドキュメントコメントを記載

## 📝 ライセンス

TBD

## 🙏 謝辞

- **[CHFS](https://github.com/otatebe/chfs)**: アーキテクチャとデザインパターンの多くを参考にしました
- **UCXプロジェクト**: 高性能通信ライブラリ
- **io_uringコミュニティ**: 高性能非同期I/Oインターフェース
- **Rustコミュニティ**: 素晴らしいエコシステムとツール

## 📧 お問い合わせ

質問や提案がある場合は、Issueを作成してください。

---

**Note**: このプロジェクトは開発中です。本番環境での使用は推奨されません。ベンチマークと研究目的での使用を想定しています。
