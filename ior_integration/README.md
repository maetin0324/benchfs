# BenchFS IOR Integration

IORベンチマークをBenchFS上で実行するための統合実装です。

## ディレクトリ構成

```
ior_integration/
├── ior/                    # IORクローン（github.com/hpc/ior）
├── benchfs_backend/        # BenchFS用バックエンド実装
│   ├── include/
│   │   └── benchfs_c_api.h  # C APIヘッダー
│   └── src/
│       └── aiori-BENCHFS.c  # IOR AIORI実装
├── scripts/                # 実行スクリプト
│   └── run_ior_simple.sh   # 簡易実行スクリプト
└── README.md               # このファイル
```

## 実装ステータス

### 完了 ✓

- [x] IORリポジトリのクローン
- [x] C APIヘッダーファイルの作成
- [x] IOR BENCHFSバックエンド（スケルトン実装）
- [x] MPI起動スクリプト
- [x] ドキュメント作成

### 今後の作業

- [ ] IORビルド設定の更新
- [ ] BenchFS C API実装（Rust FFI）
- [ ] 統合テスト
- [ ] 性能測定

## IORへのBENCHFSバックエンド追加手順

### 1. ファイルのコピー

```bash
cd ior_integration

# BENCHFSバックエンドをIORソースツリーにコピー
cp benchfs_backend/src/aiori-BENCHFS.c ior/src/
```

### 2. IORソースコードの修正

#### 2.1. `ior/src/aiori.c`の修正

`ior/src/aiori.c`に以下を追加：

```c
// ファイル先頭付近のextern宣言セクション
extern ior_aiori_t benchfs_aiori;

// aioriSelectableBackends配列にbenchfs_aioriを追加
ior_aiori_t *aioriSelectableBackends[] = {
        &posix_aiori,
        &dummy_aiori,
        &benchfs_aiori,  // <- 追加
        // ... その他のバックエンド
        NULL
};
```

#### 2.2. `ior/configure.ac`の修正（オプション）

BENCHFSを常に有効にする場合は不要ですが、オプションにする場合：

```autoconf
# BenchFS support
AC_ARG_WITH([benchfs],
    [AS_HELP_STRING([--with-benchfs],
        [support IO with BenchFS backend @<:@default=no@:>@])],
    [],
    [with_benchfs=no])

AM_CONDITIONAL([USE_BENCHFS_AIORI], [test x$with_benchfs = xyes])
```

#### 2.3. `ior/src/Makefile.am`の修正

```makefile
if USE_BENCHFS_AIORI
    ior_SOURCES += aiori-BENCHFS.c
    ior_LDFLAGS += -L/path/to/benchfs/target/release
    ior_LDADD += -lbenchfs
endif
```

### 3. IORのビルド

```bash
cd ior

# 初回のみ: bootstrapスクリプトを実行
./bootstrap

# Configure（BENCHFSサポート有効化）
./configure --with-benchfs

# ビルド
make

# 確認: BENCHFSバックエンドが利用可能か確認
./src/ior -h | grep BENCHFS
```

## 使い方

### 簡易テスト

```bash
cd ior_integration
./scripts/run_ior_simple.sh
```

### カスタム実行

```bash
# 環境変数で設定を変更
export NPROCS=8
export TRANSFER_SIZE=4m
export BLOCK_SIZE=64m
./scripts/run_ior_simple.sh
```

### 手動実行

```bash
# レジストリディレクトリの作成
mkdir -p /tmp/benchfs_registry
mkdir -p /tmp/benchfs_data

# IOR実行
mpirun -np 4 \
    ./ior/src/ior \
    -a BENCHFS \
    -t 1m \
    -b 16m \
    -i 3 \
    -w -r \
    -o /testfile \
    -O benchfs.registry="/tmp/benchfs_registry" \
    -O benchfs.datadir="/tmp/benchfs_data"
```

## MPI環境での実行

### ホストファイルの作成

```bash
# hostfile
node1 slots=4
node2 slots=4
node3 slots=4
node4 slots=4
```

### 複数ノードでの実行

```bash
mpirun --hostfile hostfile -np 16 \
    ./ior/src/ior \
    -a BENCHFS \
    -t 1m \
    -b 64m \
    -i 5 \
    -w -r \
    -F \
    -o /benchfs/testfile \
    -O benchfs.registry="/shared/nfs/benchfs_registry" \
    -O benchfs.datadir="/local/ssd/benchfs_data"
```

## オプション

### BENCHFS固有のオプション

- `benchfs.registry`: サービスディスカバリ用の共有ディレクトリ（必須）
- `benchfs.datadir`: データディレクトリ（サーバーノード用）
- `benchfs.use-mpi-rank`: MPIランクをノードIDとして使用（デフォルト: 1）

### IORの主要オプション

- `-a BENCHFS`: BENCHFSバックエンドを使用
- `-t <size>`: 転送サイズ（例: 1m, 4k）
- `-b <size>`: ブロックサイズ
- `-i <num>`: イテレーション回数
- `-w`: 書き込みテスト
- `-r`: 読み込みテスト
- `-F`: ファイル分割（各プロセスが独立したファイルを使用）
- `-v`: Verbose出力

## アーキテクチャ

### MPIランクの役割

- **ランク0**: BenchFSサーバー（メタデータ + データ）
- **ランク1-N**: BenchFSクライアント + IOR実行

### データフロー

```
IOR Benchmark (MPI Rank 1-N)
    ↓
aiori-BENCHFS.c (IOR Backend)
    ↓
benchfs_c_api.h (C API)
    ↓
BenchFS Rust Library (FFI)
    ↓
BenchFS RPC (UCX)
    ↓
BenchFS Server (MPI Rank 0)
    ↓
Storage Backend (io_uring)
```

## トラブルシューティング

### BENCHFSバックエンドが見つからない

```bash
# IORが認識しているバックエンドを確認
./ior/src/ior -h

# BENCHFSが表示されない場合、ビルド設定を確認
```

### MPIエラー

```bash
# MPIバージョンの確認
mpirun --version

# UCXとMPIの互換性確認
```

### レジストリディレクトリのエラー

```bash
# 共有ディレクトリが全ノードからアクセス可能か確認
# NFSまたは共有ファイルシステムが必要
```

## 今後の実装計画

詳細は `/claude_log/025_ior_integration_plan.md` を参照

### Phase 2: 簡略版実装（現在）

- [x] IOR BENCHFSバックエンドのスケルトン実装
- [ ] IORビルド設定の更新
- [ ] 基本的な統合テスト

### Phase 3: 完全なRust FFI実装（将来）

- [ ] `src/c_api.rs`の完全実装
- [ ] グローバルステート管理
- [ ] 非同期→同期変換
- [ ] エラーハンドリング

### Phase 4: 性能最適化

- [ ] UCXパラメータチューニング
- [ ] I/Oパイプライン最適化
- [ ] 複数ノードでの性能測定

## 参考資料

- [IOR公式リポジトリ](https://github.com/hpc/ior)
- [IORユーザーガイド](https://ior.readthedocs.io/)
- [AIORIバックエンド開発ガイド](https://github.com/hpc/ior/blob/main/doc/USER_GUIDE)
- BenchFS実装計画: `../claude_log/025_ior_integration_plan.md`

## ライセンス

BenchFS IOR統合は、IORと同じライセンス（BSD）に従います。
