# IOR統合実装計画

**日付**: 2025-10-18
**ブランチ**: feature/chfs-implementation
**目的**: IORベンチマークをBenchFS上で実行できるようにする

## 概要

IOR（Interleaved Or Random）はHPC環境で広く使用されるI/Oベンチマークツールです。
BenchFS用のカスタムバックエンド（AIORI）を実装して、MPIを使った分散ベンチマークを実行可能にします。

## アーキテクチャ

### 構成要素

```
ior_integration/
├── ior/                    # IORのクローン（github.com/hpc/ior）
├── benchfs_backend/        # BenchFS用バックエンド実装
│   ├── include/
│   │   └── benchfs_c_api.h  # C API ヘッダー
│   └── src/
│       ├── aiori-BENCHFS.c  # IOR AIO対応
│       └── benchfs_wrapper.c # C APIラッパー（簡略版）
├── scripts/                # MPI起動スクリプト
│   ├── run_ior.sh          # IOR実行スクリプト
│   └── setup_env.sh        # 環境設定
└── doc/
    └── README.md           # 使用方法

benchfs/src/
└── c_api.rs                # Rust FFI実装（将来実装）
```

### MPIランクの役割分担

- **ランク0**: メタデータサーバー + データサーバー
- **ランク1-N**: クライアント（IOR実行）

## 実装の段階

### Phase 1: 基本構造の構築 ✓

- [x] IORリポジトリのクローン
- [x] ディレクトリ構成の作成
- [x] C APIヘッダーファイルの作成

### Phase 2: 簡略版実装（現在のターゲット）

#### 2.1. IOR BENCHFSバックエンド実装

`aiori-BENCHFS.c`を実装：
- DUMMYバックエンドをベースにした基本実装
- 最初はメモリ内で動作するスタブ実装
- IORとの統合を確認

#### 2.2. MPI起動スクリプト

```bash
#!/bin/bash
# run_ior.sh

# ランク0: BenchFSサーバー起動
# ランク1-N: IOR実行

mpirun -np 4 \
  --rank 0 : benchfsd --config server.toml \
  --rank 1-3 : ior -a BENCHFS -t 1m -b 16m -F
```

### Phase 3: Rust C FFI完全実装（将来）

C API実装の課題：
1. **グローバルステート管理**
   - スレッドローカルまたはlazy_staticでランタイムとBenchFSインスタンスを保持
   - MPIランク毎に異なる設定

2. **非同期→同期変換**
   - C APIは同期的だが、BenchFSは非同期
   - 各操作でランタイムをブロッキング実行

3. **エラーハンドリング**
   - RustエラーをC error codeに変換
   - スレッドローカルなエラーメッセージバッファ

実装例：
```rust
// src/c_api.rs

use std::cell::RefCell;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int};
use std::rc::Rc;

thread_local! {
    static BENCHFS_CTX: RefCell<Option<BenchFSContext>> = RefCell::new(None);
    static LAST_ERROR: RefCell<Option<CString>> = RefCell::new(None);
}

struct BenchFSContext {
    runtime: Rc<Runtime>,
    benchfs: Rc<BenchFS>,
    node_id: String,
}

#[no_mangle]
pub extern "C" fn benchfs_init(
    node_id: *const c_char,
    registry_dir: *const c_char,
    data_dir: *const c_char,
    is_server: c_int,
) -> *mut BenchFSContext {
    // 実装...
}
```

### Phase 4: 統合とテスト

- [ ] IORビルド設定の更新
- [ ] MPI環境でのテスト
- [ ] 性能測定とチューニング

## 技術的な課題

### 1. 非同期ランタイムの管理

BenchFSはPluvioランタイムを使用していますが、これはシングルスレッドランタイムです。
C APIから呼び出す場合：

```rust
fn sync_call<F, T>(f: F) -> T
where
    F: Future<Output = T> + 'static,
    T: 'static,
{
    BENCHFS_CTX.with(|ctx| {
        let ctx = ctx.borrow();
        let ctx = ctx.as_ref().unwrap();
        let handle = ctx.runtime.spawn(f);
        ctx.runtime.run(async { handle.await })
    })
}
```

### 2. MPIとの統合

IORはMPIを使用するため、各MPIランクで正しい初期化が必要：

```c
// aiori-BENCHFS.c

static void BENCHFS_Init(aiori_mod_opt_t* options) {
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (rank == 0) {
        // サーバーとして起動
        ctx = benchfs_init("server", registry_dir, data_dir, 1);
    } else {
        // クライアントとして起動
        char node_id[32];
        sprintf(node_id, "client_%d", rank);
        ctx = benchfs_init(node_id, registry_dir, NULL, 0);
    }
}
```

### 3. UCXとMPIの共存

UCXとMPIは両方ともネットワークライブラリです。共存させる必要があります：
- UCX: BenchFSのRPC通信に使用
- MPI: IORのプロセス管理に使用

解決策：
- UCXは独自のworker addressを使用（現在の実装）
- MPIはプロセス管理とバリア同期のみに使用

## 次のステップ

### 優先度1: 基本的なIORバックエンド実装

1. `aiori-BENCHFS.c`のスケルトン実装
   - DUMMYバックエンドをベースに作成
   - メモリ内スタブ実装でIORとの統合を確認

2. IORのビルド設定更新
   - `configure.ac`にBENCHFSバックエンドを追加
   - Makefileの更新

3. 簡単なテスト
   - 単一ノードでの実行
   - 基本的な読み書き操作の確認

### 優先度2: MPI統合

1. MPI起動スクリプトの作成
2. 2ノードでの実行テスト
3. 性能測定

### 優先度3: 完全なRust FFI実装

1. `src/c_api.rs`の実装
2. 共有ライブラリのビルド
3. C APIとRustの統合テスト

## 参考実装

### 既存のIORバックエンド

- `aiori-DUMMY.c`: 最も単純な実装（参考）
- `aiori-POSIX.c`: POSIX I/O実装
- `aiori-DFS.c`: DAOS DFS実装（分散ファイルシステムの例）
- `aiori-CEPHFS.c`: CephFS実装

### AIORI インターフェース

```c
typedef struct ior_aiori {
    char *name;                  // バックエンド名 "BENCHFS"
    aiori_fd_t *(*create)(...);  // ファイル作成
    aiori_fd_t *(*open)(...);    // ファイルオープン
    IOR_offset_t (*xfer)(...);   // データ転送（read/write両方）
    void (*close)(...);          // ファイルクローズ
    void (*remove)(...);         // ファイル削除
    void (*fsync)(...);          // fsync
    IOR_offset_t (*get_file_size)(...);
    void (*initialize)(...);     // 初期化
    void (*finalize)(...);       // 終了処理
    // ... その他の関数
} ior_aiori_t;
```

## 期待される成果

1. **IORベンチマークの実行**
   - BenchFS上でIORを実行可能
   - 性能測定データの取得

2. **分散性能の評価**
   - 複数ノードでのスケーラビリティ測定
   - BenchFSの性能特性の把握

3. **標準ベンチマークとの比較**
   - Lustre、CephFS、DAOSなど他のファイルシステムとの比較
   - HPC環境での位置づけの明確化

## まとめ

IOR統合は段階的に実施します：
1. Phase 1: 基本構造（完了）
2. Phase 2: 簡略版実装（現在のターゲット）
3. Phase 3: 完全なRust FFI実装（将来）
4. Phase 4: 統合とテスト

これにより、早期にIORを実行可能にし、段階的に完全な実装に移行できます。
