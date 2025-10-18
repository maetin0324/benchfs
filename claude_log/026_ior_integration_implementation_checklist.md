# IOR統合実装 - 詳細チェックリスト

**日付**: 2025-10-18
**ブランチ**: feature/chfs-implementation
**ステータス**: 実装計画完了、実装待ち

## 📋 実装状況サマリー

### ✅ 完了項目

- [x] IORリポジトリクローン (`ior_integration/ior/`)
- [x] ディレクトリ構成作成
- [x] C APIヘッダー作成 (`benchfs_backend/include/benchfs_c_api.h`)
- [x] IOR BENCHFSバックエンド（スケルトン実装） (`benchfs_backend/src/aiori-BENCHFS.c`)
- [x] MPI起動スクリプト (`scripts/run_ior_simple.sh`)
- [x] 実装計画ドキュメント (`025_ior_integration_plan.md`)
- [x] 不足点分析ドキュメント (`024_ior_integration_missing_analysis.md`)
- [x] README作成 (`ior_integration/README.md`)

### ❌ 未実装項目（重要度順）

#### 🔴 最重要：FFI基盤（25時間見積もり）

1. **Rust C FFI実装** (10時間)
   - [ ] `src/ffi/mod.rs` - FFIモジュール
   - [ ] `src/ffi/runtime.rs` - グローバルランタイム管理
   - [ ] `src/ffi/error.rs` - エラーハンドリング
   - [ ] `src/ffi/init.rs` - 初期化・終了処理
   - [ ] `src/ffi/file_ops.rs` - ファイル操作FFI
   - [ ] `src/ffi/metadata.rs` - メタデータ操作FFI

2. **ビルドシステム** (4時間)
   - [ ] `build.rs` - pkg-config生成
   - [ ] `benchfs.pc.in` - pkg-configテンプレート
   - [ ] Cargo.tomlの更新（FFIモジュール追加）

3. **IOR統合** (5時間)
   - [ ] `ior/configure.ac` 修正
   - [ ] `ior/src/Makefile.am` 修正
   - [ ] `ior/src/aiori.h` 修正
   - [ ] `ior/src/aiori.c` 修正
   - [ ] `aiori-BENCHFS.c` をIORソースツリーにコピー

4. **テスト・検証** (6時間)
   - [ ] 統合テスト実装
   - [ ] ビルドスクリプト作成
   - [ ] ベンチマーク実行検証

---

## 📝 詳細実装チェックリスト

### Phase 1: Rust FFI基盤（必須）

#### 1.1 グローバルランタイム管理 (`src/ffi/runtime.rs`)

**目的**: 非同期→同期変換の基盤

**実装内容**:
```rust
// src/ffi/runtime.rs

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Mutex;
use once_cell::sync::Lazy;
use pluvio_runtime::executor::Runtime;
use crate::api::file_ops::BenchFS;

/// グローバルランタイム（プロセス全体で1つ）
pub static GLOBAL_RUNTIME: Lazy<Rc<Runtime>> = Lazy::new(|| {
    Runtime::new(256)
});

/// スレッドローカルなBenchFSコンテキスト
thread_local! {
    pub static BENCHFS_CTX: RefCell<Option<Rc<BenchFS>>> = RefCell::new(None);
}

/// BenchFSコンテキストを設定
pub fn set_benchfs_ctx(benchfs: Rc<BenchFS>) {
    BENCHFS_CTX.with(|ctx| {
        *ctx.borrow_mut() = Some(benchfs);
    });
}

/// BenchFSコンテキストを取得
pub fn with_benchfs_ctx<F, R>(f: F) -> Result<R, String>
where
    F: FnOnce(&BenchFS) -> R,
{
    BENCHFS_CTX.with(|ctx| {
        ctx.borrow()
            .as_ref()
            .map(|fs| f(fs.as_ref()))
            .ok_or_else(|| "BenchFS not initialized".to_string())
    })
}

/// 非同期関数を同期実行
pub fn block_on<F>(future: F) -> F::Output
where
    F: std::future::Future + 'static,
    F::Output: 'static,
{
    let handle = GLOBAL_RUNTIME.spawn(future);
    GLOBAL_RUNTIME.clone().run(async { handle.await })
}
```

**チェック項目**:
- [ ] グローバルランタイムの初期化
- [ ] スレッドローカルコンテキスト管理
- [ ] block_on実装
- [ ] エラーハンドリング

#### 1.2 エラーハンドリング (`src/ffi/error.rs`)

**実装内容**:
```rust
// src/ffi/error.rs

use std::cell::RefCell;
use std::ffi::CString;
use std::os::raw::c_char;

// エラーコード定義
pub const BENCHFS_SUCCESS: i32 = 0;
pub const BENCHFS_ERROR: i32 = -1;
pub const BENCHFS_ENOENT: i32 = -2;
pub const BENCHFS_EIO: i32 = -3;
pub const BENCHFS_ENOMEM: i32 = -4;
pub const BENCHFS_EINVAL: i32 = -5;

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = RefCell::new(None);
}

/// エラーメッセージを設定
pub fn set_error_message(msg: &str) {
    LAST_ERROR.with(|err| {
        *err.borrow_mut() = CString::new(msg).ok();
    });
}

/// エラーメッセージを取得（C互換）
pub fn get_error_message() -> *const c_char {
    LAST_ERROR.with(|err| {
        err.borrow()
            .as_ref()
            .map(|s| s.as_ptr())
            .unwrap_or(std::ptr::null())
    })
}

/// Result<T>をエラーコードに変換
pub fn result_to_error_code<T>(result: Result<T, impl std::fmt::Display>) -> i32 {
    match result {
        Ok(_) => BENCHFS_SUCCESS,
        Err(e) => {
            set_error_message(&e.to_string());
            BENCHFS_EIO
        }
    }
}
```

**チェック項目**:
- [ ] エラーコード定義
- [ ] スレッドローカルエラーメッセージ
- [ ] set_error_message実装
- [ ] get_error_message実装
- [ ] result_to_error_code実装

#### 1.3 初期化・終了処理 (`src/ffi/init.rs`)

**実装内容**:
```rust
// src/ffi/init.rs

use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::rc::Rc;

use super::runtime::{set_benchfs_ctx, GLOBAL_RUNTIME};
use super::error::*;
use crate::api::file_ops::BenchFS;
use crate::rpc::connection::ConnectionPool;

// Opaque types
#[repr(C)]
pub struct benchfs_context_t {
    _private: [u8; 0],
}

#[no_mangle]
pub extern "C" fn benchfs_init(
    node_id: *const c_char,
    registry_dir: *const c_char,
    data_dir: *const c_char,
    is_server: i32,
) -> *mut benchfs_context_t {
    if node_id.is_null() || registry_dir.is_null() {
        set_error_message("node_id and registry_dir must not be null");
        return std::ptr::null_mut();
    }

    let node_id_str = unsafe { CStr::from_ptr(node_id).to_str().unwrap() };
    let registry_dir_str = unsafe { CStr::from_ptr(registry_dir).to_str().unwrap() };

    // BenchFSインスタンスを作成
    let benchfs = if is_server != 0 {
        // サーバーモード - TODO: サーバー起動ロジック
        set_error_message("Server mode not yet implemented");
        return std::ptr::null_mut();
    } else {
        // クライアントモード
        // TODO: 実装
        set_error_message("Client initialization not yet implemented");
        return std::ptr::null_mut();
    };

    // コンテキストに保存
    // set_benchfs_ctx(benchfs);

    // Opaque pointerとして返す
    // Box::into_raw(Box::new(benchfs)) as *mut benchfs_context_t
    std::ptr::null_mut()
}

#[no_mangle]
pub extern "C" fn benchfs_finalize(ctx: *mut benchfs_context_t) {
    if !ctx.is_null() {
        // メモリ解放
        // unsafe {
        //     let _ = Box::from_raw(ctx as *mut Rc<BenchFS>);
        // }
    }
}
```

**チェック項目**:
- [ ] benchfs_init実装
- [ ] benchfs_finalize実装
- [ ] サーバーモード初期化
- [ ] クライアントモード初期化
- [ ] MPIランク情報の取得と処理
- [ ] UCXワーカー初期化

#### 1.4 ファイル操作FFI (`src/ffi/file_ops.rs`)

**実装内容**:
```rust
// src/ffi/file_ops.rs

use std::ffi::CStr;
use std::os::raw::c_char;
use std::slice;

use super::runtime::{block_on, with_benchfs_ctx};
use super::error::*;
use crate::api::types::{FileHandle, OpenFlags};

#[repr(C)]
pub struct benchfs_file_t {
    _private: [u8; 0],
}

#[no_mangle]
pub extern "C" fn benchfs_create(
    ctx: *mut super::init::benchfs_context_t,
    path: *const c_char,
    flags: i32,
    mode: u32,
) -> *mut benchfs_file_t {
    if path.is_null() {
        set_error_message("path must not be null");
        return std::ptr::null_mut();
    }

    let path_str = unsafe { CStr::from_ptr(path).to_str().unwrap() };

    // TODO: フラグをOpenFlagsに変換
    let open_flags = OpenFlags::create();

    let result = with_benchfs_ctx(|fs| {
        fs.benchfs_open(path_str, open_flags)
    });

    match result {
        Ok(Ok(handle)) => {
            Box::into_raw(Box::new(handle)) as *mut benchfs_file_t
        }
        Ok(Err(e)) => {
            set_error_message(&e.to_string());
            std::ptr::null_mut()
        }
        Err(e) => {
            set_error_message(&e);
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn benchfs_write(
    file: *mut benchfs_file_t,
    buffer: *const u8,
    size: usize,
    offset: i64,
) -> i64 {
    if file.is_null() || buffer.is_null() {
        set_error_message("file and buffer must not be null");
        return BENCHFS_EINVAL as i64;
    }

    unsafe {
        let handle = &*(file as *const FileHandle);
        let buf = slice::from_raw_parts(buffer, size);

        // 非同期→同期変換
        let result = with_benchfs_ctx(|fs| {
            block_on(async {
                fs.benchfs_write(handle, buf).await
            })
        });

        match result {
            Ok(Ok(n)) => n as i64,
            Ok(Err(e)) => {
                set_error_message(&e.to_string());
                BENCHFS_EIO as i64
            }
            Err(e) => {
                set_error_message(&e);
                BENCHFS_ERROR as i64
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn benchfs_read(
    file: *mut benchfs_file_t,
    buffer: *mut u8,
    size: usize,
    offset: i64,
) -> i64 {
    if file.is_null() || buffer.is_null() {
        set_error_message("file and buffer must not be null");
        return BENCHFS_EINVAL as i64;
    }

    unsafe {
        let handle = &*(file as *const FileHandle);
        let buf = slice::from_raw_parts_mut(buffer, size);

        // 非同期→同期変換
        let result = with_benchfs_ctx(|fs| {
            block_on(async {
                fs.benchfs_read(handle, buf).await
            })
        });

        match result {
            Ok(Ok(n)) => n as i64,
            Ok(Err(e)) => {
                set_error_message(&e.to_string());
                BENCHFS_EIO as i64
            }
            Err(e) => {
                set_error_message(&e);
                BENCHFS_ERROR as i64
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn benchfs_close(file: *mut benchfs_file_t) -> i32 {
    if file.is_null() {
        return BENCHFS_EINVAL;
    }

    unsafe {
        let handle = Box::from_raw(file as *mut FileHandle);
        let result = with_benchfs_ctx(|fs| {
            fs.benchfs_close(&handle)
        });

        result_to_error_code(result.and_then(|r| r))
    }
}

// 他の操作も同様に実装...
```

**チェック項目**:
- [ ] benchfs_create実装
- [ ] benchfs_open実装
- [ ] benchfs_write実装（async→sync変換）
- [ ] benchfs_read実装（async→sync変換）
- [ ] benchfs_close実装
- [ ] benchfs_fsync実装
- [ ] benchfs_remove実装

#### 1.5 メタデータ操作FFI (`src/ffi/metadata.rs`)

**実装内容**: benchfs_stat, benchfs_get_file_size など

**チェック項目**:
- [ ] benchfs_stat実装
- [ ] benchfs_get_file_size実装
- [ ] 必要に応じて追加のメタデータ操作

#### 1.6 FFIモジュール統合 (`src/ffi/mod.rs`)

```rust
// src/ffi/mod.rs

pub mod runtime;
pub mod error;
pub mod init;
pub mod file_ops;
pub mod metadata;

// Re-exports
pub use error::*;
pub use init::*;
pub use file_ops::*;
pub use metadata::*;
```

**チェック項目**:
- [ ] モジュール構成
- [ ] Re-export設定

---

### Phase 2: ビルドシステム

#### 2.1 build.rs

**実装内容**:
```rust
// build.rs

use std::env;
use std::fs::File;
use std::io::Write;
use std::path::Path;

fn main() {
    // pkg-config ファイルを生成
    let out_dir = env::var("OUT_DIR").unwrap();
    let dest_path = Path::new(&out_dir).join("benchfs.pc");

    let prefix = env::var("PREFIX").unwrap_or_else(|_| "/usr/local".to_string());
    let version = env::var("CARGO_PKG_VERSION").unwrap();

    let pc_content = format!(
        r#"prefix={prefix}
exec_prefix=${{prefix}}
libdir=${{exec_prefix}}/lib
includedir=${{prefix}}/include

Name: BenchFS
Description: BenchFS distributed filesystem library
Version: {version}
Libs: -L${{libdir}} -lbenchfs
Cflags: -I${{includedir}}
"#,
        prefix = prefix,
        version = version
    );

    let mut file = File::create(&dest_path).unwrap();
    file.write_all(pc_content.as_bytes()).unwrap();

    println!("cargo:rerun-if-changed=build.rs");
}
```

**チェック項目**:
- [ ] build.rs作成
- [ ] pkg-config生成ロジック
- [ ] インストールスクリプト

#### 2.2 Cargo.toml更新

```toml
[lib]
name = "benchfs"
crate-type = ["rlib", "cdylib", "staticlib"]

[dependencies]
# 既存の依存関係...
once_cell = "1.19"  # Lazy static用

[build-dependencies]
# build.rsに必要な依存関係
```

**チェック項目**:
- [ ] once_cell追加
- [ ] build-dependencies設定

---

### Phase 3: IOR統合

#### 3.1 configure.ac修正

**場所**: `ior_integration/ior/configure.ac` 行414の後

**追加内容**:
```autoconf
# BenchFS support
PKG_CHECK_MODULES([BENCHFS], [benchfs],
  [AC_DEFINE([USE_BENCHFS_AIORI], [], [Build BENCHFS backend AIORI])
   BENCHFS_RPATH=$(pkg-config --libs-only-L benchfs | sed 's/-L/-Wl,-rpath=/g')
   AC_SUBST(BENCHFS_RPATH)],
  [with_benchfs=no])
AM_CONDITIONAL([USE_BENCHFS_AIORI], [test x$with_benchfs != xno])
```

**チェック項目**:
- [ ] configure.ac修正
- [ ] PKG_CHECK_MODULES追加
- [ ] AM_CONDITIONAL追加

#### 3.2 src/Makefile.am修正

**場所**: `ior_integration/ior/src/Makefile.am` 行129の後

**追加内容**:
```makefile
if USE_BENCHFS_AIORI
extraSOURCES += aiori-BENCHFS.c
extraCPPFLAGS += @BENCHFS_CFLAGS@
extraLDFLAGS += @BENCHFS_RPATH@
extraLDADD += @BENCHFS_LIBS@
endif
```

**チェック項目**:
- [ ] Makefile.am修正
- [ ] ソース追加
- [ ] フラグ設定

#### 3.3 aiori.h修正

**場所**: `ior_integration/ior/src/aiori.h` 行142付近

**追加内容**:
```c
extern ior_aiori_t benchfs_aiori;
```

**チェック項目**:
- [ ] extern宣言追加

#### 3.4 aiori.c修正

**場所**: `ior_integration/ior/src/aiori.c`

**追加内容**:
```c
// extern宣言セクション
extern ior_aiori_t benchfs_aiori;

// バックエンドレジストリ
ior_aiori_t *aioriSelectableBackends[] = {
    &posix_aiori,
    &dummy_aiori,
    &benchfs_aiori,  // <- 追加
    // ... その他
    NULL
};
```

**チェック項目**:
- [ ] extern宣言追加
- [ ] レジストリ登録

#### 3.5 aiori-BENCHFS.c配置

```bash
cp benchfs_backend/src/aiori-BENCHFS.c ior/src/
```

**チェック項目**:
- [ ] ファイルコピー
- [ ] TODO部分の実装（benchfs C API呼び出し）

---

### Phase 4: テスト・検証

#### 4.1 統合ビルドスクリプト

**ファイル**: `ior_integration/scripts/build_all.sh`

```bash
#!/bin/bash
set -e

# 1. BenchFS cdylibビルド
echo "Building BenchFS shared library..."
cd /home/rmaeda/workspace/rust/benchfs
cargo build --release --lib

# 2. pkg-config設置
echo "Installing pkg-config file..."
sudo cp target/release/benchfs.pc /usr/local/lib/pkgconfig/
sudo ldconfig

# 3. ヘッダーファイル設置
sudo cp ior_integration/benchfs_backend/include/benchfs_c_api.h /usr/local/include/

# 4. 共有ライブラリ設置
sudo cp target/release/libbenchfs.so /usr/local/lib/

# 5. IORビルド
echo "Building IOR with BENCHFS backend..."
cd ior_integration/ior
./bootstrap
./configure
make

echo "Build complete!"
```

**チェック項目**:
- [ ] ビルドスクリプト作成
- [ ] 実行権限付与
- [ ] テスト実行

#### 4.2 統合テスト

**ファイル**: `ior_integration/tests/integration_test.sh`

```bash
#!/bin/bash
set -e

# 簡易統合テスト
export REGISTRY_DIR=/tmp/benchfs_test_registry
export DATA_DIR=/tmp/benchfs_test_data

rm -rf $REGISTRY_DIR $DATA_DIR
mkdir -p $REGISTRY_DIR $DATA_DIR

echo "Running IOR with BENCHFS backend..."
mpirun -np 2 \
    ./ior/src/ior \
    -a BENCHFS \
    -t 1m \
    -b 4m \
    -i 1 \
    -w -r \
    -o /testfile \
    -O benchfs.registry=$REGISTRY_DIR \
    -O benchfs.datadir=$DATA_DIR

echo "Test complete!"
```

**チェック項目**:
- [ ] テストスクリプト作成
- [ ] 実行確認
- [ ] エラーケーステスト

---

## 📊 実装優先順位マトリクス

| 項目 | 優先度 | 工数 | 依存性 |
|------|--------|------|--------|
| FFI runtime管理 | 🔴 最高 | 3h | なし |
| FFI error処理 | 🔴 最高 | 2h | runtime |
| FFI init/finalize | 🔴 最高 | 2h | runtime, error |
| FFI file_ops (read/write) | 🟠 高 | 4h | runtime, error, init |
| FFI metadata | 🟡 中 | 2h | runtime, error |
| build.rs | 🟠 高 | 2h | なし |
| configure.ac修正 | 🟠 高 | 1h | build.rs |
| Makefile.am修正 | 🟠 高 | 1h | configure.ac |
| aiori統合 | 🟠 高 | 2h | configure.ac |
| ビルドスクリプト | 🟡 中 | 2h | 全て |
| 統合テスト | 🟡 中 | 4h | 全て |

---

## 🎯 次のアクションアイテム

### 即座に開始可能

1. **FFI runtime管理の実装** (`src/ffi/runtime.rs`)
   - グローバルランタイム
   - スレッドローカルコンテキスト
   - block_on実装

2. **FFI error処理の実装** (`src/ffi/error.rs`)
   - エラーコード定義
   - エラーメッセージ管理

3. **build.rs作成**
   - pkg-config生成

### 順次実施

4. FFI init/finalize実装
5. FFI file_ops実装
6. IORビルド統合
7. テスト・検証

---

## 📈 進捗トラッキング

**全体進捗**: 30% (基盤準備完了、FFI実装待ち)

- ✅ Phase 1: 基本構造 (100%)
- 🟡 Phase 2: FFI実装 (0%)
- ⬜ Phase 3: IOR統合 (0%)
- ⬜ Phase 4: テスト (0%)

**見積もり完了日**: FFI実装開始から約1週間（25時間）

---

## 関連ドキュメント

- `025_ior_integration_plan.md` - 全体計画
- `024_ior_integration_missing_analysis.md` - 詳細分析
- `ior_integration/README.md` - 使用方法
