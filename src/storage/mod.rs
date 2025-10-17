// ストレージ層モジュール
pub mod error;
pub mod iouring;
pub mod local;
pub mod chunk_store;

pub use error::{StorageError, StorageResult};
pub use iouring::IOUringBackend;
pub use local::LocalFileSystem;
pub use chunk_store::{InMemoryChunkStore, FileChunkStore, ChunkStoreError, ChunkStoreResult, ChunkKey};

use std::path::Path;

/// ストレージバックエンドトレイト
///
/// Note: このトレイトは Send + Sync を要求しない。
/// BenchFSはシングルスレッド・マルチプロセスアーキテクチャを採用しており、
/// 各プロセスは独立したアドレス空間で動作する。
/// プロセス間通信はUCX (Shared Memory/RDMA) 経由で行われる。
#[async_trait::async_trait(?Send)]
pub trait StorageBackend {
    /// ファイルを開く
    async fn open(&self, path: &Path, flags: OpenFlags) -> StorageResult<FileHandle>;

    /// ファイルを閉じる
    async fn close(&self, handle: FileHandle) -> StorageResult<()>;

    /// データを読み込む
    async fn read(
        &self,
        handle: FileHandle,
        offset: u64,
        buffer: &mut [u8],
    ) -> StorageResult<usize>;

    /// データを書き込む
    async fn write(
        &self,
        handle: FileHandle,
        offset: u64,
        buffer: &[u8],
    ) -> StorageResult<usize>;

    /// ファイルを作成
    async fn create(&self, path: &Path, mode: u32) -> StorageResult<FileHandle>;

    /// ファイルを削除
    async fn unlink(&self, path: &Path) -> StorageResult<()>;

    /// ファイル情報を取得
    async fn stat(&self, path: &Path) -> StorageResult<FileStat>;

    /// ディレクトリを作成
    async fn mkdir(&self, path: &Path, mode: u32) -> StorageResult<()>;

    /// ディレクトリを削除
    async fn rmdir(&self, path: &Path) -> StorageResult<()>;

    /// データをフラッシュ
    async fn fsync(&self, handle: FileHandle) -> StorageResult<()>;
}

/// ファイルハンドル
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FileHandle(pub i32);

/// ファイルオープンフラグ
#[derive(Debug, Clone, Copy)]
pub struct OpenFlags {
    pub read: bool,
    pub write: bool,
    pub create: bool,
    pub truncate: bool,
    pub append: bool,
    pub direct: bool,  // Direct I/O (O_DIRECT)
}

impl OpenFlags {
    /// 読み取り専用
    pub fn read_only() -> Self {
        Self {
            read: true,
            write: false,
            create: false,
            truncate: false,
            append: false,
            direct: false,
        }
    }

    /// 書き込み専用
    pub fn write_only() -> Self {
        Self {
            read: false,
            write: true,
            create: true,
            truncate: false,
            append: false,
            direct: false,
        }
    }

    /// 読み書き
    pub fn read_write() -> Self {
        Self {
            read: true,
            write: true,
            create: false,
            truncate: false,
            append: false,
            direct: false,
        }
    }

    /// Direct I/Oを有効化
    pub fn with_direct(mut self) -> Self {
        self.direct = true;
        self
    }

    /// Linuxのフラグに変換
    pub fn to_linux_flags(&self) -> i32 {
        let mut flags = 0;

        if self.read && self.write {
            flags |= libc::O_RDWR;
        } else if self.write {
            flags |= libc::O_WRONLY;
        } else {
            flags |= libc::O_RDONLY;
        }

        if self.create {
            flags |= libc::O_CREAT;
        }
        if self.truncate {
            flags |= libc::O_TRUNC;
        }
        if self.append {
            flags |= libc::O_APPEND;
        }
        if self.direct {
            flags |= libc::O_DIRECT;
        }

        flags
    }
}

/// ファイル統計情報
#[derive(Debug, Clone)]
pub struct FileStat {
    pub size: u64,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub atime: i64,
    pub mtime: i64,
    pub ctime: i64,
}
