use super::{
    error::{StorageError, StorageResult},
    FileHandle, FileStat, OpenFlags, StorageBackend,
};
use pluvio_uring::file::DmaFile;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;

/// IOURINGバックエンド
pub struct IOUringBackend {
    /// オープン中のファイル (fd -> DmaFile)
    files: Arc<Mutex<HashMap<i32, DmaFile>>>,

    /// 次のファイルディスクリプタID
    next_fd: Arc<Mutex<i32>>,
}

impl IOUringBackend {
    /// 新しいIOURINGバックエンドを作成
    pub fn new() -> Self {
        Self {
            files: Arc::new(Mutex::new(HashMap::new())),
            next_fd: Arc::new(Mutex::new(100)), // 100から開始 (標準入出力を避ける)
        }
    }

    /// ファイルディスクリプタを割り当て
    async fn allocate_fd(&self) -> i32 {
        let mut next_fd = self.next_fd.lock().await;
        let fd = *next_fd;
        *next_fd += 1;
        fd
    }

    /// OpenFlagsからOpenOptionsを作成
    fn flags_to_open_options(flags: OpenFlags) -> OpenOptions {
        let mut opts = OpenOptions::new();

        opts.read(flags.read)
            .write(flags.write)
            .create(flags.create)
            .truncate(flags.truncate)
            .append(flags.append);

        if flags.direct {
            opts.custom_flags(libc::O_DIRECT);
        }

        opts
    }
}

impl Default for IOUringBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait(?Send)]
impl StorageBackend for IOUringBackend {
    async fn open(&self, path: &Path, flags: OpenFlags) -> StorageResult<FileHandle> {
        let opts = Self::flags_to_open_options(flags);

        let file = opts
            .open(path)
            .map_err(|e| match e.kind() {
                std::io::ErrorKind::NotFound => {
                    StorageError::NotFound(path.display().to_string())
                }
                std::io::ErrorKind::PermissionDenied => {
                    StorageError::PermissionDenied(path.display().to_string())
                }
                std::io::ErrorKind::AlreadyExists => {
                    StorageError::AlreadyExists(path.display().to_string())
                }
                _ => StorageError::IoError(e),
            })?;

        let dma_file = DmaFile::new(file);
        let fd = self.allocate_fd().await;

        let mut files = self.files.lock().await;
        files.insert(fd, dma_file);

        tracing::debug!("Opened file: {} with fd={}", path.display(), fd);

        Ok(FileHandle(fd))
    }

    async fn close(&self, handle: FileHandle) -> StorageResult<()> {
        let mut files = self.files.lock().await;
        files
            .remove(&handle.0)
            .ok_or(StorageError::InvalidHandle(handle))?;

        tracing::debug!("Closed file with fd={}", handle.0);

        Ok(())
    }

    async fn read(
        &self,
        handle: FileHandle,
        offset: u64,
        buffer: &mut [u8],
    ) -> StorageResult<usize> {
        let files = self.files.lock().await;
        let dma_file = files
            .get(&handle.0)
            .ok_or(StorageError::InvalidHandle(handle))?;

        // DmaFile::read は Vec<u8> を受け取るので、バッファを作成
        let temp_buffer = vec![0u8; buffer.len()];
        let bytes_read = dma_file
            .read(temp_buffer.clone(), offset)
            .await
            .map_err(StorageError::IoError)?;

        if bytes_read < 0 {
            return Err(StorageError::IoError(std::io::Error::from_raw_os_error(
                -bytes_read,
            )));
        }

        let bytes_read = bytes_read as usize;
        buffer[..bytes_read].copy_from_slice(&temp_buffer[..bytes_read]);

        tracing::trace!(
            "Read {} bytes from fd={} at offset={}",
            bytes_read,
            handle.0,
            offset
        );

        Ok(bytes_read)
    }

    async fn write(
        &self,
        handle: FileHandle,
        offset: u64,
        buffer: &[u8],
    ) -> StorageResult<usize> {
        let files = self.files.lock().await;
        let dma_file = files
            .get(&handle.0)
            .ok_or(StorageError::InvalidHandle(handle))?;

        let bytes_written = dma_file
            .write(buffer.to_vec(), offset)
            .await
            .map_err(StorageError::IoError)?;

        if bytes_written < 0 {
            return Err(StorageError::IoError(std::io::Error::from_raw_os_error(
                -bytes_written,
            )));
        }

        let bytes_written = bytes_written as usize;

        tracing::trace!(
            "Wrote {} bytes to fd={} at offset={}",
            bytes_written,
            handle.0,
            offset
        );

        Ok(bytes_written)
    }

    async fn create(&self, path: &Path, mode: u32) -> StorageResult<FileHandle> {
        let mut opts = OpenOptions::new();
        opts.write(true)
            .create(true)
            .truncate(false)
            .custom_flags(libc::O_DIRECT);

        // Unix パーミッション設定
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(mode);
        }

        let file = opts.open(path).map_err(|e| match e.kind() {
            std::io::ErrorKind::PermissionDenied => {
                StorageError::PermissionDenied(path.display().to_string())
            }
            std::io::ErrorKind::AlreadyExists => {
                StorageError::AlreadyExists(path.display().to_string())
            }
            _ => StorageError::IoError(e),
        })?;

        let dma_file = DmaFile::new(file);
        let fd = self.allocate_fd().await;

        let mut files = self.files.lock().await;
        files.insert(fd, dma_file);

        tracing::debug!("Created file: {} with fd={}", path.display(), fd);

        Ok(FileHandle(fd))
    }

    async fn unlink(&self, path: &Path) -> StorageResult<()> {
        std::fs::remove_file(path).map_err(|e| match e.kind() {
            std::io::ErrorKind::NotFound => StorageError::NotFound(path.display().to_string()),
            std::io::ErrorKind::PermissionDenied => {
                StorageError::PermissionDenied(path.display().to_string())
            }
            _ => StorageError::IoError(e),
        })?;

        tracing::debug!("Unlinked file: {}", path.display());

        Ok(())
    }

    async fn stat(&self, path: &Path) -> StorageResult<FileStat> {
        let metadata = std::fs::metadata(path).map_err(|e| match e.kind() {
            std::io::ErrorKind::NotFound => StorageError::NotFound(path.display().to_string()),
            std::io::ErrorKind::PermissionDenied => {
                StorageError::PermissionDenied(path.display().to_string())
            }
            _ => StorageError::IoError(e),
        })?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;

            Ok(FileStat {
                size: metadata.len(),
                mode: metadata.mode(),
                uid: metadata.uid(),
                gid: metadata.gid(),
                atime: metadata.atime(),
                mtime: metadata.mtime(),
                ctime: metadata.ctime(),
            })
        }

        #[cfg(not(unix))]
        {
            Ok(FileStat {
                size: metadata.len(),
                mode: 0o644,
                uid: 0,
                gid: 0,
                atime: 0,
                mtime: 0,
                ctime: 0,
            })
        }
    }

    async fn mkdir(&self, path: &Path, mode: u32) -> StorageResult<()> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            std::fs::DirBuilder::new()
                .mode(mode)
                .create(path)
                .map_err(|e| match e.kind() {
                    std::io::ErrorKind::PermissionDenied => {
                        StorageError::PermissionDenied(path.display().to_string())
                    }
                    std::io::ErrorKind::AlreadyExists => {
                        StorageError::AlreadyExists(path.display().to_string())
                    }
                    _ => StorageError::IoError(e),
                })?;
        }

        #[cfg(not(unix))]
        {
            std::fs::create_dir(path).map_err(|e| match e.kind() {
                std::io::ErrorKind::PermissionDenied => {
                    StorageError::PermissionDenied(path.display().to_string())
                }
                std::io::ErrorKind::AlreadyExists => {
                    StorageError::AlreadyExists(path.display().to_string())
                }
                _ => StorageError::IoError(e),
            })?;
        }

        tracing::debug!("Created directory: {}", path.display());

        Ok(())
    }

    async fn rmdir(&self, path: &Path) -> StorageResult<()> {
        std::fs::remove_dir(path).map_err(|e| match e.kind() {
            std::io::ErrorKind::NotFound => StorageError::NotFound(path.display().to_string()),
            std::io::ErrorKind::PermissionDenied => {
                StorageError::PermissionDenied(path.display().to_string())
            }
            _ => StorageError::IoError(e),
        })?;

        tracing::debug!("Removed directory: {}", path.display());

        Ok(())
    }

    async fn fsync(&self, _handle: FileHandle) -> StorageResult<()> {
        // DmaFile には fsync がないため、標準の File::sync_all を使用
        // 実装のためには DmaFile に fsync サポートを追加する必要がある
        tracing::warn!("fsync is not yet fully implemented for IOURING backend");

        // とりあえずエラーなしで返す (後で実装)
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_open_read_write() {
        let temp_dir = TempDir::new().unwrap();
        let test_file = temp_dir.path().join("test.txt");

        // テストデータを書き込み
        let mut file = File::create(&test_file).unwrap();
        file.write_all(b"Hello, IOURING!").unwrap();
        drop(file);

        let backend = IOUringBackend::new();

        // ファイルを開く
        let handle = backend
            .open(&test_file, OpenFlags::read_only())
            .await
            .unwrap();

        // 読み込み
        let mut buffer = vec![0u8; 15];
        let bytes_read = backend.read(handle, 0, &mut buffer).await.unwrap();

        assert_eq!(bytes_read, 15);
        assert_eq!(&buffer, b"Hello, IOURING!");

        // ファイルを閉じる
        backend.close(handle).await.unwrap();
    }

    #[tokio::test]
    async fn test_create_write() {
        let temp_dir = TempDir::new().unwrap();
        let test_file = temp_dir.path().join("new_file.txt");

        let backend = IOUringBackend::new();

        // ファイルを作成
        let handle = backend.create(&test_file, 0o644).await.unwrap();

        // 書き込み
        let data = b"Test data for IOURING";
        let bytes_written = backend.write(handle, 0, data).await.unwrap();

        assert_eq!(bytes_written, data.len());

        // ファイルを閉じる
        backend.close(handle).await.unwrap();

        // ファイルが存在することを確認
        assert!(test_file.exists());
    }

    #[tokio::test]
    async fn test_stat() {
        let temp_dir = TempDir::new().unwrap();
        let test_file = temp_dir.path().join("stat_test.txt");

        let mut file = File::create(&test_file).unwrap();
        file.write_all(b"12345").unwrap();
        drop(file);

        let backend = IOUringBackend::new();

        let stat = backend.stat(&test_file).await.unwrap();
        assert_eq!(stat.size, 5);
    }
}
