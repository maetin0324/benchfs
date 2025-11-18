use super::{
    FileHandle, FileStat, OpenFlags, StorageBackend,
    error::{StorageError, StorageResult},
};
use pluvio_uring::allocator::FixedBufferAllocator;
use pluvio_uring::file::DmaFile;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::path::Path;
use std::rc::Rc;

/// IOURINGバックエンド
///
/// Note: シングルスレッド設計のため、RefCellを使用
pub struct IOUringBackend {
    /// オープン中のファイル (fd -> Rc<DmaFile>)
    /// Rc を使用することで、async境界を跨いでも安全にファイルを参照できる
    files: RefCell<HashMap<i32, Rc<DmaFile>>>,

    /// 次のファイルディスクリプタID
    next_fd: RefCell<i32>,

    /// Registered buffer allocator
    allocator: Rc<FixedBufferAllocator>,
}

impl IOUringBackend {
    /// 新しいIOURINGバックエンドを作成
    ///
    /// # Arguments
    /// * `allocator` - Registered buffer allocator
    pub fn new(allocator: Rc<FixedBufferAllocator>) -> Self {
        Self {
            files: RefCell::new(HashMap::new()),
            next_fd: RefCell::new(100), // 100から開始 (標準入出力を避ける)
            allocator,
        }
    }

    /// ファイルディスクリプタを割り当て
    fn allocate_fd(&self) -> i32 {
        let mut next_fd = self.next_fd.borrow_mut();
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

        // O_DIRECTはアライメント要件が厳しいため、テストでは無効化
        // 本番環境では有効化を検討
        // if flags.direct {
        //     opts.custom_flags(libc::O_DIRECT);
        // }

        opts
    }

    /// Write data directly from a registered buffer (zero-copy DMA)
    ///
    /// This method takes ownership of a FixedBuffer and writes it directly to disk
    /// using DMA, avoiding the intermediate copy that `write()` performs.
    ///
    /// # Arguments
    /// * `handle` - File handle
    /// * `offset` - Offset within the file
    /// * `fixed_buffer` - Pre-populated registered buffer
    /// * `data_len` - Actual data length in the buffer
    pub async fn write_fixed_direct(
        &self,
        handle: FileHandle,
        offset: u64,
        fixed_buffer: pluvio_uring::allocator::FixedBuffer,
        data_len: usize,
    ) -> StorageResult<usize> {
        // Clone the DmaFile to avoid holding the borrow across await
        let dma_file = {
            let files = self.files.borrow();
            files
                .get(&handle.0)
                .cloned()
                .ok_or(StorageError::InvalidHandle(handle))?
        };

        // Write data using write_fixed with registered buffer (zero-copy DMA)
        let (bytes_written_raw, _fixed_buffer) = dma_file
            .write_fixed(fixed_buffer, offset)
            .await
            .map_err(StorageError::IoError)?;

        if bytes_written_raw < 0 {
            return Err(StorageError::IoError(std::io::Error::from_raw_os_error(
                -bytes_written_raw,
            )));
        }

        let bytes_written = data_len.min(bytes_written_raw as usize);

        tracing::trace!(
            "Wrote {} bytes (zero-copy) to fd={} at offset={}",
            bytes_written,
            handle.0,
            offset
        );

        Ok(bytes_written)
    }

    /// Get the allocator for acquiring registered buffers
    pub fn allocator(&self) -> &Rc<FixedBufferAllocator> {
        &self.allocator
    }
}

// Default implementation removed - allocator must be provided

#[async_trait::async_trait(?Send)]
impl StorageBackend for IOUringBackend {
    async fn open(&self, path: &Path, flags: OpenFlags) -> StorageResult<FileHandle> {
        let opts = Self::flags_to_open_options(flags);

        let file = opts.open(path).map_err(|e| match e.kind() {
            std::io::ErrorKind::NotFound => StorageError::NotFound(path.display().to_string()),
            std::io::ErrorKind::PermissionDenied => {
                StorageError::PermissionDenied(path.display().to_string())
            }
            std::io::ErrorKind::AlreadyExists => {
                StorageError::AlreadyExists(path.display().to_string())
            }
            _ => StorageError::IoError(e),
        })?;

        let dma_file = Rc::new(DmaFile::new(file));
        let fd = self.allocate_fd();

        let mut files = self.files.borrow_mut();
        files.insert(fd, dma_file);

        tracing::debug!("Opened file: {} with fd={}", path.display(), fd);

        Ok(FileHandle(fd))
    }

    async fn close(&self, handle: FileHandle) -> StorageResult<()> {
        let mut files = self.files.borrow_mut();
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
        // Clone the Rc<DmaFile> to avoid holding the borrow across await
        let dma_file = {
            let files = self.files.borrow();
            files
                .get(&handle.0)
                .cloned()
                .ok_or(StorageError::InvalidHandle(handle))?
        };

        // Acquire a registered buffer from the allocator
        let fixed_buffer = self.allocator.acquire().await;

        // Calculate how much we can read (limited by buffer size and requested size)
        let read_size = buffer.len().min(fixed_buffer.len());

        // Read data using read_fixed with registered buffer
        // New API returns (bytes_read, buffer)
        let (bytes_read, mut fixed_buffer) = dma_file
            .read_fixed(fixed_buffer, offset)
            .await
            .map_err(StorageError::IoError)?;

        if bytes_read < 0 {
            return Err(StorageError::IoError(std::io::Error::from_raw_os_error(
                -bytes_read,
            )));
        }

        let bytes_read = bytes_read as usize;
        let actual_size = bytes_read.min(read_size);

        // Copy data from fixed buffer to user buffer
        buffer[..actual_size].copy_from_slice(&fixed_buffer.as_mut_slice()[..actual_size]);

        // Fixed buffer is automatically returned to allocator when dropped

        tracing::trace!(
            "Read {} bytes from fd={} at offset={}",
            actual_size,
            handle.0,
            offset
        );

        Ok(actual_size)
    }

    async fn write(&self, handle: FileHandle, offset: u64, buffer: &[u8]) -> StorageResult<usize> {
        // Clone the Rc<DmaFile> to avoid holding the borrow across await
        let dma_file = {
            let files = self.files.borrow();
            files
                .get(&handle.0)
                .cloned()
                .ok_or(StorageError::InvalidHandle(handle))?
        };

        // Acquire a registered buffer from the allocator
        let mut fixed_buffer = self.allocator.acquire().await;

        // Calculate how much we can write (limited by buffer size and data size)
        let write_size = buffer.len().min(fixed_buffer.len());

        // Copy data from user buffer to fixed buffer
        fixed_buffer.as_mut_slice()[..write_size].copy_from_slice(&buffer[..write_size]);

        // Write data using write_fixed with registered buffer
        // New API returns (bytes_written, buffer)
        let (bytes_written_raw, _fixed_buffer) = dma_file
            .write_fixed(fixed_buffer, offset)
            .await
            .map_err(StorageError::IoError)?;

        if bytes_written_raw < 0 {
            return Err(StorageError::IoError(std::io::Error::from_raw_os_error(
                -bytes_written_raw,
            )));
        }

        // Note: io_uring may write the entire buffer, but we only care about
        // the amount of data the user actually wanted to write
        let bytes_written = write_size;

        // Fixed buffer is automatically returned to allocator when dropped

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
        opts.write(true).create(true).truncate(false);

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

        let dma_file = Rc::new(DmaFile::new(file));
        let fd = self.allocate_fd();

        let mut files = self.files.borrow_mut();
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

    async fn fsync(&self, handle: FileHandle) -> StorageResult<()> {
        // Clone the Rc<DmaFile> to avoid holding the borrow across await
        let dma_file = {
            let files = self.files.borrow();
            files
                .get(&handle.0)
                .cloned()
                .ok_or(StorageError::InvalidHandle(handle))?
        };

        // Use DmaFile's fsync which uses io_uring's Fsync operation
        dma_file.fsync().await.map_err(StorageError::IoError)?;

        tracing::debug!("Synced file with fd={}", handle.0);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pluvio_runtime::executor::Runtime;
    use pluvio_uring::reactor::IoUringReactor;
    use std::fs::File;
    use std::io::Write;
    use std::rc::Rc;
    use std::time::Duration;
    use tempfile::TempDir;

    fn setup_runtime() -> (Rc<Runtime>, Rc<FixedBufferAllocator>) {
        let runtime = Runtime::new(1024);

        // IoUringReactorを初期化して登録
        let reactor = IoUringReactor::builder()
            .queue_size(2048)
            .buffer_size(1 << 20) // 1 MiB
            .submit_depth(64)
            .wait_submit_timeout(Duration::from_millis(100))
            .wait_complete_timeout(Duration::from_millis(150))
            .build();

        // Get buffer allocator from reactor (it's a public field, not a method)
        let allocator = Rc::clone(&reactor.allocator);

        // Runtime::newは既にRc<Runtime>を返す
        runtime.register_reactor("io_uring_reactor", reactor);

        (runtime, allocator)
    }

    #[test]
    fn test_stat() {
        let (runtime, allocator) = setup_runtime();
        let temp_dir = TempDir::new().unwrap();
        let test_file = temp_dir.path().join("stat_test.txt");

        let mut file = File::create(&test_file).unwrap();
        file.write_all(b"12345").unwrap();
        drop(file);

        runtime
            .clone()
            .run_with_name_and_runtime("iouring_backend_test_stat", async move {
                let backend = IOUringBackend::new(allocator);
                let stat = backend.stat(&test_file).await.unwrap();
                assert_eq!(stat.size, 5);
            });
    }

    #[test]
    fn test_open_read_write() {
        let (runtime, allocator) = setup_runtime();
        let temp_dir = TempDir::new().unwrap();
        let test_file = temp_dir.path().join("test.txt");

        // テストデータを書き込み
        let mut file = File::create(&test_file).unwrap();
        file.write_all(b"Hello, IOURING!").unwrap();
        drop(file);

        runtime.clone().run_with_name_and_runtime(
            "iouring_backend_test_open_read_write",
            async move {
                let backend = IOUringBackend::new(allocator);

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
            },
        );
    }

    #[test]
    fn test_create_write() {
        let (runtime, allocator) = setup_runtime();
        let temp_dir = TempDir::new().unwrap();
        let test_file = temp_dir.path().join("new_file.txt");

        runtime
            .clone()
            .run_with_name_and_runtime("iouring_backend_test_create_write", async move {
                let backend = IOUringBackend::new(allocator);

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
            });
    }
}
