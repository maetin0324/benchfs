use super::{
    FileHandle, FileStat, OpenFlags, StorageBackend,
    error::{StorageError, StorageResult},
};
use crate::stats::is_stats_enabled;
use pluvio_uring::allocator::FixedBufferAllocator;
use pluvio_uring::file::DmaFile;
use pluvio_uring::reactor::IoUringReactor;
use std::cell::RefCell;
use std::collections::HashMap;
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

    /// io_uring reactor reference
    /// This ensures DmaFile uses the same reactor that registered the fixed buffers
    reactor: Rc<IoUringReactor>,
}

impl IOUringBackend {
    /// 新しいIOURINGバックエンドを作成
    ///
    /// # Arguments
    /// * `allocator` - Registered buffer allocator
    /// * `reactor` - The io_uring reactor that owns the allocator's registered buffers
    pub fn new(allocator: Rc<FixedBufferAllocator>, reactor: Rc<IoUringReactor>) -> Self {
        Self {
            files: RefCell::new(HashMap::new()),
            next_fd: RefCell::new(100), // 100から開始 (標準入出力を避ける)
            allocator,
            reactor,
        }
    }

    /// ファイルディスクリプタを割り当て
    fn allocate_fd(&self) -> i32 {
        let mut next_fd = self.next_fd.borrow_mut();
        let fd = *next_fd;
        *next_fd += 1;
        fd
    }

    /// Map std::io::Error to StorageError with path context
    fn map_io_error(e: std::io::Error, path: &Path) -> StorageError {
        match e.kind() {
            std::io::ErrorKind::NotFound => StorageError::NotFound(path.display().to_string()),
            std::io::ErrorKind::PermissionDenied => {
                StorageError::PermissionDenied(path.display().to_string())
            }
            std::io::ErrorKind::AlreadyExists => {
                StorageError::AlreadyExists(path.display().to_string())
            }
            _ => StorageError::IoError(e),
        }
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
    /// * `data_len` - Actual data length to write (must be <= fixed_buffer.len())
    ///
    /// # Notes
    /// `data_len` is forwarded to the io_uring SQE so only that many bytes are
    /// written. When the underlying file was opened with `O_DIRECT`, callers
    /// must ensure `data_len` is a multiple of the device block size; otherwise
    /// open the file in buffered mode.
    #[async_backtrace::framed]
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

        // Measure io_uring write_fixed operation time (only when stats enabled)
        let start = if is_stats_enabled() {
            Some(std::time::Instant::now())
        } else {
            None
        };

        let write_len = data_len.min(fixed_buffer.len()) as u32;
        let buf_index = fixed_buffer.index() as u16;
        let buf_ptr = fixed_buffer.as_ptr();

        // Write data using write_fixed with registered buffer (zero-copy DMA).
        // First submit consumes the FixedBuffer; we keep it alive but loop
        // over `write_fixed_raw` if the kernel returns a short write so
        // ior-hard's 47KB transfers don't leave silent gaps on disk.
        //
        // **ENOMEM handling (2026-05-31)**: kernel `io_rw_alloc_async`
        // can return CQE.res == -ENOMEM under heavy in-flight pressure.
        // `dma_file.write_fixed` surfaces this as `Err(io::Error)` with
        // raw_os_error() == Some(ENOMEM). The FixedBuffer is consumed by
        // that call, but `buf_index` + `buf_ptr` were captured above so
        // the tail short-write loop can reissue the transfer via
        // `write_fixed_raw`. Treat the first ENOMEM as a "0-byte
        // success" and let the tail loop drive retries.
        let write_fixed_res = dma_file
            .write_fixed(fixed_buffer, offset, write_len)
            .await;
        let mut total: usize = match write_fixed_res {
            Ok((first_n, _fixed_buffer)) => {
                if first_n < 0 {
                    let err_code = -first_n;
                    if err_code == libc::ENOMEM {
                        eprintln!(
                            "[write_fixed_direct] ENOMEM CQE.res=-12, fd={} offset={} len={} — handing off to tail retry",
                            handle.0, offset, write_len
                        );
                        pluvio_timer::sleep(std::time::Duration::from_millis(1)).await;
                        0
                    } else {
                        return Err(StorageError::IoError(std::io::Error::from_raw_os_error(
                            err_code,
                        )));
                    }
                } else {
                    first_n as usize
                }
            }
            Err(io_err) => {
                if io_err.raw_os_error() == Some(libc::ENOMEM) {
                    eprintln!(
                        "[write_fixed_direct] ENOMEM Err(io_err), fd={} offset={} len={} — handing off to tail retry",
                        handle.0, offset, write_len
                    );
                    pluvio_timer::sleep(std::time::Duration::from_millis(1)).await;
                    0
                } else {
                    return Err(StorageError::IoError(io_err));
                }
            }
        };
        // Carry over: did the first submit fail with ENOMEM? The tail
        // loop uses this to seed `enomem_attempts` and to know whether
        // the very first iteration should treat its result as the
        // retry-after-ENOMEM case.
        let first_submit_failed_enomem = total == 0;
        // ENOMEM retry budget (CQE.res == -ENOMEM from kernel
        // `io_rw_alloc_async` under heavy in-flight pressure). The
        // first failed submit already burned one attempt.
        const MAX_ENOMEM_ATTEMPTS: u32 = 8;
        let mut enomem_attempts: u32 = if first_submit_failed_enomem { 1 } else { 0 };
        while total < write_len as usize {
            let cur_ptr = unsafe { buf_ptr.add(total) };
            let cur_offset = offset + total as u64;
            let remaining = write_len as usize - total;
            let raw_res = dma_file
                .write_fixed_raw(cur_offset, cur_ptr, remaining as u32, buf_index)
                .await;
            let n = match raw_res {
                Ok(n) => n,
                Err(io_err) => {
                    if io_err.raw_os_error() == Some(libc::ENOMEM) {
                        // Treat the Err(ENOMEM) the same as CQE.res ==
                        // -ENOMEM so the retry logic below kicks in.
                        -(libc::ENOMEM as i32)
                    } else {
                        return Err(StorageError::IoError(io_err));
                    }
                }
            };
            if n > 0 {
                total += n as usize;
            } else if n == 0 {
                return Err(StorageError::IoError(std::io::Error::other(
                    "write_fixed_raw returned 0 on retry",
                )));
            } else if -n == libc::ENOMEM {
                enomem_attempts += 1;
                if enomem_attempts > MAX_ENOMEM_ATTEMPTS {
                    eprintln!(
                        "[write_fixed_direct] ENOMEM giving up after {} attempts fd={} offset={} remaining={}",
                        enomem_attempts, handle.0, cur_offset, remaining
                    );
                    return Err(StorageError::IoError(std::io::Error::from_raw_os_error(
                        libc::ENOMEM,
                    )));
                }
                // Exponential backoff: 1, 2, 4, 8, 16, 32, 64, 128 ms
                let sleep_ms = 1u64 << (enomem_attempts - 1).min(7);
                eprintln!(
                    "[write_fixed_direct] ENOMEM tail attempt={}/{} fd={} offset={} remaining={} sleep={}ms",
                    enomem_attempts, MAX_ENOMEM_ATTEMPTS, handle.0, cur_offset, remaining, sleep_ms
                );
                pluvio_timer::sleep(std::time::Duration::from_millis(sleep_ms)).await;
                continue;
            } else {
                return Err(StorageError::IoError(std::io::Error::from_raw_os_error(-n)));
            }
        }
        let bytes_written = data_len.min(total);

        if let Some(start) = start {
            let elapsed = start.elapsed();
            tracing::debug!(
                "write_fixed_direct: {} bytes in {:?} to fd={} at offset={} ({:.2} MiB/s)",
                bytes_written,
                elapsed,
                handle.0,
                offset,
                (bytes_written as f64 / elapsed.as_secs_f64()) / (1024.0 * 1024.0)
            );
        }

        Ok(bytes_written)
    }

    /// Get the allocator for acquiring registered buffers
    pub fn allocator(&self) -> &Rc<FixedBufferAllocator> {
        &self.allocator
    }

    /// Read data directly into a registered buffer (zero-copy DMA)
    ///
    /// This method reads data from disk directly into the provided FixedBuffer,
    /// avoiding the intermediate copy that `read()` performs.
    ///
    /// # Arguments
    /// * `handle` - File handle
    /// * `offset` - Offset within the file
    /// * `fixed_buffer` - Pre-allocated registered buffer to read into
    /// * `read_len` - Maximum number of bytes to read (capped at fixed_buffer.len())
    ///
    /// # Returns
    /// A tuple of (bytes_read, buffer) where buffer is the same FixedBuffer passed in
    ///
    /// # Notes
    /// `read_len` is forwarded to the io_uring SQE. When the underlying file was
    /// opened with `O_DIRECT`, callers must ensure `read_len` is a multiple of
    /// the device block size; otherwise open the file in buffered mode.
    #[async_backtrace::framed]
    pub async fn read_fixed_direct(
        &self,
        handle: FileHandle,
        offset: u64,
        fixed_buffer: pluvio_uring::allocator::FixedBuffer,
        read_len: usize,
    ) -> StorageResult<(usize, pluvio_uring::allocator::FixedBuffer)> {
        // Clone the DmaFile to avoid holding the borrow across await
        let dma_file = {
            let files = self.files.borrow();
            files
                .get(&handle.0)
                .cloned()
                .ok_or(StorageError::InvalidHandle(handle))?
        };

        // Measure io_uring read_fixed operation time (only when stats enabled)
        let start = if is_stats_enabled() {
            Some(std::time::Instant::now())
        } else {
            None
        };

        let read_len = read_len.min(fixed_buffer.len()) as u32;
        let buf_index = fixed_buffer.index() as u16;
        let buf_ptr_mut = fixed_buffer.as_ptr() as *mut u8;

        // Read data using read_fixed with registered buffer (zero-copy DMA).
        // Loop on short reads via `read_fixed_raw` so the caller's buffer
        // doesn't retain stale bytes past the kernel's short return (the
        // root of 158k ior-hard-read verification failures pre-fix).
        let (first_n, fixed_buffer) = dma_file
            .read_fixed(fixed_buffer, offset, read_len)
            .await
            .map_err(StorageError::IoError)?;
        if first_n < 0 {
            return Err(StorageError::IoError(std::io::Error::from_raw_os_error(
                -first_n,
            )));
        }
        let mut total = first_n as usize;
        while total < read_len as usize {
            let cur_ptr = unsafe { buf_ptr_mut.add(total) };
            let cur_offset = offset + total as u64;
            let remaining = read_len as usize - total;
            let n = dma_file
                .read_fixed_raw(cur_offset, cur_ptr, remaining as u32, buf_index)
                .await
                .map_err(StorageError::IoError)?;
            if n > 0 {
                total += n as usize;
            } else if n == 0 {
                break; // EOF
            } else {
                return Err(StorageError::IoError(std::io::Error::from_raw_os_error(-n)));
            }
        }
        let bytes_read = total;

        if let Some(start) = start {
            let elapsed = start.elapsed();
            tracing::debug!(
                "read_fixed_direct: {} bytes in {:?} from fd={} at offset={} ({:.2} MiB/s)",
                bytes_read,
                elapsed,
                handle.0,
                offset,
                (bytes_read as f64 / elapsed.as_secs_f64()) / (1024.0 * 1024.0)
            );
        }

        Ok((bytes_read, fixed_buffer))
    }

    /// Write to file directly from an externally-owned registered buffer.
    ///
    /// Lets the locusta RPC server submit `WriteFixed` against the RDMA
    /// landing buffer in-place, instead of forcing `IOUringBackend::write`'s
    /// `acquire().await + 4 MiB memcpy` path (job 17061 timing identified
    /// this as ~0.4 ms/chunk on the WRITE_READY handler hot path).
    ///
    /// # Safety
    /// `ptr` must point inside a buffer registered with this backend's
    /// allocator (i.e. acquired from `self.allocator`), and `buf_index` must
    /// be that buffer's slot index. The memory must remain valid until the
    /// returned future resolves.
    // async_backtrace removed from hot path; see chunk_store.rs note.
    pub async fn write_via_registered(
        &self,
        handle: FileHandle,
        offset: u64,
        ptr: *const u8,
        len: u32,
        buf_index: u16,
    ) -> StorageResult<usize> {
        let dma_file = {
            let files = self.files.borrow();
            files
                .get(&handle.0)
                .cloned()
                .ok_or(StorageError::InvalidHandle(handle))?
        };
        let start = if is_stats_enabled() {
            Some(std::time::Instant::now())
        } else {
            None
        };
        // Retry on transient ENOMEM and partial writes. Kernel buffered-write
        // path can return ENOMEM under burst load when filesystem mempools /
        // page-cache allocations momentarily fail; the condition clears
        // within ms. Without retry, ior-hard (1M+ concurrent 47KB writes)
        // aborts the entire job — observed in jobs 18277 and 18290.
        //
        // Partial writes (Ok(n) with 0 < n < remaining) silently corrupted
        // ior-hard-read (158k mismatches in job 20000): we used to return
        // the short count and the caller treated it as success. Now we
        // re-submit from `ptr + total` for the remaining bytes.
        let mut total: u32 = 0;
        let mut enomem_attempts: u32 = 0;
        loop {
            let remaining = len - total;
            if remaining == 0 {
                break;
            }
            let cur_ptr = unsafe { ptr.add(total as usize) };
            let cur_offset = offset + total as u64;
            match dma_file
                .write_fixed_raw(cur_offset, cur_ptr, remaining, buf_index)
                .await
            {
                Ok(n) if n > 0 => {
                    total += n as u32;
                    enomem_attempts = 0;
                }
                Ok(0) => {
                    // io_uring write returning 0 with no error: treat as EIO.
                    return Err(StorageError::IoError(std::io::Error::other(
                        "write_fixed returned 0 with no error",
                    )));
                }
                Ok(n) if -n == 12 && enomem_attempts < 5 => {
                    pluvio_timer::Delay::new(std::time::Duration::from_millis(
                        1u64 << enomem_attempts,
                    ))
                    .await;
                    enomem_attempts += 1;
                    continue;
                }
                Ok(n) => {
                    return Err(StorageError::IoError(std::io::Error::from_raw_os_error(-n)));
                }
                Err(e) if e.raw_os_error() == Some(12) && enomem_attempts < 5 => {
                    pluvio_timer::Delay::new(std::time::Duration::from_millis(
                        1u64 << enomem_attempts,
                    ))
                    .await;
                    enomem_attempts += 1;
                    continue;
                }
                Err(e) => return Err(StorageError::IoError(e)),
            }
        }
        if let Some(start) = start {
            use std::sync::atomic::{AtomicU64, Ordering};
            static N: AtomicU64 = AtomicU64::new(0);
            let n = N.fetch_add(1, Ordering::Relaxed);
            if n.is_multiple_of(1000) {
                tracing::info!(
                    target: "rpc_handler_timing",
                    kind = "write_via_registered_iouring",
                    n = n,
                    iouring_us = start.elapsed().as_micros() as u64,
                    len = len,
                    "WRITE_REGISTERED_IOURING"
                );
            }
        }
        Ok(total as usize)
    }

    /// Read from file directly into an externally-owned registered buffer.
    /// See [`write_via_registered`] for safety constraints.
    // async_backtrace removed from hot path; see chunk_store.rs note.
    pub async fn read_via_registered(
        &self,
        handle: FileHandle,
        offset: u64,
        ptr: *mut u8,
        len: u32,
        buf_index: u16,
    ) -> StorageResult<usize> {
        let dma_file = {
            let files = self.files.borrow();
            files
                .get(&handle.0)
                .cloned()
                .ok_or(StorageError::InvalidHandle(handle))?
        };
        let start = if is_stats_enabled() {
            Some(std::time::Instant::now())
        } else {
            None
        };
        // Loop until full read or EOF. Short reads can occur on regular
        // files (kernel returns partial when crossing pinned-page
        // boundaries under high concurrency); a single `Ok(short)` would
        // leave the tail of the caller's buffer with stale data and
        // corrupt verification on subsequent ior-hard-read.
        let mut total: u32 = 0;
        loop {
            let remaining = len - total;
            if remaining == 0 {
                break;
            }
            let cur_ptr = unsafe { ptr.add(total as usize) };
            let cur_offset = offset + total as u64;
            let n = dma_file
                .read_fixed_raw(cur_offset, cur_ptr, remaining, buf_index)
                .await
                .map_err(StorageError::IoError)?;
            if n > 0 {
                total += n as u32;
            } else if n == 0 {
                // EOF — caller's responsibility to detect short read.
                break;
            } else {
                return Err(StorageError::IoError(std::io::Error::from_raw_os_error(-n)));
            }
        }
        if let Some(start) = start {
            use std::sync::atomic::{AtomicU64, Ordering};
            static N: AtomicU64 = AtomicU64::new(0);
            let n = N.fetch_add(1, Ordering::Relaxed);
            if n.is_multiple_of(1000) {
                tracing::info!(
                    target: "rpc_handler_timing",
                    kind = "read_via_registered_iouring",
                    n = n,
                    iouring_us = start.elapsed().as_micros() as u64,
                    len = len,
                    "READ_REGISTERED_IOURING"
                );
            }
        }
        Ok(total as usize)
    }
}

// Default implementation removed - allocator must be provided

#[async_trait::async_trait(?Send)]
impl StorageBackend for IOUringBackend {
    async fn open(&self, path: &Path, flags: OpenFlags) -> StorageResult<FileHandle> {
        let path_str = path.to_str().ok_or_else(|| {
            StorageError::IoError(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("path contains invalid UTF-8: {}", path.display()),
            ))
        })?;

        let linux_flags = flags.to_linux_flags();
        let mode = 0o644u32;

        // Async file open via io_uring OpenAt
        let dma_file =
            DmaFile::open_with_reactor(path_str, linux_flags, mode, self.reactor.clone())
                .await
                .map_err(|e| Self::map_io_error(e, path))?;

        let dma_file = Rc::new(dma_file);
        let fd = self.allocate_fd();

        let mut files = self.files.borrow_mut();
        files.insert(fd, dma_file);

        tracing::debug!("Opened file: {} with fd={} (async)", path.display(), fd);

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
            .read_fixed(fixed_buffer, offset, read_size as u32)
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
            .write_fixed(fixed_buffer, offset, write_size as u32)
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
        let path_str = path.to_str().ok_or_else(|| {
            StorageError::IoError(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("path contains invalid UTF-8: {}", path.display()),
            ))
        })?;

        // O_WRONLY | O_CREAT (without O_TRUNC to maintain original behavior)
        let linux_flags = libc::O_WRONLY | libc::O_CREAT;

        // Async file create via io_uring OpenAt
        let dma_file =
            DmaFile::open_with_reactor(path_str, linux_flags, mode, self.reactor.clone())
                .await
                .map_err(|e| Self::map_io_error(e, path))?;

        let dma_file = Rc::new(dma_file);
        let fd = self.allocate_fd();

        let mut files = self.files.borrow_mut();
        files.insert(fd, dma_file);

        tracing::debug!("Created file: {} with fd={} (async)", path.display(), fd);

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

    fn setup_runtime() -> (Rc<Runtime>, Rc<FixedBufferAllocator>, Rc<IoUringReactor>) {
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
        let reactor_clone = reactor.clone();

        // Runtime::newは既にRc<Runtime>を返す
        runtime.register_reactor("io_uring_reactor", reactor);

        (runtime, allocator, reactor_clone)
    }

    #[test]
    fn test_stat() {
        let (runtime, allocator, reactor) = setup_runtime();
        let temp_dir = TempDir::new().unwrap();
        let test_file = temp_dir.path().join("stat_test.txt");

        let mut file = File::create(&test_file).unwrap();
        file.write_all(b"12345").unwrap();
        drop(file);

        runtime
            .clone()
            .run_with_name_and_runtime("iouring_backend_test_stat", async move {
                let backend = IOUringBackend::new(allocator, reactor);
                let stat = backend.stat(&test_file).await.unwrap();
                assert_eq!(stat.size, 5);
            });
    }

    #[test]
    fn test_open_read_write() {
        let (runtime, allocator, reactor) = setup_runtime();
        let temp_dir = TempDir::new().unwrap();
        let test_file = temp_dir.path().join("test.txt");

        // テストデータを書き込み
        let mut file = File::create(&test_file).unwrap();
        file.write_all(b"Hello, IOURING!").unwrap();
        drop(file);

        runtime.clone().run_with_name_and_runtime(
            "iouring_backend_test_open_read_write",
            async move {
                let backend = IOUringBackend::new(allocator, reactor);

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
        let (runtime, allocator, reactor) = setup_runtime();
        let temp_dir = TempDir::new().unwrap();
        let test_file = temp_dir.path().join("new_file.txt");

        runtime.clone().run_with_name_and_runtime(
            "iouring_backend_test_create_write",
            async move {
                let backend = IOUringBackend::new(allocator, reactor);

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
            },
        );
    }
}
