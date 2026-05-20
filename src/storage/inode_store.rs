//! Persistent inode store.
//!
//! Implements the CHFS fs_posix-style metadata persistence layer for BenchFS
//! using the **split-inode layout** (案 Y of `PLAN_METADATA_PERSISTENCE.md`):
//! each path-owned by a metadata server gets a dedicated 4 KiB inode file
//! under `<data_dir>/inodes/<hash[0:2]>/<hash>:`. Chunk files in `chunks/`
//! are not touched, preserving the io500-validated zero-overhead data path.
//!
//! The 4 KiB layout matches CHFS's `struct metadata` byte-for-byte in the
//! first 16 bytes (see `src/storage/inode.rs::PosixMetadataHeader`), then
//! adds two BenchFS extensions: `BenchfsChunk0Extension` (logical size) and
//! `BenchfsInodeExtension2` (mode, inode type, path length, version). The
//! remaining bytes (offset 64 to MSIZE-1) carry the path string inline so
//! that recovery can rebuild the in-memory map without auxiliary indexes.
//!
//! All disk I/O goes through `pluvio_uring::DmaFile`, which submits to the
//! same io_uring reactor the chunk store uses. Inode writes are 4 KiB
//! buffered (no `O_DIRECT`) — CHFS does the same (`fs_posix.c:139-148`), and
//! O_DIRECT brings no benefit for one-page writes.
//!
//! ## Why this module exists
//!
//! Before this module, BenchFS had no metadata persistence: `MetadataManager`
//! held everything in `HashMap` and process exit dropped all state. This
//! module is the disk side of a write-through (or write-back) cache; the
//! in-memory map remains the read fast path and the on-disk inode file is
//! the source of truth for restart.
//!
//! ## Flush policy
//!
//! - `Off`: helpers are not called from the RPC path; identical to the
//!   pre-persistence baseline. Used for the io500 benchmark mode.
//! - `WriteThrough`: every metadata mutation calls the matching `*_sync`
//!   helper before the RPC reply is sent. Latency cost: one 4 KiB pwrite
//!   (~10-50 µs on local NVMe).
//! - `WriteBack`: mutations enqueue the path into `dirty`; a background
//!   flush task drains the queue periodically. Reply latency is ~0 added;
//!   a process crash between mutation and flush loses recent state.

use std::cell::RefCell;
use std::collections::HashSet;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};

use pluvio_uring::file::DmaFile;

use crate::storage::inode::{
    EXT_OFFSET, MSIZE, OnDiskInode, OnDiskInodeType, decode_inode, encode_inode,
};

/// Hash a path to a 64-bit value, using the same `DefaultHasher` family the
/// chunk store uses for `chunk_id`. Stable within one process invocation;
/// the lower 8 bits of the hex string seed the shard directory.
fn hash_path(path: &str) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    path.hash(&mut hasher);
    hasher.finish()
}

/// Format the hash as a 16-char lowercase hex string.
fn hash_to_hex(h: u64) -> String {
    format!("{:016x}", h)
}

/// How aggressively to push in-memory metadata to disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushPolicy {
    /// Disk path disabled entirely. Identical to the pre-persistence build.
    Off,
    /// Every mutation pwrites before returning. Simple, correct, slow on
    /// burst create workloads.
    WriteThrough,
    /// Mutations mark dirty; a background task drains the queue. Crash-loses
    /// pending entries but unblocks the RPC hot path.
    WriteBack,
    /// In-memory only during the run; a single batched flush at shutdown
    /// (called from `benchfsd_mpi`'s shutdown sequence) writes every
    /// remaining inode in one pass. This is what io500's metadata
    /// persistence requirement actually demands — survival of the run,
    /// not per-mutation durability — without paying the per-RPC cost.
    OnFinalize,
}

impl FlushPolicy {
    /// True if mutating RPCs should flush their change to disk inline.
    /// Off / OnFinalize do not; WriteThrough / WriteBack do.
    pub fn flushes_per_op(self) -> bool {
        matches!(self, FlushPolicy::WriteThrough | FlushPolicy::WriteBack)
    }
}

/// On-disk persistence layer for inode metadata.
///
/// All path-based operations derive a shard `<base_dir>/<hh>/` from the
/// first hex byte of the path hash; an inode-file `<hh>/<hash>:` for files
/// and a `<hh>/<hash>/` directory + `<hh>/<hash>/.meta` for directories.
pub struct InodeStore {
    base_dir: PathBuf,
    chunk_size: u64,
    policy: FlushPolicy,
    /// Pending writebacks (path strings). Drained by `flush_once`.
    dirty: RefCell<HashSet<String>>,
}

/// File-side path encoding: `<base>/<hh>/<hash>:`
///
/// The trailing colon mirrors CHFS `key_to_path` (`fs_posix.c:112-113`),
/// which uses `:` to disambiguate file inodes from directory inodes that
/// share a hash. We need the same disambiguation because a path and its
/// parent directory could hypothetically collide.
fn inode_file_path(base: &Path, path: &str) -> PathBuf {
    let h = hash_to_hex(hash_path(path));
    let mut p = PathBuf::from(base);
    p.push(&h[..2]);
    let mut leaf = h.clone();
    leaf.push(':');
    p.push(leaf);
    p
}

/// Directory-side path encoding: `<base>/<hh>/<hash>/`
fn inode_dir_path(base: &Path, path: &str) -> PathBuf {
    let h = hash_to_hex(hash_path(path));
    let mut p = PathBuf::from(base);
    p.push(&h[..2]);
    p.push(h);
    p
}

/// The .meta header file living inside the directory inode dir.
fn inode_dir_meta_path(base: &Path, path: &str) -> PathBuf {
    let mut p = inode_dir_path(base, path);
    p.push(".meta");
    p
}

impl InodeStore {
    /// Create the store rooted at `<data_dir>/inodes/`. Ensures the base
    /// directory exists. Returns the same `InodeStore` regardless of policy
    /// — `Off` mode short-circuits inside each public method.
    pub fn new(data_dir: &Path, chunk_size: u64, policy: FlushPolicy) -> std::io::Result<Self> {
        let base_dir = data_dir.join("inodes");
        std::fs::create_dir_all(&base_dir)?;
        Ok(Self {
            base_dir,
            chunk_size,
            policy,
            dirty: RefCell::new(HashSet::new()),
        })
    }

    /// True if the store is in `Off` mode and should not be called.
    pub fn is_disabled(&self) -> bool {
        self.policy == FlushPolicy::Off
    }

    pub fn policy(&self) -> FlushPolicy {
        self.policy
    }

    fn ensure_shard_dir(&self, path: &str) -> std::io::Result<()> {
        let h = hash_to_hex(hash_path(path));
        let shard = self.base_dir.join(&h[..2]);
        std::fs::create_dir_all(&shard)
    }

    /// Persist a regular file's inode. Idempotent on disk: an existing
    /// inode file is overwritten with the new contents.
    ///
    /// `WriteThrough` writes immediately; `WriteBack` enqueues the path
    /// into `dirty` (caller-passed snapshot of size will be re-read by the
    /// flusher); `Off` returns Ok(()) without touching disk.
    pub async fn create_file(
        &self,
        path: &str,
        mode: u32,
        logical_size: u64,
    ) -> std::io::Result<()> {
        match self.policy {
            FlushPolicy::Off | FlushPolicy::OnFinalize => Ok(()),
            FlushPolicy::WriteThrough => self.write_file_inode(path, mode, logical_size).await,
            FlushPolicy::WriteBack => {
                self.dirty.borrow_mut().insert(path.to_owned());
                Ok(())
            }
        }
    }

    /// Persist a directory inode. Creates the per-path dir + .meta header.
    pub async fn create_dir(&self, path: &str, mode: u32) -> std::io::Result<()> {
        match self.policy {
            FlushPolicy::Off | FlushPolicy::OnFinalize => Ok(()),
            FlushPolicy::WriteThrough => self.write_dir_inode(path, mode).await,
            FlushPolicy::WriteBack => {
                self.dirty.borrow_mut().insert(path.to_owned());
                Ok(())
            }
        }
    }

    /// Update the logical size of an existing file's inode (read-modify-write).
    /// Used by `MetadataUpdate`. If the inode file does not yet exist (e.g.
    /// the create RPC was lost), create it with default mode 0o100644.
    pub async fn update_size(&self, path: &str, new_size: u64) -> std::io::Result<()> {
        match self.policy {
            FlushPolicy::Off | FlushPolicy::OnFinalize => Ok(()),
            FlushPolicy::WriteThrough => {
                // RMW: try read first; on miss, fall back to create_file.
                match self.read_inode(path).await? {
                    Some(mut existing) => {
                        existing.logical_size = new_size;
                        self.write_file_inode_full(path, &existing).await
                    }
                    None => self.write_file_inode(path, 0o100644, new_size).await,
                }
            }
            FlushPolicy::WriteBack => {
                self.dirty.borrow_mut().insert(path.to_owned());
                Ok(())
            }
        }
    }

    /// Unlink the inode file or rmdir the inode dir.
    pub async fn remove(&self, path: &str, is_dir: bool) -> std::io::Result<()> {
        if self.policy == FlushPolicy::Off {
            return Ok(());
        }
        // Whatever was queued for writeback is now obsolete.
        self.dirty.borrow_mut().remove(path);

        if is_dir {
            let meta = inode_dir_meta_path(&self.base_dir, path);
            let _ = std::fs::remove_file(&meta);
            let dir = inode_dir_path(&self.base_dir, path);
            // ignore ENOENT/ENOTEMPTY: the higher layer is responsible for
            // recursive child removal.
            let _ = std::fs::remove_dir(&dir);
        } else {
            let file = inode_file_path(&self.base_dir, path);
            match std::fs::remove_file(&file) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Read the on-disk inode for `path`. Returns `Ok(None)` if no such
    /// inode exists (lookup miss) and `Err` on I/O errors other than
    /// NotFound. Tries the file-shape first, then the dir-shape .meta.
    ///
    /// Uses synchronous `std::fs::read` because this is the cold path:
    /// lookup-miss recovery or eager startup walk. A single 4 KiB read on
    /// local NVMe is ~10 µs and the syscall is non-blocking from the
    /// kernel scheduler's perspective. Going async via io_uring would
    /// require ownership of the buffer (pluvio's `DmaFile::read` consumes
    /// it without giving it back), which forces fixed-buffer registration
    /// — overkill for a one-shot read.
    pub async fn read_inode(&self, path: &str) -> std::io::Result<Option<OnDiskInode>> {
        // Try regular file inode first.
        let file_path = inode_file_path(&self.base_dir, path);
        match std::fs::read(&file_path) {
            Ok(buf) if buf.len() == MSIZE as usize => return Ok(decode_inode(&buf)),
            Ok(_) => return Ok(None), // corrupt-size file; treat as miss
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }

        // Try directory inode .meta.
        let meta_path = inode_dir_meta_path(&self.base_dir, path);
        match std::fs::read(&meta_path) {
            Ok(buf) if buf.len() == MSIZE as usize => Ok(decode_inode(&buf)),
            Ok(_) => Ok(None),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Drain the writeback dirty set, re-encoding each entry from the
    /// caller-supplied snapshot of in-memory metadata. Returns the count
    /// of entries flushed. Designed to be called by a periodic task.
    ///
    /// The caller supplies `lookup`, a closure that resolves
    /// `path -> Option<(InodeType, mode, logical_size)>`. Entries the
    /// closure returns `None` for are unlinked from disk (treated as
    /// pending-delete).
    pub async fn flush_once<F>(&self, mut lookup: F) -> std::io::Result<usize>
    where
        F: FnMut(&str) -> Option<(OnDiskInodeType, u32, u64)>,
    {
        if self.policy != FlushPolicy::WriteBack {
            return Ok(0);
        }
        let pending: Vec<String> = self.dirty.borrow_mut().drain().collect();
        let mut count = 0;
        for path in pending {
            match lookup(&path) {
                Some((OnDiskInodeType::File, mode, size)) => {
                    self.write_file_inode(&path, mode, size).await?;
                }
                Some((OnDiskInodeType::Directory, mode, _)) => {
                    self.write_dir_inode(&path, mode).await?;
                }
                Some((OnDiskInodeType::Symlink, _, _)) => {
                    // not yet implemented; skip
                }
                None => {
                    // Entry was deleted between dirty-mark and flush. Best
                    // effort unlink — we don't know if it was file or dir.
                    let _ = self.remove(&path, false).await;
                    let _ = self.remove(&path, true).await;
                }
            }
            count += 1;
        }
        Ok(count)
    }

    // ---------- private helpers ----------

    async fn write_file_inode(
        &self,
        path: &str,
        mode: u32,
        logical_size: u64,
    ) -> std::io::Result<()> {
        self.ensure_shard_dir(path)?;
        let buf = encode_inode(
            path,
            OnDiskInodeType::File,
            mode,
            self.chunk_size,
            logical_size,
            0,
        )
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "path too long for inline inode",
            )
        })?;
        let file_path = inode_file_path(&self.base_dir, path);
        // Sync `std::fs::write` instead of pluvio_uring `DmaFile`. At
        // mdtest-easy 100k IOPS aggregate the io_uring open+write+close
        // chain hit EBADF on the OpenAt CQE — see project notes for the
        // 20668/20669 investigation. Sync pwrite is 30–50 µs per inode
        // on local NVMe; bounded by the RPC dispatcher's per-handler
        // serialization, this hasn't shown the same failure mode.
        std::fs::write(&file_path, &buf)?;
        Ok(())
    }

    async fn write_file_inode_full(
        &self,
        path: &str,
        existing: &OnDiskInode,
    ) -> std::io::Result<()> {
        let buf = encode_inode(
            path,
            existing.inode_type,
            existing.mode,
            existing.chunk_size,
            existing.logical_size,
            existing.flags,
        )
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "path too long for inline inode",
            )
        })?;
        self.ensure_shard_dir(path)?;
        let file_path = inode_file_path(&self.base_dir, path);
        std::fs::write(&file_path, &buf)?;
        Ok(())
    }

    async fn write_dir_inode(&self, path: &str, mode: u32) -> std::io::Result<()> {
        self.ensure_shard_dir(path)?;
        let dir = inode_dir_path(&self.base_dir, path);
        std::fs::create_dir_all(&dir)?;
        let buf =
            encode_inode(path, OnDiskInodeType::Directory, mode, 0, 0, 0).ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "path too long for inline inode",
                )
            })?;
        let meta = inode_dir_meta_path(&self.base_dir, path);
        std::fs::write(&meta, &buf)?;
        Ok(())
    }

    /// Eager recovery iterator. Walks `<base>/*/*` and decodes each inode
    /// found. Phase C opt-in; not called from Phase B startup. The current
    /// implementation is a synchronous walk: appropriate because recovery
    /// is one-shot at process start, before RPC dispatch begins.
    pub fn iter_for_recovery(&self) -> impl Iterator<Item = std::io::Result<OnDiskInode>> {
        RecoveryIter::new(self.base_dir.clone())
    }

    /// Batch-flush every (path, size) and directory listed in the
    /// snapshots to disk. Designed for `FlushPolicy::OnFinalize`: the
    /// run paid no per-RPC disk cost, so this single call must move
    /// the whole working set onto NVMe. Synchronous and blocking — meant
    /// to be invoked during the server's shutdown sequence before the
    /// process exits.
    ///
    /// Returns `(files_written, dirs_written, file_errors, dir_errors)`.
    /// Errors are logged via `eprintln!` and don't abort the loop —
    /// the goal is to make best-effort progress through the entire
    /// snapshot before the server dies.
    ///
    /// Calling under `FlushPolicy::Off` is a no-op and returns zeros.
    pub fn flush_all_sync(
        &self,
        files: &[(String, u64)],
        dirs: &[String],
    ) -> (usize, usize, usize, usize) {
        if self.policy == FlushPolicy::Off {
            return (0, 0, 0, 0);
        }
        let mut fwritten = 0usize;
        let mut dwritten = 0usize;
        let mut ferr = 0usize;
        let mut derr = 0usize;

        for path in dirs.iter() {
            if let Err(e) = self.write_dir_inode_blocking(path, 0o040755) {
                ferr += 0; // count toward dirs below
                derr += 1;
                eprintln!(
                    "[inode_store.flush_all_sync] dir {} write failed: {}",
                    path, e
                );
            } else {
                dwritten += 1;
            }
        }
        for (path, size) in files.iter() {
            if let Err(e) = self.write_file_inode_blocking(path, 0o100644, *size) {
                ferr += 1;
                eprintln!(
                    "[inode_store.flush_all_sync] file {} write failed: {}",
                    path, e
                );
            } else {
                fwritten += 1;
            }
        }
        eprintln!(
            "[inode_store.flush_all_sync] flushed dirs={}/{} files={}/{} (policy={:?})",
            dwritten,
            dirs.len(),
            fwritten,
            files.len(),
            self.policy
        );
        (fwritten, dwritten, ferr, derr)
    }

    /// Sync wrapper around `write_file_inode`. Same disk layout; this
    /// variant is callable from non-async contexts (the shutdown
    /// sequence may be invoked from a signal handler or post-runtime
    /// teardown).
    fn write_file_inode_blocking(
        &self,
        path: &str,
        mode: u32,
        logical_size: u64,
    ) -> std::io::Result<()> {
        self.ensure_shard_dir(path)?;
        let buf = encode_inode(
            path,
            OnDiskInodeType::File,
            mode,
            self.chunk_size,
            logical_size,
            0,
        )
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "path too long for inline inode",
            )
        })?;
        let file_path = inode_file_path(&self.base_dir, path);
        std::fs::write(&file_path, &buf)
    }

    /// Sync wrapper around `write_dir_inode`. See `write_file_inode_blocking`.
    fn write_dir_inode_blocking(&self, path: &str, mode: u32) -> std::io::Result<()> {
        self.ensure_shard_dir(path)?;
        let dir = inode_dir_path(&self.base_dir, path);
        std::fs::create_dir_all(&dir)?;
        let buf =
            encode_inode(path, OnDiskInodeType::Directory, mode, 0, 0, 0).ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "path too long for inline inode",
                )
            })?;
        let meta = inode_dir_meta_path(&self.base_dir, path);
        std::fs::write(&meta, &buf)
    }
}

/// Recovery iterator: produces decoded `OnDiskInode` entries one at a
/// time. Errors during traversal are surfaced as `Err` items; the caller
/// can choose to skip or abort.
struct RecoveryIter {
    shard_iter: Option<std::fs::ReadDir>,
    current_shard: Option<std::fs::ReadDir>,
}

impl RecoveryIter {
    fn new(base: PathBuf) -> Self {
        let shard_iter = std::fs::read_dir(&base).ok();
        Self {
            shard_iter,
            current_shard: None,
        }
    }
}

impl Iterator for RecoveryIter {
    type Item = std::io::Result<OnDiskInode>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(it) = self.current_shard.as_mut() {
                match it.next() {
                    Some(Ok(entry)) => {
                        let p = entry.path();
                        let buf = match std::fs::read(&p) {
                            Ok(b) => b,
                            Err(e) => return Some(Err(e)),
                        };
                        // file-shape: <hash>: → buf is the inode itself.
                        // dir-shape: <hash>/ → recurse into .meta.
                        if buf.len() == MSIZE as usize {
                            if let Some(inode) = decode_inode(&buf) {
                                return Some(Ok(inode));
                            }
                            continue;
                        }
                        if p.is_dir() {
                            let meta = p.join(".meta");
                            match std::fs::read(&meta) {
                                Ok(b) if b.len() == MSIZE as usize => {
                                    if let Some(inode) = decode_inode(&b) {
                                        return Some(Ok(inode));
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                    Some(Err(e)) => return Some(Err(e)),
                    None => {
                        self.current_shard = None;
                    }
                }
            } else if let Some(shards) = self.shard_iter.as_mut() {
                match shards.next() {
                    Some(Ok(entry)) => {
                        if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                            self.current_shard = std::fs::read_dir(entry.path()).ok();
                        }
                    }
                    Some(Err(e)) => return Some(Err(e)),
                    None => return None,
                }
            } else {
                return None;
            }
        }
    }
}

fn path_to_str(p: &Path) -> Option<&str> {
    p.as_os_str().to_str().or_else(|| {
        // Fallback for non-UTF8 paths (extremely rare on Linux).
        let s = p.as_os_str();
        OsStr::to_str(s)
    })
}

// `silence unused-import` shield: `EXT_OFFSET` is part of the on-disk
// schema and is re-exported by `storage::mod`; keep the import alive so
// changes here cause a compile-time poke if anyone moves the constant.
#[allow(dead_code)]
const _PIN: u64 = EXT_OFFSET;

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_store(policy: FlushPolicy) -> (TempDir, InodeStore) {
        let dir = TempDir::new().unwrap();
        let store = InodeStore::new(dir.path(), 4 * 1024 * 1024, policy).unwrap();
        (dir, store)
    }

    #[test]
    fn off_mode_is_a_noop() {
        // We can't easily run the async path here without a runtime, but
        // we can at least exercise the disabled flag.
        let (_d, store) = make_store(FlushPolicy::Off);
        assert!(store.is_disabled());
        assert_eq!(store.policy(), FlushPolicy::Off);
    }

    #[test]
    fn paths_use_shard_layout() {
        let (d, _store) = make_store(FlushPolicy::WriteThrough);
        let file_p = inode_file_path(d.path(), "/foo");
        let dir_p = inode_dir_path(d.path(), "/foo");
        let meta_p = inode_dir_meta_path(d.path(), "/foo");

        // Both should sit under the same shard directory.
        let file_parent = file_p.parent().unwrap();
        let dir_parent = dir_p.parent().unwrap();
        assert_eq!(file_parent, dir_parent);

        // File leaf carries a trailing colon, dir leaf does not.
        let file_leaf = file_p.file_name().unwrap().to_str().unwrap();
        let dir_leaf = dir_p.file_name().unwrap().to_str().unwrap();
        assert!(file_leaf.ends_with(':'));
        assert!(!dir_leaf.ends_with(':'));
        assert_eq!(file_leaf.len(), dir_leaf.len() + 1);

        // .meta is inside the dir leaf.
        assert_eq!(meta_p.parent().unwrap(), &dir_p);
        assert_eq!(meta_p.file_name().unwrap(), ".meta");
    }
}
