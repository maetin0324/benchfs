use super::{
    FileHandle, OpenFlags, StorageBackend,
    error::{StorageError, StorageResult},
    iouring::IOUringBackend,
};
use crate::metadata::{
    DirectoryMetadata, FileMetadata,
    types::{InodeId, InodeType},
};
use pluvio_uring::allocator::FixedBufferAllocator;
use std::cell::RefCell;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::rc::Rc;

/// ローカルファイルシステム
///
/// ファイルとディレクトリのメタデータを管理し、
/// IOURINGバックエンドを使ってデータI/Oを行う
///
/// Note: シングルスレッド設計のため、RcとRefCellを使用
pub struct LocalFileSystem {
    /// ストレージバックエンド (IOURING)
    backend: Rc<IOUringBackend>,

    /// ルートディレクトリ
    root: PathBuf,

    /// Inodeカウンター
    next_inode: RefCell<InodeId>,

    /// パス -> Inode マッピング
    path_to_inode: RefCell<HashMap<PathBuf, InodeId>>,

    /// Inode -> ファイルメタデータ
    file_metadata: RefCell<HashMap<InodeId, FileMetadata>>,

    /// Inode -> ディレクトリメタデータ
    dir_metadata: RefCell<HashMap<InodeId, DirectoryMetadata>>,
}

impl LocalFileSystem {
    /// 新しいローカルファイルシステムを作成
    ///
    /// # Arguments
    /// * `root` - ルートディレクトリのパス
    /// * `allocator` - Registered buffer allocator
    pub fn new(root: PathBuf, allocator: Rc<FixedBufferAllocator>) -> StorageResult<Self> {
        // ルートディレクトリが存在することを確認
        if !root.exists() {
            return Err(StorageError::NotFound(root.display().to_string()));
        }

        if !root.is_dir() {
            return Err(StorageError::Internal(format!(
                "Root path is not a directory: {}",
                root.display()
            )));
        }

        let backend = Rc::new(IOUringBackend::new(allocator));

        let mut path_to_inode = HashMap::new();
        let mut dir_metadata = HashMap::new();

        // ルートディレクトリのメタデータを作成
        let root_inode = 1u64;
        path_to_inode.insert(PathBuf::from("/"), root_inode);

        let mut root_meta = DirectoryMetadata::new(root_inode, "/".to_string());
        root_meta.owner_node = "local".to_string();
        dir_metadata.insert(root_inode, root_meta);

        Ok(Self {
            backend,
            root,
            next_inode: RefCell::new(2), // 1はルート用
            path_to_inode: RefCell::new(path_to_inode),
            file_metadata: RefCell::new(HashMap::new()),
            dir_metadata: RefCell::new(dir_metadata),
        })
    }

    /// 新しいInodeを割り当て
    fn allocate_inode(&self) -> InodeId {
        let mut next_inode = self.next_inode.borrow_mut();
        let inode = *next_inode;
        *next_inode += 1;
        inode
    }

    /// 仮想パスを物理パスに変換
    fn virtual_to_physical(&self, virtual_path: &Path) -> PathBuf {
        if virtual_path.is_absolute() {
            // /foo/bar -> <root>/foo/bar
            let relative = virtual_path.strip_prefix("/").unwrap_or(virtual_path);
            self.root.join(relative)
        } else {
            self.root.join(virtual_path)
        }
    }

    /// パスからInodeを取得
    fn get_inode(&self, path: &Path) -> Option<InodeId> {
        let path_to_inode = self.path_to_inode.borrow();
        path_to_inode.get(path).copied()
    }

    /// ファイルメタデータを取得
    pub fn get_file_metadata(&self, inode: InodeId) -> Option<FileMetadata> {
        let file_metadata = self.file_metadata.borrow();
        file_metadata.get(&inode).cloned()
    }

    /// ディレクトリメタデータを取得
    pub fn get_dir_metadata(&self, inode: InodeId) -> Option<DirectoryMetadata> {
        let dir_metadata = self.dir_metadata.borrow();
        dir_metadata.get(&inode).cloned()
    }

    /// ファイルを開く (メタデータも管理)
    pub async fn open_file(&self, path: &Path, flags: OpenFlags) -> StorageResult<FileHandle> {
        let physical_path = self.virtual_to_physical(path);

        // ファイルが存在するか確認
        if !physical_path.exists() && !flags.create {
            return Err(StorageError::NotFound(path.display().to_string()));
        }

        // バックエンドでファイルを開く
        let handle = self.backend.open(&physical_path, flags).await?;

        // メタデータを登録 (存在しない場合)
        let inode = match self.get_inode(path) {
            Some(inode) => inode,
            None => {
                let inode = self.allocate_inode();
                let stat = self.backend.stat(&physical_path).await?;

                let metadata = FileMetadata::new(path.display().to_string(), stat.size);

                let mut path_to_inode = self.path_to_inode.borrow_mut();
                path_to_inode.insert(path.to_path_buf(), inode);

                let mut file_metadata = self.file_metadata.borrow_mut();
                file_metadata.insert(inode, metadata);

                inode
            }
        };

        tracing::debug!(
            "Opened file: {} (inode={}, handle={:?})",
            path.display(),
            inode,
            handle
        );

        Ok(handle)
    }

    /// ファイルを作成
    pub async fn create_file(&self, path: &Path, mode: u32) -> StorageResult<FileHandle> {
        let physical_path = self.virtual_to_physical(path);

        // ファイルが既に存在する場合はエラー
        if physical_path.exists() {
            return Err(StorageError::AlreadyExists(path.display().to_string()));
        }

        // バックエンドでファイルを作成
        let handle = self.backend.create(&physical_path, mode).await?;

        // メタデータを登録
        let inode = self.allocate_inode();
        let metadata = FileMetadata::new(path.display().to_string(), 0);

        // 親ディレクトリのinodeを先に取得（borrowの競合を避けるため）
        let parent_inode_opt = path.parent().and_then(|p| self.get_inode(p));

        {
            let mut path_to_inode = self.path_to_inode.borrow_mut();
            path_to_inode.insert(path.to_path_buf(), inode);
        }

        {
            let mut file_metadata = self.file_metadata.borrow_mut();
            file_metadata.insert(inode, metadata);
        }

        // 親ディレクトリのメタデータを更新
        if let Some(parent_inode) = parent_inode_opt {
            let mut dir_metadata = self.dir_metadata.borrow_mut();
            if let Some(parent_meta) = dir_metadata.get_mut(&parent_inode) {
                let filename = path
                    .file_name()
                    .and_then(|s| s.to_str())
                    .unwrap_or("")
                    .to_string();
                parent_meta.add_child(filename, inode, InodeType::File);
            }
        }

        tracing::info!(
            "Created file: {} (inode={}, mode={:o})",
            path.display(),
            inode,
            mode
        );

        Ok(handle)
    }

    /// ファイルを削除
    pub async fn unlink_file(&self, path: &Path) -> StorageResult<()> {
        let physical_path = self.virtual_to_physical(path);

        // Inodeを取得
        let inode = self
            .get_inode(path)
            .ok_or_else(|| StorageError::NotFound(path.display().to_string()))?;

        // バックエンドでファイルを削除
        self.backend.unlink(&physical_path).await?;

        // 親ディレクトリのinodeを先に取得（borrowの競合を避けるため）
        let parent_inode_opt = path.parent().and_then(|p| self.get_inode(p));

        // メタデータを削除
        {
            let mut path_to_inode = self.path_to_inode.borrow_mut();
            path_to_inode.remove(path);
        }

        {
            let mut file_metadata = self.file_metadata.borrow_mut();
            file_metadata.remove(&inode);
        }

        // 親ディレクトリのメタデータを更新
        if let Some(parent_inode) = parent_inode_opt {
            let mut dir_metadata = self.dir_metadata.borrow_mut();
            if let Some(parent_meta) = dir_metadata.get_mut(&parent_inode) {
                let filename = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
                parent_meta.remove_child(filename);
            }
        }

        tracing::info!("Unlinked file: {} (inode={})", path.display(), inode);

        Ok(())
    }

    /// ディレクトリを作成
    pub async fn create_directory(&self, path: &Path, mode: u32) -> StorageResult<()> {
        let physical_path = self.virtual_to_physical(path);

        // ディレクトリが既に存在する場合はエラー
        if physical_path.exists() {
            return Err(StorageError::AlreadyExists(path.display().to_string()));
        }

        // バックエンドでディレクトリを作成
        self.backend.mkdir(&physical_path, mode).await?;

        // メタデータを登録
        let inode = self.allocate_inode();
        let mut metadata = DirectoryMetadata::new(inode, path.display().to_string());
        metadata.owner_node = "local".to_string();
        metadata.permissions.mode = mode;

        // 親ディレクトリのinodeを先に取得（borrowの競合を避けるため）
        let parent_inode_opt = path.parent().and_then(|p| self.get_inode(p));

        {
            let mut path_to_inode = self.path_to_inode.borrow_mut();
            path_to_inode.insert(path.to_path_buf(), inode);
        }

        {
            let mut dir_metadata = self.dir_metadata.borrow_mut();
            dir_metadata.insert(inode, metadata);

            // 親ディレクトリのメタデータを更新
            if let Some(parent_inode) = parent_inode_opt {
                if let Some(parent_meta) = dir_metadata.get_mut(&parent_inode) {
                    let dirname = path
                        .file_name()
                        .and_then(|s| s.to_str())
                        .unwrap_or("")
                        .to_string();
                    parent_meta.add_child(dirname, inode, InodeType::Directory);
                }
            }
        }

        tracing::info!(
            "Created directory: {} (inode={}, mode={:o})",
            path.display(),
            inode,
            mode
        );

        Ok(())
    }

    /// ディレクトリを削除
    pub async fn remove_directory(&self, path: &Path) -> StorageResult<()> {
        let physical_path = self.virtual_to_physical(path);

        // Inodeを取得
        let inode = self
            .get_inode(path)
            .ok_or_else(|| StorageError::NotFound(path.display().to_string()))?;

        // ディレクトリが空であることを確認
        {
            let dir_metadata = self.dir_metadata.borrow();
            if let Some(metadata) = dir_metadata.get(&inode) {
                if !metadata.children.is_empty() {
                    return Err(StorageError::Internal("Directory not empty".to_string()));
                }
            }
        }

        // バックエンドでディレクトリを削除
        self.backend.rmdir(&physical_path).await?;

        // 親ディレクトリのinodeを先に取得（borrowの競合を避けるため）
        let parent_inode_opt = path.parent().and_then(|p| self.get_inode(p));

        // メタデータを削除
        {
            let mut path_to_inode = self.path_to_inode.borrow_mut();
            path_to_inode.remove(path);
        }

        {
            let mut dir_metadata = self.dir_metadata.borrow_mut();
            dir_metadata.remove(&inode);

            // 親ディレクトリのメタデータを更新
            if let Some(parent_inode) = parent_inode_opt {
                if let Some(parent_meta) = dir_metadata.get_mut(&parent_inode) {
                    let dirname = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
                    parent_meta.remove_child(dirname);
                }
            }
        }

        tracing::info!("Removed directory: {} (inode={})", path.display(), inode);

        Ok(())
    }

    /// ディレクトリの内容を一覧
    pub async fn list_directory(&self, path: &Path) -> StorageResult<Vec<(String, InodeType)>> {
        let inode = self
            .get_inode(path)
            .ok_or_else(|| StorageError::NotFound(path.display().to_string()))?;

        let dir_metadata = self.dir_metadata.borrow();
        let metadata = dir_metadata
            .get(&inode)
            .ok_or_else(|| StorageError::NotFound(path.display().to_string()))?;

        let entries = metadata
            .children
            .iter()
            .map(|entry| (entry.name.clone(), entry.inode_type))
            .collect();

        Ok(entries)
    }

    /// バックエンドへの直接アクセス (read/write/close用)
    pub fn backend(&self) -> &IOUringBackend {
        &self.backend
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pluvio_runtime::executor::Runtime;
    use pluvio_uring::reactor::IoUringReactor;
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
    fn test_create_directory() {
        let (runtime, allocator) = setup_runtime();
        let temp_dir = TempDir::new().unwrap();

        runtime
            .clone()
            .run_with_name_and_runtime("local_fs_test_create_directory", async move {
                let fs = LocalFileSystem::new(temp_dir.path().to_path_buf(), allocator).unwrap();

                let dir_path = Path::new("/testdir");

                // ディレクトリを作成
                fs.create_directory(dir_path, 0o755).await.unwrap();

                // メタデータを確認
                let metadata = fs.get_dir_metadata(2).unwrap();
                assert_eq!(metadata.path, "/testdir");
                assert_eq!(metadata.inode, 2);
            });
    }

    #[test]
    fn test_list_directory() {
        let (runtime, allocator) = setup_runtime();
        let temp_dir = TempDir::new().unwrap();

        runtime
            .clone()
            .run_with_name_and_runtime("local_fs_test_list_directory", async move {
                let fs = LocalFileSystem::new(temp_dir.path().to_path_buf(), allocator).unwrap();

                // ディレクトリを作成
                fs.create_directory(Path::new("/dir1"), 0o755)
                    .await
                    .unwrap();

                // ルートディレクトリの内容を一覧
                let entries = fs.list_directory(Path::new("/")).await.unwrap();

                assert_eq!(entries.len(), 1);
                assert!(entries.contains(&("dir1".to_string(), InodeType::Directory)));
            });
    }

    #[test]
    fn test_create_and_open_file() {
        let (runtime, allocator) = setup_runtime();
        let temp_dir = TempDir::new().unwrap();

        runtime
            .clone()
            .run_with_name_and_runtime("local_fs_test_create_and_open_file", async move {
                let fs = LocalFileSystem::new(temp_dir.path().to_path_buf(), allocator).unwrap();

                let file_path = Path::new("/test.txt");

                // ファイルを作成
                let handle = fs.create_file(file_path, 0o644).await.unwrap();
                fs.backend().close(handle).await.unwrap();

                // ファイルを開く
                let handle = fs
                    .open_file(file_path, OpenFlags::read_only())
                    .await
                    .unwrap();
                fs.backend().close(handle).await.unwrap();

                // メタデータを確認
                let metadata = fs.get_file_metadata(2).unwrap(); // inode=2 (1はルート)
                assert_eq!(metadata.path, "/test.txt");
                assert_eq!(metadata.size, 0);
            });
    }

    #[test]
    fn test_path_to_inode_mapping() {
        let temp_dir = TempDir::new().unwrap();
        // This test doesn't need runtime, so create a dummy allocator
        // However, we need an allocator - skip this test for now or create a mock
        // For simplicity, we'll create a minimal setup
        use pluvio_runtime::executor::Runtime;
        use pluvio_uring::reactor::IoUringReactor;
        use std::time::Duration;

        let runtime = Runtime::new(1024);
        let reactor = IoUringReactor::builder()
            .queue_size(256)
            .buffer_size(4096)
            .submit_depth(32)
            .wait_submit_timeout(Duration::from_millis(100))
            .wait_complete_timeout(Duration::from_millis(150))
            .build();
        let allocator = Rc::clone(&reactor.allocator);
        runtime.register_reactor("io_uring_reactor", reactor);

        let fs = LocalFileSystem::new(temp_dir.path().to_path_buf(), allocator).unwrap();

        // ルートディレクトリのinodeを確認
        let root_inode = fs.get_inode(Path::new("/"));
        assert_eq!(root_inode, Some(1));

        // 存在しないパスはNone
        let nonexistent = fs.get_inode(Path::new("/nonexistent"));
        assert_eq!(nonexistent, None);
    }
}
