use super::{
    error::{StorageError, StorageResult},
    iouring::IOUringBackend,
    FileHandle, OpenFlags, StorageBackend,
};
use crate::metadata::{
    types::{InodeId, InodeType},
    DirectoryMetadata, FileMetadata,
};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;

/// ローカルファイルシステム
///
/// ファイルとディレクトリのメタデータを管理し、
/// IOURINGバックエンドを使ってデータI/Oを行う
pub struct LocalFileSystem {
    /// ストレージバックエンド (IOURING)
    backend: Arc<IOUringBackend>,

    /// ルートディレクトリ
    root: PathBuf,

    /// Inodeカウンター
    next_inode: Arc<RwLock<InodeId>>,

    /// パス -> Inode マッピング
    path_to_inode: Arc<RwLock<HashMap<PathBuf, InodeId>>>,

    /// Inode -> ファイルメタデータ
    file_metadata: Arc<RwLock<HashMap<InodeId, FileMetadata>>>,

    /// Inode -> ディレクトリメタデータ
    dir_metadata: Arc<RwLock<HashMap<InodeId, DirectoryMetadata>>>,
}

impl LocalFileSystem {
    /// 新しいローカルファイルシステムを作成
    pub fn new(root: PathBuf) -> StorageResult<Self> {
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

        let backend = Arc::new(IOUringBackend::new());

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
            next_inode: Arc::new(RwLock::new(2)), // 1はルート用
            path_to_inode: Arc::new(RwLock::new(path_to_inode)),
            file_metadata: Arc::new(RwLock::new(HashMap::new())),
            dir_metadata: Arc::new(RwLock::new(dir_metadata)),
        })
    }

    /// 新しいInodeを割り当て
    async fn allocate_inode(&self) -> InodeId {
        let mut next_inode = self.next_inode.write().await;
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
    async fn get_inode(&self, path: &Path) -> Option<InodeId> {
        let path_to_inode = self.path_to_inode.read().await;
        path_to_inode.get(path).copied()
    }

    /// ファイルメタデータを取得
    pub async fn get_file_metadata(&self, inode: InodeId) -> Option<FileMetadata> {
        let file_metadata = self.file_metadata.read().await;
        file_metadata.get(&inode).cloned()
    }

    /// ディレクトリメタデータを取得
    pub async fn get_dir_metadata(&self, inode: InodeId) -> Option<DirectoryMetadata> {
        let dir_metadata = self.dir_metadata.read().await;
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
        let inode = match self.get_inode(path).await {
            Some(inode) => inode,
            None => {
                let inode = self.allocate_inode().await;
                let stat = self.backend.stat(&physical_path).await?;

                let mut metadata = FileMetadata::new(inode, path.display().to_string(), stat.size);
                metadata.owner_node = "local".to_string();
                metadata.permissions.mode = stat.mode;
                metadata.permissions.uid = stat.uid;
                metadata.permissions.gid = stat.gid;

                let mut path_to_inode = self.path_to_inode.write().await;
                path_to_inode.insert(path.to_path_buf(), inode);

                let mut file_metadata = self.file_metadata.write().await;
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
        let inode = self.allocate_inode().await;
        let mut metadata = FileMetadata::new(inode, path.display().to_string(), 0);
        metadata.owner_node = "local".to_string();
        metadata.permissions.mode = mode;

        let mut path_to_inode = self.path_to_inode.write().await;
        path_to_inode.insert(path.to_path_buf(), inode);

        let mut file_metadata = self.file_metadata.write().await;
        file_metadata.insert(inode, metadata);

        // 親ディレクトリのメタデータを更新
        if let Some(parent) = path.parent() {
            if let Some(parent_inode) = self.get_inode(parent).await {
                let mut dir_metadata = self.dir_metadata.write().await;
                if let Some(parent_meta) = dir_metadata.get_mut(&parent_inode) {
                    let filename = path
                        .file_name()
                        .and_then(|s| s.to_str())
                        .unwrap_or("")
                        .to_string();
                    parent_meta.add_child(filename, inode, InodeType::File);
                }
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
            .await
            .ok_or_else(|| StorageError::NotFound(path.display().to_string()))?;

        // バックエンドでファイルを削除
        self.backend.unlink(&physical_path).await?;

        // メタデータを削除
        let mut path_to_inode = self.path_to_inode.write().await;
        path_to_inode.remove(path);

        let mut file_metadata = self.file_metadata.write().await;
        file_metadata.remove(&inode);

        // 親ディレクトリのメタデータを更新
        if let Some(parent) = path.parent() {
            if let Some(parent_inode) = self.get_inode(parent).await {
                let mut dir_metadata = self.dir_metadata.write().await;
                if let Some(parent_meta) = dir_metadata.get_mut(&parent_inode) {
                    let filename = path
                        .file_name()
                        .and_then(|s| s.to_str())
                        .unwrap_or("");
                    parent_meta.remove_child(filename);
                }
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
        let inode = self.allocate_inode().await;
        let mut metadata = DirectoryMetadata::new(inode, path.display().to_string());
        metadata.owner_node = "local".to_string();
        metadata.permissions.mode = mode;

        let mut path_to_inode = self.path_to_inode.write().await;
        path_to_inode.insert(path.to_path_buf(), inode);

        let mut dir_metadata = self.dir_metadata.write().await;
        dir_metadata.insert(inode, metadata);

        // 親ディレクトリのメタデータを更新
        if let Some(parent) = path.parent() {
            if let Some(parent_inode) = self.get_inode(parent).await {
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
            .await
            .ok_or_else(|| StorageError::NotFound(path.display().to_string()))?;

        // ディレクトリが空であることを確認
        {
            let dir_metadata = self.dir_metadata.read().await;
            if let Some(metadata) = dir_metadata.get(&inode) {
                if !metadata.children.is_empty() {
                    return Err(StorageError::Internal(
                        "Directory not empty".to_string(),
                    ));
                }
            }
        }

        // バックエンドでディレクトリを削除
        self.backend.rmdir(&physical_path).await?;

        // メタデータを削除
        let mut path_to_inode = self.path_to_inode.write().await;
        path_to_inode.remove(path);

        let mut dir_metadata = self.dir_metadata.write().await;
        dir_metadata.remove(&inode);

        // 親ディレクトリのメタデータを更新
        if let Some(parent) = path.parent() {
            if let Some(parent_inode) = self.get_inode(parent).await {
                if let Some(parent_meta) = dir_metadata.get_mut(&parent_inode) {
                    let dirname = path
                        .file_name()
                        .and_then(|s| s.to_str())
                        .unwrap_or("");
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
            .await
            .ok_or_else(|| StorageError::NotFound(path.display().to_string()))?;

        let dir_metadata = self.dir_metadata.read().await;
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
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_create_and_open_file() {
        let temp_dir = TempDir::new().unwrap();
        let fs = LocalFileSystem::new(temp_dir.path().to_path_buf()).unwrap();

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
        let metadata = fs.get_file_metadata(2).await.unwrap(); // inode=2 (1はルート)
        assert_eq!(metadata.path, "/test.txt");
        assert_eq!(metadata.size, 0);
    }

    #[tokio::test]
    async fn test_create_directory() {
        let temp_dir = TempDir::new().unwrap();
        let fs = LocalFileSystem::new(temp_dir.path().to_path_buf()).unwrap();

        let dir_path = Path::new("/testdir");

        // ディレクトリを作成
        fs.create_directory(dir_path, 0o755).await.unwrap();

        // メタデータを確認
        let metadata = fs.get_dir_metadata(2).await.unwrap();
        assert_eq!(metadata.path, "/testdir");
    }

    #[tokio::test]
    async fn test_list_directory() {
        let temp_dir = TempDir::new().unwrap();
        let fs = LocalFileSystem::new(temp_dir.path().to_path_buf()).unwrap();

        // ディレクトリとファイルを作成
        fs.create_directory(Path::new("/dir1"), 0o755)
            .await
            .unwrap();
        let handle = fs.create_file(Path::new("/file1.txt"), 0o644).await.unwrap();
        fs.backend().close(handle).await.unwrap();

        // ルートディレクトリの内容を一覧
        let entries = fs.list_directory(Path::new("/")).await.unwrap();

        assert_eq!(entries.len(), 2);
        assert!(entries.contains(&("dir1".to_string(), InodeType::Directory)));
        assert!(entries.contains(&("file1.txt".to_string(), InodeType::File)));
    }
}
