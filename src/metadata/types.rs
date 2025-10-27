use serde::{Deserialize, Serialize};
use std::time::SystemTime;

/// Inode番号
pub type InodeId = u64;

/// ファイルID (パスのハッシュまたはinode番号)
pub type FileId = u64;

/// チャンクID
pub type ChunkId = u64;

/// ノードID (サーバーアドレス)
pub type NodeId = String;

/// Inodeタイプ
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum InodeType {
    File,
    Directory,
    Symlink,
}

/// ファイルパーミッション (Unix風)
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct FilePermissions {
    pub mode: u32, // Unix mode (例: 0o644)
    pub uid: u32,  // User ID
    pub gid: u32,  // Group ID
}

impl Default for FilePermissions {
    fn default() -> Self {
        Self {
            mode: 0o644, // rw-r--r--
            uid: 0,
            gid: 0,
        }
    }
}

/// ファイルメタデータ (simplified for path-based KV design)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileMetadata {
    /// フルパス
    pub path: String,

    /// ファイルサイズ (バイト)
    pub size: u64,
}

impl FileMetadata {
    /// 新しいファイルメタデータを作成
    pub fn new(path: String, size: u64) -> Self {
        Self { path, size }
    }

    /// チャンク数を計算
    pub fn calculate_chunk_count(&self) -> u64 {
        if self.size == 0 {
            0
        } else {
            (self.size + crate::metadata::CHUNK_SIZE as u64 - 1)
                / crate::metadata::CHUNK_SIZE as u64
        }
    }

    /// チャンクオフセットを計算
    pub fn chunk_offset(&self, chunk_index: u64) -> u64 {
        chunk_index * crate::metadata::CHUNK_SIZE as u64
    }

    /// チャンクサイズを計算 (最終チャンクは小さい可能性がある)
    pub fn chunk_size(&self, chunk_index: u64) -> u64 {
        let offset = self.chunk_offset(chunk_index);
        if offset >= self.size {
            0
        } else {
            let remaining = self.size - offset;
            remaining.min(crate::metadata::CHUNK_SIZE as u64)
        }
    }
}

/// ディレクトリメタデータ
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectoryMetadata {
    /// Inode番号
    pub inode: InodeId,

    /// ディレクトリ名
    pub name: String,

    /// フルパス
    pub path: String,

    /// パーミッション
    pub permissions: FilePermissions,

    /// 作成時刻
    pub created_at: SystemTime,

    /// 最終更新時刻
    pub modified_at: SystemTime,

    /// 子エントリ (名前 -> inode)
    pub children: Vec<DirectoryEntry>,

    /// 所有ノードID
    pub owner_node: NodeId,
}

/// ディレクトリエントリ
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectoryEntry {
    pub name: String,
    pub inode: InodeId,
    pub inode_type: InodeType,
}

impl DirectoryMetadata {
    /// 新しいディレクトリメタデータを作成
    pub fn new(inode: InodeId, path: String) -> Self {
        let name = path.split('/').last().unwrap_or("").to_string();

        let now = SystemTime::now();

        Self {
            inode,
            name,
            path,
            permissions: FilePermissions {
                mode: 0o755, // rwxr-xr-x
                uid: 0,
                gid: 0,
            },
            created_at: now,
            modified_at: now,
            children: Vec::new(),
            owner_node: String::new(),
        }
    }

    /// 子エントリを追加
    pub fn add_child(&mut self, name: String, inode: InodeId, inode_type: InodeType) {
        self.children.push(DirectoryEntry {
            name,
            inode,
            inode_type,
        });
        self.modified_at = SystemTime::now();
    }

    /// 子エントリを削除
    pub fn remove_child(&mut self, name: &str) -> Option<DirectoryEntry> {
        if let Some(pos) = self.children.iter().position(|e| e.name == name) {
            self.modified_at = SystemTime::now();
            Some(self.children.remove(pos))
        } else {
            None
        }
    }

    /// 子エントリを検索
    pub fn find_child(&self, name: &str) -> Option<&DirectoryEntry> {
        self.children.iter().find(|e| e.name == name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_metadata_chunk_count() {
        let meta = FileMetadata::new("/test/file.txt".to_string(), 0);
        assert_eq!(meta.calculate_chunk_count(), 0);

        let meta = FileMetadata::new("/test/file.txt".to_string(), 1024);
        assert_eq!(meta.calculate_chunk_count(), 1);

        let meta = FileMetadata::new(
            "/test/file.txt".to_string(),
            crate::metadata::CHUNK_SIZE as u64,
        );
        assert_eq!(meta.calculate_chunk_count(), 1);

        let meta = FileMetadata::new(
            "/test/file.txt".to_string(),
            (crate::metadata::CHUNK_SIZE as u64) + 1,
        );
        assert_eq!(meta.calculate_chunk_count(), 2);

        let meta = FileMetadata::new(
            "/test/file.txt".to_string(),
            10 * crate::metadata::CHUNK_SIZE as u64,
        );
        assert_eq!(meta.calculate_chunk_count(), 10);
    }

    #[test]
    fn test_file_metadata_chunk_size() {
        // CHUNK_SIZE = 4MB
        // size = 10MB + 1KB = 3チャンク必要
        // chunk 0: 4MB
        // chunk 1: 4MB
        // chunk 2: 2MB + 1KB (最終チャンク)
        let size = 10 * 1024 * 1024 + 1024; // 10MB + 1KB
        let meta = FileMetadata::new("/test/file.txt".to_string(), size);

        assert_eq!(meta.calculate_chunk_count(), 3); // 3チャンク必要
        assert_eq!(meta.chunk_size(0), crate::metadata::CHUNK_SIZE as u64);
        assert_eq!(meta.chunk_size(1), crate::metadata::CHUNK_SIZE as u64);
        assert_eq!(meta.chunk_size(2), 2 * 1024 * 1024 + 1024); // 2MB + 1KB
        assert_eq!(meta.chunk_size(3), 0); // 範囲外
    }

    #[test]
    fn test_directory_metadata_children() {
        let mut dir = DirectoryMetadata::new(1, "/test".to_string());

        dir.add_child("file1.txt".to_string(), 2, InodeType::File);
        dir.add_child("subdir".to_string(), 3, InodeType::Directory);

        assert_eq!(dir.children.len(), 2);

        assert!(dir.find_child("file1.txt").is_some());
        assert!(dir.find_child("subdir").is_some());
        assert!(dir.find_child("nonexistent").is_none());

        let removed = dir.remove_child("file1.txt");
        assert!(removed.is_some());
        assert_eq!(dir.children.len(), 1);
    }
}
