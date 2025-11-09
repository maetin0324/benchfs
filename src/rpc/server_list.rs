// サーバーリストファイル管理モジュール
//
// サーバーの起動時にSocketAddrを含むサーバーリストファイルを作成・管理する。
// ファイルフォーマット:
//   # Comment
//   node_id ip:port
//
// 例:
//   node_0 192.168.1.10:45678
//   node_1 192.168.1.11:45679

use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

/// サーバーリストエラー型
#[derive(Debug)]
pub enum ServerListError {
    /// ファイルが見つからない
    FileNotFound(std::path::PathBuf),
    /// ファイル読み書きエラー
    IoError(io::Error),
    /// パースエラー
    ParseError { line: usize, content: String, reason: String },
    /// サーバーが見つからない
    ServerNotFound(String),
}

impl std::fmt::Display for ServerListError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerListError::FileNotFound(path) => {
                write!(f, "Server list file not found: {}", path.display())
            }
            ServerListError::IoError(e) => write!(f, "IO error: {}", e),
            ServerListError::ParseError { line, content, reason } => {
                write!(f, "Parse error at line {}: '{}' - {}", line, content, reason)
            }
            ServerListError::ServerNotFound(node_id) => {
                write!(f, "Server not found: {}", node_id)
            }
        }
    }
}

impl std::error::Error for ServerListError {}

impl From<io::Error> for ServerListError {
    fn from(e: io::Error) -> Self {
        ServerListError::IoError(e)
    }
}

/// サーバー情報
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerInfo {
    pub node_id: String,
    pub socket_addr: SocketAddr,
}

/// サーバーリストファイルにサーバー情報を追記する
///
/// # Arguments
/// * `path` - サーバーリストファイルのパス
/// * `node_id` - サーバーのノードID
/// * `socket_addr` - サーバーのSocketAddr
///
/// # Note
/// この関数は複数プロセスから同時に呼ばれる可能性があるため、
/// ファイルロックは呼び出し元（MPI Barrier等）で制御すること
pub fn append_to_server_list(
    path: &Path,
    node_id: &str,
    socket_addr: SocketAddr,
) -> Result<(), ServerListError> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;

    writeln!(file, "{} {}", node_id, socket_addr)?;
    file.flush()?;

    tracing::debug!("Appended to server list: {} -> {}", node_id, socket_addr);
    Ok(())
}

/// サーバーリストファイルを読み込む
///
/// # Arguments
/// * `path` - サーバーリストファイルのパス
///
/// # Returns
/// サーバー情報のベクタ（node_id, SocketAddrのペア）
pub fn read_server_list(path: &Path) -> Result<Vec<ServerInfo>, ServerListError> {
    if !path.exists() {
        return Err(ServerListError::FileNotFound(path.to_path_buf()));
    }

    let file = File::open(path)?;
    let reader = BufReader::new(file);

    let mut servers = Vec::new();

    for (line_num, line) in reader.lines().enumerate() {
        let line = line?;
        let line_num = line_num + 1; // 1-indexed for user-friendly error messages

        // 空行とコメント行をスキップ
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        // "node_id ip:port" 形式をパース
        let parts: Vec<&str> = trimmed.split_whitespace().collect();
        if parts.len() != 2 {
            return Err(ServerListError::ParseError {
                line: line_num,
                content: line.clone(),
                reason: format!("Expected 2 fields (node_id socket_addr), found {}", parts.len()),
            });
        }

        let node_id = parts[0].to_string();
        let socket_addr = parts[1].parse::<SocketAddr>().map_err(|e| {
            ServerListError::ParseError {
                line: line_num,
                content: line.clone(),
                reason: format!("Failed to parse SocketAddr: {}", e),
            }
        })?;

        servers.push(ServerInfo { node_id, socket_addr });
    }

    tracing::debug!("Read {} servers from server list", servers.len());
    Ok(servers)
}

/// リトライ付きでサーバーリストを読み込む
///
/// サーバーリストファイルがまだ作成されていない場合など、
/// 一時的なエラーに対してリトライする。
///
/// # Arguments
/// * `path` - サーバーリストファイルのパス
/// * `max_retries` - 最大リトライ回数
/// * `delay` - リトライ間隔
pub fn read_server_list_with_retry(
    path: &Path,
    max_retries: usize,
    delay: Duration,
) -> Result<Vec<ServerInfo>, ServerListError> {
    for attempt in 1..=max_retries {
        match read_server_list(path) {
            Ok(list) => {
                tracing::info!(
                    "Successfully read server list (attempt {}/{}): {} servers",
                    attempt,
                    max_retries,
                    list.len()
                );
                return Ok(list);
            }
            Err(ServerListError::FileNotFound(_)) if attempt < max_retries => {
                tracing::debug!(
                    "Server list not found, retrying in {:?} (attempt {}/{})",
                    delay,
                    attempt,
                    max_retries
                );
                std::thread::sleep(delay);
                continue;
            }
            Err(e) => {
                tracing::error!("Failed to read server list (attempt {}/{}): {}", attempt, max_retries, e);
                return Err(e);
            }
        }
    }

    Err(ServerListError::FileNotFound(path.to_path_buf()))
}

/// サーバーリストから特定のノードを検索
pub fn find_server<'a>(servers: &'a [ServerInfo], node_id: &str) -> Result<&'a ServerInfo, ServerListError> {
    servers
        .iter()
        .find(|s| s.node_id == node_id)
        .ok_or_else(|| ServerListError::ServerNotFound(node_id.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_append_and_read_server_list() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        // 書き込みテスト
        append_to_server_list(path, "node_0", "127.0.0.1:1234".parse().unwrap()).unwrap();
        append_to_server_list(path, "node_1", "192.168.1.1:5678".parse().unwrap()).unwrap();

        // 読み込みテスト
        let servers = read_server_list(path).unwrap();
        assert_eq!(servers.len(), 2);
        assert_eq!(servers[0].node_id, "node_0");
        assert_eq!(servers[0].socket_addr.port(), 1234);
        assert_eq!(servers[1].node_id, "node_1");
        assert_eq!(servers[1].socket_addr.port(), 5678);
    }

    #[test]
    fn test_read_with_comments_and_empty_lines() {
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, "# This is a comment").unwrap();
        writeln!(temp_file, "").unwrap();
        writeln!(temp_file, "node_0 127.0.0.1:1234").unwrap();
        writeln!(temp_file, "   # Another comment").unwrap();
        writeln!(temp_file, "node_1 127.0.0.1:5678").unwrap();
        temp_file.flush().unwrap();

        let servers = read_server_list(temp_file.path()).unwrap();
        assert_eq!(servers.len(), 2);
        assert_eq!(servers[0].node_id, "node_0");
        assert_eq!(servers[1].node_id, "node_1");
    }

    #[test]
    fn test_invalid_format() {
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, "invalid format without port").unwrap();
        temp_file.flush().unwrap();

        let result = read_server_list(temp_file.path());
        assert!(matches!(result, Err(ServerListError::ParseError { .. })));
    }

    #[test]
    fn test_invalid_socket_addr() {
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, "node_0 invalid:addr").unwrap();
        temp_file.flush().unwrap();

        let result = read_server_list(temp_file.path());
        assert!(matches!(result, Err(ServerListError::ParseError { .. })));
    }

    #[test]
    fn test_file_not_found() {
        let result = read_server_list(Path::new("/nonexistent/path"));
        assert!(matches!(result, Err(ServerListError::FileNotFound(_))));
    }

    #[test]
    fn test_find_server() {
        let servers = vec![
            ServerInfo {
                node_id: "node_0".to_string(),
                socket_addr: "127.0.0.1:1234".parse().unwrap(),
            },
            ServerInfo {
                node_id: "node_1".to_string(),
                socket_addr: "127.0.0.1:5678".parse().unwrap(),
            },
        ];

        let found = find_server(&servers, "node_1").unwrap();
        assert_eq!(found.node_id, "node_1");
        assert_eq!(found.socket_addr.port(), 5678);

        let not_found = find_server(&servers, "node_2");
        assert!(matches!(not_found, Err(ServerListError::ServerNotFound(_))));
    }
}
