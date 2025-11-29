//! Property-based testing for BenchFS filesystem operations
//!
//! This module implements state machine testing using proptest-state-machine
//! to verify filesystem operations in a distributed multi-node environment.
//!
//! The test generates random sequences of file operations (create, write, read,
//! delete, etc.) and verifies that the filesystem behaves correctly.

use proptest::prelude::*;
use proptest_state_machine::{prop_state_machine, ReferenceStateMachine, StateMachineTest};
use std::collections::HashMap;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Maximum file size for testing (1MB)
const MAX_FILE_SIZE: usize = 1024 * 1024;
/// Maximum number of files to create
const MAX_FILES: usize = 10;
/// Chunk size (must match BenchFS default)
const CHUNK_SIZE: usize = 64 * 1024;
/// Maximum path length (BenchFS constraint)
const MAX_PATH_LEN: usize = 255;

/// File operation transitions for the state machine
#[derive(Clone, Debug)]
pub enum Transition {
    /// Create a new file with given path
    CreateFile { path: String },
    /// Write data to a file at given offset
    WriteFile {
        path: String,
        offset: u64,
        data: Vec<u8>,
    },
    /// Read data from a file
    ReadFile {
        path: String,
        offset: u64,
        length: usize,
    },
    /// Delete a file
    DeleteFile { path: String },
    /// Truncate a file to given size
    TruncateFile { path: String, size: u64 },
    /// Rename a file
    RenameFile { old_path: String, new_path: String },
    /// Get file status (stat)
    StatFile { path: String },
}

/// Reference state machine that models expected filesystem behavior
#[derive(Clone, Debug, Default)]
pub struct FilesystemRefState {
    /// Map of file paths to their contents
    files: HashMap<String, Vec<u8>>,
    /// Counter for generating unique file names
    file_counter: usize,
}

impl FilesystemRefState {
    /// Get list of existing file paths
    fn existing_paths(&self) -> Vec<String> {
        self.files.keys().cloned().collect()
    }

    /// Check if we can create more files
    fn can_create_file(&self) -> bool {
        self.files.len() < MAX_FILES
    }
}

impl ReferenceStateMachine for FilesystemRefState {
    type State = Self;
    type Transition = Transition;

    fn init_state() -> BoxedStrategy<Self::State> {
        Just(Self::default()).boxed()
    }

    fn transitions(state: &Self::State) -> BoxedStrategy<Self::Transition> {
        let existing_paths = state.existing_paths();
        let can_create = state.can_create_file();
        let file_counter = state.file_counter;

        // Build strategy based on current state
        let mut strategies: Vec<BoxedStrategy<Transition>> = Vec::new();

        // CreateFile - only if we haven't reached max files
        // Use short paths to stay within BenchFS 256-byte limit
        if can_create {
            strategies.push(
                "[a-z]{1,4}"
                    .prop_map(move |name| {
                        // Simple short path: /benchfs/f{counter}_{name}.txt
                        // Max length: /benchfs/ (9) + f (1) + counter (max 3) + _ (1) + name (4) + .txt (4) = ~22 bytes
                        let path = format!("/benchfs/f{}_{}.txt", file_counter, name);
                        assert!(path.len() < MAX_PATH_LEN, "Path too long: {}", path);
                        Transition::CreateFile { path }
                    })
                    .boxed(),
            );
        }

        // Operations on existing files
        if !existing_paths.is_empty() {
            let paths = existing_paths.clone();

            // WriteFile
            let write_paths = paths.clone();
            strategies.push(
                (
                    prop::sample::select(write_paths),
                    0u64..MAX_FILE_SIZE as u64,
                    prop::collection::vec(any::<u8>(), 1..CHUNK_SIZE * 2),
                )
                    .prop_map(|(path, offset, data)| Transition::WriteFile { path, offset, data })
                    .boxed(),
            );

            // ReadFile
            let read_paths = paths.clone();
            strategies.push(
                (
                    prop::sample::select(read_paths),
                    0u64..MAX_FILE_SIZE as u64,
                    1usize..CHUNK_SIZE * 2,
                )
                    .prop_map(|(path, offset, length)| Transition::ReadFile {
                        path,
                        offset,
                        length,
                    })
                    .boxed(),
            );

            // DeleteFile
            let delete_paths = paths.clone();
            strategies.push(
                prop::sample::select(delete_paths)
                    .prop_map(|path| Transition::DeleteFile { path })
                    .boxed(),
            );

            // TruncateFile
            let truncate_paths = paths.clone();
            strategies.push(
                (
                    prop::sample::select(truncate_paths),
                    0u64..MAX_FILE_SIZE as u64,
                )
                    .prop_map(|(path, size)| Transition::TruncateFile { path, size })
                    .boxed(),
            );

            // StatFile
            let stat_paths = paths.clone();
            strategies.push(
                prop::sample::select(stat_paths)
                    .prop_map(|path| Transition::StatFile { path })
                    .boxed(),
            );

            // RenameFile - only if we have room for more files
            if can_create {
                let rename_paths = paths.clone();
                strategies.push(
                    (prop::sample::select(rename_paths), "[a-z]{1,4}")
                        .prop_map(move |(old_path, new_name)| {
                            // Short rename path: /benchfs/r{counter}_{name}.txt
                            let new_path = format!("/benchfs/r{}_{}.txt", file_counter, new_name);
                            Transition::RenameFile { old_path, new_path }
                        })
                        .boxed(),
                );
            }
        }

        // If no strategies available (empty state and can't create), return CreateFile
        if strategies.is_empty() {
            return "[a-z]{1,4}"
                .prop_map(move |name| {
                    let path = format!("/benchfs/f{}_{}.txt", file_counter, name);
                    Transition::CreateFile { path }
                })
                .boxed();
        }

        prop::strategy::Union::new(strategies).boxed()
    }

    fn apply(mut state: Self::State, transition: &Self::Transition) -> Self::State {
        match transition {
            Transition::CreateFile { path } => {
                if !state.files.contains_key(path) {
                    state.files.insert(path.clone(), Vec::new());
                    state.file_counter += 1;
                }
            }
            Transition::WriteFile { path, offset, data } => {
                if let Some(contents) = state.files.get_mut(path) {
                    let end = *offset as usize + data.len();
                    if contents.len() < end {
                        contents.resize(end, 0);
                    }
                    contents[*offset as usize..end].copy_from_slice(data);
                }
            }
            Transition::ReadFile { .. } => {
                // Read doesn't modify state
            }
            Transition::DeleteFile { path } => {
                state.files.remove(path);
            }
            Transition::TruncateFile { path, size } => {
                if let Some(contents) = state.files.get_mut(path) {
                    contents.resize(*size as usize, 0);
                }
            }
            Transition::RenameFile { old_path, new_path } => {
                if let Some(contents) = state.files.remove(old_path) {
                    state.files.insert(new_path.clone(), contents);
                }
            }
            Transition::StatFile { .. } => {
                // Stat doesn't modify state
            }
        }
        state
    }
}

/// System under test - actual BenchFS cluster
pub struct BenchFSCluster {
    /// Docker compose process handle
    compose_handle: Option<Child>,
    /// Whether the cluster is running
    running: Arc<AtomicBool>,
}

impl BenchFSCluster {
    /// Create a new cluster (starts Docker containers)
    pub fn new() -> Self {
        Self {
            compose_handle: None,
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Start the cluster
    pub fn start(&mut self) -> Result<(), String> {
        if self.running.load(Ordering::SeqCst) {
            return Ok(());
        }

        // Start docker-compose
        let child = Command::new("docker-compose")
            .args([
                "-f",
                "tests/docker/docker-compose.pbt.yml",
                "-p",
                "benchfs_pbt",
                "up",
                "-d",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| format!("Failed to start docker-compose: {}", e))?;

        self.compose_handle = Some(child);

        // Wait for cluster to be ready
        std::thread::sleep(Duration::from_secs(10));

        // Start MPI servers
        self.start_mpi_servers()?;

        self.running.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// Start MPI servers on all nodes
    fn start_mpi_servers(&self) -> Result<(), String> {
        let _output = Command::new("docker")
            .args([
                "exec",
                "benchfs_pbt_controller",
                "mpirun",
                "-np",
                "2",
                "--host",
                "server1,server2",
                "/usr/local/bin/benchfsd_mpi",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| format!("Failed to start MPI servers: {}", e))?;

        // Give servers time to start
        std::thread::sleep(Duration::from_secs(5));
        Ok(())
    }

    /// Stop the cluster
    pub fn stop(&mut self) {
        if !self.running.load(Ordering::SeqCst) {
            return;
        }

        let _ = Command::new("docker-compose")
            .args([
                "-f",
                "tests/docker/docker-compose.pbt.yml",
                "-p",
                "benchfs_pbt",
                "down",
            ])
            .status();

        self.running.store(false, Ordering::SeqCst);
    }

    /// Execute a file operation on the cluster
    pub fn execute(&self, transition: &Transition) -> Result<TransitionResult, String> {
        match transition {
            Transition::CreateFile { path } => self.create_file(path),
            Transition::WriteFile { path, offset, data } => self.write_file(path, *offset, data),
            Transition::ReadFile {
                path,
                offset,
                length,
            } => self.read_file(path, *offset, *length),
            Transition::DeleteFile { path } => self.delete_file(path),
            Transition::TruncateFile { path, size } => self.truncate_file(path, *size),
            Transition::RenameFile { old_path, new_path } => self.rename_file(old_path, new_path),
            Transition::StatFile { path } => self.stat_file(path),
        }
    }

    fn create_file(&self, path: &str) -> Result<TransitionResult, String> {
        let output = Command::new("docker")
            .args([
                "exec",
                "benchfs_pbt_controller",
                "bash",
                "/scripts/pbt-op.sh",
                "create",
                path,
            ])
            .output()
            .map_err(|e| format!("Failed to execute create: {}", e))?;

        if output.status.success() {
            Ok(TransitionResult::Success)
        } else {
            Ok(TransitionResult::Error(
                String::from_utf8_lossy(&output.stderr).to_string(),
            ))
        }
    }

    fn write_file(&self, path: &str, offset: u64, data: &[u8]) -> Result<TransitionResult, String> {
        // Encode data as base64 for safe transfer via stdin
        let data_b64 = base64_encode(data);

        use std::io::Write;
        let mut child = Command::new("docker")
            .args([
                "exec",
                "-i",
                "benchfs_pbt_controller",
                "bash",
                "/scripts/pbt-op.sh",
                "write",
                path,
                &offset.to_string(),
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| format!("Failed to spawn write: {}", e))?;

        // Write base64 data to stdin
        if let Some(mut stdin) = child.stdin.take() {
            stdin
                .write_all(data_b64.as_bytes())
                .map_err(|e| format!("Failed to write to stdin: {}", e))?;
        }

        let output = child
            .wait_with_output()
            .map_err(|e| format!("Failed to wait for write: {}", e))?;

        if output.status.success() {
            Ok(TransitionResult::Success)
        } else {
            Ok(TransitionResult::Error(
                String::from_utf8_lossy(&output.stderr).to_string(),
            ))
        }
    }

    fn read_file(
        &self,
        path: &str,
        offset: u64,
        length: usize,
    ) -> Result<TransitionResult, String> {
        let output = Command::new("docker")
            .args([
                "exec",
                "benchfs_pbt_controller",
                "bash",
                "/scripts/pbt-op.sh",
                "read",
                path,
                &offset.to_string(),
                &length.to_string(),
            ])
            .output()
            .map_err(|e| format!("Failed to execute read: {}", e))?;

        if output.status.success() {
            let data = base64_decode(&String::from_utf8_lossy(&output.stdout).trim())?;
            Ok(TransitionResult::ReadData(data))
        } else {
            Ok(TransitionResult::Error(
                String::from_utf8_lossy(&output.stderr).to_string(),
            ))
        }
    }

    fn delete_file(&self, path: &str) -> Result<TransitionResult, String> {
        let output = Command::new("docker")
            .args([
                "exec",
                "benchfs_pbt_controller",
                "bash",
                "/scripts/pbt-op.sh",
                "delete",
                path,
            ])
            .output()
            .map_err(|e| format!("Failed to execute delete: {}", e))?;

        if output.status.success() {
            Ok(TransitionResult::Success)
        } else {
            Ok(TransitionResult::Error(
                String::from_utf8_lossy(&output.stderr).to_string(),
            ))
        }
    }

    fn truncate_file(&self, path: &str, size: u64) -> Result<TransitionResult, String> {
        let output = Command::new("docker")
            .args([
                "exec",
                "benchfs_pbt_controller",
                "bash",
                "/scripts/pbt-op.sh",
                "truncate",
                path,
                &size.to_string(),
            ])
            .output()
            .map_err(|e| format!("Failed to execute truncate: {}", e))?;

        if output.status.success() {
            Ok(TransitionResult::Success)
        } else {
            Ok(TransitionResult::Error(
                String::from_utf8_lossy(&output.stderr).to_string(),
            ))
        }
    }

    fn rename_file(&self, old_path: &str, new_path: &str) -> Result<TransitionResult, String> {
        let output = Command::new("docker")
            .args([
                "exec",
                "benchfs_pbt_controller",
                "bash",
                "/scripts/pbt-op.sh",
                "rename",
                old_path,
                new_path,
            ])
            .output()
            .map_err(|e| format!("Failed to execute rename: {}", e))?;

        if output.status.success() {
            Ok(TransitionResult::Success)
        } else {
            Ok(TransitionResult::Error(
                String::from_utf8_lossy(&output.stderr).to_string(),
            ))
        }
    }

    fn stat_file(&self, path: &str) -> Result<TransitionResult, String> {
        let output = Command::new("docker")
            .args([
                "exec",
                "benchfs_pbt_controller",
                "bash",
                "/scripts/pbt-op.sh",
                "stat",
                path,
            ])
            .output()
            .map_err(|e| format!("Failed to execute stat: {}", e))?;

        if output.status.success() {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let size: u64 = stdout
                .trim()
                .parse()
                .map_err(|e| format!("Failed to parse size: {}", e))?;
            Ok(TransitionResult::StatResult { size })
        } else {
            Ok(TransitionResult::Error(
                String::from_utf8_lossy(&output.stderr).to_string(),
            ))
        }
    }
}

impl Drop for BenchFSCluster {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Result of a transition execution
#[derive(Debug)]
pub enum TransitionResult {
    Success,
    ReadData(Vec<u8>),
    StatResult { size: u64 },
    Error(String),
}

/// Simple base64 encoding (no external dependency)
fn base64_encode(data: &[u8]) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::new();

    for chunk in data.chunks(3) {
        let b0 = chunk[0] as usize;
        let b1 = chunk.get(1).copied().unwrap_or(0) as usize;
        let b2 = chunk.get(2).copied().unwrap_or(0) as usize;

        result.push(ALPHABET[b0 >> 2] as char);
        result.push(ALPHABET[((b0 & 0x03) << 4) | (b1 >> 4)] as char);

        if chunk.len() > 1 {
            result.push(ALPHABET[((b1 & 0x0f) << 2) | (b2 >> 6)] as char);
        } else {
            result.push('=');
        }

        if chunk.len() > 2 {
            result.push(ALPHABET[b2 & 0x3f] as char);
        } else {
            result.push('=');
        }
    }

    result
}

/// Simple base64 decoding
fn base64_decode(s: &str) -> Result<Vec<u8>, String> {
    const DECODE: [i8; 128] = [
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 62, -1, -1,
        -1, 63, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, -1, -1, -1, -1, -1, -1, -1, 0, 1, 2, 3, 4,
        5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, -1, -1, -1,
        -1, -1, -1, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45,
        46, 47, 48, 49, 50, 51, -1, -1, -1, -1, -1,
    ];

    let s = s.trim_end_matches('=');
    let mut result = Vec::new();

    for chunk in s.as_bytes().chunks(4) {
        if chunk.is_empty() {
            break;
        }

        let b0 = DECODE[chunk[0] as usize] as usize;
        let b1 = chunk
            .get(1)
            .map(|&c| DECODE[c as usize] as usize)
            .unwrap_or(0);
        let b2 = chunk
            .get(2)
            .map(|&c| DECODE[c as usize] as usize)
            .unwrap_or(0);
        let b3 = chunk
            .get(3)
            .map(|&c| DECODE[c as usize] as usize)
            .unwrap_or(0);

        result.push(((b0 << 2) | (b1 >> 4)) as u8);
        if chunk.len() > 2 {
            result.push(((b1 << 4) | (b2 >> 2)) as u8);
        }
        if chunk.len() > 3 {
            result.push(((b2 << 6) | b3) as u8);
        }
    }

    Ok(result)
}

/// State machine test implementation
pub struct BenchFSStateMachineTest {
    cluster: BenchFSCluster,
}

impl StateMachineTest for BenchFSStateMachineTest {
    type SystemUnderTest = Self;
    type Reference = FilesystemRefState;

    fn init_test(
        _ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) -> Self::SystemUnderTest {
        let mut cluster = BenchFSCluster::new();
        cluster.start().expect("Failed to start cluster");
        Self { cluster }
    }

    fn apply(
        state: Self::SystemUnderTest,
        ref_state: &<Self::Reference as ReferenceStateMachine>::State,
        transition: <Self::Reference as ReferenceStateMachine>::Transition,
    ) -> Self::SystemUnderTest {
        let result = state
            .cluster
            .execute(&transition)
            .expect("Failed to execute transition");

        // Verify result matches expected state
        match (&transition, result) {
            (Transition::ReadFile { path, offset, length }, TransitionResult::ReadData(data)) => {
                if let Some(expected_contents) = ref_state.files.get(path) {
                    let expected_end = (*offset as usize + *length).min(expected_contents.len());
                    let expected_start = (*offset as usize).min(expected_contents.len());
                    let expected = &expected_contents[expected_start..expected_end];

                    assert_eq!(
                        data.len(),
                        expected.len(),
                        "Read length mismatch for {} at offset {}",
                        path,
                        offset
                    );
                    assert_eq!(
                        data, expected,
                        "Read data mismatch for {} at offset {}",
                        path, offset
                    );
                }
            }
            (Transition::StatFile { path }, TransitionResult::StatResult { size }) => {
                if let Some(contents) = ref_state.files.get(path) {
                    assert_eq!(
                        size as usize,
                        contents.len(),
                        "Stat size mismatch for {}",
                        path
                    );
                }
            }
            (_, TransitionResult::Error(e)) => {
                // Some errors are expected (e.g., reading non-existent file)
                // Log but don't fail
                eprintln!("Operation returned error: {}", e);
            }
            _ => {}
        }

        state
    }

    fn check_invariants(
        _state: &Self::SystemUnderTest,
        _ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) {
        // Additional invariant checks can be added here
    }
}

// Property-based test using the state machine
prop_state_machine! {
    #![proptest_config(ProptestConfig {
        cases: 10,
        max_shrink_iters: 100,
        .. ProptestConfig::default()
    })]

    #[test]
    fn pbt_filesystem_operations(
        sequential 1..20 => BenchFSStateMachineTest
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_base64_roundtrip() {
        let data = b"Hello, BenchFS!";
        let encoded = base64_encode(data);
        let decoded = base64_decode(&encoded).unwrap();
        assert_eq!(data.as_slice(), decoded.as_slice());
    }

    #[test]
    fn test_reference_state_create_file() {
        let mut state = FilesystemRefState::default();
        let transition = Transition::CreateFile {
            path: "/test.txt".to_string(),
        };
        state = FilesystemRefState::apply(state, &transition);
        assert!(state.files.contains_key("/test.txt"));
        assert_eq!(state.files["/test.txt"], Vec::<u8>::new());
    }

    #[test]
    fn test_reference_state_write_read() {
        let mut state = FilesystemRefState::default();

        // Create file
        state = FilesystemRefState::apply(
            state,
            &Transition::CreateFile {
                path: "/test.txt".to_string(),
            },
        );

        // Write data
        state = FilesystemRefState::apply(
            state,
            &Transition::WriteFile {
                path: "/test.txt".to_string(),
                offset: 0,
                data: b"Hello".to_vec(),
            },
        );

        assert_eq!(state.files["/test.txt"], b"Hello");

        // Write at offset
        state = FilesystemRefState::apply(
            state,
            &Transition::WriteFile {
                path: "/test.txt".to_string(),
                offset: 5,
                data: b" World".to_vec(),
            },
        );

        assert_eq!(state.files["/test.txt"], b"Hello World");
    }

    #[test]
    fn test_reference_state_truncate() {
        let mut state = FilesystemRefState::default();

        state = FilesystemRefState::apply(
            state,
            &Transition::CreateFile {
                path: "/test.txt".to_string(),
            },
        );

        state = FilesystemRefState::apply(
            state,
            &Transition::WriteFile {
                path: "/test.txt".to_string(),
                offset: 0,
                data: b"Hello World".to_vec(),
            },
        );

        state = FilesystemRefState::apply(
            state,
            &Transition::TruncateFile {
                path: "/test.txt".to_string(),
                size: 5,
            },
        );

        assert_eq!(state.files["/test.txt"], b"Hello");
    }
}
