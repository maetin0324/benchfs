//! Daemon launcher module.
//!
//! This module handles automatic daemon startup when the first client connects.

use std::fs::File;
use std::io;
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, Instant};

use tracing::{debug, info, warn};

use super::default_shm_name;
use super::error::DaemonError;
use super::protocol::DEFAULT_STARTUP_TIMEOUT_MS;
use super::shm::{SharedMemoryRegion, ShmConfig};

/// Lock file path for coordinating daemon startup.
fn lock_file_path(shm_name: &str) -> PathBuf {
    let name = shm_name.trim_start_matches('/');
    PathBuf::from(format!("/tmp/.benchfs_daemon_{}.lock", name))
}

/// Try to acquire exclusive lock on the lock file.
/// Returns the file handle if lock acquired, None if another process holds the lock.
fn try_acquire_lock(shm_name: &str) -> io::Result<Option<File>> {
    let path = lock_file_path(shm_name);

    // Create or open lock file
    let file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&path)?;

    // Try to acquire exclusive lock (non-blocking)
    let fd = file.as_raw_fd();
    let ret = unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) };

    if ret == 0 {
        Ok(Some(file))
    } else {
        let err = io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::EWOULDBLOCK) {
            // Another process holds the lock
            Ok(None)
        } else {
            Err(err)
        }
    }
}

/// Configuration for daemon launcher.
#[derive(Debug, Clone)]
pub struct LauncherConfig {
    /// Shared memory name
    pub shm_name: String,
    /// Shared memory configuration
    pub shm_config: ShmConfig,
    /// Path to daemon binary
    pub daemon_binary: PathBuf,
    /// Registry directory for BenchFS
    pub registry_dir: PathBuf,
    /// Data directory for BenchFS
    pub data_dir: PathBuf,
    /// Startup timeout
    pub startup_timeout: Duration,
    /// Additional environment variables
    pub env_vars: Vec<(String, String)>,
}

impl Default for LauncherConfig {
    fn default() -> Self {
        Self {
            shm_name: default_shm_name(),
            shm_config: ShmConfig::default(),
            daemon_binary: PathBuf::from("benchfs_daemon"),
            registry_dir: PathBuf::from("/tmp/benchfs_registry"),
            data_dir: PathBuf::from("/tmp/benchfs_data"),
            startup_timeout: Duration::from_millis(DEFAULT_STARTUP_TIMEOUT_MS),
            env_vars: Vec::new(),
        }
    }
}

/// Result of trying to connect to daemon.
pub enum ConnectResult {
    /// Successfully connected to existing daemon
    Connected(SharedMemoryRegion),
    /// Need to spawn daemon (we hold the lock)
    NeedSpawn(File),
    /// Another process is spawning daemon, wait for it
    WaitForSpawn,
}

/// Try to connect to an existing daemon or determine if we need to spawn one.
pub fn try_connect_or_spawn(shm_name: &str) -> Result<ConnectResult, DaemonError> {
    use super::shm::ShmError;

    // First, try to attach to existing shared memory
    match SharedMemoryRegion::attach(shm_name) {
        Ok(shm) => {
            // Shared memory exists, check if daemon is ready
            if shm.is_daemon_ready() {
                debug!("Connected to existing daemon");
                return Ok(ConnectResult::Connected(shm));
            }
            // Shared memory exists but daemon not ready - wait for it
            debug!("Shared memory exists but daemon not ready, waiting");
            // Drop shm so it can be recreated if needed
            drop(shm);
            return Ok(ConnectResult::WaitForSpawn);
        }
        Err(ShmError::OpenFailed(e)) if e.raw_os_error() == Some(libc::ENOENT) => {
            debug!("No existing shared memory found");
        }
        Err(ShmError::InvalidMagic) => {
            // Shared memory exists but magic not set - daemon is still initializing
            debug!("Shared memory exists but magic not initialized, waiting for spawn");
            return Ok(ConnectResult::WaitForSpawn);
        }
        Err(e) => {
            return Err(DaemonError::Shm(e));
        }
    }

    // No shared memory exists, try to acquire lock for spawning
    match try_acquire_lock(shm_name).map_err(DaemonError::Io)? {
        Some(lock_file) => {
            // We got the lock - check again if shm was created while we were waiting
            match SharedMemoryRegion::attach(shm_name) {
                Ok(shm) if shm.is_daemon_ready() => {
                    debug!("Daemon became ready while acquiring lock");
                    return Ok(ConnectResult::Connected(shm));
                }
                Ok(_) => {
                    // Shared memory exists but not ready - unusual, but we should wait
                    debug!("Shared memory exists but not ready after acquiring lock");
                    return Ok(ConnectResult::WaitForSpawn);
                }
                Err(ShmError::InvalidMagic) => {
                    // Someone else is initializing - wait for them
                    debug!("Shared memory has invalid magic while holding lock, waiting");
                    return Ok(ConnectResult::WaitForSpawn);
                }
                Err(ShmError::OpenFailed(e)) if e.raw_os_error() == Some(libc::ENOENT) => {
                    // Still no shm - we need to spawn the daemon
                    debug!("We need to spawn daemon");
                    return Ok(ConnectResult::NeedSpawn(lock_file));
                }
                Err(e) => {
                    return Err(DaemonError::Shm(e));
                }
            }
        }
        None => {
            // Another process is spawning
            debug!("Another process is spawning daemon");
            return Ok(ConnectResult::WaitForSpawn);
        }
    }
}

/// Spawn the daemon process.
///
/// # Arguments
/// * `config` - Launcher configuration
/// * `_lock_file` - Lock file (kept open to maintain lock)
///
/// # Returns
/// Returns the shared memory region once daemon is ready.
pub fn spawn_daemon(
    config: &LauncherConfig,
    _lock_file: File, // Keep lock until daemon is ready
) -> Result<SharedMemoryRegion, DaemonError> {
    info!("Spawning daemon: binary={:?}", config.daemon_binary);

    // Build command
    let mut cmd = Command::new(&config.daemon_binary);

    cmd.arg("--shm-name")
        .arg(&config.shm_name)
        .arg("--registry-dir")
        .arg(&config.registry_dir)
        .arg("--data-dir")
        .arg(&config.data_dir)
        .arg("--num-slots")
        .arg(config.shm_config.num_slots.to_string())
        .arg("--data-buffer-size")
        .arg(config.shm_config.data_buffer_size.to_string())
        .arg("--request-ring-size")
        .arg(config.shm_config.request_ring_size.to_string())
        .arg("--response-ring-size")
        .arg(config.shm_config.response_ring_size.to_string());

    // Add environment variables
    for (key, value) in &config.env_vars {
        cmd.env(key, value);
    }

    // Inherit stdin/stdout/stderr for debugging
    // In production, you might want to redirect to logs
    cmd.stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit());

    // Spawn daemon process
    let child = cmd
        .spawn()
        .map_err(|e| DaemonError::DaemonStartupFailed(format!("Failed to spawn: {}", e)))?;

    info!("Daemon spawned with pid {}", child.id());

    // Wait for daemon to become ready
    wait_for_daemon_ready(&config.shm_name, config.startup_timeout)
}

/// Wait for daemon to become ready.
fn wait_for_daemon_ready(
    shm_name: &str,
    timeout: Duration,
) -> Result<SharedMemoryRegion, DaemonError> {
    use super::shm::ShmError;

    let start = Instant::now();
    let mut last_log = Instant::now();

    loop {
        // Try to attach to shared memory
        // Note: We handle InvalidMagic as "daemon still initializing" because
        // there's a race between shm_open and magic number initialization
        match SharedMemoryRegion::attach(shm_name) {
            Ok(shm) => {
                if shm.is_daemon_ready() {
                    info!("Daemon is ready");
                    return Ok(shm);
                }
                // Shared memory exists but daemon not ready yet
                debug!("Shared memory attached but daemon not ready yet");
            }
            Err(ShmError::OpenFailed(e)) if e.raw_os_error() == Some(libc::ENOENT) => {
                // Shared memory doesn't exist yet - daemon still starting
                debug!("Shared memory doesn't exist yet, waiting...");
            }
            Err(ShmError::InvalidMagic) => {
                // Shared memory exists but magic not set yet - daemon still initializing
                // This is a race condition: shm_open succeeded but daemon hasn't
                // finished writing the magic number to the control block yet
                if last_log.elapsed() >= Duration::from_secs(1) {
                    debug!("Shared memory exists but magic not initialized yet, waiting...");
                    last_log = Instant::now();
                }
            }
            Err(e) => {
                // Other errors are real failures
                return Err(DaemonError::Shm(e));
            }
        }

        // Check timeout
        if start.elapsed() >= timeout {
            return Err(DaemonError::DaemonStartupTimeout);
        }

        // Small sleep to avoid busy spinning too aggressively
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// Connect to daemon, spawning if necessary.
///
/// This is the main entry point for clients to connect to the daemon.
pub fn connect_to_daemon(config: &LauncherConfig) -> Result<SharedMemoryRegion, DaemonError> {
    loop {
        match try_connect_or_spawn(&config.shm_name)? {
            ConnectResult::Connected(shm) => {
                return Ok(shm);
            }
            ConnectResult::NeedSpawn(lock_file) => {
                return spawn_daemon(config, lock_file);
            }
            ConnectResult::WaitForSpawn => {
                // Wait for another process to spawn the daemon
                match wait_for_daemon_ready(&config.shm_name, config.startup_timeout) {
                    Ok(shm) => return Ok(shm),
                    Err(DaemonError::DaemonStartupTimeout) => {
                        warn!("Timeout waiting for daemon, retrying");
                        // Another process may have failed, retry the whole process
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lock_file_path() {
        assert_eq!(
            lock_file_path("/benchfs_daemon_test"),
            PathBuf::from("/tmp/.benchfs_daemon_benchfs_daemon_test.lock")
        );
        assert_eq!(
            lock_file_path("benchfs_daemon_test"),
            PathBuf::from("/tmp/.benchfs_daemon_benchfs_daemon_test.lock")
        );
    }
}
