//! Minimal BenchFS library using Active Message API
//!
//! This implementation uses UCX Active Message API instead of Stream API
//! to avoid message boundary issues with small sends.
//! All code is contained in this single module for simplicity.

use std::collections::HashMap;
use std::net::{SocketAddr, ToSocketAddrs};
use std::rc::Rc;
use std::sync::Arc;
use std::future::Future;

use futures::lock::Mutex as AsyncMutex;
use futures::FutureExt;
use pluvio_runtime::executor::Runtime;
use pluvio_ucx::endpoint::Endpoint;
use pluvio_ucx::{Context, Worker};
use pluvio_ucx::async_ucx::ucp::AmMsg;
use pluvio_uring::file::DmaFile;
use pluvio_uring::reactor::IoUringReactor;

// ============================================================================
// FFI Types and Constants
// ============================================================================

#[repr(C)]
pub struct BenchfsMiniFile {
    path: *const libc::c_char,
    rank: i32,
}

const BENCHFS_MINI_SUCCESS: i32 = 0;
const BENCHFS_MINI_ERROR: i32 = -1;

// ============================================================================
// Global State
// ============================================================================

struct GlobalState {
    runtime: Rc<Runtime>,
    ucx_reactor: Rc<pluvio_ucx::UCXReactor>,
    uring_reactor: Rc<IoUringReactor>,
    worker: Rc<Worker>,
    context: Arc<Context>,
    server_started: bool,
    rank: i32,
    size: i32,
    registry_dir: String,
    storage_dir: String,
    // For client: map from server_rank to endpoint
    server_endpoints: Arc<AsyncMutex<HashMap<i32, Endpoint>>>,
    // For server: listener (must be kept alive for clients to connect)
    listener: Option<pluvio_ucx::listener::Listener>,
}

static mut GLOBAL_STATE: Option<Box<GlobalState>> = None;

// ============================================================================
// Active Message Protocol
// ============================================================================
//
// AM ID 1 (RPC_WRITE):
//   Header: [path_len(4)] [data_len(4)] [path(variable)]
//   Data: [file_data(variable)] or immediate data in header if small
//   Reply: [status(4)]
//
// AM ID 2 (RPC_READ):
//   Header: [path_len(4)] [buffer_len(4)] [path(variable)]
//   Data: none
//   Reply Header: [status(4)] [data_len(4)]
//   Reply Data: [file_data(variable)]
//
// Note: We use AM ID 1 and 2 directly (no need for RPC_REPLY_WRITE/READ IDs,
// as replies are handled through AmMsg::reply() mechanism)

// ============================================================================
// File-based Storage using pluvio-uring
// ============================================================================

struct FileStorage {
    base_dir: String,
    files: AsyncMutex<HashMap<String, (Rc<DmaFile>, u64)>>, // path -> (file, size)
}

impl FileStorage {
    fn new(base_dir: String) -> Self {
        // Create base directory if it doesn't exist
        std::fs::create_dir_all(&base_dir).unwrap();

        Self {
            base_dir,
            files: AsyncMutex::new(HashMap::new()),
        }
    }

    fn get_file_path(&self, path: &str) -> String {
        // Convert path to safe filename
        let safe_name = path.replace("/", "_");
        format!("{}/{}", self.base_dir, safe_name)
    }

    async fn write(&self, path: &str, data: &[u8]) -> Result<(), String> {
        let start = std::time::Instant::now();
        let file_path = self.get_file_path(path);

        eprintln!("[FileStorage] Writing {} bytes to {}", data.len(), file_path);

        // Open or create file
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&file_path)
            .map_err(|e| format!("Failed to open file {}: {:?}", file_path, e))?;

        let after_open = start.elapsed();

        // Set file size
        file.set_len(data.len() as u64)
            .map_err(|e| format!("Failed to set file length: {:?}", e))?;

        let after_set_len = start.elapsed();

        let dma_file = Rc::new(DmaFile::new(file));

        // Write data
        let data_vec = data.to_vec();
        let after_copy = start.elapsed();

        let bytes_written = dma_file.write(data_vec, 0)
            .await
            .map_err(|e| format!("Write failed: {:?}", e))?;

        let after_io = start.elapsed();

        eprintln!("[FileStorage] Wrote {} bytes: open={:?}, set_len={:?}, copy={:?}, io={:?}, total={:?}",
                  bytes_written, after_open, after_set_len - after_open,
                  after_copy - after_set_len, after_io - after_copy, after_io);

        // Note: fsync is intentionally removed for performance
        // IOR benchmarks typically don't require immediate persistence
        // For production use, consider adding fsync only on explicit flush/close

        // Store file handle for later reads
        let mut files = self.files.lock().await;
        files.insert(path.to_string(), (dma_file, data.len() as u64));

        Ok(())
    }

    async fn read(&self, path: &str, buffer: &mut [u8]) -> Result<usize, String> {
        let file_path = self.get_file_path(path);

        eprintln!("[FileStorage] Reading from {} (max {} bytes)", file_path, buffer.len());

        // Check if file exists in cache
        let mut files = self.files.lock().await;

        let (dma_file, file_size) = if let Some((file, size)) = files.get(path) {
            (file.clone(), *size)
        } else {
            // Open file if not in cache
            let file = std::fs::OpenOptions::new()
                .read(true)
                .open(&file_path)
                .map_err(|e| format!("File not found: {}: {:?}", file_path, e))?;

            let metadata = file.metadata()
                .map_err(|e| format!("Failed to get file metadata: {:?}", e))?;
            let size = metadata.len();

            let dma_file = Rc::new(DmaFile::new(file));
            files.insert(path.to_string(), (dma_file.clone(), size));
            (dma_file, size)
        };

        // Read data
        let read_size = file_size.min(buffer.len() as u64) as usize;
        if read_size == 0 {
            return Ok(0);
        }

        let read_buffer = vec![0u8; read_size];
        let bytes_read = dma_file.read(read_buffer.clone(), 0)
            .await
            .map_err(|e| format!("Read failed: {:?}", e))?;

        eprintln!("[FileStorage] Read {} bytes from {}", bytes_read, file_path);

        // Copy to output buffer
        let actual_bytes = bytes_read.min(buffer.len() as i32) as usize;
        buffer[..actual_bytes].copy_from_slice(&read_buffer[..actual_bytes]);

        Ok(actual_bytes)
    }
}

// ============================================================================
// Simple RPC Protocol - Fixed-length header
// ============================================================================

const RPC_WRITE: u16 = 1;
const RPC_READ: u16 = 2;
const MAX_PATH_LEN: usize = 1024;

#[repr(C)]
struct WriteRequest {
    path_len: u32,
    data_len: u32,
}

#[repr(C)]
struct WriteResponse {
    status: i32,
}

#[repr(C)]
struct ReadRequest {
    path_len: u32,
    buffer_len: u32,
}

#[repr(C)]
struct ReadResponse {
    status: i32,
    data_len: u32,
}

// ============================================================================
// Server Implementation - Active Message Based
// ============================================================================

/// Handle Write Active Messages (AM ID = RPC_WRITE = 1)
async fn handle_write_am(mut msg: AmMsg, storage: Rc<FileStorage>) -> Result<(), String> {
    let start = std::time::Instant::now();

    // Parse header: [path_len(4)] [data_len(4)] [path(variable)]
    let header = msg.header();
    if header.len() < 8 {
        return Err(format!("Write header too short: {} bytes", header.len()));
    }

    let path_len = u32::from_le_bytes([header[0], header[1], header[2], header[3]]) as usize;
    let data_len = u32::from_le_bytes([header[4], header[5], header[6], header[7]]) as usize;

    if header.len() < 8 + path_len {
        return Err(format!("Write header truncated: expected {}, got {}", 8 + path_len, header.len()));
    }

    let path_bytes = &header[8..8 + path_len];
    let path = String::from_utf8_lossy(path_bytes).to_string();  // Copy to owned String to drop header borrow

    let after_header = start.elapsed();
    eprintln!("[Server AM] Write request: path={}, data_len={}, header_time={:?}",
              path, data_len, after_header);

    // Get data: either from immediate data or recv_data()
    // Note: We need to drop the header borrow before calling recv_data() (which takes &mut self)
    let data_buf = if let Some(immediate_data) = msg.get_data() {
        eprintln!("[Server AM] Using immediate data: {} bytes", immediate_data.len());
        immediate_data.to_vec()
    } else {
        eprintln!("[Server AM] Receiving large data...");
        msg.recv_data().await.map_err(|e| format!("recv_data failed: {:?}", e))?
    };

    let after_data_recv = start.elapsed();
    eprintln!("[Server AM] Data recv time: {:?} (total: {:?})",
              after_data_recv - after_header, after_data_recv);

    // Write to storage
    storage.write(&path, &data_buf).await?;

    let after_storage_write = start.elapsed();
    eprintln!("[Server AM] Storage write time: {:?} (total: {:?})",
              after_storage_write - after_data_recv, after_storage_write);

    eprintln!("[Server AM] Wrote {} bytes to {}", data_buf.len(), path);

    // Send reply: [status(4)]
    let status = BENCHFS_MINI_SUCCESS.to_le_bytes();
    unsafe {
        msg.reply(0, &status, &[], false, None).await
            .map_err(|e| format!("reply failed: {:?}", e))?;
    }

    let total_time = start.elapsed();
    eprintln!("[Server AM] Response send time: {:?}, TOTAL: {:?}",
              total_time - after_storage_write, total_time);

    Ok(())
}

/// Handle Read Active Messages (AM ID = RPC_READ = 2)
async fn handle_read_am(msg: AmMsg, storage: Rc<FileStorage>) -> Result<(), String> {
    // Parse header: [path_len(4)] [buffer_len(4)] [path(variable)]
    let header = msg.header();
    if header.len() < 8 {
        return Err(format!("Read header too short: {} bytes", header.len()));
    }

    let path_len = u32::from_le_bytes([header[0], header[1], header[2], header[3]]) as usize;
    let buffer_len = u32::from_le_bytes([header[4], header[5], header[6], header[7]]) as usize;

    if header.len() < 8 + path_len {
        return Err(format!("Read header truncated: expected {}, got {}", 8 + path_len, header.len()));
    }

    let path_bytes = &header[8..8 + path_len];
    let path = String::from_utf8_lossy(path_bytes).to_string();  // Copy to owned String

    eprintln!("[Server AM] Read request: path={}, buffer_len={}", path, buffer_len);

    // Read from storage
    let mut data_buf = vec![0u8; buffer_len];
    let data_len = match storage.read(&path, &mut data_buf).await {
        Ok(len) => len,
        Err(_) => 0,
    };

    eprintln!("[Server AM] Read {} bytes from {}", data_len, path);

    // Send reply: Header=[status(4)] [data_len(4)], Data=[file_data]
    let mut reply_header = Vec::with_capacity(8);
    reply_header.extend_from_slice(&BENCHFS_MINI_SUCCESS.to_le_bytes());
    reply_header.extend_from_slice(&(data_len as u32).to_le_bytes());

    let reply_data = if data_len > 0 {
        &data_buf[..data_len]
    } else {
        &[]
    };

    unsafe {
        msg.reply(0, &reply_header, reply_data, false, None).await
            .map_err(|e| format!("reply failed: {:?}", e))?;
    }

    Ok(())
}

/// Server main loop: process Active Messages from both AM streams
async fn run_server_am(worker: Rc<Worker>, storage: Rc<FileStorage>) {
    eprintln!("[Server AM] Creating AM streams for RPC_WRITE and RPC_READ...");

    // Create AM streams for Write and Read
    let am_write_stream = worker.am_stream(RPC_WRITE).expect("Failed to create AM stream for WRITE");
    let am_read_stream = worker.am_stream(RPC_READ).expect("Failed to create AM stream for READ");

    eprintln!("[Server AM] Waiting for Active Messages...");

    // Process messages from both streams
    loop {
        // Use select! to wait for messages from either stream
        futures::select! {
            msg_opt = am_write_stream.wait_msg().fuse() => {
                if let Some(msg) = msg_opt {
                    eprintln!("[Server AM] Received Write AM");
                    let storage_clone = storage.clone();
                    pluvio_runtime::spawn(async move {
                        if let Err(e) = handle_write_am(msg, storage_clone).await {
                            eprintln!("[Server AM] Write handler error: {}", e);
                        }
                    });
                } else {
                    eprintln!("[Server AM] Write stream closed");
                    break;
                }
            }
            msg_opt = am_read_stream.wait_msg().fuse() => {
                if let Some(msg) = msg_opt {
                    eprintln!("[Server AM] Received Read AM");
                    let storage_clone = storage.clone();
                    pluvio_runtime::spawn(async move {
                        if let Err(e) = handle_read_am(msg, storage_clone).await {
                            eprintln!("[Server AM] Read handler error: {}", e);
                        }
                    });
                } else {
                    eprintln!("[Server AM] Read stream closed");
                    break;
                }
            }
        }
    }

    eprintln!("[Server AM] Server loop ended");
}

// ============================================================================
// Client Implementation - Active Message Based
// ============================================================================

/// Rust client Write using Active Message
async fn rpc_write_am(endpoint: &Endpoint, worker: &Rc<Worker>, path: &str, data: &[u8]) -> Result<(), String> {
    eprintln!("[Client AM] rpc_write: path={}, data_len={}", path, data.len());

    let path_bytes = path.as_bytes();
    let path_len = path_bytes.len();

    // Build header: [path_len(4)] [data_len(4)] [path(variable)]
    let mut header = Vec::with_capacity(8 + path_len);
    header.extend_from_slice(&(path_len as u32).to_le_bytes());
    header.extend_from_slice(&(data.len() as u32).to_le_bytes());
    header.extend_from_slice(path_bytes);

    eprintln!("[Client AM] Sending Write AM: header_len={}, data_len={}", header.len(), data.len());

    // Send Active Message with reply expected
    endpoint.am_send(RPC_WRITE as u32, &header, data, true, None).await
        .map_err(|e| format!("am_send failed: {:?}", e))?;

    eprintln!("[Client AM] Write AM sent, waiting for reply...");

    // Create AM stream to receive reply (AM ID 0 is used for replies by default)
    // We need to receive the reply, which will come as an Active Message
    // For simplicity, we create a temporary AM stream for reply reception
    let reply_stream = worker.am_stream(0)
        .map_err(|e| format!("Failed to create reply stream: {:?}", e))?;

    // Wait for reply
    if let Some(reply_msg) = reply_stream.wait_msg().await {
        let reply_header = reply_msg.header();
        if reply_header.len() < 4 {
            return Err(format!("Reply too short: {} bytes", reply_header.len()));
        }

        let status = i32::from_le_bytes([reply_header[0], reply_header[1], reply_header[2], reply_header[3]]);
        if status != BENCHFS_MINI_SUCCESS {
            return Err(format!("Write failed with status: {}", status));
        }

        eprintln!("[Client AM] Write completed successfully");
        Ok(())
    } else {
        Err("No reply received".to_string())
    }
}

/// Rust client Read using Active Message
async fn rpc_read_am(endpoint: &Endpoint, worker: &Rc<Worker>, path: &str, buffer: &mut [u8]) -> Result<usize, String> {
    let path_bytes = path.as_bytes();
    let path_len = path_bytes.len();

    // Build header: [path_len(4)] [buffer_len(4)] [path(variable)]
    let mut header = Vec::with_capacity(8 + path_len);
    header.extend_from_slice(&(path_len as u32).to_le_bytes());
    header.extend_from_slice(&(buffer.len() as u32).to_le_bytes());
    header.extend_from_slice(path_bytes);

    eprintln!("[Client AM] Sending Read AM: header_len={}", header.len());

    // Send Active Message with reply expected
    endpoint.am_send(RPC_READ as u32, &header, &[], true, None).await
        .map_err(|e| format!("am_send failed: {:?}", e))?;

    eprintln!("[Client AM] Read AM sent, waiting for reply...");

    // Create AM stream to receive reply
    let reply_stream = worker.am_stream(0)
        .map_err(|e| format!("Failed to create reply stream: {:?}", e))?;

    // Wait for reply
    if let Some(mut reply_msg) = reply_stream.wait_msg().await {
        let reply_header = reply_msg.header();
        if reply_header.len() < 8 {
            return Err(format!("Reply header too short: {} bytes", reply_header.len()));
        }

        let status = i32::from_le_bytes([reply_header[0], reply_header[1], reply_header[2], reply_header[3]]);
        let data_len = u32::from_le_bytes([reply_header[4], reply_header[5], reply_header[6], reply_header[7]]) as usize;

        if status != BENCHFS_MINI_SUCCESS {
            return Err(format!("Read failed with status: {}", status));
        }

        // Get reply data
        if data_len > 0 {
            let reply_data = if let Some(immediate_data) = reply_msg.get_data() {
                immediate_data.to_vec()
            } else {
                reply_msg.recv_data().await.map_err(|e| format!("recv_data failed: {:?}", e))?
            };

            let copy_len = reply_data.len().min(buffer.len());
            buffer[..copy_len].copy_from_slice(&reply_data[..copy_len]);
            eprintln!("[Client AM] Read completed: {} bytes", copy_len);
            Ok(copy_len)
        } else {
            eprintln!("[Client AM] Read completed: 0 bytes");
            Ok(0)
        }
    } else {
        Err("No reply received".to_string())
    }
}

// ============================================================================
// Pluvio Runtime Helper - Custom block_on implementation
// ============================================================================

/// Block on a future using the Pluvio runtime.
///
/// Unlike futures::executor::block_on which spawns a new thread and is incompatible
/// with thread-local storage (TLS) based runtimes like Pluvio, this implementation
/// uses a oneshot channel to retrieve the future's result after spawning it.
///
/// We cannot use JoinHandle with run_with_runtime because run_with_runtime spawns
/// the future again and run_queue() loops while task_pool.len() > 0. A task awaiting
/// a JoinHandle stays Pending in the pool, causing an infinite loop.
///
/// Instead, we spawn the wrapped future once, then manually drive the runtime with
/// progress() until the result arrives via the oneshot channel.
fn block_on_with_runtime<F, T>(runtime: &Rc<Runtime>, future: F) -> T
where
    F: Future<Output = T> + 'static,
    T: 'static,
{
    use futures::channel::oneshot;
    use std::cell::RefCell;
    use std::rc::Rc;

    // Create oneshot channel to receive the result
    let (sender, receiver) = oneshot::channel::<T>();

    // Wrap the future to send its result through the channel
    let wrapper = async move {
        let result = future.await;
        let _ = sender.send(result);  // Ignore send error if channel is closed
    };

    // Spawn the wrapper future to polling queue
    // Using spawn_polling_with_runtime ensures the task is processed by progress()
    // Regular spawn_with_runtime may not process tasks immediately due to 100-task limit
    runtime.spawn_polling_with_runtime(wrapper);

    // Pin the receiver to the stack so we can poll it
    use std::pin::Pin;
    use std::task::{Context, Poll, Wake};
    use std::sync::Arc;

    struct DummyWaker;
    impl Wake for DummyWaker {
        fn wake(self: Arc<Self>) {}
    }

    let waker = Arc::new(DummyWaker).into();
    let mut cx = Context::from_waker(&waker);
    let mut receiver_boxed = Box::new(receiver);
    let mut receiver = Pin::new(&mut *receiver_boxed);

    // Drive the runtime until receiver is ready
    loop {
        // First, progress the runtime to process tasks
        runtime.progress();

        // Check if receiver has a value
        match receiver.as_mut().poll(&mut cx) {
            Poll::Ready(result) => {
                match result {
                    Ok(value) => return value,
                    Err(_) => panic!("Sender was dropped before sending result"),
                }
            }
            Poll::Pending => {
                // Not ready yet, continue driving the runtime
                continue;
            }
        }
    }
}

// ============================================================================
// Registry (Shared Filesystem)
// ============================================================================

fn register_server(registry_dir: &str, rank: i32, hostname: &str, port: u16) -> Result<(), String> {
    let path = format!("{}/server_{}.txt", registry_dir, rank);
    let content = format!("{}:{}", hostname, port);
    std::fs::write(&path, content)
        .map_err(|e| format!("Failed to register server: {:?}", e))
}

fn register_server_worker_addr(registry_dir: &str, rank: i32, worker_addr: &[u8]) -> Result<(), String> {
    let path = format!("{}/server_{}_worker.bin", registry_dir, rank);
    std::fs::write(&path, worker_addr)
        .map_err(|e| format!("Failed to register worker address: {:?}", e))
}

fn lookup_server_worker_addr(registry_dir: &str, rank: i32) -> Result<Vec<u8>, String> {
    let path = format!("{}/server_{}_worker.bin", registry_dir, rank);

    // Wait for file to appear
    for _ in 0..30 {
        if std::path::Path::new(&path).exists() {
            return std::fs::read(&path)
                .map_err(|e| format!("Failed to read worker address: {:?}", e));
        }
        std::thread::sleep(std::time::Duration::from_secs(1));
    }

    Err(format!("Server {} worker address not found in registry", rank))
}

fn lookup_server(registry_dir: &str, rank: i32) -> Result<SocketAddr, String> {
    let path = format!("{}/server_{}.txt", registry_dir, rank);

    // Wait for file to appear
    for _ in 0..30 {
        if std::path::Path::new(&path).exists() {
            let content = std::fs::read_to_string(&path)
                .map_err(|e| format!("Failed to read registry: {:?}", e))?;

            // Parse hostname:port format
            let trimmed = content.trim();
            let parts: Vec<&str> = trimmed.split(':').collect();
            if parts.len() != 2 {
                return Err(format!("Invalid address format: {}", trimmed));
            }

            let hostname = parts[0];
            let port: u16 = parts[1]
                .parse()
                .map_err(|e| format!("Failed to parse port: {:?}", e))?;

            // Resolve hostname to IP address using DNS
            let addrs: Vec<SocketAddr> = format!("{}:{}", hostname, port)
                .to_socket_addrs()
                .map_err(|e| format!("Failed to resolve hostname {}: {:?}", hostname, e))?
                .collect();

            if let Some(addr) = addrs.first() {
                return Ok(*addr);
            } else {
                return Err(format!("No addresses found for hostname: {}", hostname));
            }
        }
        std::thread::sleep(std::time::Duration::from_secs(1));
    }

    Err(format!("Server {} not found in registry", rank))
}

// ============================================================================
// FFI Implementation
// ============================================================================

#[unsafe(no_mangle)]
pub extern "C" fn benchfs_mini_init(
    registry_dir: *const libc::c_char,
    is_server: bool,
) -> i32 {
    unsafe {
        // Check if MPI is already initialized
        let mut flag: libc::c_int = 0;
        mpi_sys::MPI_Initialized(&mut flag as *mut libc::c_int);

        let (rank, size) = if flag == 0 {
            // MPI not initialized yet, initialize it using mpi_sys
            let mut argc: libc::c_int = 0;
            let mut argv: *mut *mut libc::c_char = std::ptr::null_mut();
            mpi_sys::MPI_Init(&mut argc as *mut libc::c_int, &mut argv as *mut *mut *mut libc::c_char);

            let mut rank_raw: libc::c_int = 0;
            let mut size_raw: libc::c_int = 0;
            mpi_sys::MPI_Comm_rank(mpi_sys::RSMPI_COMM_WORLD, &mut rank_raw as *mut libc::c_int);
            mpi_sys::MPI_Comm_size(mpi_sys::RSMPI_COMM_WORLD, &mut size_raw as *mut libc::c_int);
            (rank_raw, size_raw)
        } else {
            // MPI already initialized, just get rank and size using mpi_sys
            let mut rank_raw: libc::c_int = 0;
            let mut size_raw: libc::c_int = 0;
            mpi_sys::MPI_Comm_rank(mpi_sys::RSMPI_COMM_WORLD, &mut rank_raw as *mut libc::c_int);
            mpi_sys::MPI_Comm_size(mpi_sys::RSMPI_COMM_WORLD, &mut size_raw as *mut libc::c_int);
            (rank_raw, size_raw)
        };

        let registry_dir_str = std::ffi::CStr::from_ptr(registry_dir)
            .to_string_lossy()
            .to_string();

        eprintln!("[Rank {}] Initializing BenchFS Mini: is_server={}, registry_dir={}",
                  rank, is_server, registry_dir_str);

        // Create runtime
        let runtime = Runtime::new(256);

        // Set runtime in thread-local storage
        pluvio_runtime::set_runtime(runtime.clone());

        // Create and register IoUring reactor
        let uring_reactor = IoUringReactor::builder()
            .queue_size(2048)
            .buffer_size(1 << 20)  // 1 MiB
            .submit_depth(64)
            .wait_submit_timeout(std::time::Duration::from_millis(100))
            .wait_complete_timeout(std::time::Duration::from_millis(150))
            .build();
        runtime.register_reactor("io_uring_reactor", uring_reactor.clone());

        // Create and register UCX reactor
        let ucx_reactor = pluvio_ucx::UCXReactor::current();
        runtime.register_reactor("ucx_reactor", ucx_reactor.clone());

        // Create UCX context and worker
        let context = Arc::new(Context::new().unwrap());
        let worker = context.create_worker().unwrap();

        // Register worker with reactor
        ucx_reactor.register_worker(worker.clone());

        // For AM-based implementation, clients connect to servers to create endpoints
        // Then use Active Messages through those endpoints
        // Servers must keep the listener alive for clients to connect
        let listener = if is_server {
            // Register server address for clients to discover
            // We need to bind a port and keep listener alive for clients to connect
            let listen_addr: SocketAddr = "0.0.0.0:0".parse().unwrap();
            let listener = worker.create_listener(listen_addr).unwrap();
            let bound_addr = listener.socket_addr().unwrap();

            eprintln!("[Rank {}] Server listening on {}", rank, bound_addr);

            // Register server (socket address for backward compatibility)
            let hostname = std::env::var("HOSTNAME")
                .unwrap_or_else(|_| gethostname::gethostname().to_string_lossy().to_string());
            register_server(&registry_dir_str, rank, &hostname, bound_addr.port()).unwrap();

            // Also register WorkerAddress for Active Message
            let worker_addr = worker.address().unwrap();
            register_server_worker_addr(&registry_dir_str, rank, worker_addr.as_ref()).unwrap();

            eprintln!("[Rank {}] Registered as {}:{} (socket) and WorkerAddress (AM)", rank, hostname, bound_addr.port());

            // Keep listener alive - clients need it to connect
            Some(listener)
        } else {
            None
        };

        // Create storage directory
        let storage_dir = format!("{}/storage_rank_{}", registry_dir_str, rank);
        std::fs::create_dir_all(&storage_dir).unwrap();

        GLOBAL_STATE = Some(Box::new(GlobalState {
            runtime,
            ucx_reactor,
            uring_reactor,
            worker,
            context,
            server_started: false,
            rank,
            size,
            registry_dir: registry_dir_str,
            storage_dir,
            server_endpoints: Arc::new(AsyncMutex::new(HashMap::new())),
            listener,
        }));

        eprintln!("[Rank {}] BenchFS Mini initialized", rank);
        BENCHFS_MINI_SUCCESS
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn benchfs_mini_start_server() -> i32 {
    unsafe {
        let state_ptr = std::ptr::addr_of_mut!(GLOBAL_STATE);
        let state_opt = &mut *state_ptr;

        if let Some(state) = state_opt {
            if state.server_started {
                eprintln!("[Rank {}] Server already started", state.rank);
                return BENCHFS_MINI_SUCCESS;
            }

            let worker = state.worker.clone();
            let storage_dir = state.storage_dir.clone();
            let storage = Rc::new(FileStorage::new(storage_dir));
            let rank = state.rank;
            let runtime = state.runtime.clone();

            eprintln!("[Rank {}] Starting AM server loop", rank);

            // Wait for clients to connect
            eprintln!("[Rank {}] Calling worker.wait_connect()...", rank);
            worker.wait_connect();
            eprintln!("[Rank {}] worker.wait_connect() returned", rank);

            // Spawn AM server task in the background
            pluvio_runtime::spawn(async move {
                run_server_am(worker, storage).await;
            });

            state.server_started = true;
            eprintln!("[Rank {}] AM server task spawned, entering runtime loop", rank);

            // Server processes must continuously drive the runtime
            // This infinite loop processes spawned tasks and handles Active Messages
            // To stop the server, use pkill or similar
            loop {
                runtime.progress();
            }
        }

        BENCHFS_MINI_SUCCESS
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn benchfs_mini_connect(server_rank: i32) -> i32 {
    unsafe {
        let state = (*std::ptr::addr_of!(GLOBAL_STATE)).as_ref().unwrap();

        eprintln!("[Rank {}] Connecting to server rank {} ========== NEW BUILD v2025-01-13 ==========", state.rank, server_rank);

        let registry_dir = state.registry_dir.clone();
        let worker = state.worker.clone();
        let endpoints = state.server_endpoints.clone();
        let runtime = state.runtime.clone();

        // Use block_on_with_runtime to drive the Pluvio runtime
        let result = block_on_with_runtime(&runtime, async move {
            // Lookup server WorkerAddress
            let worker_addr_bytes = lookup_server_worker_addr(&registry_dir, server_rank)?;
            eprintln!("[Client] Found WorkerAddress for server {} ({} bytes)", server_rank, worker_addr_bytes.len());

            // Convert to WorkerAddressInner
            let worker_addr = pluvio_ucx::WorkerAddressInner::from(worker_addr_bytes.as_slice());
            eprintln!("[Client] Connecting to server {} using WorkerAddress", server_rank);

            // Connect using WorkerAddress
            let endpoint = worker
                .connect_addr(&worker_addr)
                .map_err(|e| format!("Failed to connect: {:?}", e))?;

            eprintln!("[Client] Connected to server");
            endpoint.print_to_stderr();

            // Store endpoint with server_rank as key
            endpoints.lock().await.insert(server_rank, endpoint);

            Ok::<_, String>(())
        });

        match result {
            Ok(_) => BENCHFS_MINI_SUCCESS,
            Err(e) => {
                eprintln!("[Rank {}] Connect failed: {}", state.rank, e);
                BENCHFS_MINI_ERROR
            }
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn benchfs_mini_write(
    path: *const libc::c_char,
    data: *const u8,
    data_len: usize,
    server_rank: i32,
) -> i32 {
    unsafe {
        let state = (*std::ptr::addr_of!(GLOBAL_STATE)).as_ref().unwrap();
        let path_str = std::ffi::CStr::from_ptr(path).to_string_lossy();
        let data_slice = std::slice::from_raw_parts(data, data_len);

        eprintln!("[FFI] benchfs_mini_write: path={}, data_len={}, server_rank={}", path_str, data_len, server_rank);

        let endpoints = state.server_endpoints.clone();
        let worker = state.worker.clone();
        let runtime = state.runtime.clone();

        eprintln!("[FFI AM] Entering block_on_with_runtime for write");
        let result = block_on_with_runtime(&runtime, async move {
            eprintln!("[FFI AM async] Inside async block, acquiring endpoints lock");
            let endpoints_guard = endpoints.lock().await;
            eprintln!("[FFI AM async] Lock acquired, checking endpoint for server {}", server_rank);
            if let Some(endpoint) = endpoints_guard.get(&server_rank) {
                eprintln!("[FFI AM async] Endpoint found, calling rpc_write_am");
                rpc_write_am(endpoint, &worker, &path_str, data_slice).await
            } else {
                eprintln!("[FFI AM async] Endpoint not found!");
                Err(format!("No connection to server rank {}", server_rank))
            }
        });
        eprintln!("[FFI AM] block_on_with_runtime returned");

        match result {
            Ok(_) => BENCHFS_MINI_SUCCESS,
            Err(e) => {
                eprintln!("[Write] Error: {}", e);
                BENCHFS_MINI_ERROR
            }
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn benchfs_mini_read(
    path: *const libc::c_char,
    buffer: *mut u8,
    buffer_len: usize,
    server_rank: i32,
) -> i32 {
    unsafe {
        let state = (*std::ptr::addr_of!(GLOBAL_STATE)).as_ref().unwrap();
        let path_str = std::ffi::CStr::from_ptr(path).to_string_lossy();
        let buffer_slice = std::slice::from_raw_parts_mut(buffer, buffer_len);

        let endpoints = state.server_endpoints.clone();
        let worker = state.worker.clone();
        let runtime = state.runtime.clone();

        let result = block_on_with_runtime(&runtime, async move {
            let endpoints_guard = endpoints.lock().await;
            if let Some(endpoint) = endpoints_guard.get(&server_rank) {
                rpc_read_am(endpoint, &worker, &path_str, buffer_slice).await
            } else {
                Err(format!("No connection to server rank {}", server_rank))
            }
        });

        match result {
            Ok(bytes_read) => bytes_read as i32,
            Err(e) => {
                eprintln!("[Read] Error: {}", e);
                BENCHFS_MINI_ERROR
            }
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn benchfs_mini_finalize() -> i32 {
    unsafe {
        if let Some(state) = (*std::ptr::addr_of_mut!(GLOBAL_STATE)).take() {
            eprintln!("[Rank {}] Finalizing BenchFS Mini", state.rank);

            // Drop all resources
            drop(state);
        }

        // Explicitly finalize MPI using mpi_sys
        // (since we used forget() on Universe, we need to manually finalize)
        mpi_sys::MPI_Finalize();

        BENCHFS_MINI_SUCCESS
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn benchfs_mini_progress() {
    unsafe {
        if let Some(state) = (*std::ptr::addr_of!(GLOBAL_STATE)).as_ref() {
            // The runtime drives itself when block_on is called
            // For server processes, we need to yield to allow spawned tasks to run
            let _runtime = state.runtime.clone();
            // The worker's progress is driven by block_on in other FFI functions
            // For a minimal implementation, this can be a no-op
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn benchfs_mini_yield() {
    unsafe {
        if let Some(state) = (*std::ptr::addr_of!(GLOBAL_STATE)).as_ref() {
            let runtime = state.runtime.clone();
            // Use the new progress() method to drive runtime forward incrementally
            // This polls reactors and processes pending tasks
            runtime.progress();
        }
    }
}
