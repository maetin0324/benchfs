//! Minimal BenchFS library for debugging Stream RPC
//!
//! This is a minimal implementation to isolate and debug Stream RPC connection issues.
//! All code is contained in this single module for simplicity.

use std::collections::HashMap;
use std::net::{SocketAddr, ToSocketAddrs};
use std::rc::Rc;
use std::sync::Arc;
use std::future::Future;

use futures::lock::Mutex as AsyncMutex;
use pluvio_runtime::executor::Runtime;
use pluvio_ucx::endpoint::Endpoint;
use pluvio_ucx::{Context, Worker};

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
    worker: Rc<Worker>,
    context: Arc<Context>,
    listener: Option<pluvio_ucx::listener::Listener>,
    server_started: bool,
    rank: i32,
    size: i32,
    registry_dir: String,
    server_endpoints: Arc<AsyncMutex<Vec<Endpoint>>>,
}

static mut GLOBAL_STATE: Option<Box<GlobalState>> = None;

// ============================================================================
// Stream RPC Helpers
// ============================================================================

async fn stream_send(endpoint: &Endpoint, data: &[u8]) -> Result<(), String> {
    endpoint
        .stream_send(data)
        .await
        .map(|_bytes_sent| ()) // Discard the number of bytes sent
        .map_err(|e| format!("stream_send failed: {:?}", e))
}

async fn stream_recv(endpoint: &Endpoint, buffer: &mut [u8]) -> Result<usize, String> {
    // stream_recv requires &mut [MaybeUninit<u8>], so we need to convert
    use std::mem::MaybeUninit;

    let uninit_buffer = unsafe {
        std::slice::from_raw_parts_mut(buffer.as_mut_ptr() as *mut MaybeUninit<u8>, buffer.len())
    };

    endpoint
        .stream_recv(uninit_buffer)
        .await
        .map_err(|e| format!("stream_recv failed: {:?}", e))
}

// ============================================================================
// Simple Storage (in-memory)
// ============================================================================

struct SimpleStorage {
    data: AsyncMutex<HashMap<String, Vec<u8>>>,
}

impl SimpleStorage {
    fn new() -> Self {
        Self {
            data: AsyncMutex::new(HashMap::new()),
        }
    }

    async fn write(&self, path: &str, data: &[u8]) -> Result<(), String> {
        let mut storage = self.data.lock().await;
        storage.insert(path.to_string(), data.to_vec());
        Ok(())
    }

    async fn read(&self, path: &str, buffer: &mut [u8]) -> Result<usize, String> {
        let storage = self.data.lock().await;
        if let Some(data) = storage.get(path) {
            let len = data.len().min(buffer.len());
            buffer[..len].copy_from_slice(&data[..len]);
            Ok(len)
        } else {
            Err(format!("File not found: {}", path))
        }
    }
}

// ============================================================================
// Simple RPC Protocol
// ============================================================================

const RPC_WRITE: u16 = 1;
const RPC_READ: u16 = 2;

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
// Server Implementation
// ============================================================================

async fn handle_client(endpoint: Endpoint, storage: Arc<SimpleStorage>) {
    eprintln!("[Server] New client connection");
    endpoint.print_to_stderr();

    loop {
        // Read RPC ID
        let mut rpc_id_buf = [0u8; 2];
        if let Err(e) = stream_recv(&endpoint, &mut rpc_id_buf).await {
            eprintln!("[Server] Failed to receive RPC ID: {}", e);
            break;
        }
        let rpc_id = u16::from_le_bytes(rpc_id_buf);

        eprintln!("[Server] Received RPC ID: {}", rpc_id);

        match rpc_id {
            RPC_WRITE => {
                if let Err(e) = handle_write(&endpoint, &storage).await {
                    eprintln!("[Server] Write handler error: {}", e);
                }
            }
            RPC_READ => {
                if let Err(e) = handle_read(&endpoint, &storage).await {
                    eprintln!("[Server] Read handler error: {}", e);
                }
            }
            _ => {
                eprintln!("[Server] Unknown RPC ID: {}", rpc_id);
                break;
            }
        }
    }

    eprintln!("[Server] Client disconnected");
}

async fn handle_write(endpoint: &Endpoint, storage: &SimpleStorage) -> Result<(), String> {
    // Read request header
    let mut header_buf = [0u8; 8];
    stream_recv(endpoint, &mut header_buf).await?;
    let path_len = u32::from_le_bytes([header_buf[0], header_buf[1], header_buf[2], header_buf[3]]);
    let data_len = u32::from_le_bytes([header_buf[4], header_buf[5], header_buf[6], header_buf[7]]);

    eprintln!("[Server] Write request: path_len={}, data_len={}", path_len, data_len);

    // Read path
    let mut path_buf = vec![0u8; path_len as usize];
    stream_recv(endpoint, &mut path_buf).await?;
    let path = String::from_utf8_lossy(&path_buf);

    // Read data
    let mut data_buf = vec![0u8; data_len as usize];
    stream_recv(endpoint, &mut data_buf).await?;

    // Write to storage
    storage.write(&path, &data_buf).await?;

    eprintln!("[Server] Wrote {} bytes to {}", data_len, path);

    // Send response
    let response = WriteResponse {
        status: BENCHFS_MINI_SUCCESS,
    };
    stream_send(endpoint, &response.status.to_le_bytes()).await?;

    Ok(())
}

async fn handle_read(endpoint: &Endpoint, storage: &SimpleStorage) -> Result<(), String> {
    // Read request header
    let mut header_buf = [0u8; 8];
    stream_recv(endpoint, &mut header_buf).await?;
    let path_len = u32::from_le_bytes([header_buf[0], header_buf[1], header_buf[2], header_buf[3]]);
    let buffer_len = u32::from_le_bytes([header_buf[4], header_buf[5], header_buf[6], header_buf[7]]);

    eprintln!("[Server] Read request: path_len={}, buffer_len={}", path_len, buffer_len);

    // Read path
    let mut path_buf = vec![0u8; path_len as usize];
    stream_recv(endpoint, &mut path_buf).await?;
    let path = String::from_utf8_lossy(&path_buf);

    // Read from storage
    let mut data_buf = vec![0u8; buffer_len as usize];
    let data_len = match storage.read(&path, &mut data_buf).await {
        Ok(len) => len,
        Err(_) => 0,
    };

    eprintln!("[Server] Read {} bytes from {}", data_len, path);

    // Send response
    let response = ReadResponse {
        status: BENCHFS_MINI_SUCCESS,
        data_len: data_len as u32,
    };
    stream_send(endpoint, &response.status.to_le_bytes()).await?;
    stream_send(endpoint, &response.data_len.to_le_bytes()).await?;

    // Send data
    if data_len > 0 {
        stream_send(endpoint, &data_buf[..data_len]).await?;
    }

    Ok(())
}

async fn run_server(worker: Rc<Worker>, mut listener: pluvio_ucx::listener::Listener, storage: Arc<SimpleStorage>) {
    eprintln!("[Server] Waiting for connections...");

    loop {
        // Get next connection from listener
        let connection = listener.next().await;

        // Accept the connection using the worker
        match worker.accept(connection).await {
            Ok(endpoint) => {
                eprintln!("[Server] Accepted connection");
                let storage_clone = storage.clone();
                pluvio_runtime::spawn(async move {
                    handle_client(endpoint, storage_clone).await;
                });
            }
            Err(e) => {
                eprintln!("[Server] Accept error: {:?}", e);
                break;
            }
        }
    }
}

// ============================================================================
// Client Implementation
// ============================================================================

async fn rpc_write(endpoint: &Endpoint, path: &str, data: &[u8]) -> Result<(), String> {
    // Send RPC ID
    let rpc_id = RPC_WRITE.to_le_bytes();
    stream_send(endpoint, &rpc_id).await?;

    // Send request
    let path_bytes = path.as_bytes();
    let header = WriteRequest {
        path_len: path_bytes.len() as u32,
        data_len: data.len() as u32,
    };
    stream_send(endpoint, &header.path_len.to_le_bytes()).await?;
    stream_send(endpoint, &header.data_len.to_le_bytes()).await?;
    stream_send(endpoint, path_bytes).await?;
    stream_send(endpoint, data).await?;

    // Receive response
    let mut status_buf = [0u8; 4];
    stream_recv(endpoint, &mut status_buf).await?;
    let status = i32::from_le_bytes(status_buf);

    if status != BENCHFS_MINI_SUCCESS {
        return Err(format!("Write failed with status: {}", status));
    }

    Ok(())
}

async fn rpc_read(endpoint: &Endpoint, path: &str, buffer: &mut [u8]) -> Result<usize, String> {
    // Send RPC ID
    let rpc_id = RPC_READ.to_le_bytes();
    stream_send(endpoint, &rpc_id).await?;

    // Send request
    let path_bytes = path.as_bytes();
    let header = ReadRequest {
        path_len: path_bytes.len() as u32,
        buffer_len: buffer.len() as u32,
    };
    stream_send(endpoint, &header.path_len.to_le_bytes()).await?;
    stream_send(endpoint, &header.buffer_len.to_le_bytes()).await?;
    stream_send(endpoint, path_bytes).await?;

    // Receive response
    let mut status_buf = [0u8; 4];
    stream_recv(endpoint, &mut status_buf).await?;
    let status = i32::from_le_bytes(status_buf);

    let mut data_len_buf = [0u8; 4];
    stream_recv(endpoint, &mut data_len_buf).await?;
    let data_len = u32::from_le_bytes(data_len_buf) as usize;

    if status != BENCHFS_MINI_SUCCESS {
        return Err(format!("Read failed with status: {}", status));
    }

    // Receive data
    if data_len > 0 {
        stream_recv(endpoint, &mut buffer[..data_len]).await?;
    }

    Ok(data_len)
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

    // Spawn the wrapper future
    runtime.spawn_with_runtime(wrapper);

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

        // Create UCX context and worker
        let context = Arc::new(Context::new().unwrap());
        // context.create_worker() internally calls Worker::new() which registers with UCXReactor
        let worker = context.create_worker().unwrap();

        let listener = if is_server {
            // Create listener
            let listen_addr: SocketAddr = "0.0.0.0:0".parse().unwrap();
            let listener = worker.create_listener(listen_addr).unwrap();
            let bound_addr = listener.socket_addr().unwrap();

            eprintln!("[Rank {}] Server listening on {}", rank, bound_addr);

            // Register server
            let hostname = std::env::var("HOSTNAME")
                .unwrap_or_else(|_| gethostname::gethostname().to_string_lossy().to_string());
            register_server(&registry_dir_str, rank, &hostname, bound_addr.port()).unwrap();

            eprintln!("[Rank {}] Registered as {}:{}", rank, hostname, bound_addr.port());

            Some(listener)
        } else {
            None
        };

        GLOBAL_STATE = Some(Box::new(GlobalState {
            runtime,
            worker,
            context,
            listener,
            server_started: false,
            rank,
            size,
            registry_dir: registry_dir_str,
            server_endpoints: Arc::new(AsyncMutex::new(Vec::new())),
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

            if let Some(listener) = state.listener.take() {
                let worker = state.worker.clone();
                let storage = Arc::new(SimpleStorage::new());
                let rank = state.rank;

                eprintln!("[Rank {}] Starting server loop", rank);

                // Spawn server task in the background
                // The task will run when the runtime is driven by block_on calls
                pluvio_runtime::spawn(async move {
                    run_server(worker, listener, storage).await;
                });

                state.server_started = true;
                eprintln!("[Rank {}] Server loop started", rank);
            } else {
                eprintln!("[Rank {}] No listener available (not a server rank?)", state.rank);
            }
        }

        BENCHFS_MINI_SUCCESS
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn benchfs_mini_connect(server_rank: i32) -> i32 {
    unsafe {
        let state = (*std::ptr::addr_of!(GLOBAL_STATE)).as_ref().unwrap();

        eprintln!("[Rank {}] Connecting to server rank {}", state.rank, server_rank);

        let registry_dir = state.registry_dir.clone();
        let worker = state.worker.clone();
        let endpoints = state.server_endpoints.clone();
        let runtime = state.runtime.clone();

        // Use block_on_with_runtime to drive the Pluvio runtime
        let result = block_on_with_runtime(&runtime, async move {
            // Lookup server address
            let server_addr = lookup_server(&registry_dir, server_rank)?;
            eprintln!("[Client] Connecting to {}", server_addr);

            // Connect
            let endpoint = worker
                .connect_socket(server_addr)
                .await
                .map_err(|e| format!("Failed to connect: {:?}", e))?;

            eprintln!("[Client] Connected to server");
            endpoint.print_to_stderr();

            // Store endpoint
            endpoints.lock().await.push(endpoint);

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

        let endpoints = state.server_endpoints.clone();
        let runtime = state.runtime.clone();

        let result = block_on_with_runtime(&runtime, async move {
            let endpoints_guard = endpoints.lock().await;
            if let Some(endpoint) = endpoints_guard.get(server_rank as usize) {
                rpc_write(endpoint, &path_str, data_slice).await
            } else {
                Err(format!("No connection to server rank {}", server_rank))
            }
        });

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
        let runtime = state.runtime.clone();

        let result = block_on_with_runtime(&runtime, async move {
            let endpoints_guard = endpoints.lock().await;
            if let Some(endpoint) = endpoints_guard.get(server_rank as usize) {
                rpc_read(endpoint, &path_str, buffer_slice).await
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
