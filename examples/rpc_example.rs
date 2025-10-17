//! Example demonstrating the UCX ActiveMessage-based RPC system

use benchfs::rpc::{
    client::RpcClient, server::RpcServer, AmRpc, Connection, RpcError, RpcId,
};
use pluvio_runtime::executor::Runtime;
use pluvio_ucx::{async_ucx::ucp::AmMsg, Context, UCXReactor};
use std::io::IoSlice;
use std::rc::Rc;

// Example RPC IDs
const RPC_READ: RpcId = 1;
const RPC_WRITE: RpcId = 2;

// Reply stream IDs (separate from request stream IDs)
const REPLY_STREAM_READ: u16 = 101;
const REPLY_STREAM_WRITE: u16 = 102;

// ========== Read RPC ==========

/// Read request header
#[repr(C)]
#[derive(
    Debug,
    Clone,
    Copy,
    zerocopy::FromBytes,
    zerocopy::IntoBytes,
    zerocopy::KnownLayout,
    zerocopy::Immutable,
)]
struct ReadRequestHeader {
    offset: u64,
    len: u64,
}

/// Read response header
#[repr(C)]
#[derive(
    Debug,
    Clone,
    Copy,
    zerocopy::FromBytes,
    zerocopy::IntoBytes,
    zerocopy::KnownLayout,
    zerocopy::Immutable,
)]
struct ReadResponseHeader {
    bytes_read: u64,
    status: u32,
    _padding: u32,
}

/// Read RPC implementation
struct ReadRequest {
    header: ReadRequestHeader,
}

impl AmRpc for ReadRequest {
    type RequestHeader = ReadRequestHeader;
    type ResponseHeader = ReadResponseHeader;

    fn rpc_id() -> RpcId {
        RPC_READ
    }

    fn reply_stream_id() -> u16 {
        REPLY_STREAM_READ
    }

    fn request_header(&self) -> &Self::RequestHeader {
        &self.header
    }

    async fn call(&self, client: &RpcClient) -> Result<Self::ResponseHeader, RpcError> {
        client.execute(self).await
    }

    async fn call_no_reply(&self, client: &RpcClient) -> Result<(), RpcError> {
        client.execute_no_reply(self).await
    }

    async fn server_handler(am_msg: AmMsg) -> Result<Self::ResponseHeader, RpcError> {
        // Parse request header
        let header_bytes = am_msg.header();
        let request: ReadRequestHeader = zerocopy::FromBytes::read_from_bytes(header_bytes)
            .map_err(|_| RpcError::InvalidHeader)?;

        println!(
            "Server: Read request - offset={}, len={}",
            request.offset, request.len
        );

        // Simulate read operation
        let response = ReadResponseHeader {
            bytes_read: request.len,
            status: 0,
            _padding: 0,
        };

        // Send reply only if the client expects one
        if am_msg.need_reply() {
            unsafe {
                am_msg
                    .reply(
                        REPLY_STREAM_READ as u32,
                        zerocopy::IntoBytes::as_bytes(&response),
                        &[],
                        false,
                        None,
                    )
                    .await
                    .map_err(|e| RpcError::TransportError(format!("Failed to send reply: {:?}", e)))?;
            }
            println!("Server: Read reply sent");
        } else {
            println!("Server: Read request completed (no reply needed)");
        }

        Ok(response)
    }
}

// ========== Write RPC ==========

/// Write request header
#[repr(C)]
#[derive(
    Debug,
    Clone,
    Copy,
    zerocopy::FromBytes,
    zerocopy::IntoBytes,
    zerocopy::KnownLayout,
    zerocopy::Immutable,
)]
struct WriteRequestHeader {
    offset: u64,
    len: u64,
}

/// Write response header
#[repr(C)]
#[derive(
    Debug,
    Clone,
    Copy,
    zerocopy::FromBytes,
    zerocopy::IntoBytes,
    zerocopy::KnownLayout,
    zerocopy::Immutable,
)]
struct WriteResponseHeader {
    bytes_written: u64,
    status: u32,
    _padding: u32,
}

/// Write RPC implementation
struct WriteRequest<'a> {
    header: WriteRequestHeader,
    data: &'a [u8],
}

impl<'a> AmRpc for WriteRequest<'a> {
    type RequestHeader = WriteRequestHeader;
    type ResponseHeader = WriteResponseHeader;

    fn rpc_id() -> RpcId {
        RPC_WRITE
    }

    fn reply_stream_id() -> u16 {
        REPLY_STREAM_WRITE
    }

    fn request_header(&self) -> &Self::RequestHeader {
        &self.header
    }

    fn request_data(&self) -> &[IoSlice<'_>] {
        // This is a limitation - we need to return a slice of IoSlice
        // For now, we'll use an empty slice and handle data differently
        &[]
    }

    fn proto(&self) -> Option<pluvio_ucx::async_ucx::ucp::AmProto> {
        // Use Rndv (Rendezvous) for large writes
        if self.data.len() > 8192 {
            Some(pluvio_ucx::async_ucx::ucp::AmProto::Rndv)
        } else {
            Some(pluvio_ucx::async_ucx::ucp::AmProto::Eager)
        }
    }

    async fn call(&self, client: &RpcClient) -> Result<Self::ResponseHeader, RpcError> {
        client.execute(self).await
    }

    async fn call_no_reply(&self, client: &RpcClient) -> Result<(), RpcError> {
        client.execute_no_reply(self).await
    }

    async fn server_handler(mut am_msg: AmMsg) -> Result<Self::ResponseHeader, RpcError> {
        // Parse request header
        let header_bytes = am_msg.header();
        let request: WriteRequestHeader = zerocopy::FromBytes::read_from_bytes(header_bytes)
            .map_err(|_| RpcError::InvalidHeader)?;

        println!(
            "Server: Write request - offset={}, len={}",
            request.offset, request.len
        );

        // Receive data if present
        let data = if am_msg.contains_data() {
            am_msg
                .recv_data()
                .await
                .map_err(|e| RpcError::TransportError(format!("Failed to recv data: {:?}", e)))?
        } else {
            Vec::new()
        };

        println!("Server: Received {} bytes of data", data.len());

        // Simulate write operation
        let response = WriteResponseHeader {
            bytes_written: data.len() as u64,
            status: 0,
            _padding: 0,
        };

        // Send reply only if the client expects one
        if am_msg.need_reply() {
            unsafe {
                am_msg
                    .reply(
                        REPLY_STREAM_WRITE as u32,
                        zerocopy::IntoBytes::as_bytes(&response),
                        &[],
                        false,
                        None,
                    )
                    .await
                    .map_err(|e| RpcError::TransportError(format!("Failed to send reply: {:?}", e)))?;
            }
            println!("Server: Write reply sent");
        } else {
            println!("Server: Write request completed (no reply needed)");
        }

        Ok(response)
    }
}


// ========== Server and Client ==========

static SHARED_FILE: &str = "worker_addr.bin";

async fn server(
    runtime: Rc<Runtime>,
    reactor: Rc<UCXReactor>,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Server: Starting...");

    let context = Context::new()?;
    let worker = context.create_worker()?;
    reactor.register_worker(worker.clone());

    // Save worker address for client
    let worker_addr = worker.address()?;
    std::fs::write(SHARED_FILE, worker_addr.as_ref())?;
    println!("Server: Worker address saved");

    // Set worker to wait for connections
    worker.wait_connect();

    // Create server instances with the same worker
    let server = RpcServer::new(worker.clone());
    let server2 = RpcServer::new(worker.clone());

    // Start listening for Read RPCs
    let runtime_clone = runtime.clone();
    runtime.spawn(async move {
        if let Err(e) = server
            .listen::<ReadRequest, ReadRequestHeader, ReadResponseHeader>(runtime_clone)
            .await
        {
            eprintln!("Server error (Read): {:?}", e);
        }
    });

    // Start listening for Write RPCs
    let runtime_clone2 = runtime.clone();
    runtime.spawn(async move {
        if let Err(e) = server2
            .listen::<WriteRequest, WriteRequestHeader, WriteResponseHeader>(runtime_clone2)
            .await
        {
            eprintln!("Server error (Write): {:?}", e);
        }
    });

    println!("Server: Running and listening for RPCs");
    
    // Keep server alive
    std::future::pending::<()>().await;
    Ok(())
}



async fn client(
    _runtime: Rc<Runtime>,
    reactor: Rc<UCXReactor>,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Client: Starting...");

    // Wait for server to be ready
    std::thread::sleep(std::time::Duration::from_secs(1));

    let context = Context::new()?;
    let worker = context.create_worker()?;
    reactor.register_worker(worker.clone());

    // Load server address
    let addr_bytes = std::fs::read(SHARED_FILE)?;
    let worker_addr = pluvio_ucx::WorkerAddressInner::from(addr_bytes.as_slice());
    println!("Client: Connecting to server...");

    let endpoint = worker.connect_addr(&worker_addr)?;
    println!("Client: Connected");

    let conn = Connection::new(worker, endpoint);
    let client = RpcClient::new(conn);

    // Example 1: Send a read request
    println!("\n=== Example 1: Read Request ===");
    let read_req = ReadRequest {
        header: ReadRequestHeader {
            offset: 0,
            len: 4096,
        },
    };

    println!("Client: Sending read request (offset=0, len=4096)");
    match read_req.call(&client).await {
        Ok(response) => {
            println!(
                "Client: Read response - bytes_read={}, status={}",
                response.bytes_read, response.status
            );
        }
        Err(e) => {
            eprintln!("Client: Read request failed: {:?}", e);
        }
    }

    // Example 2: Send a write request with data
    println!("\n=== Example 2: Write Request ===");
    let data = vec![0x42u8; 1024]; // 1KB of data
    let write_req = WriteRequest {
        header: WriteRequestHeader {
            offset: 4096,
            len: data.len() as u64,
        },
        data: &data,
    };

    println!("Client: Sending write request (offset=4096, len={})", data.len());
    match write_req.call(&client).await {
        Ok(response) => {
            println!(
                "Client: Write response - bytes_written={}, status={}",
                response.bytes_written, response.status
            );
        }
        Err(e) => {
            eprintln!("Client: Write request failed: {:?}", e);
        }
    }

    // Example 3: Fire-and-forget write
    println!("\n=== Example 3: Fire-and-forget Write ===");
    let data2 = vec![0xFFu8; 512];
    let write_req2 = WriteRequest {
        header: WriteRequestHeader {
            offset: 8192,
            len: data2.len() as u64,
        },
        data: &data2,
    };

    println!("Client: Sending fire-and-forget write");
    match write_req2.call_no_reply(&client).await {
        Ok(_) => {
            println!("Client: Fire-and-forget write sent successfully");
        }
        Err(e) => {
            eprintln!("Client: Fire-and-forget write failed: {:?}", e);
        }
    }

    println!("\nClient: All requests completed");
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("UCX RPC Example");

    let mode = std::env::args().nth(1);
    
    let runtime = Runtime::new(1024);
    let reactor = UCXReactor::current();
    runtime.register_reactor("ucx_reactor", reactor.clone());

    match mode.as_deref() {
        Some("server") => {
            runtime.clone().run(async move {
                server(runtime.clone(), reactor).await.unwrap();
            });
        }
        Some("client") => {
            runtime.clone().run(async move {
                client(runtime.clone(), reactor).await.unwrap();
            });
        }
        _ => {
            eprintln!("Usage: {} [server|client]", std::env::args().next().unwrap());
            std::process::exit(1);
        }
    }

    Ok(())
}
