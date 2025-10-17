fn main() {}

// //! Example demonstrating the UCX ActiveMessage-based RPC system

// use benchfs::rpc::{Connection,client::RpcClient, server::RpcServer, RpcId};
// use pluvio_runtime::executor::Runtime;
// use pluvio_ucx::{UCXReactor, Context};
// use std::rc::Rc;

// // Example RPC IDs
// const RPC_READ: RpcId = 1;
// const RPC_WRITE: RpcId = 2;

// #[allow(dead_code)]
// struct ReadRequest {
//     header: RpcHeader,
// }

// #[allow(dead_code)]
// struct WriteRequest {
//     header: RpcHeader,
//     data: Vec<u8>,
// }

// async fn server(runtime: Rc<Runtime>, reactor: Rc<UCXReactor>) -> Result<(), Box<dyn std::error::Error>> {
//     println!("Server: Starting...");
    
//     let context = Context::new()?;
//     let worker = context.create_worker()?;
//     reactor.register_worker(worker.clone());

//     // Create listener
//     let mut listener = worker.create_listener("0.0.0.0:9999".parse().unwrap())?;
//     println!("Server: Listening on port 9999");

//     // Save worker address for client
//     let worker_addr = worker.address()?;
//     std::fs::write("worker_addr.bin", worker_addr.as_ref())?;
//     println!("Server: Worker address saved");

//     // Accept connection
//     let conn_req = listener.next().await;
//     let endpoint = worker.accept(conn_req).await?;
//     println!("Server: Connection accepted");

//     let _conn = Connection::new(Rc::clone(&worker), endpoint);
//     let server = RpcServer::new(Rc::clone(&worker));

//     // Register handlers
//     // Note: Handler registration currently simplified due to async closure limitations
//     println!("Server: Handlers registered (simplified)");

//     // Start listening for RPCs
//     runtime.spawn_polling(async move {
//         // if let Err(e) = server.listen(10).await {
//         //     eprintln!("Server error: {:?}", e);
//         // }
//     });

//     println!("Server: Running");
//     Ok(())
// }

// async fn client(_runtime: Rc<Runtime>, reactor: Rc<UCXReactor>) -> Result<(), Box<dyn std::error::Error>> {
//     println!("Client: Starting...");
    
//     // Wait for server to be ready
//     std::thread::sleep(std::time::Duration::from_secs(1));

//     let context = Context::new()?;
//     let worker = context.create_worker()?;
//     reactor.register_worker(worker.clone());

//     // Load server address
//     let addr_bytes = std::fs::read("worker_addr.bin")?;
//     let worker_addr = pluvio_ucx::WorkerAddressInner::from(addr_bytes.as_slice());
//     println!("Client: Connecting to server...");

//     let endpoint = worker.connect_addr(&worker_addr)?;
//     println!("Client: Connected");

//     let conn = Connection::new(worker, endpoint);
//     let _client = RpcClient::new(conn);

//     // Example: Send a read request
//     let _header = RpcHeader::new(0, 4096, 0);
//     println!("Client: Sending read request (offset=0, len=4096)");

//     // In a real implementation, you would call client.call() here
//     println!("Client: Request sent (implementation pending)");

//     Ok(())
// }

// fn main() -> Result<(), Box<dyn std::error::Error>> {
//     println!("UCX RPC Example");

//     let mode = std::env::args().nth(1);
    
//     let runtime = Runtime::new(1024);
//     let reactor = UCXReactor::current();
//     runtime.register_reactor("ucx_reactor", reactor.clone());

//     match mode.as_deref() {
//         Some("server") => {
//             runtime.clone().run(async move {
//                 server(runtime.clone(), reactor).await.unwrap();
//             });
//         }
//         Some("client") => {
//             runtime.clone().run(async move {
//                 client(runtime.clone(), reactor).await.unwrap();
//             });
//         }
//         _ => {
//             eprintln!("Usage: {} [server|client]", std::env::args().next().unwrap());
//             std::process::exit(1);
//         }
//     }

//     Ok(())
// }
