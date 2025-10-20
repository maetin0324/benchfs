/// Example demonstrating how to set up and run an RPC server with handlers
///
/// This example shows:
/// 1. Creating a UCX worker and RPC server
/// 2. Setting up handler context with metadata manager and chunk store
/// 3. Registering all RPC handlers
/// 4. Running the server
///
/// Note: This is a minimal example. A production server would include:
/// - Configuration file loading
/// - Graceful shutdown handling
/// - Logging setup
/// - Error recovery

use std::rc::Rc;

use benchfs::metadata::MetadataManager;
use benchfs::storage::InMemoryChunkStore;
use benchfs::rpc::handlers::RpcHandlerContext;
use benchfs::rpc::server::RpcServer;

fn main() {
    println!("RPC Server Example");
    println!("==================");
    println!();
    println!("This example demonstrates how to set up an RPC server with handlers.");
    println!("In a real application, you would:");
    println!("  1. Initialize UCX context and worker");
    println!("  2. Create handler context with metadata manager and chunk store");
    println!("  3. Create RPC server with worker and handler context");
    println!("  4. Register all handlers using register_all_handlers()");
    println!("  5. Run the event loop");
    println!();
    println!("Example code structure:");
    println!();
    println!("```rust");
    println!("// 1. Initialize UCX");
    println!("let ctx = pluvio_ucx::Context::new()?;");
    println!("let worker = Rc::new(ctx.create_worker()?);");
    println!();
    println!("// 2. Create handler context");
    println!("let metadata_manager = Rc::new(MetadataManager::new(\"node1\".to_string()));");
    println!("let chunk_store = Rc::new(InMemoryChunkStore::new());");
    println!("let handler_ctx = Rc::new(RpcHandlerContext::new(");
    println!("    metadata_manager,");
    println!("    chunk_store,");
    println!("));");
    println!();
    println!("// 3. Create RPC server");
    println!("let server = RpcServer::new(worker.clone(), handler_ctx);");
    println!();
    println!("// 4. Register all handlers");
    println!("let runtime = Rc::new(Runtime::new());");
    println!("server.register_all_handlers(runtime.clone()).await?;");
    println!();
    println!("// 5. Run the event loop");
    println!("runtime.run();");
    println!("```");
    println!();
    println!("The following RPC handlers will be registered:");
    println!("  - ReadChunk (RPC ID 10): Read chunk data from storage");
    println!("  - WriteChunk (RPC ID 11): Write chunk data to storage");
    println!("  - MetadataLookup (RPC ID 20): Look up file/directory metadata");
    println!("  - MetadataCreateFile (RPC ID 21): Create new file metadata");
    println!("  - MetadataCreateDir (RPC ID 22): Create new directory metadata");
    println!("  - MetadataDelete (RPC ID 23): Delete file/directory metadata");
}
