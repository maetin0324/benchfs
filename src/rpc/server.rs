use std::rc::Rc;

use pluvio_runtime::executor::Runtime;
use pluvio_ucx::{async_ucx::ucp::WorkerAddress, Worker};

use crate::rpc::{AmRpc, RpcError, Serializable};

// /// Type alias for RPC handler function
// /// Handler receives: RPC header, optional data buffer
// /// Handler returns: Response data to send back to client
// pub type RpcHandlerFn = Box<
//     dyn Fn(
//         super::RpcHeader,
//         Option<Vec<u8>>,
//     ) -> std::pin::Pin<
//         Box<dyn std::future::Future<Output = Result<Vec<u8>, super::RpcError>>>,
//     >,
// >;

/// RPC server that receives and dispatches ActiveMessages
pub struct RpcServer {
    worker: Rc<Worker>,
}

impl RpcServer {
    pub fn new(worker: Rc<Worker>) -> Self {
        Self { worker }
    }

    pub fn get_address(&self) -> Result<WorkerAddress, RpcError> {
        self.worker.address().map_err(|e| {
            RpcError::TransportError(format!("Failed to get worker address: {:?}", e))
        })
    }


    /// Start listening for RPC requests on the given AM stream ID
    ///
    /// Note: This implementation assumes clients send their requests and wait for replies
    /// on a separate reply stream. The server processes requests and sends responses
    /// using the registered connection endpoints.
    pub async fn listen<Rpc, ReqH, ResH>(
        &self,
        runtime: Rc<Runtime>,
    ) -> Result<(), RpcError>
    where
        ResH: Serializable + 'static,
        ReqH: Serializable + 'static,
        Rpc: AmRpc<RequestHeader = ReqH, ResponseHeader = ResH> + 'static,
    {
        let stream = self.worker.am_stream(Rpc::rpc_id()).map_err(|e| {
            RpcError::TransportError(format!("Failed to create AM stream: {:?}", e))
        })?;

        println!("RpcServer: Listening on AM stream ID {}", Rpc::rpc_id());

        loop {
            let msg = stream.wait_msg().await;
            if msg.is_none() {
                println!("RpcServer: Stream closed");
                break;
            }

            let msg = msg
                .ok_or_else(|| RpcError::TransportError("Failed to receive message".to_string()))?;

            // Handle request data
            runtime.spawn(Rpc::server_handler(msg));


        }

        Ok(())
    }
}
