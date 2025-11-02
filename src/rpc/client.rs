use std::cell::RefCell;

use crate::rpc::{AmRpc, Connection, RpcError};

/// RPC client for making RPC calls
///
/// The client executes RPCs by taking any type that implements the `RpcCall` trait.
pub struct RpcClient {
    conn: Connection,
    // Store reply stream opaquely since pluvio_ucx may not export AmStream
    #[allow(dead_code)]
    reply_stream_id: RefCell<Option<u16>>,
    /// Client's own WorkerAddress for direct response
    worker_address: Vec<u8>,
}

impl RpcClient {
    pub fn new(conn: Connection) -> Self {
        // Get worker address from the connection
        let worker_address = conn
            .worker()
            .address()
            .map(|addr| addr.as_ref().to_vec())
            .unwrap_or_else(|e| {
                tracing::error!("Failed to get worker address: {:?}", e);
                vec![]
            });

        Self {
            conn,
            reply_stream_id: RefCell::new(None),
            worker_address,
        }
    }

    /// Get the client's WorkerAddress
    pub fn worker_address(&self) -> &[u8] {
        &self.worker_address
    }

    /// Get the underlying connection
    pub fn connection(&self) -> &Connection {
        &self.conn
    }

    /// Initialize the reply stream for receiving RPC responses
    /// This should be called before executing any RPCs that expect replies
    ///
    /// Note: Currently simplified - full implementation would need pluvio_ucx
    /// to export AmStream or provide a different API
    pub fn init_reply_stream(&self, am_id: u16) -> Result<(), RpcError> {
        // Store the AM ID for future use
        *self.reply_stream_id.borrow_mut() = Some(am_id);
        Ok(())
    }

    /// Execute an RPC call using a request that implements RpcCall
    ///
    /// This is the main entry point for executing RPCs. Pass any struct that implements
    /// RpcCall and it will be sent to the server, with the response being returned.
    ///
    /// # Example
    /// ```ignore
    /// let request = ReadRequest { offset: 0, len: 4096 };
    /// let response: ReadResponse = client.execute(&request).await?;
    /// ```
    pub async fn execute<T: AmRpc>(&self, request: &T) -> Result<T::ResponseHeader, RpcError> {
        let rpc_id = T::rpc_id();
        let _span = tracing::trace_span!("rpc_call", rpc_id).entered();

        let reply_stream_id = T::reply_stream_id();
        let header = request.request_header();
        let data = request.request_data();
        let need_reply = request.need_reply();
        let proto = request.proto(); // TODO: Use when pluvio_ucx exports AmProto

        tracing::debug!(
            "RPC call: rpc_id={}, reply_stream_id={}, has_data={}, data_len={}",
            rpc_id,
            reply_stream_id,
            !data.is_empty(),
            data.iter().map(|s| s.len()).sum::<usize>()
        );

        let reply_stream = self.conn.worker.am_stream(reply_stream_id).map_err(|e| {
            RpcError::TransportError(format!(
                "Failed to create reply AM stream: {:?}",
                e.to_string()
            ))
        })?;

        tracing::trace!("Created reply stream: stream_id={}", reply_stream_id);

        if !need_reply {
            // No reply expected
            return Err(RpcError::HandlerError(
                "No reply expected for this RPC".to_string(),
            ));
        }

        // Send the RPC request (proto is set to None for now)
        tracing::debug!(
            "Sending AM request: rpc_id={}, header_size={}, need_reply={}, proto={:?}",
            rpc_id,
            std::mem::size_of_val(zerocopy::IntoBytes::as_bytes(header)),
            need_reply,
            proto
        );

        self.conn
            .endpoint()
            .am_send_vectorized(
                rpc_id as u32,
                zerocopy::IntoBytes::as_bytes(header),
                &data,
                need_reply,
                proto,
            )
            .await
            .map_err(|e| {
                tracing::error!("Failed to send AM request: rpc_id={}, error={:?}", rpc_id, e);
                RpcError::TransportError(format!("Failed to send AM: {:?}", e))
            })?;

        tracing::debug!(
            "Waiting for reply on stream_id={}, rpc_id={}",
            reply_stream_id,
            rpc_id
        );

        // Wait for reply
        let mut msg = reply_stream
            .wait_msg()
            .await
            .ok_or_else(|| {
                tracing::error!(
                    "RPC timeout: no reply received (rpc_id={}, reply_stream_id={})",
                    rpc_id,
                    reply_stream_id
                );
                RpcError::Timeout
            })?;

        tracing::debug!("Received reply message: rpc_id={}, reply_stream_id={}", rpc_id, reply_stream_id);

        // Deserialize the response header
        let response_header = msg
            .header()
            .get(..std::mem::size_of::<T::ResponseHeader>())
            .and_then(|bytes| zerocopy::FromBytes::read_from_bytes(bytes).ok())
            .ok_or_else(|| RpcError::InvalidHeader)?;

        // Receive response data if present
        let response_buffer = request.response_buffer();
        if !response_buffer.is_empty() && msg.contains_data() {
            msg.recv_data_vectored(response_buffer).await.map_err(|e| {
                RpcError::TransportError(format!("Failed to recv response data: {:?}", e))
            })?;
        }

        // Note: The caller should check the status field in the response header
        // to determine if the RPC succeeded or failed on the server side
        Ok(response_header)
    }

    /// Execute an RPC without expecting a reply
    /// Useful for fire-and-forget operations
    pub async fn execute_no_reply<T: AmRpc>(&self, request: &T) -> Result<(), RpcError> {
        let rpc_id = T::rpc_id();
        let header = request.request_header();
        let data = request.request_data();
        let proto = request.proto(); // TODO: Use when pluvio_ucx exports AmProto

        self.conn
            .endpoint()
            .am_send_vectorized(
                rpc_id as u32,
                zerocopy::IntoBytes::as_bytes(header),
                &data,
                false, // need_reply = false
                proto, // proto - TODO: pass actual proto when available
            )
            .await
            .map_err(|e| RpcError::TransportError(format!("Failed to send AM: {:?}", e)))?;

        Ok(())
    }
}
