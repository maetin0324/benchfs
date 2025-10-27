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
}

impl RpcClient {
    pub fn new(conn: Connection) -> Self {
        Self {
            conn,
            reply_stream_id: RefCell::new(None),
        }
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
        let reply_stream_id = T::reply_stream_id();
        let header = request.request_header();
        let data = request.request_data();
        let need_reply = request.need_reply();
        let proto = request.proto(); // TODO: Use when pluvio_ucx exports AmProto

        tracing::debug!(
            "RpcClient::execute: rpc_id={}, reply_stream_id={}, has_data={}",
            rpc_id,
            reply_stream_id,
            !data.is_empty()
        );

        let reply_stream = self.conn.worker.am_stream(reply_stream_id).map_err(|e| {
            RpcError::TransportError(format!(
                "Failed to create reply AM stream: {:?}",
                e.to_string()
            ))
        })?;

        tracing::debug!("RpcClient::execute: Created reply stream");

        if !need_reply {
            // No reply expected
            return Err(RpcError::HandlerError(
                "No reply expected for this RPC".to_string(),
            ));
        }

        // Send the RPC request (proto is set to None for now)
        tracing::debug!("RpcClient::execute: Sending AM request...");
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
            .map_err(|e| RpcError::TransportError(format!("Failed to send AM: {:?}", e)))?;

        tracing::debug!("RpcClient::execute: AM sent successfully, waiting for reply...");

        // Wait for reply
        let mut msg = reply_stream
            .wait_msg()
            .await
            .ok_or_else(|| RpcError::Timeout)?;

        tracing::debug!("RpcClient::execute: Received reply message");

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
