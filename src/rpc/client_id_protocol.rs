//! Client ID registration protocol
//!
//! This protocol is used for clients to register their unique ID with the server
//! immediately after establishing a socket connection.

use std::rc::Rc;

use pluvio_ucx::async_ucx::ucp::AmMsg;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::rpc::handlers::RpcHandlerContext;
use crate::rpc::{AmRpc, AmRpcCallType, RpcClient, RpcError, RpcId, ServerResponse};

/// RPC ID for client registration
pub const RPC_CLIENT_REGISTER: RpcId = 1;

/// Maximum length for client ID string
pub const MAX_CLIENT_ID_LEN: usize = 64;

/// Client registration request header
///
/// Sent by client immediately after socket connection to register its unique ID.
#[repr(C)]
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable)]
pub struct ClientRegisterRequest {
    /// Length of client_id string (excluding null terminator)
    pub client_id_len: u32,
    /// Client ID as fixed-size byte array
    pub client_id_bytes: [u8; MAX_CLIENT_ID_LEN],
}

impl ClientRegisterRequest {
    /// Create a new client registration request
    pub fn new(client_id: &str) -> Result<Self, RpcError> {
        let client_id_bytes_slice = client_id.as_bytes();
        let client_id_len = client_id_bytes_slice.len();

        if client_id_len > MAX_CLIENT_ID_LEN {
            return Err(RpcError::InvalidHeader);
        }

        let mut client_id_bytes = [0u8; MAX_CLIENT_ID_LEN];
        client_id_bytes[..client_id_len].copy_from_slice(client_id_bytes_slice);

        Ok(Self {
            client_id_len: client_id_len as u32,
            client_id_bytes,
        })
    }

    /// Extract client ID as string
    pub fn client_id(&self) -> Result<String, RpcError> {
        let len = self.client_id_len as usize;
        if len > MAX_CLIENT_ID_LEN {
            return Err(RpcError::InvalidHeader);
        }

        String::from_utf8(self.client_id_bytes[..len].to_vec())
            .map_err(|_| RpcError::InvalidHeader)
    }
}

/// Client registration response header
#[repr(C)]
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable)]
pub struct ClientRegisterResponse {
    /// Status: 0 = success, non-zero = error
    pub status: i32,
    /// Whether a previous client was evicted (1 = yes, 0 = no)
    pub evicted: u32,
}

impl ClientRegisterResponse {
    /// Create a success response
    pub fn success(evicted: bool) -> Self {
        Self {
            status: 0,
            evicted: if evicted { 1 } else { 0 },
        }
    }

    /// Create an error response
    pub fn error(status: i32) -> Self {
        Self { status, evicted: 0 }
    }
}

/// Client registration RPC
pub struct ClientRegisterRpc {
    request: ClientRegisterRequest,
}

impl ClientRegisterRpc {
    /// Create a new client registration RPC
    pub fn new(client_id: &str) -> Result<Self, RpcError> {
        Ok(Self {
            request: ClientRegisterRequest::new(client_id)?,
        })
    }
}

impl AmRpc for ClientRegisterRpc {
    type RequestHeader = ClientRegisterRequest;
    type ResponseHeader = ClientRegisterResponse;

    fn rpc_id() -> RpcId {
        RPC_CLIENT_REGISTER
    }

    fn call_type(&self) -> AmRpcCallType {
        AmRpcCallType::None
    }

    fn request_header(&self) -> &Self::RequestHeader {
        &self.request
    }

    async fn call(&self, client: &RpcClient) -> Result<Self::ResponseHeader, RpcError> {
        client.execute(self).await
    }

    async fn call_no_reply(&self, _client: &RpcClient) -> Result<(), RpcError> {
        // Registration always requires a reply
        Err(RpcError::HandlerError(
            "Client registration requires reply".to_string(),
        ))
    }

    async fn server_handler(
        ctx: Rc<RpcHandlerContext>,
        am_msg: AmMsg,
    ) -> Result<(ServerResponse<Self::ResponseHeader>, AmMsg), (RpcError, AmMsg)> {
        // Parse request
        let header_bytes = am_msg.header();
        let request = match ClientRegisterRequest::read_from_bytes(header_bytes) {
            Ok(req) => req,
            Err(_) => {
                tracing::error!("Failed to parse ClientRegisterRequest");
                return Err((RpcError::InvalidHeader, am_msg));
            }
        };

        let client_id = match request.client_id() {
            Ok(id) => id,
            Err(e) => {
                tracing::error!("Invalid client ID in request: {:?}", e);
                return Err((e, am_msg));
            }
        };

        tracing::info!("Received client registration request: {}", client_id);

        // Register client in ClientRegistry if available
        // Note: In socket-based connections, UCX automatically manages bidirectional endpoints.
        // The reply_ep in AmMsg is the persistent endpoint established during accept().
        // ClientRegistry could be used for server-initiated messages in the future,
        // but for request-response RPC, the automatic reply_ep mechanism is sufficient.
        let evicted = if let Some(ref _registry) = ctx.client_registry {
            // Future: If we need server-initiated messages, extract endpoint from AmMsg
            // and register it: registry.register(client_id, endpoint);
            tracing::debug!("Client registration received (using automatic UCX endpoint management)");
            false
        } else {
            tracing::debug!("ClientRegistry not available in context");
            false
        };

        let response = ClientRegisterResponse::success(evicted);
        let server_response = ServerResponse::new(response);

        Ok((server_response, am_msg))
    }

    fn error_response(error: &RpcError) -> Self::ResponseHeader {
        let status = match error {
            RpcError::InvalidHeader => -1,
            RpcError::TransportError(_) => -2,
            RpcError::HandlerError(_) => -3,
            RpcError::ConnectionError(_) => -4,
            RpcError::Timeout => -5,
        };
        ClientRegisterResponse::error(status)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_register_request_creation() {
        let req = ClientRegisterRequest::new("client_123").unwrap();
        assert_eq!(req.client_id_len, 10);
        assert_eq!(req.client_id().unwrap(), "client_123");
    }

    #[test]
    fn test_client_register_request_too_long() {
        let long_id = "a".repeat(MAX_CLIENT_ID_LEN + 1);
        let result = ClientRegisterRequest::new(&long_id);
        assert!(result.is_err());
    }

    #[test]
    fn test_client_register_response() {
        let resp = ClientRegisterResponse::success(false);
        assert_eq!(resp.status, 0);
        assert_eq!(resp.evicted, 0);

        let resp = ClientRegisterResponse::success(true);
        assert_eq!(resp.status, 0);
        assert_eq!(resp.evicted, 1);

        let resp = ClientRegisterResponse::error(-1);
        assert_eq!(resp.status, -1);
        assert_eq!(resp.evicted, 0);
    }
}
