//! Common helper functions and utilities for RPC operations
//!
//! This module provides shared functionality to reduce code duplication across
//! RPC request/response handlers, including:
//! - Safe IoSlice management with extended lifetimes
//! - Standard header parsing
//! - Worker address and data receiving patterns

use std::cell::UnsafeCell;
use std::io::IoSlice;
use std::rc::Rc;

use pluvio_ucx::async_ucx::ucp::AmMsg;
use pluvio_ucx::endpoint::Endpoint;
use zerocopy::FromBytes;

use super::RpcError;
use crate::rpc::handlers::RpcHandlerContext;

/// Helper for creating IoSlices with extended lifetimes for RPC requests
///
/// This struct safely manages the lifetime extension of IoSlices by ensuring
/// that the underlying buffers outlive the IoSlices.
///
/// # Safety
///
/// This is safe in the context of BenchFS RPC operations because:
/// 1. The buffers live as long as the RpcIoSliceHelper instance
/// 2. The IoSlices are only accessed through the `get()` method
/// 3. The RPC client only uses them during the RPC call lifetime
/// 4. All RPC operations run in single-threaded Pluvio runtime context
pub struct RpcIoSliceHelper<const N: usize> {
    #[allow(dead_code)]
    buffers: Vec<Vec<u8>>,
    slices: UnsafeCell<[IoSlice<'static>; N]>,
}

// SAFETY: Only used in single-threaded context (Pluvio runtime)
unsafe impl<const N: usize> Send for RpcIoSliceHelper<N> {}

impl<const N: usize> RpcIoSliceHelper<N> {
    /// Create IoSlices from buffers
    ///
    /// # Panics
    ///
    /// Panics if the number of buffers doesn't match N
    pub fn new(buffers: Vec<Vec<u8>>) -> Self {
        assert_eq!(
            buffers.len(),
            N,
            "Buffer count must match N (expected {}, got {})",
            N,
            buffers.len()
        );

        // SAFETY: We're creating 'static IoSlices by transmuting the lifetime.
        // This is safe because:
        // 1. The buffers live as long as RpcIoSliceHelper
        // 2. The IoSlices are only accessed through get()
        // 3. The RPC client will only use them during the RPC call
        let slices = unsafe {
            let mut slice_array: [IoSlice<'static>; N] = std::mem::zeroed();
            for (i, buf) in buffers.iter().enumerate() {
                let static_slice: &'static [u8] = std::mem::transmute(buf.as_slice());
                slice_array[i] = IoSlice::new(static_slice);
            }
            slice_array
        };

        Self {
            buffers,
            slices: UnsafeCell::new(slices),
        }
    }

    /// Get the IoSlices for use in RPC operations
    pub fn get(&self) -> &[IoSlice<'_>] {
        unsafe { &*self.slices.get() }
    }
}

/// Parse RPC request header from AmMsg
///
/// This is a generic function that works with any header type implementing
/// `FromBytes` and `Clone` from the zerocopy crate.
///
/// # Returns
///
/// - `Ok(H)` if header was successfully parsed
/// - `Err(RpcError::InvalidHeader)` if header is missing or malformed
pub fn parse_header<H>(am_msg: &AmMsg) -> Result<H, RpcError>
where
    H: FromBytes + Clone,
{
    am_msg
        .header()
        .get(..std::mem::size_of::<H>())
        .and_then(|bytes| H::read_from_prefix(bytes).ok().map(|(h, _)| h.clone()))
        .ok_or(RpcError::InvalidHeader)
}

/// Parse request header, returning RpcError with AmMsg on failure
///
/// This is a convenience wrapper around `parse_header` that returns
/// the AmMsg alongside the error, which is the pattern used in server handlers.
pub fn parse_header_with_msg<H>(am_msg: &AmMsg) -> Result<H, (RpcError, &AmMsg)>
where
    H: FromBytes + Clone,
{
    parse_header(am_msg).map_err(|e| (e, am_msg))
}

/// Standard error code mapping for RPC errors
///
/// Maps RpcError variants to integer error codes used in RPC responses.
/// These codes follow a convention where negative values indicate errors.
pub fn rpc_error_to_errno(error: &RpcError) -> i32 {
    match error {
        RpcError::InvalidHeader => -1,
        RpcError::TransportError(_) => -2,
        RpcError::HandlerError(_) => -3,
        RpcError::ConnectionError(_) => -4,
        RpcError::Timeout => -5,
    }
}

/// Receive path data from AmMsg (without worker address)
///
/// This helper receives only the path string from an RPC request,
/// used when the response will be sent via reply_ep instead of worker address.
///
/// # Arguments
///
/// * `am_msg` - The active message to receive data from
/// * `path_len` - The expected length of the path string
///
/// # Returns
///
/// - `Ok(path)` on success
/// - `Err(RpcError)` if data reception fails or path is invalid UTF-8
pub async fn receive_path(
    ctx: &RpcHandlerContext,
    am_msg: &mut AmMsg,
    path_len: u32,
) -> Result<String, RpcError> {
    if !am_msg.contains_data() {
        return Err(RpcError::TransportError(
            "Request contains no data".to_string(),
        ));
    }

    let mut buffer = ctx.acquire_path_buffer();
    let len = path_len as usize;
    if len > buffer.capacity() {
        return Err(RpcError::TransportError(format!(
            "Path length {} exceeds maximum {}",
            len,
            buffer.capacity()
        )));
    }

    am_msg
        .recv_data_vectored(&[std::io::IoSliceMut::new(buffer.as_mut_slice(len))])
        .await
        .map_err(|e| RpcError::TransportError(format!("Failed to receive path: {:?}", e)))?;

    buffer
        .as_str(len)
        .map(|s| s.to_owned())
        .map_err(|e| RpcError::TransportError(format!("Invalid UTF-8 in path: {:?}", e)))
}

/// Send RPC response using AmMsg reply_ep mechanism
///
/// This helper uses UCX's native reply endpoint to send responses,
/// eliminating the need to send worker addresses in requests.
///
/// # Arguments
///
/// * `reply_stream_id` - The reply stream ID for this RPC type
/// * `response_header` - The response header to send
/// * `response_data` - Optional response data payload
/// * `am_msg` - The active message containing reply_ep
///
/// # Returns
///
/// - `Ok((ServerResponse, AmMsg))` on success
/// - `Err((RpcError, AmMsg))` if sending fails
///
/// # Safety
///
/// Uses unsafe AmMsg::reply() method, but is safe because:
/// - reply_ep is managed by UCX and valid for AmMsg lifetime
/// - We check need_reply() before attempting to reply
pub async fn send_rpc_response_via_reply<H>(
    reply_stream_id: u16,
    response_header: &H,
    response_data: Option<&[u8]>,
    am_msg: pluvio_ucx::async_ucx::ucp::AmMsg,
) -> Result<
    (
        crate::rpc::ServerResponse<H>,
        pluvio_ucx::async_ucx::ucp::AmMsg,
    ),
    (RpcError, pluvio_ucx::async_ucx::ucp::AmMsg),
>
where
    H: zerocopy::IntoBytes + zerocopy::KnownLayout + zerocopy::Immutable + Clone,
{
    if !am_msg.need_reply() {
        tracing::error!("Message does not support reply");
        return Err((
            RpcError::HandlerError("Message does not support reply".to_string()),
            am_msg,
        ));
    }

    let header_bytes = zerocopy::IntoBytes::as_bytes(response_header);
    let data = response_data.unwrap_or(&[]);

    // Determine protocol based on data size
    let proto = if data.is_empty() {
        None
    } else if crate::rpc::should_use_rdma(data.len() as u64) {
        Some(pluvio_ucx::async_ucx::ucp::AmProto::Rndv)
    } else {
        None
    };

    let result = unsafe {
        am_msg
            .reply(
                reply_stream_id as u32,
                header_bytes,
                data,
                false, // need_reply
                proto,
            )
            .await
    };

    if let Err(e) = result {
        tracing::error!("Failed to send reply: {:?}", e);
        return Err((
            RpcError::TransportError(format!("Failed to send reply: {:?}", e)),
            am_msg,
        ));
    }

    Ok((
        crate::rpc::ServerResponse::new(response_header.clone()),
        am_msg,
    ))
}

/// Send RPC response via persistent client endpoint (Socket mode)
///
/// This helper sends responses through pre-established persistent endpoints
/// obtained from ClientRegistry, instead of using AmMsg's reply_ep.
///
/// # Arguments
///
/// * `endpoint` - The client's persistent endpoint from ClientRegistry
/// * `reply_stream_id` - The reply stream ID for this RPC type
/// * `response_header` - The response header to send
/// * `response_data` - Optional response data payload as IoSlice array
///
/// # Returns
///
/// - `Ok(())` on success
/// - `Err(RpcError)` if sending fails
///
/// # Example
///
/// ```ignore
/// let endpoint = client_registry.get(client_id).ok_or(...)?;
/// send_rpc_response_via_endpoint(
///     &endpoint,
///     ReadChunkRequest::reply_stream_id(),
///     &response_header,
///     &[IoSlice::new(&data)],
/// ).await?;
/// ```
pub async fn send_rpc_response_via_endpoint<H>(
    endpoint: &Rc<Endpoint>,
    reply_stream_id: u16,
    response_header: &H,
    response_data: &[IoSlice<'_>],
) -> Result<(), RpcError>
where
    H: zerocopy::IntoBytes + zerocopy::KnownLayout + zerocopy::Immutable,
{
    let header_bytes = zerocopy::IntoBytes::as_bytes(response_header);

    // Calculate total data size
    let total_data_len: usize = response_data.iter().map(|slice| slice.len()).sum();

    // Determine protocol based on data size
    let proto = if total_data_len == 0 {
        None
    } else if crate::rpc::should_use_rdma(total_data_len as u64) {
        Some(pluvio_ucx::async_ucx::ucp::AmProto::Rndv)
    } else {
        None
    };

    // Send AM message via endpoint
    endpoint
        .am_send_vectorized(
            reply_stream_id as u32,
            header_bytes,
            response_data,
            false, // need_reply
            proto,
        )
        .await
        .map_err(|e| {
            RpcError::TransportError(format!("Failed to send via endpoint: {:?}", e))
        })?;

    Ok(())
}

/// Send RPC response with automatic mode detection (Socket vs WorkerAddress)
///
/// This helper automatically detects the connection mode and sends responses accordingly:
/// - Socket mode: Uses persistent endpoint from ClientRegistry
/// - WorkerAddress mode: Uses reply_ep mechanism
///
/// # Arguments
///
/// * `ctx` - RPC handler context containing optional ClientRegistry
/// * `client_id` - Client identifier from request header
/// * `reply_stream_id` - The reply stream ID for this RPC type
/// * `response_header` - The response header to send
/// * `response_data` - Optional response data payload
/// * `am_msg` - The active message
pub async fn send_rpc_response<H>(
    ctx: &std::rc::Rc<crate::rpc::handlers::RpcHandlerContext>,
    client_id: u32,
    reply_stream_id: u16,
    response_header: &H,
    response_data: Option<&[u8]>,
    am_msg: pluvio_ucx::async_ucx::ucp::AmMsg,
) -> Result<
    (
        crate::rpc::ServerResponse<H>,
        pluvio_ucx::async_ucx::ucp::AmMsg,
    ),
    (RpcError, pluvio_ucx::async_ucx::ucp::AmMsg),
>
where
    H: zerocopy::IntoBytes + zerocopy::KnownLayout + zerocopy::Immutable + Clone,
{
    if let Some(client_registry) = &ctx.client_registry {
        // Socket mode: Use persistent endpoint from ClientRegistry
        let endpoint = match client_registry.get(client_id) {
            Some(ep) => ep,
            None => {
                return Err((
                    RpcError::HandlerError(format!("Unknown client_id: {}", client_id)),
                    am_msg,
                ));
            }
        };

        let data_slices = if let Some(data) = response_data {
            vec![std::io::IoSlice::new(data)]
        } else {
            vec![]
        };

        match send_rpc_response_via_endpoint(&endpoint, reply_stream_id, response_header, &data_slices)
            .await
        {
            Ok(()) => Ok((crate::rpc::ServerResponse::new(response_header.clone()), am_msg)),
            Err(e) => Err((e, am_msg)),
        }
    } else {
        // WorkerAddress mode: Use reply method
        send_rpc_response_via_reply(reply_stream_id, response_header, response_data, am_msg).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rpc_ioslice_helper_creation() {
        let buf1 = vec![1, 2, 3, 4];
        let buf2 = vec![5, 6, 7, 8];
        let buffers = vec![buf1.clone(), buf2.clone()];

        let helper = RpcIoSliceHelper::<2>::new(buffers);
        let slices = helper.get();

        assert_eq!(slices.len(), 2);
    }

    #[test]
    #[should_panic(expected = "Buffer count must match N")]
    fn test_rpc_ioslice_helper_wrong_count() {
        let buf1 = vec![1, 2, 3, 4];
        let buffers = vec![buf1];

        // Should panic because we're trying to create a helper for 2 slices
        // but only providing 1 buffer
        let _helper = RpcIoSliceHelper::<2>::new(buffers);
    }

    #[test]
    fn test_rpc_error_to_errno() {
        assert_eq!(rpc_error_to_errno(&RpcError::InvalidHeader), -1);
        assert_eq!(
            rpc_error_to_errno(&RpcError::TransportError("test".to_string())),
            -2
        );
        assert_eq!(
            rpc_error_to_errno(&RpcError::HandlerError("test".to_string())),
            -3
        );
        assert_eq!(
            rpc_error_to_errno(&RpcError::ConnectionError("test".to_string())),
            -4
        );
        assert_eq!(rpc_error_to_errno(&RpcError::Timeout), -5);
    }
}
