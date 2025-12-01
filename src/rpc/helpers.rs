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
use pluvio_ucx::Worker;
use zerocopy::FromBytes;

use super::RpcError;
use crate::rpc::handlers::RpcHandlerContext;
use crate::rpc::RpcRequestPrefix;

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

/// Parse RPC request with prefix containing client's worker address
///
/// All RPC requests now include an `RpcRequestPrefix` at the beginning of the header
/// containing the client's worker address for the server to reply to.
///
/// # Returns
///
/// - `Ok((RpcRequestPrefix, H))` if both prefix and header were successfully parsed
/// - `Err(RpcError)` if parsing fails
pub fn parse_request_with_prefix<H>(am_msg: &AmMsg) -> Result<(RpcRequestPrefix, H), RpcError>
where
    H: FromBytes + Clone + zerocopy::KnownLayout,
{
    let header_bytes = am_msg.header();
    let prefix_size = std::mem::size_of::<RpcRequestPrefix>();
    let header_size = std::mem::size_of::<H>();

    if header_bytes.len() < prefix_size {
        tracing::error!(
            "Header too short for prefix: got {} bytes, need {} bytes",
            header_bytes.len(),
            prefix_size
        );
        return Err(RpcError::InvalidHeader);
    }

    let prefix: RpcRequestPrefix = zerocopy::FromBytes::read_from_bytes(&header_bytes[..prefix_size])
        .map_err(|_| {
            tracing::error!("Failed to parse RpcRequestPrefix");
            RpcError::InvalidHeader
        })?;

    if header_bytes.len() < prefix_size + header_size {
        tracing::error!(
            "Header too short for request header: got {} bytes, need {} + {} = {} bytes",
            header_bytes.len(),
            prefix_size,
            header_size,
            prefix_size + header_size
        );
        return Err(RpcError::InvalidHeader);
    }

    let header: H = zerocopy::FromBytes::read_from_prefix(&header_bytes[prefix_size..])
        .map(|(h, _rest): (H, &[u8])| h.clone())
        .map_err(|_| {
            tracing::error!("Failed to parse request header");
            RpcError::InvalidHeader
        })?;

    Ok((prefix, header))
}

/// Send RPC response using client's worker address
///
/// This is the new response mechanism that replaces the unstable `reply_ep`.
/// The server creates a new endpoint to the client using the worker address
/// from the request prefix, sends the response, and then closes the endpoint.
///
/// # Arguments
///
/// * `worker` - The server's worker for creating endpoint
/// * `reply_stream_id` - The reply stream ID for this RPC type
/// * `client_worker_address` - The client's worker address bytes
/// * `response_header` - The response header to send
/// * `response_data` - Optional response data payload
///
/// # Returns
///
/// - `Ok(())` on success
/// - `Err(RpcError)` if endpoint creation or sending fails
#[async_backtrace::framed]
pub async fn send_rpc_response<H>(
    worker: &Rc<Worker>,
    reply_stream_id: u16,
    client_worker_address: &[u8],
    response_header: &H,
    response_data: Option<&[u8]>,
) -> Result<(), RpcError>
where
    H: zerocopy::IntoBytes + zerocopy::KnownLayout + zerocopy::Immutable,
{
    // Create endpoint to client using worker address
    let addr = pluvio_ucx::WorkerAddressInner::from(client_worker_address);
    let endpoint = worker.connect_addr(&addr).map_err(|e| {
        tracing::error!("Failed to connect to client worker address: {:?}", e);
        RpcError::TransportError(format!("Failed to connect to client: {:?}", e))
    })?;

    // Send response
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

    endpoint
        .am_send(reply_stream_id as u32, header_bytes, data, false, proto)
        .await
        .map_err(|e| {
            tracing::error!("Failed to send RPC response: {:?}", e);
            RpcError::TransportError(format!("Failed to send response: {:?}", e))
        })?;

    // Close endpoint after sending
    // Use force=true to avoid waiting for graceful close
    let _ = endpoint.close(true).await;

    Ok(())
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
#[async_backtrace::framed]
pub async fn receive_path(
    ctx: &RpcHandlerContext,
    am_msg: &mut AmMsg,
    path_len: u32,
) -> Result<String, RpcError> {
    tracing::debug!(
        "receive_path: path_len={}, contains_data={}, data_len={}, data_type={:?}",
        path_len,
        am_msg.contains_data(),
        am_msg.data_len(),
        am_msg.data_type()
    );

    // Try to get data directly first (for Eager/Data messages)
    if let Some(data) = am_msg.get_data() {
        tracing::debug!("receive_path: got data directly, len={}", data.len());
        if data.len() != path_len as usize {
            tracing::warn!(
                "receive_path: data length mismatch: expected={}, got={}",
                path_len,
                data.len()
            );
        }
        // Copy data for string conversion
        let path_bytes = data.to_vec();
        return String::from_utf8(path_bytes)
            .map_err(|e| RpcError::TransportError(format!("Invalid UTF-8 in path: {:?}", e)));
    }

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

    tracing::debug!(
        "receive_path: using recv_data_vectored, buffer_len={}, requested_len={}",
        buffer.as_mut_slice(len).len(),
        len
    );

    am_msg
        .recv_data_vectored(&[std::io::IoSliceMut::new(buffer.as_mut_slice(len))])
        .await
        .map_err(|e| {
            tracing::error!(
                "recv_data_vectored failed: path_len={}, buffer_len={}, data_len={}, error={:?}",
                path_len,
                len,
                am_msg.data_len(),
                e
            );
            RpcError::TransportError(format!("Failed to receive path: {:?}", e))
        })?;

    buffer
        .as_str(len)
        .map(|s| s.to_owned())
        .map_err(|e| RpcError::TransportError(format!("Invalid UTF-8 in path: {:?}", e)))
}

/// Send RPC response using worker address (wrapper for server handlers)
///
/// This is a convenience wrapper around `send_rpc_response` that returns
/// the AmMsg alongside the result, which is the pattern used in server handlers.
///
/// # Arguments
///
/// * `worker` - The server's worker for creating endpoint
/// * `reply_stream_id` - The reply stream ID for this RPC type
/// * `client_worker_address` - The client's WorkerAddress bytes from RpcRequestPrefix
/// * `response_header` - The response header to send
/// * `response_data` - Optional response data payload
/// * `am_msg` - The active message (returned for ownership)
///
/// # Returns
///
/// - `Ok((ServerResponse, AmMsg))` on success
/// - `Err((RpcError, AmMsg))` if sending fails
#[async_backtrace::framed]
pub async fn send_rpc_response_with_msg<H>(
    worker: &Rc<Worker>,
    reply_stream_id: u16,
    client_worker_address: &[u8],
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
    match send_rpc_response(
        worker,
        reply_stream_id,
        client_worker_address,
        response_header,
        response_data,
    )
    .await
    {
        Ok(()) => Ok((
            crate::rpc::ServerResponse::new(response_header.clone()),
            am_msg,
        )),
        Err(e) => {
            tracing::error!("Failed to send response: {:?}", e);
            Err((e, am_msg))
        }
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
