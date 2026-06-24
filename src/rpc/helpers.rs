//! Common helper functions and utilities for RPC operations
//!
//! This module provides shared functionality to reduce code duplication across
//! RPC request/response handlers, including:
//! - Safe IoSlice management with extended lifetimes
//! - Standard header parsing
//! - Worker address and data receiving patterns

use std::cell::UnsafeCell;
use std::io::IoSlice;

use pluvio_ucx::async_ucx::ucp::AmMsg;
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

/// 4-byte correlation_id prefix on every AM header (request + reply).
///
/// Wire layout:
///
/// ```text
/// ┌──────────────┬─────────────────────────┐
/// │ corr_id u32  │  AmRpc::*Header bytes   │
/// │  (LE)        │  (sizeof::<H>())        │
/// └──────────────┴─────────────────────────┘
/// ```
///
/// 2026-06-02: introduced so the UCX-daemon relay (`ucx_relay.rs`) can
/// demultiplex concurrent replies on the same `am_stream(rpc_id+100)`
/// SegQueue via slab-key. Embedded UCX (`transport_ucx.rs`) keeps the
/// same wire format for uniformity but always sends corr_id=0 because
/// each embedded call has its own short-lived reply stream.
pub const CORR_ID_PREFIX_LEN: usize = 4;

/// Pack `[corr_id u32 LE][header bytes]` into one buffer suitable for
/// `am_send_vectorized` / `AmMsg::reply`. All senders (`RpcClient`,
/// `helpers.rs` reply, `UcxRelayDaemon` forward) use this so the wire
/// layout stays consistent.
pub fn pack_header_with_corr_id<H>(corr_id: u32, header: &H) -> Vec<u8>
where
    H: zerocopy::IntoBytes + zerocopy::Immutable,
{
    let header_bytes = zerocopy::IntoBytes::as_bytes(header);
    let mut out = Vec::with_capacity(CORR_ID_PREFIX_LEN + header_bytes.len());
    out.extend_from_slice(&corr_id.to_le_bytes());
    out.extend_from_slice(header_bytes);
    out
}

/// Parse the 4-byte corr_id prefix from raw AM header bytes without
/// decoding the typed header. Used by `ucx_relay.rs` reply-side demux
/// where only the corr_id is needed to find the right pending entry.
pub fn read_corr_id_prefix(header_bytes: &[u8]) -> Option<u32> {
    if header_bytes.len() < CORR_ID_PREFIX_LEN {
        return None;
    }
    Some(u32::from_le_bytes([
        header_bytes[0],
        header_bytes[1],
        header_bytes[2],
        header_bytes[3],
    ]))
}

/// Parse RPC request header from AmMsg.
///
/// Returns the correlation_id (first 4 bytes LE) and the decoded header.
/// Server-side handlers must capture corr_id and pass it back to
/// [`send_rpc_response_via_reply`] so daemon-side clients can demux
/// concurrent replies on a shared stream.
///
/// # Returns
/// - `Ok((corr_id, H))` on success
/// - `Err(RpcError::InvalidHeader)` if buffer too short or zerocopy decode fails
pub fn parse_header<H>(am_msg: &AmMsg) -> Result<(u32, H), RpcError>
where
    H: FromBytes + Clone,
{
    let bytes = am_msg.header();
    if bytes.len() < CORR_ID_PREFIX_LEN + std::mem::size_of::<H>() {
        return Err(RpcError::InvalidHeader);
    }
    let corr_id = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    let header = bytes
        .get(CORR_ID_PREFIX_LEN..CORR_ID_PREFIX_LEN + std::mem::size_of::<H>())
        .and_then(|b| H::read_from_prefix(b).ok().map(|(h, _)| h.clone()))
        .ok_or(RpcError::InvalidHeader)?;
    Ok((corr_id, header))
}

/// Convenience wrapper around `parse_header` that returns the AmMsg
/// alongside the error.
pub fn parse_header_with_msg<H>(am_msg: &AmMsg) -> Result<(u32, H), (RpcError, &AmMsg)>
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
#[async_backtrace::framed]
pub async fn send_rpc_response_via_reply<H>(
    reply_stream_id: u16,
    corr_id: u32,
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

    // Echo the corr_id from the request as the prefix on the reply
    // header so the daemon-side client can demux which pending entry
    // this reply belongs to (see `ucx_relay.rs::complete_with_status`).
    let packed_header = pack_header_with_corr_id(corr_id, response_header);
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
                &packed_header,
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
