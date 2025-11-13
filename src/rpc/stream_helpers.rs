//! Helper functions for Stream + RMA based RPC
//!
//! Provides utility functions for:
//! - Stream send/recv operations
//! - Memory registration and rkey management
//! - Message serialization/deserialization
//! - Error handling

use std::mem::MaybeUninit;

use pluvio_ucx::endpoint::Endpoint;

use super::{RpcError, Serializable};

/// Maximum header size in bytes
pub const MAX_HEADER_SIZE: usize = 256;

/// Maximum rkey size in bytes (UCX typical rkey size is ~100 bytes)
pub const MAX_RKEY_SIZE: usize = 256;

/// Send a message via stream
///
/// Helper function to send arbitrary bytes through UCX stream.
pub async fn stream_send(endpoint: &Endpoint, data: &[u8]) -> Result<usize, RpcError> {
    endpoint
        .stream_send(data)
        .await
        .map_err(|e| RpcError::TransportError(format!("Failed to send via stream: {:?}", e)))
}

/// Receive a message via stream
///
/// Helper function to receive arbitrary bytes through UCX stream.
pub async fn stream_recv(
    endpoint: &Endpoint,
    buffer: &mut [MaybeUninit<u8>],
) -> Result<usize, RpcError> {
    endpoint
        .stream_recv(buffer)
        .await
        .map_err(|e| RpcError::TransportError(format!("Failed to recv via stream: {:?}", e)))
}

/// Send a serializable header via stream
pub async fn stream_send_header<H: Serializable>(
    endpoint: &Endpoint,
    header: &H,
) -> Result<usize, RpcError> {
    let header_bytes = zerocopy::IntoBytes::as_bytes(header);
    stream_send(endpoint, header_bytes).await
}

/// Receive a serializable header via stream
pub async fn stream_recv_header<H: Serializable>(endpoint: &Endpoint) -> Result<H, RpcError> {
    let header_size = std::mem::size_of::<H>();
    if header_size > MAX_HEADER_SIZE {
        return Err(RpcError::TransportError(format!(
            "Header size {} exceeds maximum {}",
            header_size, MAX_HEADER_SIZE
        )));
    }

    let mut buffer = vec![MaybeUninit::<u8>::uninit(); header_size];
    let len = stream_recv(endpoint, &mut buffer).await?;

    if len != header_size {
        return Err(RpcError::TransportError(format!(
            "Expected {} bytes, received {}",
            header_size, len
        )));
    }

    // SAFETY: We just received exactly header_size bytes
    let bytes = unsafe { std::slice::from_raw_parts(buffer.as_ptr() as *const u8, header_size) };

    H::read_from_bytes(bytes)
        .map_err(|e| RpcError::TransportError(format!("Failed to deserialize header: {:?}", e)))
}

/// Send a u64 value via stream
pub async fn stream_send_u64(endpoint: &Endpoint, value: u64) -> Result<usize, RpcError> {
    // Use Vec to ensure the buffer lives long enough for UCX zcopy operations
    let buffer = value.to_le_bytes().to_vec();
    stream_send(endpoint, &buffer).await
}

/// Receive a u64 value via stream
pub async fn stream_recv_u64(endpoint: &Endpoint) -> Result<u64, RpcError> {
    let mut buffer = [MaybeUninit::<u8>::uninit(); 8];
    let len = stream_recv(endpoint, &mut buffer).await?;

    if len != 8 {
        return Err(RpcError::TransportError(format!(
            "Expected 8 bytes for u64, received {}",
            len
        )));
    }

    // SAFETY: We just received exactly 8 bytes
    let bytes = unsafe { std::mem::transmute::<[MaybeUninit<u8>; 8], [u8; 8]>(buffer) };

    Ok(u64::from_le_bytes(bytes))
}

/// Send a u32 value via stream
pub async fn stream_send_u32(endpoint: &Endpoint, value: u32) -> Result<usize, RpcError> {
    // Use Vec to ensure the buffer lives long enough for UCX zcopy operations
    let buffer = value.to_le_bytes().to_vec();
    stream_send(endpoint, &buffer).await
}

/// Receive a u32 value via stream
pub async fn stream_recv_u32(endpoint: &Endpoint) -> Result<u32, RpcError> {
    let mut buffer = [MaybeUninit::<u8>::uninit(); 4];
    let len = stream_recv(endpoint, &mut buffer).await?;

    if len != 4 {
        return Err(RpcError::TransportError(format!(
            "Expected 4 bytes for u32, received {}",
            len
        )));
    }

    // SAFETY: We just received exactly 4 bytes
    let bytes = unsafe { std::mem::transmute::<[MaybeUninit<u8>; 4], [u8; 4]>(buffer) };

    Ok(u32::from_le_bytes(bytes))
}

/// Receive exactly `len` bytes from stream.
pub async fn stream_recv_exact(endpoint: &Endpoint, len: usize) -> Result<Vec<u8>, RpcError> {
    let mut output = vec![0u8; len];
    let mut offset = 0usize;

    while offset < len {
        let remaining = len - offset;
        let mut buffer = vec![MaybeUninit::<u8>::uninit(); remaining];
        let received = stream_recv(endpoint, &mut buffer).await?;

        if received == 0 {
            return Err(RpcError::TransportError(
                "Stream closed while receiving data".to_string(),
            ));
        }

        // SAFETY: `received` bytes were initialized by stream_recv.
        let chunk = unsafe { std::slice::from_raw_parts(buffer.as_ptr() as *const u8, received) };
        output[offset..offset + received].copy_from_slice(chunk);
        offset += received;
    }

    Ok(output)
}

/// Send a completion notification via stream
pub async fn stream_send_completion(endpoint: &Endpoint) -> Result<usize, RpcError> {
    // Use Vec to ensure the buffer lives long enough for UCX zcopy operations
    let buffer = b"DONE".to_vec();
    stream_send(endpoint, &buffer).await
}

/// Receive a completion notification via stream
pub async fn stream_recv_completion(endpoint: &Endpoint) -> Result<(), RpcError> {
    let mut buffer = [MaybeUninit::<u8>::uninit(); 4];
    let len = stream_recv(endpoint, &mut buffer).await?;

    if len != 4 {
        return Err(RpcError::TransportError(format!(
            "Expected 4 bytes for completion, received {}",
            len
        )));
    }

    // SAFETY: We just received exactly 4 bytes
    let bytes = unsafe { std::mem::transmute::<[MaybeUninit<u8>; 4], [u8; 4]>(buffer) };

    if &bytes != b"DONE" {
        return Err(RpcError::TransportError(format!(
            "Invalid completion message: {:?}",
            bytes
        )));
    }

    Ok(())
}

/// Send an RPC ID via stream
pub async fn stream_send_rpc_id(endpoint: &Endpoint, rpc_id: u16) -> Result<usize, RpcError> {
    // Use Vec to ensure the buffer lives long enough for UCX zcopy operations
    let buffer = rpc_id.to_le_bytes().to_vec();
    stream_send(endpoint, &buffer).await
}

/// Receive an RPC ID via stream
pub async fn stream_recv_rpc_id(endpoint: &Endpoint) -> Result<u16, RpcError> {
    let mut buffer = [MaybeUninit::<u8>::uninit(); 2];
    let len = stream_recv(endpoint, &mut buffer).await?;

    if len != 2 {
        return Err(RpcError::TransportError(format!(
            "Expected 2 bytes for RPC ID, received {}",
            len
        )));
    }

    // SAFETY: We just received exactly 2 bytes
    let bytes = unsafe { std::mem::transmute::<[MaybeUninit<u8>; 2], [u8; 2]>(buffer) };

    Ok(u16::from_le_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constants() {
        assert!(MAX_HEADER_SIZE >= 256);
        assert!(MAX_RKEY_SIZE >= 100);
    }
}
