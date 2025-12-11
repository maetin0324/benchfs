//! Client daemon module for BenchFS.
//!
//! This module implements a client-side daemon that aggregates connections from
//! multiple IOR processes on the same node, reducing the total number of connections
//! to the BenchFS servers.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
//! │IOR Rank 0   │  │IOR Rank 1   │  │IOR Rank N   │
//! └──────┬──────┘  └──────┬──────┘  └──────┬──────┘
//!        │                │                │
//!        └────────────────┼────────────────┘
//!                         │ Shared Memory (Ring Buffers)
//!                         ▼
//! ┌──────────────────────────────────────────────────────┐
//! │                  BenchFS Client Daemon                │
//! │  UCX Worker → UCX Endpoints → BenchFS Servers        │
//! └──────────────────────────────────────────────────────┘
//! ```
//!
//! # Communication
//!
//! - Client processes communicate with the daemon via shared memory ring buffers
//! - Each client gets a dedicated slot with request/response rings and a data buffer
//! - The daemon polls all active slots and forwards requests to BenchFS servers
//! - Responses are written back to the client's response ring
//!
//! # Zero-Copy Data Transfer
//!
//! - Clients write data to their slot's data buffer
//! - The daemon reads the data directly from shared memory for UCX transmission
//! - Only one memory copy occurs (application buffer → shared memory)

pub mod client_stub;
pub mod daemon_server;
pub mod error;
pub mod launcher;
pub mod protocol;
pub mod ring;
pub mod shm;

pub use client_stub::DaemonClientStub;
pub use daemon_server::ClientDaemon;
pub use error::DaemonError;
pub use protocol::{OpType, RequestEntry, ResponseEntry};
pub use ring::{RequestRing, ResponseRing, SlotView};
pub use shm::{SharedMemoryRegion, ShmConfig, ShmError};

/// Default shared memory name prefix
pub const DEFAULT_SHM_NAME_PREFIX: &str = "benchfs_daemon";

/// Generate default shared memory name based on hostname
pub fn default_shm_name() -> String {
    let hostname = gethostname::gethostname()
        .to_string_lossy()
        .into_owned()
        .replace('.', "_");
    format!("{}_{}", DEFAULT_SHM_NAME_PREFIX, hostname)
}
