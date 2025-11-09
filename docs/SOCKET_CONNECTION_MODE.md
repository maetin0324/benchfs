# Socket Connection Mode Implementation

## Overview

BenchFS supports two connection modes for RPC communication:

1. **Socket Connection Mode** (default, recommended) - New persistent endpoint model
2. **WorkerAddress Mode** (legacy) - Original reply endpoint model

This document describes the Socket Connection Mode implementation, which provides better scalability and performance for high-concurrency scenarios.

## Architecture

### Socket Connection Mode

```
┌─────────────┐                    ┌─────────────┐
│   Client    │                    │   Server    │
│             │                    │             │
│  - Read     │  ──────────────>   │  - Listener │
│    server_  │    Connect via     │  - Accept   │
│    list.txt │    socket addr     │  - Client   │
│             │                    │    Registry │
│  - Connect  │  <──────────────   │    (LRU)    │
│    via      │   Persistent EP    │             │
│    socket   │                    │             │
└─────────────┘                    └─────────────┘
```

**Key Components:**
- **UCX Listener**: Server creates a listener on `0.0.0.0:0` (dynamic port)
- **server_list.txt**: Service discovery file containing `node_id` and `socket_addr`
- **ClientRegistry**: LRU cache for persistent client endpoints (max 1024 clients)
- **Persistent Endpoints**: Reused across multiple RPC requests

**Benefits:**
- Better scalability with many concurrent clients
- LRU cache automatically manages endpoint lifecycle
- Persistent connections reduce endpoint creation overhead
- Supports up to 1024 concurrent clients

### WorkerAddress Mode (Legacy)

```
┌─────────────┐                    ┌─────────────┐
│   Client    │                    │   Server    │
│             │                    │             │
│  - Read     │  ──────────────>   │  - Worker   │
│    *.addr   │    Connect via     │    Address  │
│    files    │    WorkerAddress   │             │
│             │                    │             │
│  - Connect  │  <──────────────   │  - reply_ep │
│    and get  │   Reply endpoint   │    per RPC  │
│    reply_ep │                    │             │
└─────────────┘                    └─────────────┘
```

**Characteristics:**
- Each RPC request creates a new reply endpoint
- Service discovery via `*.addr` files
- May have scalability issues with many concurrent clients
- Avoids `epoll_wait` overhead in `ucp_worker_progress`

## Implementation Details

### Configuration

Socket connection mode is controlled by the `use_socket_connection` flag in `benchfs.toml`:

```toml
[network]
# Connection mode: Socket vs WorkerAddress
# - true (default): Use socket-based connections with persistent endpoints
# - false: Use WorkerAddress-based connections (legacy)
use_socket_connection = true
```

### Server-Side Implementation

**File**: `src/bin/benchfsd_mpi.rs`

```rust
// Read configuration flag
let use_socket_connection = config.network.use_socket_connection;

if use_socket_connection {
    // Socket connection mode
    tracing::info!("Socket connection mode enabled, creating listener...");

    // Create UCX Listener
    let bind_addr: SocketAddr = "0.0.0.0:0".parse().unwrap();
    let listener = worker_clone.create_listener(bind_addr)?;

    // Get actual listening address
    let listen_addr = listener.socket_addr()?;

    // Resolve hostname to IPv4 (important for Docker)
    let actual_addr = resolve_ipv4_address(&listen_addr);

    // Write to server_list.txt
    append_to_server_list(&server_list_path, &node_id, actual_addr)?;

    tracing::info!("Node {} written to server_list.txt: {}", node_id, actual_addr);
} else {
    // WorkerAddress mode
    tracing::info!("WorkerAddress connection mode (default) - no listener needed");
}
```

**Key Points:**
- Listener is created on dynamic port (`0.0.0.0:0`)
- Hostname is resolved to IPv4 address (critical for Docker environments)
- `server_list.txt` is written with `node_id` and `socket_addr`

### Client-Side Implementation

**File**: `src/ffi/init.rs`

```rust
// Automatic mode detection based on server_list.txt existence
let server_list_path = Path::new(registry_dir).join("server_list.txt");
let use_socket_connection = server_list_path.exists();

if use_socket_connection {
    // Socket connection mode
    tracing::info!("Socket connection mode detected");

    // Read server_list.txt
    let servers = read_server_list_with_retry(&server_list_path, 30, Duration::from_secs(1))?;

    // Select target server using consistent hashing
    let target_server = select_server_by_hash(&servers, client_id);

    // Connect via socket
    connection_pool.connect_via_socket(
        &target_server.node_id,
        target_server.socket_addr
    ).await?;
} else {
    // WorkerAddress mode
    tracing::info!("WorkerAddress connection mode");

    // Discover nodes from *.addr files
    let nodes = discover_data_nodes(registry_dir)?;

    // Connect via WorkerAddress
    connection_pool.wait_and_connect(&target_node, 30).await?;
}
```

**Key Points:**
- Mode is automatically detected by checking `server_list.txt` existence
- No manual configuration required on client side
- Supports both modes transparently

### ClientRegistry (LRU Cache)

**File**: `src/rpc/client_registry.rs`

```rust
pub struct ClientRegistry {
    max_size: usize,
    clients: RefCell<HashMap<String, ClientInfo>>,
    lru_queue: RefCell<VecDeque<String>>,  // front=MRU, back=LRU
}

impl ClientRegistry {
    pub fn new(max_size: usize) -> Self {
        // Default: 1024 clients
    }

    pub fn register(&self, client_id: String, endpoint: Rc<Endpoint>) -> Option<String> {
        // Returns evicted client_id if cache is full
        // Automatically manages LRU eviction
    }

    pub fn get(&self, client_id: &str) -> Option<Rc<Endpoint>> {
        // Updates last_used timestamp
        // Moves to front of LRU queue
    }
}
```

**LRU Strategy:**
- Front of VecDeque: Most Recently Used (MRU)
- Back of VecDeque: Least Recently Used (LRU)
- When full, evicts LRU client and closes endpoint
- Supports up to 1024 concurrent clients by default

### server_list.txt Format

**File**: `src/rpc/server_list.rs`

```
# BenchFS Server List
# Format: node_id socket_addr
node_0 192.168.1.100:37421
node_1 192.168.1.101:42315
node_2 192.168.1.102:38912
node_3 192.168.1.103:40127
```

**Functions:**
```rust
// Append server info (atomic operation)
pub fn append_to_server_list(path: &Path, node_id: &str, socket_addr: SocketAddr)

// Read server list with retry logic
pub fn read_server_list_with_retry(
    path: &Path,
    max_retries: usize,
    delay: Duration
) -> Result<Vec<ServerInfo>>

// Find specific server by node_id
pub fn find_server<'a>(servers: &'a [ServerInfo], node_id: &str)
    -> Result<&'a ServerInfo>
```

## Migration Guide

### From WorkerAddress to Socket Mode

1. **Update Configuration**:
   ```toml
   [network]
   use_socket_connection = true
   ```

2. **Rebuild and Deploy**:
   ```bash
   cargo build --release --features mpi-support --bin benchfsd_mpi
   ```

3. **No Client Changes Required**:
   - Clients automatically detect mode from `server_list.txt`
   - Both modes work transparently

### From Socket to WorkerAddress Mode

1. **Update Configuration**:
   ```toml
   [network]
   use_socket_connection = false
   ```

2. **Rebuild and Deploy**: Same as above

## Performance Comparison

| Metric | Socket Mode | WorkerAddress Mode |
|--------|-------------|-------------------|
| Max Concurrent Clients | 1024 | Unlimited (limited by system) |
| Endpoint Reuse | Yes (persistent) | No (per-request) |
| LRU Cache | Yes | No |
| `epoll_wait` Overhead | Yes (Listener) | No |
| Scalability | Better for high-concurrency | Better for low-concurrency |
| Setup Overhead | Higher (Listener) | Lower |

## Troubleshooting

### Server Not Accepting Connections

**Symptoms**: Clients fail to connect with socket timeout

**Solutions**:
1. Check `server_list.txt` exists and contains correct addresses
2. Verify firewall allows connection to listener port
3. Check server logs for "Socket listener created" message
4. Ensure hostname resolves to correct IP (especially in Docker)

### ClientRegistry Full

**Symptoms**: Warning messages about evicting clients

**Solutions**:
1. Increase `max_size` in `ClientRegistry::new()`
2. Reduce number of concurrent clients
3. Implement connection pooling on client side

### IPv6 vs IPv4 Issues

**Symptoms**: Connection fails in Docker environments

**Solutions**:
1. Server automatically resolves hostname to IPv4
2. Check `/etc/hosts` for correct hostname mapping
3. Verify Docker network configuration

## Testing

### Unit Tests

```bash
# Test server_list.txt functions
cargo test --lib -- server_list

# Test ClientRegistry LRU cache
cargo test --lib -- client_registry
```

### Integration Tests

```bash
# Test with Socket connection mode
cd tests/docker
make test-ior  # Uses benchfs_test.toml (Socket mode)

# Test with WorkerAddress mode
USE_CONFIG=benchfs_workeraddr.toml make test-ior
```

## Future Work

1. **Accept Loop Implementation**: Currently, Listener is created but accept loop is not yet implemented
2. **Dynamic Client Limits**: Make `ClientRegistry` size configurable via TOML
3. **Connection Pool Metrics**: Add monitoring for endpoint reuse and evictions
4. **Benchmarking**: Compare performance of both modes under various workloads

## References

- **Main Implementation**: `src/bin/benchfsd_mpi.rs:426-502`
- **Client Detection**: `src/ffi/init.rs:502-624`
- **ClientRegistry**: `src/rpc/client_registry.rs`
- **server_list.txt**: `src/rpc/server_list.rs`
- **Configuration**: `src/config.rs:148-172`
