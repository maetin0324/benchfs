# BenchFS MPI Usage Guide

## Overview

`benchfsd_mpi` is an MPI-enabled version of BenchFS server daemon that allows horizontal deployment across multiple nodes in an HPC cluster.

## Prerequisites

1. **MPI Implementation**: OpenMPI, MPICH, or Intel MPI must be installed
2. **Shared Filesystem**: A shared directory accessible from all nodes for service discovery
3. **UCX Library**: Required for RDMA communication

## Building

Build the MPI-enabled binary with:

```bash
cargo build --release --features mpi-support --bin benchfsd_mpi
```

## Usage

### Basic Command

```bash
mpirun -n <num_nodes> benchfsd_mpi <registry_dir> [config_file]
```

### Parameters

- `<num_nodes>`: Number of MPI processes (servers) to launch
- `<registry_dir>`: Shared directory for service discovery (required)
  - Must be accessible from all nodes
  - Used for UCX worker address exchange
- `[config_file]`: Optional configuration file (default: benchfs.toml)

### Example: Running on 4 Nodes

```bash
# Create registry directory on shared filesystem
mkdir -p /shared/benchfs_registry

# Launch 4 server instances
mpirun -n 4 benchfsd_mpi /shared/benchfs_registry
```

### Example: With Configuration File

```bash
mpirun -n 8 benchfsd_mpi /shared/benchfs_registry my_config.toml
```

### Example: With Hostfile

```bash
# Create hostfile
cat > hostfile << EOF
node1 slots=2
node2 slots=2
node3 slots=2
node4 slots=2
EOF

# Run with hostfile
mpirun -n 8 --hostfile hostfile benchfsd_mpi /shared/benchfs_registry
```

## Node Configuration

Each MPI rank automatically configures itself:

- **Node ID**: `node_<rank>` (e.g., `node_0`, `node_1`, ...)
- **Data Directory**: `<base_data_dir>/rank_<rank>`
- **Rank 0**: Acts as the primary metadata server
- **Other Ranks**: Act as storage servers and secondary metadata servers

## Architecture

### Service Discovery

1. Each node registers its UCX worker address in `<registry_dir>/node_<rank>.addr`
2. Nodes wait for all peers to register before starting
3. Timeout: 60 seconds

### Data Storage

- Each rank stores its data in a separate directory
- Local storage: `<data_dir>/rank_<rank>/chunks/`
- Metadata is distributed using consistent hashing

### Network Communication

- Uses UCX for high-performance RDMA communication
- Connection pool manages inter-node connections
- Automatic reconnection on failures

## Monitoring and Logs

### Log Levels

- **Rank 0**: Uses configured log level (default: info)
- **Other ranks**: Uses "warn" level to reduce output

### Log Location

Logs are written to stdout/stderr. Redirect as needed:

```bash
mpirun -n 4 benchfsd_mpi /shared/registry > benchfs.log 2>&1
```

### Per-Rank Logs

To separate logs by rank:

```bash
mpirun -n 4 benchfsd_mpi /shared/registry \
  > benchfs_rank_\$OMPI_COMM_WORLD_RANK.log 2>&1
```

## Troubleshooting

### Issue: "Registry directory does not exist"

**Solution**: Create the directory before running:
```bash
mkdir -p /shared/benchfs_registry
```

### Issue: "Failed to register node address"

**Causes**:
- Registry directory not accessible from all nodes
- Insufficient permissions
- Network issues

**Solution**: Verify shared filesystem is mounted on all nodes

### Issue: Timeout waiting for nodes

**Causes**:
- Some nodes failed to start
- Network connectivity issues
- UCX configuration problems

**Solution**:
- Check MPI process status: `mpirun -n <N> hostname`
- Verify UCX_TLS environment variable
- Check firewall settings

### Issue: "Failed to create UCX context"

**Solution**: Ensure UCX is properly installed and configured:
```bash
export UCX_TLS=rc,ud,sm,self
```

## Performance Tuning

### UCX Transport Selection

```bash
# Use RDMA only
export UCX_TLS=rc

# Use mixed transports
export UCX_TLS=rc,ud,sm,self
```

### MPI Process Binding

```bash
# Bind to cores
mpirun -n 4 --bind-to core benchfsd_mpi /shared/registry

# Bind to sockets
mpirun -n 4 --bind-to socket benchfsd_mpi /shared/registry
```

### I/O Performance

Adjust in configuration file:
```toml
[storage]
queue_size = 2048
buffer_size = 1048576  # 1 MiB
submit_depth = 64
```

## Integration with IOR

### Example: 4 Servers + 4 Clients

Terminal 1 (Servers):
```bash
mpirun -n 4 benchfsd_mpi /shared/registry
```

Terminal 2 (Clients):
```bash
# Wait for servers to start, then run IOR
mpirun -n 4 ior -a BENCHFS -t 1m -b 100m -F
```

## Shutdown

Press Ctrl+C on the mpirun process. All ranks will receive SIGINT and shutdown gracefully.

## Directory Structure

```
<data_dir>/
├── rank_0/
│   └── chunks/
│       ├── inode_1_chunk_0
│       └── ...
├── rank_1/
│   └── chunks/
│       └── ...
└── ...

<registry_dir>/
├── node_0.addr
├── node_1.addr
└── ...
```

## Known Limitations

1. All nodes must use the same configuration file
2. Registry directory must be on a shared filesystem
3. Dynamic node addition/removal not supported (requires restart)
4. MPI must be initialized before any UCX operations

## Environment Variables

### Required
- None (MPI and UCX use defaults)

### Optional
- `UCX_TLS`: UCX transport selection
- `UCX_NET_DEVICES`: Network device selection
- `RUST_LOG`: Override log level (e.g., `RUST_LOG=debug`)

## See Also

- [BenchFS Configuration Guide](CONFIG.md)
- [Performance Tuning Guide](PERFORMANCE.md)
- [IOR Integration Guide](IOR_INTEGRATION.md)
