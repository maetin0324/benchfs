# BenchFS C Client Docker Test Infrastructure

This directory contains Docker-based test infrastructure for BenchFS using the pure C client implementation with IOR benchmarks.

## Overview

The C client test setup provides:
- **Pure C Client Library**: `libbenchfs_c_api.so` - C implementation of BenchFS client using UCX Active Messages
- **IOR Integration**: Industry-standard parallel I/O benchmark using the C client
- **Docker Environment**: Complete test cluster with 4 BenchFS servers + IOR client
- **Automated Testing**: Scripts for running various benchmark configurations

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Docker Network (172.20.0.0/16)           │
│                                                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │ Server 1     │  │ Server 2     │  │ Server 3     │     │
│  │ 172.20.0.11  │  │ 172.20.0.12  │  │ 172.20.0.13  │     │
│  │ benchfsd_mpi │  │ benchfsd_mpi │  │ benchfsd_mpi │ ... │
│  │ (AM RPC)     │  │ (AM RPC)     │  │ (AM RPC)     │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
│         │                 │                  │              │
│         └─────────────────┴──────────────────┘              │
│                           │                                 │
│                  AM RPC Protocol (UCX)                      │
│                           │                                 │
│         ┌─────────────────┴──────────────────┐              │
│         │                                    │              │
│  ┌──────────────┐                  ┌──────────────┐        │
│  │ Controller   │                  │ IOR Client   │        │
│  │ 172.20.0.10  │                  │ 172.20.0.20  │        │
│  │ (MPI coord)  │                  │ benchfs-c-   │        │
│  │              │                  │ client image │        │
│  └──────────────┘                  └──────────────┘        │
│                                            │                │
│                                     libbenchfs.so           │
│                                     (C client impl)         │
└─────────────────────────────────────────────────────────────┘
```

### Key Differences from Rust FFI Version

| Component | Rust FFI Version | C Client Version |
|-----------|------------------|------------------|
| Client Library | `libbenchfs.so` (Rust FFI) | `libbenchfs_c_api.so` → `libbenchfs.so` (Pure C) |
| RPC Backend | Stream RPC + RDMA (RMA) | Active Messages (AM) only |
| Dependencies | Rust runtime | UCX only (libucp, libucs) |
| AIORI Adapter | Same (`aiori-BENCHFS.c`) | Same (`aiori-BENCHFS.c`) |
| API Interface | Same (`benchfs_c_api.h`) | Same (`benchfs_c_api.h`) |

## Files

### Docker Configuration
- `Dockerfile.c-client` - Multi-stage build for C client + IOR
- `docker-compose.c-client.yml` - Complete test cluster configuration

### Test Scripts
- `scripts/test-ior-c-client.sh` - IOR benchmark runner (runs inside client container)
- `scripts/run-c-client-test.sh` - Complete test orchestration (runs on host)

### Source Code
- `../../c_client/` - Pure C client implementation
  - `benchfs_c_api.c` - Implementation of `benchfs_c_api.h` using C client
  - `benchfs_client.c` - UCX AM-based RPC client
  - `benchfs_ops.c` - Metadata and data operations
  - `Makefile` - Build configuration

## Building

### 1. Build BenchFS Server Image

```bash
cd /path/to/benchfs
make docker-build-optimized
```

This creates the `benchfs:latest` image with `benchfsd_mpi` (AM RPC only).

### 2. Build C Client Image

```bash
cd /path/to/benchfs
docker build -f tests/docker/Dockerfile.c-client -t benchfs-c-client:latest .
```

This creates the `benchfs-c-client:latest` image with:
- C client library (`libbenchfs_c_api.so` installed as `libbenchfs.so`)
- IOR with BENCHFS AIORI adapter
- MPI runtime for distributed benchmarks

## Running Tests

### Quick Start with Make (Recommended)

Run tests using the Makefile:

```bash
cd tests/docker

# Quick test
make test-c-client-quick

# Basic test (default)
make test-c-client

# Large tests
make test-c-client-4gib
make test-c-client-8gib
```

Available Make targets:
- `make test-c-client-quick` - Quick sanity check (4MB per rank)
- `make test-c-client` - Basic write/read test (16MB per rank, default)
- `make test-c-client-4gib` - 4GiB per rank shared single file
- `make test-c-client-8gib` - 8GiB per rank shared single file

### Using Scripts Directly

Run the complete test (automated setup + IOR test + cleanup):

```bash
cd tests/docker
./scripts/run-c-client-test.sh basic 4
```

Available tests:
- `basic` - Basic write/read test (16MB per rank, default)
- `write` - Write-only test (32MB per rank)
- `read` - Read-only test (32MB per rank)
- `quick` - Quick sanity check (4MB per rank)
- `4gib-ssf` - 4GiB per rank shared single file
- `8gib-ssf` - 8GiB per rank shared single file

### Manual Testing

For manual control and debugging:

```bash
cd tests/docker

# 1. Start cluster
docker-compose -f docker-compose.c-client.yml up -d

# 2. Clean registry
docker exec benchfs_controller rm -rf /shared/registry/*

# 3. Start servers (from controller)
docker exec benchfs_controller bash -c "
    mpirun --hostfile /tmp/hostfile -np 4 \
        --mca btl tcp,self \
        -x UCX_TLS=tcp,self \
        -x RUST_LOG=info \
        benchfsd_mpi /shared/registry /configs/benchfs_test.toml
"

# 4. Run IOR test (from client container)
docker exec benchfs_ior_client /scripts/test-ior-c-client.sh basic 4

# 5. Cleanup
docker-compose -f docker-compose.c-client.yml down -v
```

## Test Results

Results are saved to `tests/docker/results/c-client-YYYYMMDD-HHMMSS/`:
- `ior_output.txt` - IOR benchmark output with performance metrics
- `server_stdout.log` - Server stdout logs
- `server_stderr.log` - Server stderr logs (Rust logs)

### Understanding IOR Output

Key metrics to look for:

```
Summary of all tests:
Operation   Max(MiB)   Min(MiB)  Mean(MiB)     StdDev   Max(OPs)   Min(OPs)  Mean(OPs)
---------   --------   --------  ---------     ------   --------   --------  ---------
write          450.12     440.23     445.67      4.12     450.12     440.23     445.67
read           890.45     870.34     880.23      8.34     890.45     870.34     880.23
```

- **write** - Write bandwidth (MiB/s)
- **read** - Read bandwidth (MiB/s)
- Higher is better

## Debugging

### Check Container Logs

```bash
# All containers
docker-compose -f docker-compose.c-client.yml logs

# Specific container
docker-compose -f docker-compose.c-client.yml logs ior_client
docker-compose -f docker-compose.c-client.yml logs server1

# Follow logs in real-time
docker-compose -f docker-compose.c-client.yml logs -f ior_client
```

### Verify C Client Library

Inside the IOR client container:

```bash
docker exec -it benchfs_ior_client bash

# Check library linkage
ldd /usr/local/bin/ior | grep benchfs
# Should show: libbenchfs.so => /usr/local/lib/libbenchfs.so

# Verify it's the C implementation
file /usr/local/lib/libbenchfs.so
# Should show: ELF 64-bit LSO shared object

# Check library symbols
nm -D /usr/local/lib/libbenchfs.so | grep benchfs_init
```

### Check Server Registration

```bash
# From controller
docker exec benchfs_controller ls -la /shared/registry/

# Should see files like:
# node_0.am_hostname
# node_1.am_hostname
# node_2.am_hostname
# node_3.am_hostname
```

### Interactive Debugging

Start a shell in the IOR client container:

```bash
docker exec -it benchfs_ior_client bash

# Manually run IOR with debug output
mpirun -np 4 \
    -x LD_LIBRARY_PATH=/usr/local/lib \
    -x UCX_LOG_LEVEL=info \
    ior -a BENCHFS \
        --benchfs.registry /shared/registry \
        --benchfs.datadir /shared/data \
        -w -r -t 1m -b 4m -s 2 \
        -o /debugtest \
        -v
```

### Server Debug Mode

To run servers with trace-level logging:

```bash
docker exec benchfs_controller bash -c "
    mpirun --hostfile /tmp/hostfile -np 4 \
        -x RUST_LOG=trace \
        -x UCX_LOG_LEVEL=debug \
        benchfsd_mpi /shared/registry /configs/benchfs_test.toml
"
```

## Performance Tuning

### UCX Configuration

The C client and servers use UCX for communication. Key environment variables:

- `UCX_TLS=tcp,self` - Use TCP transport (reliable in Docker)
- `UCX_LOG_LEVEL=warn` - UCX logging level (debug/info/warn/error)

### MPI Configuration

IOR uses MPI for coordination:

- `--mca btl tcp,self` - Use TCP for MPI communication
- `--mca btl_tcp_if_include eth0` - Force TCP over eth0 (Docker network)

### IOR Parameters

Adjust for different workloads:

- `-t SIZE` - Transfer size (I/O request size)
- `-b SIZE` - Block size (contiguous I/O per task)
- `-s COUNT` - Segment count (number of blocks per task)
- `-F` - File-per-process (vs shared file)

Example:
```bash
# Large sequential I/O: 8MB transfers, 256MB blocks
ior -t 8m -b 256m -s 32

# Small random-like I/O: 4KB transfers, 1MB blocks
ior -t 4k -b 1m -s 1024
```

## Troubleshooting

### IOR Fails with "Failed to connect to BenchFS server"

**Cause**: Servers not running or not registered

**Fix**:
```bash
# Check server logs
docker exec benchfs_controller cat /shared/results/server_stderr.log

# Verify registration
docker exec benchfs_controller ls -la /shared/registry/
```

### "Library not found" Error

**Cause**: IOR not linked with libbenchfs.so

**Fix**: Rebuild C client image:
```bash
docker build -f tests/docker/Dockerfile.c-client -t benchfs-c-client:latest .
```

### Timeout Errors

**Cause**: Network issues or server overload

**Fix**: Increase timeout:
```bash
docker exec benchfs_controller bash -c "export BENCHFS_OPERATION_TIMEOUT=60; ..."
```

### MPI Errors

**Cause**: Hostname resolution or network configuration

**Fix**: Check hostfile and network:
```bash
docker exec benchfs_controller cat /tmp/hostfile
docker exec benchfs_controller ping -c 3 server1
```

## Comparison with Rust FFI Version

To compare C client vs Rust FFI performance:

1. Run C client test:
   ```bash
   ./scripts/run-c-client-test.sh 4gib-ssf 4
   ```

2. Run Rust FFI test (regular docker-compose):
   ```bash
   # Use standard test infrastructure
   make test-ior
   ```

3. Compare bandwidth metrics from IOR output

Expected differences:
- **C client**: Lower memory usage, simpler dependency chain
- **Rust FFI**: May have different performance characteristics due to FFI overhead
- **Protocol**: C client uses AM-only, Rust FFI may use Stream+RMA (depends on version)

## Advanced Usage

### Custom IOR Scripts

Create custom test scripts in `scripts/`:

```bash
#!/bin/bash
# Custom IOR test
mpirun -np 4 ior \
    -a BENCHFS \
    --benchfs.registry /shared/registry \
    --benchfs.datadir /shared/data \
    -w -r \
    -t 4m -b 64m -s 64 \
    -o /custom_test \
    -v
```

### Performance Profiling

Enable profiling in server containers:

```bash
# Start with perf available
docker exec server1 perf record -a -g -F 99 -o /tmp/perf.data -- sleep 60 &

# Run test
docker exec benchfs_ior_client /scripts/test-ior-c-client.sh basic 4

# Generate report
docker exec server1 perf report -i /tmp/perf.data --stdio
```

## See Also

- [BenchFS Architecture](../../README.md)
- [C Client README](../../c_client/README.md)
- [IOR Documentation](https://ior.readthedocs.io/)
- [UCX Documentation](https://openucx.readthedocs.io/)
