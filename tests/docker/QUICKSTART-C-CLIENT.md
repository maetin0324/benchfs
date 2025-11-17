# BenchFS C Client - Quick Start Guide

Quick reference for running BenchFS IOR tests with the pure C client implementation.

## Prerequisites

- Docker and docker-compose installed
- At least 32GB RAM available for Docker
- BenchFS source code

## One-Command Test

### Using Make (Recommended)

From the repository root:

```bash
cd tests/docker
make test-c-client
```

This will:
1. Build required Docker images (if needed)
2. Start 4 BenchFS servers with AM RPC
3. Run IOR benchmark with C client
4. Display results
5. Clean up

### Using Script Directly

```bash
cd tests/docker
./scripts/run-c-client-test.sh basic 4
```

## Available Tests

### Using Make (Recommended)

```bash
# Quick sanity check (4MB per rank)
make test-c-client-quick

# Basic test (16MB per rank)
make test-c-client

# 4GiB shared single file test
make test-c-client-4gib

# 8GiB shared single file test
make test-c-client-8gib
```

### Using Scripts Directly

```bash
# Quick sanity check (4MB per rank)
./scripts/run-c-client-test.sh quick 4

# Basic test (16MB per rank)
./scripts/run-c-client-test.sh basic 4

# Write-only test (32MB per rank)
./scripts/run-c-client-test.sh write 4

# Read-only test (32MB per rank)
./scripts/run-c-client-test.sh read 4

# 4GiB shared single file test
./scripts/run-c-client-test.sh 4gib-ssf 4

# 8GiB shared single file test
./scripts/run-c-client-test.sh 8gib-ssf 4
```

## Expected Output

```
==========================================
BenchFS C Client Test Runner
==========================================
Test: basic
Nodes: 4

Working directory: /path/to/benchfs/tests/docker

✓ Docker images ready

Starting BenchFS cluster...
✓ All containers running

Preparing test environment...
Starting BenchFS servers (AM RPC only)...
✓ All servers registered: 4/4

Running IOR test with C client...
✓ IOR is linked with libbenchfs.so (C client)

Test: Basic IOR write/read test with C client
Configuration:
  - Transfer size: 1MB
  - Block size: 4MB
  - Segments per rank: 4
  - Total per rank: 16MB
  - Total aggregate: 64MB

==========================================
IOR Results:
==========================================
[... benchmark results ...]

Key Performance Metrics:
write        445.67 MiB/s
read         880.23 MiB/s

==========================================
✓ Test Result: PASS
==========================================

Results saved to: ./results/c-client-20251114-120000
✓ C Client Test PASSED
==========================================
```

## Results Location

Test results are saved to: `tests/docker/results/c-client-TIMESTAMP/`

Files:
- `ior_output.txt` - IOR benchmark results
- `server_stdout.log` - Server output
- `server_stderr.log` - Server logs (RUST_LOG)

## Troubleshooting

### Build Failed

```bash
# Clean and rebuild
docker rmi benchfs-c-client:latest
cd /path/to/benchfs
docker build -f tests/docker/Dockerfile.c-client -t benchfs-c-client:latest .
```

### Test Timeout

```bash
# Check server logs
docker logs benchfs_controller
docker logs benchfs_server1

# Verify containers are running
docker ps | grep benchfs
```

### Clean Start

```bash
# Stop and remove all containers
docker-compose -f docker-compose.c-client.yml down -v

# Remove volumes
docker volume prune -f

# Re-run test
./scripts/run-c-client-test.sh basic 4
```

## Manual Testing

For debugging or custom tests:

```bash
# 1. Start cluster
docker-compose -f docker-compose.c-client.yml up -d

# 2. Check status
docker-compose -f docker-compose.c-client.yml ps

# 3. View logs
docker-compose -f docker-compose.c-client.yml logs -f ior_client

# 4. Run custom IOR command
docker exec -it benchfs_ior_client bash
# Inside container:
mpirun -np 4 ior -a BENCHFS \
    --benchfs.registry /shared/registry \
    --benchfs.datadir /shared/data \
    -w -r -t 1m -b 4m -s 4 -o /test

# 5. Cleanup
docker-compose -f docker-compose.c-client.yml down -v
```

## Architecture Summary

```
C Client Architecture:
┌─────────────────────────────────────────────┐
│ IOR Benchmark (MPI processes)              │
│         ↓ (AIORI interface)                 │
│ aiori-BENCHFS.c                            │
│         ↓ (calls benchfs_c_api.h)          │
│ libbenchfs.so = libbenchfs_c_api.so        │
│   ├── benchfs_c_api.c (API wrapper)        │
│   ├── benchfs_client.c (UCX AM client)     │
│   └── benchfs_ops.c (RPC operations)       │
│         ↓ (UCX Active Messages)            │
│ BenchFS Servers (benchfsd_mpi)             │
│   └── AM RPC handlers                       │
└─────────────────────────────────────────────┘
```

Key difference from Rust FFI version:
- **Library**: Pure C implementation vs Rust FFI
- **Protocol**: AM-only vs Stream+RMA
- **API**: Same `benchfs_c_api.h` interface

## Next Steps

1. Run basic test to verify setup: `./scripts/run-c-client-test.sh basic 4`
2. Try larger tests: `./scripts/run-c-client-test.sh 4gib-ssf 4`
3. Compare with Rust FFI version performance
4. Review detailed documentation: `README-C-CLIENT.md`

## Support

For detailed documentation, see:
- `README-C-CLIENT.md` - Complete documentation
- `../../c_client/README.md` - C client library documentation
- Main BenchFS documentation
