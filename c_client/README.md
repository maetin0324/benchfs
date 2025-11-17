# BenchFS C Client

Pure C implementation of BenchFS client using UCX Active Messages (AM).

## Overview

This is a C language client library for BenchFS that uses UCX Active Messages for RPC communication. It does not use Rust FFI and is implemented entirely in C with UCX.

## Features

- **Pure C + UCX**: No Rust dependencies, uses UCX Active Messages directly
- **Metadata Operations**: Create, lookup, update, and delete files and directories
- **Data Operations**: Read and write file chunks
- **Simple API**: Easy-to-use C API for BenchFS operations

## Building

### Prerequisites

- GCC compiler
- UCX library (libucp, libucs)
- BenchFS server running with AM RPC support

### Compile

```bash
cd c_client
make
```

This will build:
- `libbenchfs_client.a` - Static library
- `test_client` - Test program

### Clean

```bash
make clean
```

## Usage

### Example Code

```c
#include "benchfs_client.h"

int main() {
    /* Create client */
    benchfs_client_t *client = benchfs_client_create();

    /* Connect to server */
    benchfs_client_connect(client, "192.168.1.10:50051");

    /* Create a file */
    uint64_t inode;
    benchfs_metadata_create_file(client, "/myfile.txt", 0, 0644, &inode);

    /* Write data */
    char data[1024] = "Hello, BenchFS!";
    uint64_t bytes_written;
    benchfs_write_chunk(client, "/myfile.txt", 0, 0, data, 1024, &bytes_written);

    /* Read data */
    char buffer[1024];
    uint64_t bytes_read;
    benchfs_read_chunk(client, "/myfile.txt", 0, 0, buffer, 1024, &bytes_read);

    /* Cleanup */
    benchfs_client_destroy(client);
    return 0;
}
```

### Running Tests

```bash
# Start BenchFS server first (with AM RPC support)
mpirun -n 4 benchfsd_mpi /path/to/registry

# Run test client
./test_client 192.168.1.10:50051
```

## API Reference

### Client Management

- `benchfs_client_create()` - Create a new client
- `benchfs_client_connect(client, addr)` - Connect to server
- `benchfs_client_destroy(client)` - Destroy client

### Metadata Operations

- `benchfs_metadata_lookup(client, path, response)` - Lookup file/directory
- `benchfs_metadata_create_file(client, path, size, mode, inode)` - Create file
- `benchfs_metadata_create_dir(client, path, mode, inode)` - Create directory
- `benchfs_metadata_delete_file(client, path)` - Delete file
- `benchfs_metadata_delete_dir(client, path)` - Delete directory
- `benchfs_metadata_update(client, path, new_size, new_mode, mask)` - Update metadata

### Data Operations

- `benchfs_read_chunk(client, path, chunk_idx, offset, buffer, length, bytes_read)` - Read chunk
- `benchfs_write_chunk(client, path, chunk_idx, offset, data, length, bytes_written)` - Write chunk

### Server Management

- `benchfs_shutdown_server(client)` - Send shutdown request to server

## Protocol

The C client uses the same AM-based RPC protocol as the Rust implementation:

1. **Request Flow**:
   - Client sends AM with RPC ID and request header + data
   - Server receives on AM handler for that RPC ID
   - Server processes request

2. **Response Flow**:
   - Server sends reply on reply stream (RPC_ID + 100)
   - Client waits on reply stream
   - Client receives response header + optional data

3. **RPC IDs**:
   - Data ops: 10-11 (READ_CHUNK, WRITE_CHUNK)
   - Metadata ops: 20-25 (LOOKUP, CREATE_FILE, CREATE_DIR, DELETE, UPDATE, SHUTDOWN)

## File Structure

```
c_client/
├── benchfs_protocol.h   # RPC protocol definitions (headers, constants)
├── benchfs_client.h     # Client API definitions
├── benchfs_client.c     # Client implementation (UCX management)
├── benchfs_ops.c        # Operations implementation
├── test_client.c        # Test program
├── Makefile             # Build configuration
└── README.md            # This file
```

## Notes

- Single-threaded design (uses UCS_THREAD_MODE_SINGLE)
- Blocking RPC calls with timeout (5 seconds default)
- Direct AM communication without RDMA for simplicity
- Compatible with benchfsd_mpi running in AM-only mode

## Troubleshooting

### Connection Failures

- Ensure server is running and listening on specified port
- Check firewall settings
- Verify UCX is properly installed

### Timeouts

- Increase timeout in `wait_for_am_reply()` if network is slow
- Check server logs for errors

### Build Errors

- Ensure UCX development files are installed
- Check that `ucp/api/ucp.h` is in your include path
- Link with `-lucp -lucs`
