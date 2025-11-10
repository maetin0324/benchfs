# IOR Hang Fix - Timeout Configuration

This document describes the timeout mechanisms added to prevent IOR from hanging due to infinite loops in the pluvio runtime.

## Problem Summary

IOR was hanging because:
1. One node (out of 16) became unresponsive during execution
2. UCX Worker stuck in `WaitConnect` state with no timeout
3. Runtime's `run_queue()` entered an infinite loop waiting for the reactor
4. No safety mechanisms to break out of the loop

## Solution: Multi-Layer Timeout Protection

### Environment Variables

Configure these environment variables to control timeout behavior:

#### 1. `PLUVIO_CONNECTION_TIMEOUT` (default: 30 seconds)
- **Purpose**: Maximum time to wait for UCX connections
- **Usage**: `export PLUVIO_CONNECTION_TIMEOUT=30`
- **Location**: Used in `pluvio_ucx/src/reactor.rs`
- **Effect**: Workers in `WaitConnect` state timeout after this duration

#### 2. `PLUVIO_MAX_STUCK_ITERATIONS` (default: 10000)
- **Purpose**: Maximum iterations without progress before breaking runtime loop
- **Usage**: `export PLUVIO_MAX_STUCK_ITERATIONS=10000`
- **Location**: Used in `pluvio_runtime/src/executor/mod.rs`
- **Effect**: Runtime exits loop if no progress is made after this many iterations

#### 3. `BENCHFS_OPERATION_TIMEOUT` (default: 120 seconds)
- **Purpose**: Global timeout for any BenchFS operation
- **Usage**: `export BENCHFS_OPERATION_TIMEOUT=120`
- **Location**: Used in `benchfs/src/ffi/runtime.rs`
- **Effect**: Aborts the process if operation doesn't complete

## Recommended Settings

### For Production (Supercomputer)
```bash
export PLUVIO_CONNECTION_TIMEOUT=30
export PLUVIO_MAX_STUCK_ITERATIONS=10000
export BENCHFS_OPERATION_TIMEOUT=300  # 5 minutes for large operations
```

### For Testing/Debugging
```bash
export PLUVIO_CONNECTION_TIMEOUT=10
export PLUVIO_MAX_STUCK_ITERATIONS=1000
export BENCHFS_OPERATION_TIMEOUT=60
export RUST_LOG=debug  # Enable debug logging
```

## How It Works

1. **Connection Level**: If a node fails to connect, timeout after `PLUVIO_CONNECTION_TIMEOUT`
2. **Runtime Level**: If no progress is made, break loop after `PLUVIO_MAX_STUCK_ITERATIONS`
3. **Operation Level**: Force abort if operation exceeds `BENCHFS_OPERATION_TIMEOUT`

## Monitoring

The system will log warnings when:
- A worker connection times out
- Runtime appears stuck (every 1000 iterations)
- Runtime breaks out of stuck loop
- Operation timeout is triggered

## Files Modified

1. `/home/rmaeda/workspace/rust/pluvio/pluvio_ucx/src/reactor.rs`
   - Added timeout checking in `status()` method

2. `/home/rmaeda/workspace/rust/pluvio/pluvio_ucx/src/worker/mod.rs`
   - Added `wait_connect_start` field to track connection start time

3. `/home/rmaeda/workspace/rust/pluvio/pluvio_runtime/src/executor/mod.rs`
   - Added stuck detection in `run_queue()` method

4. `/home/rmaeda/workspace/rust/benchfs/src/ffi/runtime.rs`
   - Added global operation timeout in `block_on()` function
   - **UPDATE**: Changed to thread-less implementation using `futures::select!` to avoid join() hang

## Testing

To test the timeout mechanisms:

1. Set short timeouts:
   ```bash
   export PLUVIO_CONNECTION_TIMEOUT=5
   export PLUVIO_MAX_STUCK_ITERATIONS=100
   export BENCHFS_OPERATION_TIMEOUT=10
   ```

2. Run IOR with the problematic configuration

3. Verify that the system exits cleanly instead of hanging

## Troubleshooting

If timeouts are triggered too frequently:
1. Increase `PLUVIO_CONNECTION_TIMEOUT` if nodes are slow to connect
2. Increase `PLUVIO_MAX_STUCK_ITERATIONS` for legitimate long-running operations
3. Increase `BENCHFS_OPERATION_TIMEOUT` for large file operations

If hangs still occur:
1. Check logs for which timeout is not working
2. Enable debug logging: `export RUST_LOG=debug`
3. Check if all 16 nodes are healthy before starting