# IOR Hang Fix - Thread-less Timeout Implementation

## Problem with Previous Solution

The previous timeout implementation used a separate monitoring thread which caused a new hang:
- Timeout thread would `sleep()` for 120 seconds
- Even if operation completed quickly, `join()` would wait for the full timeout
- This caused benchfs_create/write/read to hang for up to 120 seconds

## New Solution: futures::select! Based Timeout

### Implementation Details

**File**: `/home/rmaeda/workspace/rust/benchfs/src/ffi/runtime.rs`

The new implementation uses `futures::select!` macro to run the operation and timeout concurrently:

```rust
use futures::{select, FutureExt};
use futures_timer::Delay;

pub fn block_on<F>(future: F) -> F::Output {
    // ... runtime setup ...

    let combined_future = async move {
        futures::pin_mut!(future);
        let mut user_future = future.fuse();
        let mut timeout_future = Delay::new(timeout_duration).fuse();

        select! {
            result = user_future => {
                // Operation completed normally
                *result_holder.borrow_mut() = Some(result);
            },
            _ = timeout_future => {
                // Timeout occurred
                eprintln!("ERROR: Operation timed out after {} seconds", timeout_secs);
                std::process::abort();
            }
        }
    };

    rt.run(combined_future);
    // Extract result...
}
```

### Key Benefits

1. **No separate threads**: Eliminates thread creation overhead and join() hang
2. **Immediate completion**: Operations complete as soon as they finish, no waiting
3. **Clean timeout**: If timeout occurs, process aborts cleanly
4. **Works with pluvio runtime**: Integrates naturally with existing async runtime

### Environment Variables

Still uses the same environment variable:
- `BENCHFS_OPERATION_TIMEOUT`: Global timeout in seconds (default: 120)

### Testing

To verify the fix works:

1. Set a short timeout:
   ```bash
   export BENCHFS_OPERATION_TIMEOUT=5
   ```

2. Run a quick operation:
   - Should complete immediately without waiting

3. Run a long operation:
   - Should abort after 5 seconds with timeout message

### Technical Notes

- Uses `futures::pin_mut!` to pin the future for use with select!
- `futures_timer::Delay` provides the timeout future
- Both futures are `fuse()`d to make them work with select!
- The first future to complete wins, the other is cancelled

This implementation completely eliminates the thread join() hang issue while maintaining timeout protection.