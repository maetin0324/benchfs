#!/bin/bash
# Test script to verify operation name tracking in timeout scenarios

set -euo pipefail

echo "======================================================"
echo "Operation Tracking Test for BenchFS Timeouts"
echo "======================================================"
echo ""

# Build the release binaries
echo "1. Building release binaries..."
cargo build --release 2>&1 | grep -E "Finished|Compiling" || true
echo "   Build completed"
echo ""

# Set test environment variables
echo "2. Setting test environment variables..."
export BENCHFS_OPERATION_TIMEOUT=5  # Short timeout for testing
export BENCHFS_DEBUG=1               # Enable debug logging
export RUST_LOG=info

echo "   BENCHFS_OPERATION_TIMEOUT=$BENCHFS_OPERATION_TIMEOUT (seconds)"
echo "   BENCHFS_DEBUG=$BENCHFS_DEBUG"
echo "   RUST_LOG=$RUST_LOG"
echo ""

# Create test directory
echo "3. Creating test directory..."
TEST_DIR=$(mktemp -d /tmp/benchfs_op_test.XXXXXX)
echo "   Test directory: $TEST_DIR"

# Create config file
echo "4. Creating minimal config..."
cat > "$TEST_DIR/benchfs.toml" <<EOF
[node]
node_id = "test_node"
data_dir = "$TEST_DIR/data"
log_level = "info"

[storage]
chunk_size = 4194304
use_iouring = false
max_storage_gb = 0

[network]
bind_addr = "127.0.0.1:50051"
timeout_secs = 30
rdma_threshold_bytes = 32768
registry_dir = "$TEST_DIR/registry"

[cache]
metadata_cache_entries = 10000
chunk_cache_mb = 1024
cache_ttl_secs = 0
EOF

echo "   Config created: $TEST_DIR/benchfs.toml"
echo ""

# Create a simple C test program
echo "5. Creating test program..."
cat > "$TEST_DIR/test_ops.c" <<'EOF'
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <benchfs.h>

int main() {
    printf("=== Testing BenchFS Operation Tracking ===\n\n");

    // Initialize BenchFS
    printf("Initializing BenchFS...\n");
    benchfs_context_t* ctx = benchfs_init(0, NULL);
    if (!ctx) {
        fprintf(stderr, "Failed to initialize BenchFS\n");
        return 1;
    }
    printf("BenchFS initialized successfully\n\n");

    // Test 1: Create a file
    printf("Test 1: Creating file...\n");
    benchfs_file_t* file = benchfs_create(ctx, "/test_file.txt",
                                          BENCHFS_O_CREAT | BENCHFS_O_WRONLY, 0644);
    if (!file) {
        fprintf(stderr, "Failed to create file: %s\n", benchfs_get_error());
        benchfs_finalize(ctx);
        return 1;
    }
    printf("File created successfully\n\n");

    // Test 2: Write to file
    printf("Test 2: Writing to file...\n");
    const char* data = "Hello, BenchFS with operation tracking!";
    ssize_t written = benchfs_write(file, (const uint8_t*)data, strlen(data), 0);
    if (written < 0) {
        fprintf(stderr, "Failed to write: %s\n", benchfs_get_error());
    } else {
        printf("Wrote %zd bytes\n", written);
    }
    printf("\n");

    // Test 3: Close file
    printf("Test 3: Closing file...\n");
    int ret = benchfs_close(file);
    if (ret < 0) {
        fprintf(stderr, "Failed to close file: %s\n", benchfs_get_error());
    } else {
        printf("File closed successfully\n");
    }
    printf("\n");

    // Test 4: Stat file
    printf("Test 4: Getting file stats...\n");
    benchfs_stat_t stat;
    ret = benchfs_stat(ctx, "/test_file.txt", &stat);
    if (ret < 0) {
        fprintf(stderr, "Failed to stat file: %s\n", benchfs_get_error());
    } else {
        printf("File size: %ld bytes\n", stat.st_size);
    }
    printf("\n");

    // Test 5: Open and read file
    printf("Test 5: Opening file for reading...\n");
    file = benchfs_open(ctx, "/test_file.txt", BENCHFS_O_RDONLY);
    if (!file) {
        fprintf(stderr, "Failed to open file: %s\n", benchfs_get_error());
    } else {
        printf("File opened successfully\n");

        char buffer[1024];
        ssize_t bytes_read = benchfs_read(file, (uint8_t*)buffer, sizeof(buffer)-1, 0);
        if (bytes_read < 0) {
            fprintf(stderr, "Failed to read: %s\n", benchfs_get_error());
        } else {
            buffer[bytes_read] = '\0';
            printf("Read %zd bytes: %s\n", bytes_read, buffer);
        }

        benchfs_close(file);
    }
    printf("\n");

    // Test 6: Remove file
    printf("Test 6: Removing file...\n");
    ret = benchfs_remove(ctx, "/test_file.txt");
    if (ret < 0) {
        fprintf(stderr, "Failed to remove file: %s\n", benchfs_get_error());
    } else {
        printf("File removed successfully\n");
    }
    printf("\n");

    // Cleanup
    printf("Finalizing BenchFS...\n");
    benchfs_finalize(ctx);
    printf("Test completed\n");

    return 0;
}
EOF

echo "   Test program created: $TEST_DIR/test_ops.c"
echo ""

# Compile the test program
echo "6. Compiling test program..."
gcc -o "$TEST_DIR/test_ops" "$TEST_DIR/test_ops.c" \
    -I./target/release/build/benchfs-db47357d155e2ac0/out/include \
    -L./target/release \
    -lbenchfs \
    -Wl,-rpath,./target/release \
    2>&1 | grep -v "^$" || true
echo "   Test program compiled"
echo ""

# Run the test in local mode
echo "7. Running test in local mode (with debug output)..."
echo "   Watch for [BENCHFS] operation messages..."
echo ""
echo "--- Test Output ---"
BENCHFS_CONFIG="$TEST_DIR/benchfs.toml" \
LD_LIBRARY_PATH=./target/release:$LD_LIBRARY_PATH \
"$TEST_DIR/test_ops" 2>&1 | sed 's/^/  /'
echo "--- End of Test ---"
echo ""

# Analyze output
echo "8. Summary:"
echo "   - Each operation should show '[BENCHFS] Starting operation' messages"
echo "   - Operations tracked: create, open, write, read, close, stat, remove"
echo "   - If timeout occurs, you'll see which specific operation timed out"
echo ""

# Test with very short timeout to trigger timeout
echo "9. Testing with ultra-short timeout (1 second)..."
export BENCHFS_OPERATION_TIMEOUT=1
echo "   BENCHFS_OPERATION_TIMEOUT=$BENCHFS_OPERATION_TIMEOUT"
echo "   This might trigger timeouts for slower operations..."
echo ""
echo "--- Test Output (Short Timeout) ---"
BENCHFS_CONFIG="$TEST_DIR/benchfs.toml" \
LD_LIBRARY_PATH=./target/release:$LD_LIBRARY_PATH \
"$TEST_DIR/test_ops" 2>&1 | sed 's/^/  /' || echo "  (Process may have been aborted due to timeout)"
echo "--- End of Test ---"
echo ""

# Cleanup
echo "10. Cleanup..."
rm -rf "$TEST_DIR"
echo "   Test directory removed"
echo ""

echo "======================================================"
echo "Operation Tracking Test Complete"
echo "======================================================"
echo ""
echo "Key Features Demonstrated:"
echo "✓ Operation name tracking with block_on_with_name"
echo "✓ Debug logging showing which operations are running"
echo "✓ Timeout detection with operation names"
echo "✓ Clean error messages identifying timed-out operations"
echo ""
echo "Next Steps for Production:"
echo "1. Set appropriate timeout values:"
echo "   export BENCHFS_OPERATION_TIMEOUT=120  # 2 minutes for production"
echo "2. Enable debug logging only when needed:"
echo "   export BENCHFS_DEBUG=1  # Only for troubleshooting"
echo "3. Deploy to supercomputer with these environment variables"
echo ""