#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "../ior_integration/benchfs_backend/include/benchfs_c_api.h"

int main() {
    printf("=== BenchFS Simple FFI Test ===\n");

    // Initialize BenchFS client
    printf("1. Initializing BenchFS client...\n");
    benchfs_context_t* ctx = benchfs_init(
        "simple_test_client",
        "/shared/registry",
        NULL,  // data_dir (not needed for client)
        0      // is_server = false
    );

    if (!ctx) {
        const char* err = benchfs_get_error();
        fprintf(stderr, "ERROR: Failed to initialize BenchFS: %s\n", err);
        return 1;
    }
    printf("   SUCCESS: BenchFS initialized\n");

    // Open/Create a file
    printf("2. Creating file /simple_test.txt...\n");
    benchfs_file_t* file = benchfs_create(
        ctx,
        "/simple_test.txt",
        BENCHFS_O_CREAT | BENCHFS_O_WRONLY,
        0644
    );

    if (!file) {
        const char* err = benchfs_get_error();
        fprintf(stderr, "ERROR: Failed to create file: %s\n", err);
        benchfs_finalize(ctx);
        return 1;
    }
    printf("   SUCCESS: File created\n");

    // Write data (small write - 1KB)
    printf("3. Writing 1KB data...\n");
    char write_buf[1024];
    memset(write_buf, 'A', sizeof(write_buf));

    ssize_t written = benchfs_write(file, write_buf, sizeof(write_buf), 0);
    if (written < 0) {
        const char* err = benchfs_get_error();
        fprintf(stderr, "ERROR: Write failed: %s\n", err);
        benchfs_close(file);
        benchfs_finalize(ctx);
        return 1;
    }
    printf("   SUCCESS: Wrote %zd bytes\n", written);

    // Close file
    printf("4. Closing file...\n");
    int close_ret = benchfs_close(file);
    if (close_ret < 0) {
        const char* err = benchfs_get_error();
        fprintf(stderr, "ERROR: Close failed: %s\n", err);
        benchfs_finalize(ctx);
        return 1;
    }
    printf("   SUCCESS: File closed\n");

    // Finalize
    printf("5. Finalizing BenchFS...\n");
    benchfs_finalize(ctx);
    printf("   SUCCESS: BenchFS finalized\n");

    printf("\n=== All tests PASSED ===\n");
    return 0;
}
