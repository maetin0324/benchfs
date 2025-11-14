/**
 * Test program for BenchFS Mini C Client
 *
 * Simple test program that writes and reads data using the BenchFS Mini C client.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "benchfs_mini_client.h"

#define TEST_DATA_SIZE (1024 * 1024)  // 1 MB
#define NUM_SERVERS 2

int main(int argc, char **argv) {
    char *registry_dir;
    char *write_data;
    char *read_buffer;
    int ret;
    int i;

    printf("========================================\n");
    printf("BenchFS Mini C Client Test\n");
    printf("========================================\n\n");

    // Get registry directory from command line or use default
    if (argc > 1) {
        registry_dir = argv[1];
    } else {
        registry_dir = "/shared/registry_mini";
    }

    printf("[Test] Registry directory: %s\n", registry_dir);
    printf("[Test] Test data size: %d bytes\n", TEST_DATA_SIZE);
    printf("[Test] Number of servers: %d\n\n", NUM_SERVERS);

    // Initialize client
    printf("[Test] Initializing client...\n");
    ret = benchfs_mini_client_init(registry_dir);
    if (ret != 0) {
        fprintf(stderr, "[Test] Failed to initialize client\n");
        return 1;
    }
    printf("[Test] Client initialized successfully\n\n");

    // Connect to servers
    printf("[Test] Connecting to %d servers...\n", NUM_SERVERS);
    for (i = 0; i < NUM_SERVERS; i++) {
        printf("[Test] Connecting to server %d...\n", i);
        ret = benchfs_mini_client_connect(i);
        if (ret != 0) {
            fprintf(stderr, "[Test] Failed to connect to server %d\n", i);
            benchfs_mini_client_finalize();
            return 1;
        }
        printf("[Test] Connected to server %d successfully\n", i);
    }
    printf("[Test] All servers connected\n\n");

    // Allocate test data
    write_data = (char *)malloc(TEST_DATA_SIZE);
    read_buffer = (char *)malloc(TEST_DATA_SIZE);
    if (!write_data || !read_buffer) {
        fprintf(stderr, "[Test] Failed to allocate memory\n");
        benchfs_mini_client_finalize();
        return 1;
    }

    // Fill write data with pattern
    for (i = 0; i < TEST_DATA_SIZE; i++) {
        write_data[i] = (char)(i % 256);
    }

    // Test write and read for each server
    for (i = 0; i < NUM_SERVERS; i++) {
        char path[64];
        snprintf(path, sizeof(path), "/test_file_%d.dat", i);

        printf("========================================\n");
        printf("[Test] Testing server %d\n", i);
        printf("========================================\n");

        // Write test
        printf("[Test] Writing %d bytes to %s on server %d...\n",
               TEST_DATA_SIZE, path, i);
        ret = benchfs_mini_client_write(path, write_data, TEST_DATA_SIZE, i);
        if (ret != 0) {
            fprintf(stderr, "[Test] Write failed for server %d\n", i);
            free(write_data);
            free(read_buffer);
            benchfs_mini_client_finalize();
            return 1;
        }
        printf("[Test] Write completed successfully\n");

        // Read test
        printf("[Test] Reading from %s on server %d...\n", path, i);
        memset(read_buffer, 0, TEST_DATA_SIZE);
        ret = benchfs_mini_client_read(path, read_buffer, TEST_DATA_SIZE, i);
        if (ret < 0) {
            fprintf(stderr, "[Test] Read failed for server %d\n", i);
            free(write_data);
            free(read_buffer);
            benchfs_mini_client_finalize();
            return 1;
        }
        printf("[Test] Read %d bytes successfully\n", ret);

        // Verify data
        if (ret != TEST_DATA_SIZE) {
            fprintf(stderr, "[Test] Read size mismatch: expected %d, got %d\n",
                    TEST_DATA_SIZE, ret);
            free(write_data);
            free(read_buffer);
            benchfs_mini_client_finalize();
            return 1;
        }

        if (memcmp(write_data, read_buffer, TEST_DATA_SIZE) != 0) {
            fprintf(stderr, "[Test] Data verification failed for server %d\n", i);
            free(write_data);
            free(read_buffer);
            benchfs_mini_client_finalize();
            return 1;
        }

        printf("[Test] Data verification passed for server %d\n\n", i);
    }

    // Cleanup
    free(write_data);
    free(read_buffer);

    printf("========================================\n");
    printf("[Test] All tests passed!\n");
    printf("========================================\n\n");

    printf("[Test] Finalizing client...\n");
    benchfs_mini_client_finalize();
    printf("[Test] Test complete\n");

    return 0;
}
