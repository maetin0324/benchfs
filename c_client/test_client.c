#include "benchfs_client.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define TEST_FILE_PATH "/test_file.txt"
#define TEST_DIR_PATH  "/test_dir"
#define TEST_CHUNK_SIZE (4 * 1024 * 1024)  /* 4MB chunks */

static void print_usage(const char *prog) {
    printf("Usage: %s <server_address>\n", prog);
    printf("  server_address: Server IP:port (e.g., 192.168.1.10:50051)\n");
}

static int test_metadata_operations(benchfs_client_t *client) {
    int ret;
    uint64_t inode;

    printf("\n=== Testing Metadata Operations ===\n");

    /* Test: Create directory */
    printf("Creating directory: %s\n", TEST_DIR_PATH);
    ret = benchfs_metadata_create_dir(client, TEST_DIR_PATH, 0755, &inode);
    if (ret != BENCHFS_SUCCESS) {
        fprintf(stderr, "Failed to create directory\n");
        return ret;
    }
    printf("Created directory with inode: %lu\n", inode);

    /* Test: Create file */
    printf("Creating file: %s\n", TEST_FILE_PATH);
    ret = benchfs_metadata_create_file(client, TEST_FILE_PATH, 0, 0644, &inode);
    if (ret != BENCHFS_SUCCESS) {
        fprintf(stderr, "Failed to create file\n");
        return ret;
    }
    printf("Created file with inode: %lu\n", inode);

    /* Test: Lookup file */
    printf("Looking up file: %s\n", TEST_FILE_PATH);
    benchfs_metadata_lookup_response_t lookup_resp;
    ret = benchfs_metadata_lookup(client, TEST_FILE_PATH, &lookup_resp);
    if (ret != BENCHFS_SUCCESS) {
        fprintf(stderr, "Failed to lookup file\n");
        return ret;
    }
    printf("Lookup result: entry_type=%u, size=%lu\n",
           lookup_resp.entry_type, lookup_resp.size);

    /* Test: Update file size */
    printf("Updating file size to 1MB\n");
    ret = benchfs_metadata_update(client, TEST_FILE_PATH, 1024 * 1024, 0, UPDATE_SIZE);
    if (ret != BENCHFS_SUCCESS) {
        fprintf(stderr, "Failed to update file metadata\n");
        return ret;
    }
    printf("File size updated successfully\n");

    /* Test: Lookup file again to verify update */
    ret = benchfs_metadata_lookup(client, TEST_FILE_PATH, &lookup_resp);
    if (ret == BENCHFS_SUCCESS) {
        printf("Updated file size: %lu bytes\n", lookup_resp.size);
    }

    printf("Metadata operations test completed successfully\n");
    return BENCHFS_SUCCESS;
}

static int test_data_operations(benchfs_client_t *client) {
    int ret;
    uint64_t bytes_written, bytes_read;

    printf("\n=== Testing Data Operations ===\n");

    /* Allocate test buffers */
    size_t chunk_size = 1024 * 1024;  /* 1MB test chunk */
    char *write_buffer = malloc(chunk_size);
    char *read_buffer = malloc(chunk_size);

    if (!write_buffer || !read_buffer) {
        fprintf(stderr, "Failed to allocate test buffers\n");
        free(write_buffer);
        free(read_buffer);
        return BENCHFS_NO_MEMORY;
    }

    /* Fill write buffer with test pattern */
    for (size_t i = 0; i < chunk_size; i++) {
        write_buffer[i] = (char)(i % 256);
    }

    /* Test: Write chunk */
    printf("Writing %zu bytes to chunk 0\n", chunk_size);
    ret = benchfs_write_chunk(client, TEST_FILE_PATH, 0, 0,
                              write_buffer, chunk_size, &bytes_written);
    if (ret != BENCHFS_SUCCESS) {
        fprintf(stderr, "Failed to write chunk\n");
        free(write_buffer);
        free(read_buffer);
        return ret;
    }
    printf("Wrote %lu bytes successfully\n", bytes_written);

    /* Test: Read chunk */
    printf("Reading %zu bytes from chunk 0\n", chunk_size);
    ret = benchfs_read_chunk(client, TEST_FILE_PATH, 0, 0,
                            read_buffer, chunk_size, &bytes_read);
    if (ret != BENCHFS_SUCCESS) {
        fprintf(stderr, "Failed to read chunk\n");
        free(write_buffer);
        free(read_buffer);
        return ret;
    }
    printf("Read %lu bytes successfully\n", bytes_read);

    /* Verify data */
    if (bytes_read == bytes_written) {
        int mismatch = 0;
        for (size_t i = 0; i < bytes_read; i++) {
            if (read_buffer[i] != write_buffer[i]) {
                fprintf(stderr, "Data mismatch at offset %zu: expected 0x%02x, got 0x%02x\n",
                        i, (unsigned char)write_buffer[i], (unsigned char)read_buffer[i]);
                mismatch = 1;
                break;
            }
        }

        if (!mismatch) {
            printf("Data verification successful - all %lu bytes match\n", bytes_read);
        } else {
            free(write_buffer);
            free(read_buffer);
            return BENCHFS_ERROR;
        }
    } else {
        fprintf(stderr, "Size mismatch: wrote %lu bytes, read %lu bytes\n",
                bytes_written, bytes_read);
        free(write_buffer);
        free(read_buffer);
        return BENCHFS_ERROR;
    }

    free(write_buffer);
    free(read_buffer);

    printf("Data operations test completed successfully\n");
    return BENCHFS_SUCCESS;
}

static int test_cleanup(benchfs_client_t *client) {
    int ret;

    printf("\n=== Cleaning Up Test Data ===\n");

    /* Delete file */
    printf("Deleting file: %s\n", TEST_FILE_PATH);
    ret = benchfs_metadata_delete_file(client, TEST_FILE_PATH);
    if (ret != BENCHFS_SUCCESS) {
        fprintf(stderr, "Failed to delete file (might not exist)\n");
    } else {
        printf("File deleted successfully\n");
    }

    /* Delete directory */
    printf("Deleting directory: %s\n", TEST_DIR_PATH);
    ret = benchfs_metadata_delete_dir(client, TEST_DIR_PATH);
    if (ret != BENCHFS_SUCCESS) {
        fprintf(stderr, "Failed to delete directory (might not exist)\n");
    } else {
        printf("Directory deleted successfully\n");
    }

    return BENCHFS_SUCCESS;
}

int main(int argc, char *argv[]) {
    if (argc != 2) {
        print_usage(argv[0]);
        return EXIT_FAILURE;
    }

    const char *server_addr = argv[1];

    printf("BenchFS C Client Test Program\n");
    printf("==============================\n");
    printf("Server address: %s\n", server_addr);

    /* Create client */
    printf("\nCreating BenchFS client...\n");
    benchfs_client_t *client = benchfs_client_create();
    if (!client) {
        fprintf(stderr, "Failed to create client\n");
        return EXIT_FAILURE;
    }

    /* Connect to server */
    printf("Connecting to server...\n");
    int ret = benchfs_client_connect(client, server_addr);
    if (ret != BENCHFS_SUCCESS) {
        fprintf(stderr, "Failed to connect to server\n");
        benchfs_client_destroy(client);
        return EXIT_FAILURE;
    }

    /* Run tests */
    int test_failed = 0;

    if (test_metadata_operations(client) != BENCHFS_SUCCESS) {
        fprintf(stderr, "Metadata operations test failed\n");
        test_failed = 1;
    }

    if (!test_failed && test_data_operations(client) != BENCHFS_SUCCESS) {
        fprintf(stderr, "Data operations test failed\n");
        test_failed = 1;
    }

    /* Cleanup */
    test_cleanup(client);

    /* Destroy client */
    printf("\nDestroying client...\n");
    benchfs_client_destroy(client);

    if (test_failed) {
        printf("\n*** TESTS FAILED ***\n");
        return EXIT_FAILURE;
    }

    printf("\n*** ALL TESTS PASSED ***\n");
    return EXIT_SUCCESS;
}
