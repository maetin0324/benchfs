/* Simple standalone test for worker address exchange */
#include "benchfs_client.h"
#include <stdio.h>
#include <stdlib.h>

int main(int argc, char **argv) {
    printf("[TEST] Starting worker address exchange test\n");

    /* Create client */
    printf("[TEST] Creating BenchFS client...\n");
    benchfs_client_t *client = benchfs_client_create();
    if (!client) {
        fprintf(stderr, "[TEST] ERROR: Failed to create client\n");
        return 1;
    }
    printf("[TEST] Client created successfully\n");

    /* Connect to server1 */
    const char *server_addr = "server1:50051";
    printf("[TEST] Connecting to %s...\n", server_addr);

    int ret = benchfs_client_connect(client, server_addr);
    if (ret != BENCHFS_SUCCESS) {
        fprintf(stderr, "[TEST] ERROR: Failed to connect: %d\n", ret);
        benchfs_client_destroy(client);
        return 1;
    }

    printf("[TEST] Connection successful!\n");
    printf("[TEST] Worker address exchange completed\n");

    /* Cleanup */
    printf("[TEST] Destroying client...\n");
    benchfs_client_destroy(client);

    printf("[TEST] === TEST PASSED ===\n");
    return 0;
}
