/**
 * BenchFS Mini C Client API
 *
 * Public API for BenchFS Mini C client library.
 */

#ifndef BENCHFS_MINI_CLIENT_H
#define BENCHFS_MINI_CLIENT_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Initialize the BenchFS Mini client
 *
 * @param registry_dir Path to the registry directory where servers register
 * @return 0 on success, -1 on error
 */
int benchfs_mini_client_init(const char *registry_dir);

/**
 * Connect to a BenchFS server
 *
 * @param server_rank Rank of the server to connect to
 * @return 0 on success, -1 on error
 */
int benchfs_mini_client_connect(int server_rank);

/**
 * Write data to a file on a server
 *
 * @param path File path
 * @param data Data buffer to write
 * @param data_len Length of data buffer
 * @param server_rank Server to write to
 * @return 0 on success, -1 on error
 */
int benchfs_mini_client_write(const char *path, const void *data, size_t data_len, int server_rank);

/**
 * Read data from a file on a server
 *
 * @param path File path
 * @param buffer Buffer to read into
 * @param buffer_len Size of buffer
 * @param server_rank Server to read from
 * @return Number of bytes read on success, -1 on error
 */
int benchfs_mini_client_read(const char *path, void *buffer, size_t buffer_len, int server_rank);

/**
 * Finalize and cleanup the client
 */
void benchfs_mini_client_finalize(void);

#ifdef __cplusplus
}
#endif

#endif /* BENCHFS_MINI_CLIENT_H */
