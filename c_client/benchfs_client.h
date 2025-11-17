#ifndef BENCHFS_CLIENT_H
#define BENCHFS_CLIENT_H

#include "benchfs_protocol.h"
#include <ucp/api/ucp.h>

/* Error codes */
#define BENCHFS_SUCCESS     0
#define BENCHFS_ERROR      -1
#define BENCHFS_TIMEOUT    -2
#define BENCHFS_NO_MEMORY  -3
#define BENCHFS_INVALID_ARG -4

/* Forward declarations */
typedef struct benchfs_client benchfs_client_t;

/* ========================================================================== */
/* Client Initialization and Cleanup                                         */
/* ========================================================================== */

/**
 * Create a new BenchFS client
 *
 * @return Pointer to client structure, or NULL on error
 */
benchfs_client_t *benchfs_client_create(void);

/**
 * Connect to a BenchFS server using socket address
 *
 * @param client Client handle
 * @param server_addr Server address (IP:port format, e.g., "192.168.1.10:50051")
 * @return BENCHFS_SUCCESS on success, error code otherwise
 */
int benchfs_client_connect(benchfs_client_t *client, const char *server_addr);

/**
 * Connect to a BenchFS server using worker address
 *
 * @param client Client handle
 * @param worker_addr_bytes Worker address bytes
 * @param addr_len Length of worker address
 * @return BENCHFS_SUCCESS on success, error code otherwise
 */
int benchfs_client_connect_worker_addr(benchfs_client_t *client, const unsigned char *worker_addr_bytes, size_t addr_len);

/**
 * Destroy a BenchFS client and release all resources
 *
 * @param client Client handle
 */
void benchfs_client_destroy(benchfs_client_t *client);

/* ========================================================================== */
/* Data Operations                                                            */
/* ========================================================================== */

/**
 * Read a chunk from a file
 *
 * @param client Client handle
 * @param path File path
 * @param chunk_index Chunk index
 * @param offset Offset within chunk
 * @param buffer Buffer to read into
 * @param length Number of bytes to read
 * @param bytes_read Output: number of bytes actually read
 * @return BENCHFS_SUCCESS on success, error code otherwise
 */
int benchfs_read_chunk(
    benchfs_client_t *client,
    const char *path,
    uint64_t chunk_index,
    uint64_t offset,
    void *buffer,
    uint64_t length,
    uint64_t *bytes_read
);

/**
 * Write a chunk to a file
 *
 * @param client Client handle
 * @param path File path
 * @param chunk_index Chunk index
 * @param offset Offset within chunk
 * @param data Data to write
 * @param length Number of bytes to write
 * @param bytes_written Output: number of bytes actually written
 * @return BENCHFS_SUCCESS on success, error code otherwise
 */
int benchfs_write_chunk(
    benchfs_client_t *client,
    const char *path,
    uint64_t chunk_index,
    uint64_t offset,
    const void *data,
    uint64_t length,
    uint64_t *bytes_written
);

/* ========================================================================== */
/* Metadata Operations                                                        */
/* ========================================================================== */

/**
 * Lookup file or directory metadata
 *
 * @param client Client handle
 * @param path File or directory path
 * @param response Output: lookup response
 * @return BENCHFS_SUCCESS on success, error code otherwise
 */
int benchfs_metadata_lookup(
    benchfs_client_t *client,
    const char *path,
    benchfs_metadata_lookup_response_t *response
);

/**
 * Create a new file
 *
 * @param client Client handle
 * @param path File path
 * @param size Initial file size
 * @param mode File permissions (e.g., 0644)
 * @param inode Output: assigned inode number
 * @return BENCHFS_SUCCESS on success, error code otherwise
 */
int benchfs_metadata_create_file(
    benchfs_client_t *client,
    const char *path,
    uint64_t size,
    uint32_t mode,
    uint64_t *inode
);

/**
 * Create a new directory
 *
 * @param client Client handle
 * @param path Directory path
 * @param mode Directory permissions (e.g., 0755)
 * @param inode Output: assigned inode number
 * @return BENCHFS_SUCCESS on success, error code otherwise
 */
int benchfs_metadata_create_dir(
    benchfs_client_t *client,
    const char *path,
    uint32_t mode,
    uint64_t *inode
);

/**
 * Delete a file
 *
 * @param client Client handle
 * @param path File path
 * @return BENCHFS_SUCCESS on success, error code otherwise
 */
int benchfs_metadata_delete_file(
    benchfs_client_t *client,
    const char *path
);

/**
 * Delete a directory
 *
 * @param client Client handle
 * @param path Directory path
 * @return BENCHFS_SUCCESS on success, error code otherwise
 */
int benchfs_metadata_delete_dir(
    benchfs_client_t *client,
    const char *path
);

/**
 * Update file metadata (size and/or mode)
 *
 * @param client Client handle
 * @param path File path
 * @param new_size New file size (or 0 if not updating size)
 * @param new_mode New file mode (or 0 if not updating mode)
 * @param update_mask Bit mask: UPDATE_SIZE and/or UPDATE_MODE
 * @return BENCHFS_SUCCESS on success, error code otherwise
 */
int benchfs_metadata_update(
    benchfs_client_t *client,
    const char *path,
    uint64_t new_size,
    uint32_t new_mode,
    uint8_t update_mask
);

/**
 * Send shutdown request to server
 *
 * @param client Client handle
 * @return BENCHFS_SUCCESS on success, error code otherwise
 */
int benchfs_shutdown_server(benchfs_client_t *client);

/* ========================================================================== */
/* Internal Helper (exported for benchfs_ops.c)                              */
/* ========================================================================== */

/**
 * Send an AM request and wait for reply (internal use)
 *
 * @param client Client handle
 * @param rpc_id RPC identifier
 * @param reply_stream_id Reply stream identifier
 * @param request_header Request header data
 * @param header_size Size of request header
 * @param request_data Request payload data (or NULL)
 * @param data_size Size of request payload
 * @param response_header Buffer for response header
 * @param response_header_size Size of response header buffer
 * @param response_data Buffer for response data (or NULL)
 * @param response_data_size Input: buffer size, Output: actual data size
 * @return BENCHFS_SUCCESS on success, error code otherwise
 */
int send_am_request(
    benchfs_client_t *client,
    uint32_t rpc_id,
    uint32_t reply_stream_id,
    const void *request_header,
    size_t header_size,
    const void *request_data,
    size_t data_size,
    void *response_header,
    size_t response_header_size,
    void *response_data,
    size_t *response_data_size
);

#endif /* BENCHFS_CLIENT_H */
