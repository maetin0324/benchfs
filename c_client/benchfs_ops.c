#include "benchfs_client.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* ========================================================================== */
/* Metadata Operations                                                        */
/* ========================================================================== */

int benchfs_metadata_lookup(
    benchfs_client_t *client,
    const char *path,
    benchfs_metadata_lookup_response_t *response
) {
    if (!client || !path || !response) {
        return BENCHFS_INVALID_ARG;
    }

    size_t path_len = strlen(path);
    if (path_len == 0 || path_len > MAX_PATH_LENGTH) {
        fprintf(stderr, "Invalid path length: %zu\n", path_len);
        return BENCHFS_INVALID_ARG;
    }

    /* Prepare request */
    benchfs_metadata_lookup_request_t req;
    memset(&req, 0, sizeof(req));
    req.path_len = (uint32_t)path_len;

    /* Send request and receive response */
    int ret = send_am_request(
        client,
        RPC_METADATA_LOOKUP,
        REPLY_METADATA_LOOKUP,
        &req, sizeof(req),
        path, path_len,
        response, sizeof(*response),
        NULL, NULL
    );

    if (ret != BENCHFS_SUCCESS) {
        fprintf(stderr, "MetadataLookup RPC failed\n");
        return ret;
    }

    /* Check response status */
    if (response->status != 0) {
        fprintf(stderr, "MetadataLookup failed with status: %d\n", response->status);
        return BENCHFS_ERROR;
    }

    return BENCHFS_SUCCESS;
}

int benchfs_metadata_create_file(
    benchfs_client_t *client,
    const char *path,
    uint64_t size,
    uint32_t mode,
    uint64_t *inode
) {
    if (!client || !path) {
        return BENCHFS_INVALID_ARG;
    }

    size_t path_len = strlen(path);
    if (path_len == 0 || path_len > MAX_PATH_LENGTH) {
        return BENCHFS_INVALID_ARG;
    }

    /* Prepare request */
    benchfs_metadata_create_file_request_t req;
    memset(&req, 0, sizeof(req));
    req.size = size;
    req.mode = mode;
    req.path_len = (uint32_t)path_len;

    /* Send request and receive response */
    benchfs_metadata_create_file_response_t resp;
    int ret = send_am_request(
        client,
        RPC_METADATA_CREATE_FILE,
        REPLY_METADATA_CREATE_FILE,
        &req, sizeof(req),
        path, path_len,
        &resp, sizeof(resp),
        NULL, NULL
    );

    if (ret != BENCHFS_SUCCESS) {
        fprintf(stderr, "MetadataCreateFile RPC failed\n");
        return ret;
    }

    /* Check response status */
    if (resp.status != 0) {
        fprintf(stderr, "MetadataCreateFile failed with status: %d\n", resp.status);
        return BENCHFS_ERROR;
    }

    if (inode) {
        *inode = resp.inode;
    }

    return BENCHFS_SUCCESS;
}

int benchfs_metadata_create_dir(
    benchfs_client_t *client,
    const char *path,
    uint32_t mode,
    uint64_t *inode
) {
    if (!client || !path) {
        return BENCHFS_INVALID_ARG;
    }

    size_t path_len = strlen(path);
    if (path_len == 0 || path_len > MAX_PATH_LENGTH) {
        return BENCHFS_INVALID_ARG;
    }

    /* Prepare request */
    benchfs_metadata_create_dir_request_t req;
    memset(&req, 0, sizeof(req));
    req.mode = mode;
    req.path_len = (uint32_t)path_len;

    /* Send request and receive response */
    benchfs_metadata_create_dir_response_t resp;
    int ret = send_am_request(
        client,
        RPC_METADATA_CREATE_DIR,
        REPLY_METADATA_CREATE_DIR,
        &req, sizeof(req),
        path, path_len,
        &resp, sizeof(resp),
        NULL, NULL
    );

    if (ret != BENCHFS_SUCCESS) {
        fprintf(stderr, "MetadataCreateDir RPC failed\n");
        return ret;
    }

    /* Check response status */
    if (resp.status != 0) {
        fprintf(stderr, "MetadataCreateDir failed with status: %d\n", resp.status);
        return BENCHFS_ERROR;
    }

    if (inode) {
        *inode = resp.inode;
    }

    return BENCHFS_SUCCESS;
}

int benchfs_metadata_delete_file(
    benchfs_client_t *client,
    const char *path
) {
    if (!client || !path) {
        return BENCHFS_INVALID_ARG;
    }

    size_t path_len = strlen(path);
    if (path_len == 0 || path_len > MAX_PATH_LENGTH) {
        return BENCHFS_INVALID_ARG;
    }

    /* Prepare request */
    benchfs_metadata_delete_request_t req;
    memset(&req, 0, sizeof(req));
    req.path_len = (uint32_t)path_len;
    req.entry_type = 1;  /* File */

    /* Send request and receive response */
    benchfs_metadata_delete_response_t resp;
    int ret = send_am_request(
        client,
        RPC_METADATA_DELETE,
        REPLY_METADATA_DELETE,
        &req, sizeof(req),
        path, path_len,
        &resp, sizeof(resp),
        NULL, NULL
    );

    if (ret != BENCHFS_SUCCESS) {
        fprintf(stderr, "MetadataDeleteFile RPC failed\n");
        return ret;
    }

    /* Check response status */
    if (resp.status != 0) {
        fprintf(stderr, "MetadataDeleteFile failed with status: %d\n", resp.status);
        return BENCHFS_ERROR;
    }

    return BENCHFS_SUCCESS;
}

int benchfs_metadata_delete_dir(
    benchfs_client_t *client,
    const char *path
) {
    if (!client || !path) {
        return BENCHFS_INVALID_ARG;
    }

    size_t path_len = strlen(path);
    if (path_len == 0 || path_len > MAX_PATH_LENGTH) {
        return BENCHFS_INVALID_ARG;
    }

    /* Prepare request */
    benchfs_metadata_delete_request_t req;
    memset(&req, 0, sizeof(req));
    req.path_len = (uint32_t)path_len;
    req.entry_type = 2;  /* Directory */

    /* Send request and receive response */
    benchfs_metadata_delete_response_t resp;
    int ret = send_am_request(
        client,
        RPC_METADATA_DELETE,
        REPLY_METADATA_DELETE,
        &req, sizeof(req),
        path, path_len,
        &resp, sizeof(resp),
        NULL, NULL
    );

    if (ret != BENCHFS_SUCCESS) {
        fprintf(stderr, "MetadataDeleteDir RPC failed\n");
        return ret;
    }

    /* Check response status */
    if (resp.status != 0) {
        fprintf(stderr, "MetadataDeleteDir failed with status: %d\n", resp.status);
        return BENCHFS_ERROR;
    }

    return BENCHFS_SUCCESS;
}

int benchfs_metadata_update(
    benchfs_client_t *client,
    const char *path,
    uint64_t new_size,
    uint32_t new_mode,
    uint8_t update_mask
) {
    if (!client || !path) {
        return BENCHFS_INVALID_ARG;
    }

    size_t path_len = strlen(path);
    if (path_len == 0 || path_len > MAX_PATH_LENGTH) {
        return BENCHFS_INVALID_ARG;
    }

    /* Prepare request */
    benchfs_metadata_update_request_t req;
    memset(&req, 0, sizeof(req));
    req.new_size = new_size;
    req.new_mode = new_mode;
    req.path_len = (uint32_t)path_len;
    req.update_mask = update_mask;

    /* Send request and receive response */
    benchfs_metadata_update_response_t resp;
    int ret = send_am_request(
        client,
        RPC_METADATA_UPDATE,
        REPLY_METADATA_UPDATE,
        &req, sizeof(req),
        path, path_len,
        &resp, sizeof(resp),
        NULL, NULL
    );

    if (ret != BENCHFS_SUCCESS) {
        fprintf(stderr, "MetadataUpdate RPC failed\n");
        return ret;
    }

    /* Check response status */
    if (resp.status != 0) {
        fprintf(stderr, "MetadataUpdate failed with status: %d\n", resp.status);
        return BENCHFS_ERROR;
    }

    return BENCHFS_SUCCESS;
}

/* ========================================================================== */
/* Data Operations                                                            */
/* ========================================================================== */

int benchfs_read_chunk(
    benchfs_client_t *client,
    const char *path,
    uint64_t chunk_index,
    uint64_t offset,
    void *buffer,
    uint64_t length,
    uint64_t *bytes_read
) {
    if (!client || !path || !buffer || length == 0) {
        return BENCHFS_INVALID_ARG;
    }

    size_t path_len = strlen(path);
    if (path_len == 0 || path_len > MAX_PATH_LENGTH) {
        return BENCHFS_INVALID_ARG;
    }

    /* Prepare request */
    benchfs_read_chunk_request_t req;
    memset(&req, 0, sizeof(req));
    req.chunk_index = chunk_index;
    req.offset = offset;
    req.length = length;
    req.path_len = (uint64_t)path_len;

    /* Send request and receive response */
    benchfs_read_chunk_response_t resp;
    size_t data_size = length;
    int ret = send_am_request(
        client,
        RPC_READ_CHUNK,
        REPLY_READ_CHUNK,
        &req, sizeof(req),
        path, path_len,
        &resp, sizeof(resp),
        buffer, &data_size
    );

    if (ret != BENCHFS_SUCCESS) {
        fprintf(stderr, "ReadChunk RPC failed\n");
        return ret;
    }

    /* Check response status */
    if (resp.status != 0) {
        fprintf(stderr, "ReadChunk failed with status: %d\n", resp.status);
        return BENCHFS_ERROR;
    }

    if (bytes_read) {
        *bytes_read = resp.bytes_read;
    }

    return BENCHFS_SUCCESS;
}

int benchfs_write_chunk(
    benchfs_client_t *client,
    const char *path,
    uint64_t chunk_index,
    uint64_t offset,
    const void *data,
    uint64_t length,
    uint64_t *bytes_written
) {
    if (!client || !path || !data || length == 0) {
        return BENCHFS_INVALID_ARG;
    }

    size_t path_len = strlen(path);
    if (path_len == 0 || path_len > MAX_PATH_LENGTH) {
        return BENCHFS_INVALID_ARG;
    }

    /* Prepare request */
    benchfs_write_chunk_request_t req;
    memset(&req, 0, sizeof(req));
    req.chunk_index = chunk_index;
    req.offset = offset;
    req.length = length;
    req.path_len = (uint64_t)path_len;

    /* Prepare payload: path + data */
    size_t payload_size = path_len + length;
    void *payload = malloc(payload_size);
    if (!payload) {
        return BENCHFS_NO_MEMORY;
    }

    memcpy(payload, path, path_len);
    memcpy((char *)payload + path_len, data, length);

    /* Send request and receive response */
    benchfs_write_chunk_response_t resp;
    int ret = send_am_request(
        client,
        RPC_WRITE_CHUNK,
        REPLY_WRITE_CHUNK,
        &req, sizeof(req),
        payload, payload_size,
        &resp, sizeof(resp),
        NULL, NULL
    );

    free(payload);

    if (ret != BENCHFS_SUCCESS) {
        fprintf(stderr, "WriteChunk RPC failed\n");
        return ret;
    }

    /* Check response status */
    if (resp.status != 0) {
        fprintf(stderr, "WriteChunk failed with status: %d\n", resp.status);
        return BENCHFS_ERROR;
    }

    if (bytes_written) {
        *bytes_written = resp.bytes_written;
    }

    return BENCHFS_SUCCESS;
}

/* ========================================================================== */
/* Shutdown Operation                                                         */
/* ========================================================================== */

int benchfs_shutdown_server(benchfs_client_t *client) {
    if (!client) {
        return BENCHFS_INVALID_ARG;
    }

    /* Prepare request */
    benchfs_shutdown_request_t req;
    memset(&req, 0, sizeof(req));
    req.magic = SHUTDOWN_MAGIC;

    /* Send request and receive response */
    benchfs_shutdown_response_t resp;
    int ret = send_am_request(
        client,
        RPC_SHUTDOWN,
        REPLY_SHUTDOWN,
        &req, sizeof(req),
        NULL, 0,
        &resp, sizeof(resp),
        NULL, NULL
    );

    if (ret != BENCHFS_SUCCESS) {
        fprintf(stderr, "Shutdown RPC failed\n");
        return ret;
    }

    /* Check response */
    if (resp.success != 1) {
        fprintf(stderr, "Shutdown request was not accepted\n");
        return BENCHFS_ERROR;
    }

    return BENCHFS_SUCCESS;
}
