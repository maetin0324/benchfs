/*
 * BenchFS C API Implementation using C Client Library
 *
 * This file implements the benchfs_c_api.h interface using the
 * pure C BenchFS client library.
 */

#include "benchfs_c_api.h"
#include "benchfs_client.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/stat.h>
#include <unistd.h>

/* Thread-local error message storage */
static __thread char error_msg[256] = {0};

/* Context structure */
struct benchfs_context_t {
    benchfs_client_t *client;
    char *node_id;
    char *registry_dir;
    int is_server;
};

/* File handle structure */
struct benchfs_file_t {
    char *path;
    int flags;
    uint64_t file_size;
    uint64_t current_offset;
    benchfs_context_t *ctx;
};

#define CHUNK_SIZE (4 * 1024 * 1024)  /* 4MB */

static void set_error(const char *msg) {
    strncpy(error_msg, msg, sizeof(error_msg) - 1);
    error_msg[sizeof(error_msg) - 1] = '\0';
}

/* ========================================================================== */
/* Context Management                                                         */
/* ========================================================================== */

benchfs_context_t* benchfs_init(
    const char* node_id,
    const char* registry_dir,
    const char* data_dir,
    int is_server
) {
    (void)data_dir;  /* Not used in client-only mode */

    /* Allocate context */
    benchfs_context_t *ctx = calloc(1, sizeof(benchfs_context_t));
    if (!ctx) {
        set_error("Failed to allocate context");
        return NULL;
    }

    ctx->node_id = strdup(node_id);
    ctx->registry_dir = strdup(registry_dir);
    ctx->is_server = is_server;

    /* Create client */
    ctx->client = benchfs_client_create();
    if (!ctx->client) {
        set_error("Failed to create BenchFS client");
        free(ctx->node_id);
        free(ctx->registry_dir);
        free(ctx);
        return NULL;
    }

    /* Read server socket address from registry (node_0.am_hostname) */
    char addr_file[512];
    snprintf(addr_file, sizeof(addr_file), "%s/node_0.am_hostname", registry_dir);

    printf("[DEBUG] Reading socket address from %s\n", addr_file);

    FILE *af = fopen(addr_file, "r");
    if (!af) {
        fprintf(stderr, "[ERROR] Failed to open address file: %s (errno=%d: %s)\n",
                addr_file, errno, strerror(errno));
        set_error("Failed to open socket address file");
        benchfs_client_destroy(ctx->client);
        free(ctx->node_id);
        free(ctx->registry_dir);
        free(ctx);
        return NULL;
    }

    /* Read socket address (IP:port format) */
    char server_addr[256];
    if (!fgets(server_addr, sizeof(server_addr), af)) {
        fprintf(stderr, "[ERROR] Failed to read socket address from %s\n", addr_file);
        set_error("Failed to read socket address");
        fclose(af);
        benchfs_client_destroy(ctx->client);
        free(ctx->node_id);
        free(ctx->registry_dir);
        free(ctx);
        return NULL;
    }
    fclose(af);

    /* Remove trailing newline if present */
    size_t len = strlen(server_addr);
    if (len > 0 && server_addr[len-1] == '\n') {
        server_addr[len-1] = '\0';
    }

    printf("[DEBUG] Connecting to server at %s using socket connection\n", server_addr);

    int ret = benchfs_client_connect(ctx->client, server_addr);

    if (ret != BENCHFS_SUCCESS) {
        fprintf(stderr, "[ERROR] benchfs_client_connect() failed with code %d\n", ret);
        set_error("Failed to connect to BenchFS server");
        benchfs_client_destroy(ctx->client);
        free(ctx->node_id);
        free(ctx->registry_dir);
        free(ctx);
        return NULL;
    }

    printf("[DEBUG] Successfully connected to BenchFS server using socket connection\n");

    return ctx;
}

void benchfs_finalize(benchfs_context_t* ctx) {
    if (!ctx) return;

    if (ctx->client) {
        benchfs_client_destroy(ctx->client);
    }
    free(ctx->node_id);
    free(ctx->registry_dir);
    free(ctx);
}

/* ========================================================================== */
/* File Operations                                                            */
/* ========================================================================== */

benchfs_file_t* benchfs_create(
    benchfs_context_t* ctx,
    const char* path,
    int flags,
    mode_t mode
) {
    if (!ctx || !path) {
        set_error("Invalid arguments");
        return NULL;
    }

    /* Create file metadata */
    uint64_t inode;
    int ret = benchfs_metadata_create_file(ctx->client, path, 0, mode, &inode);
    if (ret != BENCHFS_SUCCESS && ret != BENCHFS_ERROR) {
        set_error("Failed to create file metadata");
        return NULL;
    }

    /* Allocate file handle */
    benchfs_file_t *file = calloc(1, sizeof(benchfs_file_t));
    if (!file) {
        set_error("Failed to allocate file handle");
        return NULL;
    }

    file->path = strdup(path);
    file->flags = flags;
    file->file_size = 0;
    file->current_offset = 0;
    file->ctx = ctx;

    return file;
}

benchfs_file_t* benchfs_open(
    benchfs_context_t* ctx,
    const char* path,
    int flags
) {
    if (!ctx || !path) {
        set_error("Invalid arguments");
        return NULL;
    }

    /* Look up file metadata */
    benchfs_metadata_lookup_response_t lookup_resp;
    int ret = benchfs_metadata_lookup(ctx->client, path, &lookup_resp);
    if (ret != BENCHFS_SUCCESS || lookup_resp.entry_type != 1) {
        set_error("File not found");
        return NULL;
    }

    /* Allocate file handle */
    benchfs_file_t *file = calloc(1, sizeof(benchfs_file_t));
    if (!file) {
        set_error("Failed to allocate file handle");
        return NULL;
    }

    file->path = strdup(path);
    file->flags = flags;
    file->file_size = lookup_resp.size;
    file->current_offset = 0;
    file->ctx = ctx;

    return file;
}

int benchfs_close(benchfs_file_t* file) {
    if (!file) return BENCHFS_ERROR;

    free(file->path);
    free(file);
    return BENCHFS_SUCCESS;
}

ssize_t benchfs_write(
    benchfs_file_t* file,
    const void* buffer,
    size_t size,
    off_t offset
) {
    if (!file || !buffer) {
        set_error("Invalid arguments");
        return -1;
    }

    size_t remaining = size;
    size_t total_written = 0;
    const char *buf_ptr = (const char *)buffer;

    while (remaining > 0) {
        uint64_t chunk_index = (offset + total_written) / CHUNK_SIZE;
        uint64_t chunk_offset = (offset + total_written) % CHUNK_SIZE;
        uint64_t chunk_remaining = CHUNK_SIZE - chunk_offset;
        uint64_t xfer_size = (remaining < chunk_remaining) ? remaining : chunk_remaining;

        uint64_t bytes_written;
        int ret = benchfs_write_chunk(file->ctx->client, file->path,
                                      chunk_index, chunk_offset,
                                      buf_ptr + total_written, xfer_size,
                                      &bytes_written);
        if (ret != BENCHFS_SUCCESS) {
            set_error("Write chunk failed");
            return (total_written > 0) ? (ssize_t)total_written : -1;
        }

        total_written += bytes_written;
        remaining -= bytes_written;

        if (bytes_written < xfer_size) break;
    }

    /* Update file size if extended */
    uint64_t new_size = offset + total_written;
    if (new_size > file->file_size) {
        benchfs_metadata_update(file->ctx->client, file->path, new_size, 0, UPDATE_SIZE);
        file->file_size = new_size;
    }

    return (ssize_t)total_written;
}

ssize_t benchfs_read(
    benchfs_file_t* file,
    void* buffer,
    size_t size,
    off_t offset
) {
    if (!file || !buffer) {
        set_error("Invalid arguments");
        return -1;
    }

    size_t remaining = size;
    size_t total_read = 0;
    char *buf_ptr = (char *)buffer;

    while (remaining > 0) {
        uint64_t chunk_index = (offset + total_read) / CHUNK_SIZE;
        uint64_t chunk_offset = (offset + total_read) % CHUNK_SIZE;
        uint64_t chunk_remaining = CHUNK_SIZE - chunk_offset;
        uint64_t xfer_size = (remaining < chunk_remaining) ? remaining : chunk_remaining;

        uint64_t bytes_read;
        int ret = benchfs_read_chunk(file->ctx->client, file->path,
                                     chunk_index, chunk_offset,
                                     buf_ptr + total_read, xfer_size,
                                     &bytes_read);
        if (ret != BENCHFS_SUCCESS) {
            set_error("Read chunk failed");
            return (total_read > 0) ? (ssize_t)total_read : -1;
        }

        total_read += bytes_read;
        remaining -= bytes_read;

        if (bytes_read < xfer_size) break;
    }

    return (ssize_t)total_read;
}

int benchfs_fsync(benchfs_file_t* file) {
    /* Writes are synchronous in BenchFS */
    (void)file;
    return BENCHFS_SUCCESS;
}

int benchfs_remove(benchfs_context_t* ctx, const char* path) {
    if (!ctx || !path) {
        set_error("Invalid arguments");
        return BENCHFS_ERROR;
    }

    int ret = benchfs_metadata_delete_file(ctx->client, path);
    if (ret != BENCHFS_SUCCESS) {
        set_error("Failed to delete file");
        return BENCHFS_ERROR;
    }

    return BENCHFS_SUCCESS;
}

off_t benchfs_get_file_size(benchfs_context_t* ctx, const char* path) {
    if (!ctx || !path) {
        set_error("Invalid arguments");
        return -1;
    }

    benchfs_metadata_lookup_response_t lookup_resp;
    int ret = benchfs_metadata_lookup(ctx->client, path, &lookup_resp);
    if (ret != BENCHFS_SUCCESS || lookup_resp.entry_type != 1) {
        set_error("File not found");
        return -1;
    }

    return (off_t)lookup_resp.size;
}

int benchfs_mkdir(benchfs_context_t* ctx, const char* path, mode_t mode) {
    if (!ctx || !path) {
        set_error("Invalid arguments");
        return BENCHFS_ERROR;
    }

    uint64_t inode;
    int ret = benchfs_metadata_create_dir(ctx->client, path, mode, &inode);
    if (ret != BENCHFS_SUCCESS) {
        set_error("Failed to create directory");
        return BENCHFS_ERROR;
    }

    return BENCHFS_SUCCESS;
}

int benchfs_rmdir(benchfs_context_t* ctx, const char* path) {
    if (!ctx || !path) {
        set_error("Invalid arguments");
        return BENCHFS_ERROR;
    }

    int ret = benchfs_metadata_delete_dir(ctx->client, path);
    if (ret != BENCHFS_SUCCESS) {
        set_error("Failed to delete directory");
        return BENCHFS_ERROR;
    }

    return BENCHFS_SUCCESS;
}

int benchfs_stat(benchfs_context_t* ctx, const char* path, struct stat* buf) {
    if (!ctx || !path || !buf) {
        set_error("Invalid arguments");
        return BENCHFS_ERROR;
    }

    benchfs_metadata_lookup_response_t lookup_resp;
    int ret = benchfs_metadata_lookup(ctx->client, path, &lookup_resp);
    if (ret != BENCHFS_SUCCESS || lookup_resp.entry_type == 0) {
        set_error("File not found");
        errno = ENOENT;
        return BENCHFS_ERROR;
    }

    memset(buf, 0, sizeof(*buf));
    buf->st_ino = lookup_resp.inode;
    buf->st_size = lookup_resp.size;
    buf->st_blksize = CHUNK_SIZE;
    buf->st_blocks = (lookup_resp.size + 511) / 512;

    if (lookup_resp.entry_type == 1) {
        buf->st_mode = S_IFREG | 0644;
        buf->st_nlink = 1;
    } else if (lookup_resp.entry_type == 2) {
        buf->st_mode = S_IFDIR | 0755;
        buf->st_nlink = 2;
    }

    return BENCHFS_SUCCESS;
}

int benchfs_access(benchfs_context_t* ctx, const char* path, int mode) {
    (void)mode;

    if (!ctx || !path) {
        set_error("Invalid arguments");
        return BENCHFS_ERROR;
    }

    benchfs_metadata_lookup_response_t lookup_resp;
    int ret = benchfs_metadata_lookup(ctx->client, path, &lookup_resp);
    if (ret != BENCHFS_SUCCESS || lookup_resp.entry_type == 0) {
        errno = ENOENT;
        return BENCHFS_ERROR;
    }

    return BENCHFS_SUCCESS;
}

/* Stub implementations for unsupported operations */
int benchfs_rename(benchfs_context_t* ctx, const char* oldpath, const char* newpath) {
    (void)ctx; (void)oldpath; (void)newpath;
    set_error("Rename not implemented");
    return BENCHFS_ERROR;
}

int benchfs_truncate(benchfs_context_t* ctx, const char* path, off_t size) {
    if (!ctx || !path) {
        set_error("Invalid arguments");
        return BENCHFS_ERROR;
    }

    int ret = benchfs_metadata_update(ctx->client, path, size, 0, UPDATE_SIZE);
    if (ret != BENCHFS_SUCCESS) {
        set_error("Failed to truncate file");
        return BENCHFS_ERROR;
    }

    return BENCHFS_SUCCESS;
}

off_t benchfs_lseek(benchfs_file_t* file, off_t offset, int whence) {
    if (!file) {
        set_error("Invalid file handle");
        return -1;
    }

    switch (whence) {
        case SEEK_SET:
            file->current_offset = offset;
            break;
        case SEEK_CUR:
            file->current_offset += offset;
            break;
        case SEEK_END:
            file->current_offset = file->file_size + offset;
            break;
        default:
            set_error("Invalid whence");
            return -1;
    }

    return (off_t)file->current_offset;
}

const char* benchfs_get_error(void) {
    return error_msg[0] ? error_msg : "No error";
}

/* Not implemented - stubs for compatibility */
int benchfs_stat_bfs(benchfs_context_t* ctx, const char* path, benchfs_stat_t* buf) {
    (void)ctx; (void)path; (void)buf;
    set_error("benchfs_stat_bfs not implemented");
    return BENCHFS_ERROR;
}
