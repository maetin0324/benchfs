/*
 * BenchFS C API for IOR Integration
 *
 * This header provides a C interface to the BenchFS distributed filesystem.
 * It is designed to be used as a backend for the IOR benchmark tool.
 */

#ifndef BENCHFS_C_API_H
#define BENCHFS_C_API_H

#include <stdint.h>
#include <stddef.h>
#include <sys/types.h>
#include <sys/stat.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque handle types */
typedef struct benchfs_context_t benchfs_context_t;
typedef struct benchfs_file_t benchfs_file_t;

/* File open flags (matching Linux O_* flags) */
#define BENCHFS_O_RDONLY    0x0000
#define BENCHFS_O_WRONLY    0x0001
#define BENCHFS_O_RDWR      0x0002
#define BENCHFS_O_CREAT     0x0040
#define BENCHFS_O_EXCL      0x0080
#define BENCHFS_O_TRUNC     0x0200
#define BENCHFS_O_APPEND    0x0400

/* Error codes */
#define BENCHFS_SUCCESS     0
#define BENCHFS_ERROR      -1
#define BENCHFS_ENOENT     -2
#define BENCHFS_EIO        -3
#define BENCHFS_ENOMEM     -4
#define BENCHFS_EINVAL     -5

/**
 * Initialize the BenchFS context
 *
 * This must be called before any other BenchFS operations.
 * The function will determine the node role (server/client) based on
 * MPI rank or environment variables.
 *
 * @param node_id Node identifier (typically MPI rank)
 * @param registry_dir Shared directory for service discovery
 * @param data_dir Data directory (for server nodes)
 * @param is_server 1 if this node is a server, 0 for client
 * @return Pointer to context on success, NULL on failure
 */
benchfs_context_t* benchfs_init(
    const char* node_id,
    const char* registry_dir,
    const char* data_dir,
    int is_server
);

/**
 * Finalize and cleanup the BenchFS context
 *
 * @param ctx Context to finalize
 */
void benchfs_finalize(benchfs_context_t* ctx);

/**
 * Create a new file
 *
 * @param ctx BenchFS context
 * @param path File path
 * @param flags Open flags (BENCHFS_O_* constants)
 * @param mode Permission mode
 * @return File handle on success, NULL on failure
 */
benchfs_file_t* benchfs_create(
    benchfs_context_t* ctx,
    const char* path,
    int flags,
    mode_t mode
);

/**
 * Open an existing file
 *
 * @param ctx BenchFS context
 * @param path File path
 * @param flags Open flags (BENCHFS_O_* constants)
 * @return File handle on success, NULL on failure
 */
benchfs_file_t* benchfs_open(
    benchfs_context_t* ctx,
    const char* path,
    int flags
);

/**
 * Close a file
 *
 * @param file File handle to close
 * @return BENCHFS_SUCCESS on success, error code on failure
 */
int benchfs_close(benchfs_file_t* file);

/**
 * Write data to a file
 *
 * @param file File handle
 * @param buffer Data buffer
 * @param size Number of bytes to write
 * @param offset Offset in the file
 * @return Number of bytes written on success, negative error code on failure
 */
ssize_t benchfs_write(
    benchfs_file_t* file,
    const void* buffer,
    size_t size,
    off_t offset
);

/**
 * Read data from a file
 *
 * @param file File handle
 * @param buffer Buffer to receive data
 * @param size Number of bytes to read
 * @param offset Offset in the file
 * @return Number of bytes read on success, negative error code on failure
 */
ssize_t benchfs_read(
    benchfs_file_t* file,
    void* buffer,
    size_t size,
    off_t offset
);

/**
 * Synchronize file data to storage
 *
 * @param file File handle
 * @return BENCHFS_SUCCESS on success, error code on failure
 */
int benchfs_fsync(benchfs_file_t* file);

/**
 * Remove (delete) a file
 *
 * @param ctx BenchFS context
 * @param path File path
 * @return BENCHFS_SUCCESS on success, error code on failure
 */
int benchfs_remove(benchfs_context_t* ctx, const char* path);

/**
 * Get file status
 *
 * @param ctx BenchFS context
 * @param path File path
 * @param buf Buffer to receive file status
 * @return BENCHFS_SUCCESS on success, error code on failure
 */
int benchfs_stat(benchfs_context_t* ctx, const char* path, struct stat* buf);

/**
 * Get file size
 *
 * @param ctx BenchFS context
 * @param path File path
 * @return File size on success, negative error code on failure
 */
off_t benchfs_get_file_size(benchfs_context_t* ctx, const char* path);

/**
 * Get the last error message
 *
 * @return Pointer to error message string
 */
const char* benchfs_get_error(void);

#ifdef __cplusplus
}
#endif

#endif /* BENCHFS_C_API_H */
