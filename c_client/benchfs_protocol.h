#ifndef BENCHFS_PROTOCOL_H
#define BENCHFS_PROTOCOL_H

#include <stdint.h>

/* RPC IDs for data operations */
#define RPC_READ_CHUNK  10
#define RPC_WRITE_CHUNK 11

/* RPC IDs for metadata operations */
#define RPC_METADATA_LOOKUP      20
#define RPC_METADATA_CREATE_FILE 21
#define RPC_METADATA_CREATE_DIR  22
#define RPC_METADATA_DELETE      23
#define RPC_METADATA_UPDATE      24
#define RPC_SHUTDOWN             25

/* Reply stream IDs (RPC_ID + 100) */
#define REPLY_READ_CHUNK         110
#define REPLY_WRITE_CHUNK        111
#define REPLY_METADATA_LOOKUP    120
#define REPLY_METADATA_CREATE_FILE 121
#define REPLY_METADATA_CREATE_DIR  122
#define REPLY_METADATA_DELETE      123
#define REPLY_METADATA_UPDATE      124
#define REPLY_SHUTDOWN             125

/* Maximum path length */
#define MAX_PATH_LENGTH 4096

/* Shutdown magic number */
#define SHUTDOWN_MAGIC 0xDEADBEEFCAFEBABEULL

/* ========================================================================== */
/* Data Operations - ReadChunk/WriteChunk                                    */
/* ========================================================================== */

/* ReadChunk request header */
typedef struct {
    uint64_t chunk_index;  /* Chunk index to read */
    uint64_t offset;       /* Offset within the chunk */
    uint64_t length;       /* Length to read */
    uint64_t path_len;     /* Path length (path is sent in data section) */
} __attribute__((packed)) benchfs_read_chunk_request_t;

/* ReadChunk response header */
typedef struct {
    uint64_t bytes_read;   /* Number of bytes actually read */
    int32_t  status;       /* Status code (0 = success, non-zero = error) */
    uint8_t  _padding[4];  /* Padding for alignment */
} __attribute__((packed)) benchfs_read_chunk_response_t;

/* WriteChunk request header */
typedef struct {
    uint64_t chunk_index;  /* Chunk index to write */
    uint64_t offset;       /* Offset within the chunk */
    uint64_t length;       /* Length to write */
    uint64_t path_len;     /* Path length (path is sent in data section) */
} __attribute__((packed)) benchfs_write_chunk_request_t;

/* WriteChunk response header */
typedef struct {
    uint64_t bytes_written; /* Number of bytes actually written */
    int32_t  status;        /* Status code (0 = success, non-zero = error) */
    uint8_t  _padding[4];   /* Padding for alignment */
} __attribute__((packed)) benchfs_write_chunk_response_t;

/* ========================================================================== */
/* Metadata Operations                                                        */
/* ========================================================================== */

/* MetadataLookup request header */
typedef struct {
    uint32_t path_len;     /* Path length */
    uint8_t  _padding[4];  /* Padding for alignment */
} __attribute__((packed)) benchfs_metadata_lookup_request_t;

/* MetadataLookup response header */
typedef struct {
    uint64_t inode;        /* Inode number */
    uint64_t size;         /* File size (0 for directories) */
    int32_t  status;       /* Status code (0 = success, non-zero = error) */
    uint8_t  entry_type;   /* Entry type: 0 = not found, 1 = file, 2 = directory */
    uint8_t  _padding[3];  /* Padding for alignment */
} __attribute__((packed)) benchfs_metadata_lookup_response_t;

/* MetadataCreateFile request header */
typedef struct {
    uint64_t size;         /* Initial file size */
    uint32_t mode;         /* File mode (permissions) */
    uint32_t path_len;     /* Path length */
} __attribute__((packed)) benchfs_metadata_create_file_request_t;

/* MetadataCreateFile response header */
typedef struct {
    uint64_t inode;        /* Assigned inode number */
    int32_t  status;       /* Status code (0 = success, non-zero = error) */
    uint8_t  _padding[4];  /* Padding for alignment */
} __attribute__((packed)) benchfs_metadata_create_file_response_t;

/* MetadataCreateDir request header */
typedef struct {
    uint32_t mode;         /* Directory mode (permissions) */
    uint32_t path_len;     /* Path length */
} __attribute__((packed)) benchfs_metadata_create_dir_request_t;

/* MetadataCreateDir response header (same as CreateFile) */
typedef benchfs_metadata_create_file_response_t benchfs_metadata_create_dir_response_t;

/* MetadataDelete request header */
typedef struct {
    uint32_t path_len;     /* Path length */
    uint8_t  entry_type;   /* Entry type: 1 = file, 2 = directory */
    uint8_t  _padding[3];  /* Padding for alignment */
} __attribute__((packed)) benchfs_metadata_delete_request_t;

/* MetadataDelete response header */
typedef struct {
    int32_t  status;       /* Status code (0 = success, non-zero = error) */
    uint8_t  _padding[4];  /* Padding for alignment */
} __attribute__((packed)) benchfs_metadata_delete_response_t;

/* MetadataUpdate request header */
#define UPDATE_SIZE 0x01   /* Update file size */
#define UPDATE_MODE 0x02   /* Update file mode/permissions */

typedef struct {
    uint64_t new_size;     /* New file size (if UPDATE_SIZE is set) */
    uint32_t new_mode;     /* New file mode (if UPDATE_MODE is set) */
    uint32_t path_len;     /* Path length */
    uint8_t  update_mask;  /* Update mask (which fields to update) */
    uint8_t  _padding[7];  /* Padding for alignment */
} __attribute__((packed)) benchfs_metadata_update_request_t;

/* MetadataUpdate response header */
typedef struct {
    int32_t  status;       /* Status code (0 = success, non-zero = error) */
    uint8_t  _padding[4];  /* Padding for alignment */
} __attribute__((packed)) benchfs_metadata_update_response_t;

/* Shutdown request header */
typedef struct {
    uint64_t magic;        /* Magic number to verify request integrity */
} __attribute__((packed)) benchfs_shutdown_request_t;

/* Shutdown response header */
typedef struct {
    uint64_t success;      /* 1 if shutdown accepted, 0 otherwise */
} __attribute__((packed)) benchfs_shutdown_response_t;

#endif /* BENCHFS_PROTOCOL_H */
