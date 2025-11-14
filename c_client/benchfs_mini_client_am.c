/**
 * BenchFS Mini C Client - Active Message Version
 *
 * Pure C implementation of BenchFS Mini client using UCX Active Message API.
 * This client connects to BenchFS Mini servers and performs read/write operations
 * using UCX Active Message API, matching the Rust server-side AM implementation.
 *
 * Protocol:
 * - AM ID 1 (RPC_WRITE):
 *   Header: [path_len(4)] [data_len(4)] [path(variable)]
 *   Data: [file_data(variable)] or immediate data in header
 *   Reply: [status(4)]
 *
 * - AM ID 2 (RPC_READ):
 *   Header: [path_len(4)] [buffer_len(4)] [path(variable)]
 *   Data: none
 *   Reply Header: [status(4)] [data_len(4)]
 *   Reply Data: [file_data(variable)]
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <pthread.h>

#include <ucp/api/ucp.h>

// ============================================================================
// Constants
// ============================================================================

#define BENCHFS_MINI_SUCCESS 0
#define BENCHFS_MINI_ERROR -1

#define RPC_WRITE 1
#define RPC_READ 2
#define RPC_REPLY 0  // Reply messages use AM ID 0

#define MAX_SERVERS 16
#define MAX_PATH_LEN 1024
#define REGISTRY_CHECK_INTERVAL_SEC 1
#define MAX_REGISTRY_WAIT_SEC 30

// ============================================================================
// Global State
// ============================================================================

// Reply data structure
typedef struct {
    void *data;
    size_t data_len;
    uint8_t header[1024];  // For reply header
    size_t header_len;
    int ready;             // Flag indicating reply has been received
    pthread_mutex_t mutex;
    pthread_cond_t cond;
} reply_context_t;

typedef struct {
    ucp_context_h ucp_context;
    ucp_worker_h ucp_worker;
    ucp_ep_h endpoints[MAX_SERVERS];
    int num_servers;
    char registry_dir[256];
    int initialized;

    // Reply context for receiving AM replies
    reply_context_t reply_ctx;
} benchfs_client_t;

static benchfs_client_t g_client = {0};

// ============================================================================
// Active Message Reply Handler
// ============================================================================

/**
 * Callback for handling reply Active Messages (AM ID 0)
 */
static ucs_status_t am_reply_handler(void *arg, const void *header, size_t header_length,
                                     void *data, size_t length,
                                     const ucp_am_recv_param_t *param)
{
    reply_context_t *reply_ctx = (reply_context_t *)arg;

    fprintf(stderr, "[AM Reply Handler] Received reply: header_len=%zu, data_len=%zu\n",
            header_length, length);

    pthread_mutex_lock(&reply_ctx->mutex);

    // Copy header
    if (header_length > sizeof(reply_ctx->header)) {
        fprintf(stderr, "[AM Reply Handler] Header too large: %zu bytes\n", header_length);
        pthread_mutex_unlock(&reply_ctx->mutex);
        return UCS_OK;
    }
    memcpy(reply_ctx->header, header, header_length);
    reply_ctx->header_len = header_length;

    // Check if this is rendezvous (data needs to be received separately)
    if (param && (param->recv_attr & UCP_AM_RECV_ATTR_FLAG_RNDV)) {
        fprintf(stderr, "[AM Reply Handler] Rendezvous mode - data descriptor received\n");
        // For rendezvous, we would need to call ucp_am_recv_data_nbx()
        // For now, we'll handle this later if needed
    } else {
        // Eager mode - data is available immediately
        if (length > 0 && data != NULL) {
            reply_ctx->data = malloc(length);
            if (reply_ctx->data) {
                memcpy(reply_ctx->data, data, length);
                reply_ctx->data_len = length;
                fprintf(stderr, "[AM Reply Handler] Copied %zu bytes of data\n", length);
            }
        } else {
            reply_ctx->data = NULL;
            reply_ctx->data_len = 0;
        }
    }

    reply_ctx->ready = 1;
    pthread_cond_signal(&reply_ctx->cond);
    pthread_mutex_unlock(&reply_ctx->mutex);

    return UCS_OK;
}

// ============================================================================
// UCX Helper Functions
// ============================================================================

/**
 * Wait for reply Active Message
 */
static int wait_for_reply(reply_context_t *reply_ctx, int timeout_ms) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += timeout_ms / 1000;
    ts.tv_nsec += (timeout_ms % 1000) * 1000000;
    if (ts.tv_nsec >= 1000000000) {
        ts.tv_sec++;
        ts.tv_nsec -= 1000000000;
    }

    pthread_mutex_lock(&reply_ctx->mutex);

    while (!reply_ctx->ready) {
        // Progress worker while waiting
        pthread_mutex_unlock(&reply_ctx->mutex);
        ucp_worker_progress(g_client.ucp_worker);
        usleep(100);  // 0.1ms
        pthread_mutex_lock(&reply_ctx->mutex);

        struct timespec now;
        clock_gettime(CLOCK_REALTIME, &now);
        if (now.tv_sec > ts.tv_sec ||
            (now.tv_sec == ts.tv_sec && now.tv_nsec > ts.tv_nsec)) {
            pthread_mutex_unlock(&reply_ctx->mutex);
            fprintf(stderr, "[wait_for_reply] Timeout waiting for reply\n");
            return -1;
        }
    }

    pthread_mutex_unlock(&reply_ctx->mutex);
    return 0;
}

/**
 * Reset reply context before sending a new request
 */
static void reset_reply_context(reply_context_t *reply_ctx) {
    pthread_mutex_lock(&reply_ctx->mutex);

    if (reply_ctx->data) {
        free(reply_ctx->data);
        reply_ctx->data = NULL;
    }
    reply_ctx->data_len = 0;
    reply_ctx->header_len = 0;
    reply_ctx->ready = 0;

    pthread_mutex_unlock(&reply_ctx->mutex);
}

// ============================================================================
// Registry Functions
// ============================================================================

/**
 * Lookup server WorkerAddress from registry
 */
static int lookup_server_worker_addr(const char *registry_dir, int server_rank,
                                     void **worker_addr, size_t *addr_len) {
    char path[512];
    FILE *fp;
    int attempts;
    long file_size;

    snprintf(path, sizeof(path), "%s/server_%d_worker.bin", registry_dir, server_rank);

    // Wait for registry file to appear
    for (attempts = 0; attempts < MAX_REGISTRY_WAIT_SEC; attempts++) {
        fp = fopen(path, "rb");
        if (fp != NULL) {
            // Get file size
            fseek(fp, 0, SEEK_END);
            file_size = ftell(fp);
            fseek(fp, 0, SEEK_SET);

            if (file_size > 0) {
                *worker_addr = malloc(file_size);
                if (*worker_addr == NULL) {
                    fclose(fp);
                    fprintf(stderr, "[Registry] Failed to allocate memory for worker address\n");
                    return -1;
                }

                size_t read_size = fread(*worker_addr, 1, file_size, fp);
                fclose(fp);

                if (read_size == file_size) {
                    *addr_len = file_size;
                    fprintf(stderr, "[Registry] Found server %d WorkerAddress (%zu bytes)\n",
                            server_rank, *addr_len);
                    return 0;
                }

                free(*worker_addr);
                *worker_addr = NULL;
                fprintf(stderr, "[Registry] Failed to read worker address\n");
                return -1;
            }
            fclose(fp);
        }

        fprintf(stderr, "[Registry] Waiting for server %d WorkerAddress (attempt %d/%d)...\n",
                server_rank, attempts + 1, MAX_REGISTRY_WAIT_SEC);
        sleep(REGISTRY_CHECK_INTERVAL_SEC);
    }

    fprintf(stderr, "[Registry] Server %d WorkerAddress not found after %d seconds\n",
            server_rank, MAX_REGISTRY_WAIT_SEC);
    return -1;
}

// ============================================================================
// UCX Initialization
// ============================================================================

/**
 * Initialize UCX context and worker with Active Message support
 */
int benchfs_mini_client_init(const char *registry_dir) {
    ucp_params_t ucp_params;
    ucp_worker_params_t worker_params;
    ucp_worker_attr_t worker_attr;
    ucp_config_t *config;
    ucs_status_t status;
    ucp_am_handler_param_t am_handler_param;

    if (g_client.initialized) {
        fprintf(stderr, "[Client] Already initialized\n");
        return BENCHFS_MINI_SUCCESS;
    }

    fprintf(stderr, "[Client] Initializing BenchFS Mini C client (Active Message version)\n");
    fprintf(stderr, "[Client] Registry directory: %s\n", registry_dir);

    // Save registry directory
    strncpy(g_client.registry_dir, registry_dir, sizeof(g_client.registry_dir) - 1);

    // Initialize reply context
    pthread_mutex_init(&g_client.reply_ctx.mutex, NULL);
    pthread_cond_init(&g_client.reply_ctx.cond, NULL);
    g_client.reply_ctx.ready = 0;
    g_client.reply_ctx.data = NULL;
    g_client.reply_ctx.data_len = 0;

    // Read UCX configuration
    status = ucp_config_read(NULL, NULL, &config);
    if (status != UCS_OK) {
        fprintf(stderr, "[Client] Failed to read UCX config: %s\n", ucs_status_string(status));
        return BENCHFS_MINI_ERROR;
    }

    // Initialize UCP context with Active Message support
    memset(&ucp_params, 0, sizeof(ucp_params));
    ucp_params.field_mask = UCP_PARAM_FIELD_FEATURES;
    ucp_params.features = UCP_FEATURE_AM;  // Active Message feature

    status = ucp_init(&ucp_params, config, &g_client.ucp_context);
    ucp_config_release(config);

    if (status != UCS_OK) {
        fprintf(stderr, "[Client] Failed to initialize UCP context: %s\n", ucs_status_string(status));
        return BENCHFS_MINI_ERROR;
    }

    fprintf(stderr, "[Client] UCP context created with Active Message support\n");

    // Create worker
    memset(&worker_params, 0, sizeof(worker_params));
    worker_params.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
    worker_params.thread_mode = UCS_THREAD_MODE_SINGLE;

    status = ucp_worker_create(g_client.ucp_context, &worker_params, &g_client.ucp_worker);
    if (status != UCS_OK) {
        fprintf(stderr, "[Client] Failed to create UCP worker: %s\n", ucs_status_string(status));
        ucp_cleanup(g_client.ucp_context);
        return BENCHFS_MINI_ERROR;
    }

    fprintf(stderr, "[Client] UCP worker created\n");

    // Register Active Message handler for replies (AM ID 0)
    memset(&am_handler_param, 0, sizeof(am_handler_param));
    am_handler_param.field_mask = UCP_AM_HANDLER_PARAM_FIELD_ID |
                                   UCP_AM_HANDLER_PARAM_FIELD_CB |
                                   UCP_AM_HANDLER_PARAM_FIELD_ARG;
    am_handler_param.id = RPC_REPLY;
    am_handler_param.cb = am_reply_handler;
    am_handler_param.arg = &g_client.reply_ctx;

    status = ucp_worker_set_am_recv_handler(g_client.ucp_worker, &am_handler_param);
    if (status != UCS_OK) {
        fprintf(stderr, "[Client] Failed to set AM reply handler: %s\n", ucs_status_string(status));
        ucp_worker_destroy(g_client.ucp_worker);
        ucp_cleanup(g_client.ucp_context);
        return BENCHFS_MINI_ERROR;
    }

    fprintf(stderr, "[Client] Registered AM reply handler (ID=%d)\n", RPC_REPLY);

    g_client.num_servers = 0;
    g_client.initialized = 1;

    fprintf(stderr, "[Client] Initialization complete\n");
    return BENCHFS_MINI_SUCCESS;
}

// ============================================================================
// Connection Management
// ============================================================================

/**
 * Connect to a server using WorkerAddress
 */
int benchfs_mini_client_connect(int server_rank) {
    void *worker_addr = NULL;
    size_t addr_len = 0;
    ucp_ep_params_t ep_params;
    ucs_status_t status;

    if (!g_client.initialized) {
        fprintf(stderr, "[Client] Not initialized\n");
        return BENCHFS_MINI_ERROR;
    }

    if (server_rank >= MAX_SERVERS) {
        fprintf(stderr, "[Client] Server rank %d exceeds MAX_SERVERS %d\n", server_rank, MAX_SERVERS);
        return BENCHFS_MINI_ERROR;
    }

    fprintf(stderr, "[Client] Connecting to server rank %d using WorkerAddress\n", server_rank);

    // Lookup server WorkerAddress from registry
    if (lookup_server_worker_addr(g_client.registry_dir, server_rank, &worker_addr, &addr_len) != 0) {
        return BENCHFS_MINI_ERROR;
    }

    fprintf(stderr, "[Client] Found WorkerAddress for server %d (%zu bytes)\n", server_rank, addr_len);

    // Create endpoint using WorkerAddress
    memset(&ep_params, 0, sizeof(ep_params));
    ep_params.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS;
    ep_params.address = (const ucp_address_t *)worker_addr;

    status = ucp_ep_create(g_client.ucp_worker, &ep_params, &g_client.endpoints[server_rank]);

    free(worker_addr);  // Can free after ep_create

    if (status != UCS_OK) {
        fprintf(stderr, "[Client] Failed to create endpoint: %s\n", ucs_status_string(status));
        return BENCHFS_MINI_ERROR;
    }

    // Progress the worker to establish the connection
    fprintf(stderr, "[Client] Progressing worker to establish connection...\n");
    for (int i = 0; i < 100; i++) {
        ucp_worker_progress(g_client.ucp_worker);
        usleep(1000);  // 1ms delay
    }

    fprintf(stderr, "[Client] Connected to server rank %d\n", server_rank);

    if (server_rank >= g_client.num_servers) {
        g_client.num_servers = server_rank + 1;
    }

    return BENCHFS_MINI_SUCCESS;
}

// ============================================================================
// RPC Operations using Active Messages
// ============================================================================

/**
 * Send completion callback
 */
static void am_send_callback(void *request, ucs_status_t status, void *user_data) {
    (void)user_data;
    if (status != UCS_OK) {
        fprintf(stderr, "[AM Send] Completed with error: %s\n", ucs_status_string(status));
    }
    ucp_request_free(request);
}

/**
 * Write operation using Active Messages
 */
int benchfs_mini_client_write(const char *path, const void *data, size_t data_len, int server_rank) {
    ucp_ep_h ep;
    ucp_request_param_t send_param;
    ucs_status_ptr_t status_ptr;
    ucs_status_t status;
    int ret;

    if (!g_client.initialized || server_rank >= g_client.num_servers) {
        fprintf(stderr, "[Client] Invalid state or server rank\n");
        return BENCHFS_MINI_ERROR;
    }

    ep = g_client.endpoints[server_rank];
    if (ep == NULL) {
        fprintf(stderr, "[Client] No connection to server %d\n", server_rank);
        return BENCHFS_MINI_ERROR;
    }

    uint32_t path_len = (uint32_t)strlen(path);
    if (path_len > MAX_PATH_LEN) {
        fprintf(stderr, "[Client] Path length %u exceeds MAX_PATH_LEN %d\n", path_len, MAX_PATH_LEN);
        return BENCHFS_MINI_ERROR;
    }

    fprintf(stderr, "[Client] rpc_write_am: path=%s, path_len=%u, data_len=%zu, server_rank=%d\n",
            path, path_len, data_len, server_rank);

    // Build header: [path_len(4)] [data_len(4)] [path(variable)]
    size_t header_size = 8 + path_len;
    uint8_t *header = (uint8_t *)malloc(header_size);
    if (header == NULL) {
        fprintf(stderr, "[Client] Failed to allocate header buffer\n");
        return BENCHFS_MINI_ERROR;
    }

    // path_len (little-endian)
    header[0] = (path_len >> 0) & 0xFF;
    header[1] = (path_len >> 8) & 0xFF;
    header[2] = (path_len >> 16) & 0xFF;
    header[3] = (path_len >> 24) & 0xFF;

    // data_len (little-endian)
    uint32_t data_len_u32 = (uint32_t)data_len;
    header[4] = (data_len_u32 >> 0) & 0xFF;
    header[5] = (data_len_u32 >> 8) & 0xFF;
    header[6] = (data_len_u32 >> 16) & 0xFF;
    header[7] = (data_len_u32 >> 24) & 0xFF;

    // path
    memcpy(header + 8, path, path_len);

    // Reset reply context
    reset_reply_context(&g_client.reply_ctx);

    // Send Active Message with reply flag
    memset(&send_param, 0, sizeof(send_param));
    send_param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                              UCP_OP_ATTR_FIELD_FLAGS;
    send_param.cb.send = am_send_callback;
    send_param.flags = UCP_AM_SEND_FLAG_REPLY;  // Request reply

    fprintf(stderr, "[Client] Sending AM (ID=%d) with header_len=%zu, data_len=%zu\n",
            RPC_WRITE, header_size, data_len);

    status_ptr = ucp_am_send_nbx(ep, RPC_WRITE, header, header_size,
                                 data, data_len, &send_param);

    free(header);

    if (status_ptr == NULL) {
        // Completed immediately
        fprintf(stderr, "[Client] AM send completed immediately\n");
    } else if (UCS_PTR_IS_ERR(status_ptr)) {
        status = UCS_PTR_STATUS(status_ptr);
        fprintf(stderr, "[Client] AM send failed: %s\n", ucs_status_string(status));
        return BENCHFS_MINI_ERROR;
    } else {
        // Need to wait for send completion
        fprintf(stderr, "[Client] Waiting for AM send completion...\n");
        do {
            ucp_worker_progress(g_client.ucp_worker);
            status = ucp_request_check_status(status_ptr);
        } while (status == UCS_INPROGRESS);

        if (status != UCS_OK) {
            fprintf(stderr, "[Client] AM send failed after wait: %s\n", ucs_status_string(status));
            ucp_request_free(status_ptr);
            return BENCHFS_MINI_ERROR;
        }
        ucp_request_free(status_ptr);
        fprintf(stderr, "[Client] AM send completed\n");
    }

    // Wait for reply
    fprintf(stderr, "[Client] Waiting for reply...\n");
    ret = wait_for_reply(&g_client.reply_ctx, 10000);  // 10 second timeout
    if (ret != 0) {
        fprintf(stderr, "[Client] Failed to receive reply\n");
        return BENCHFS_MINI_ERROR;
    }

    // Parse reply header: [status(4)]
    if (g_client.reply_ctx.header_len < 4) {
        fprintf(stderr, "[Client] Reply header too short: %zu bytes\n", g_client.reply_ctx.header_len);
        return BENCHFS_MINI_ERROR;
    }

    int32_t status_response =
        (int32_t)g_client.reply_ctx.header[0] |
        ((int32_t)g_client.reply_ctx.header[1] << 8) |
        ((int32_t)g_client.reply_ctx.header[2] << 16) |
        ((int32_t)g_client.reply_ctx.header[3] << 24);

    if (status_response != BENCHFS_MINI_SUCCESS) {
        fprintf(stderr, "[Client] Write failed with status: %d\n", status_response);
        return BENCHFS_MINI_ERROR;
    }

    fprintf(stderr, "[Client] Write completed successfully\n");
    return BENCHFS_MINI_SUCCESS;
}

/**
 * Read operation using Active Messages
 */
int benchfs_mini_client_read(const char *path, void *buffer, size_t buffer_len, int server_rank) {
    ucp_ep_h ep;
    ucp_request_param_t send_param;
    ucs_status_ptr_t status_ptr;
    ucs_status_t status;
    int ret;

    if (!g_client.initialized || server_rank >= g_client.num_servers) {
        fprintf(stderr, "[Client] Invalid state or server rank\n");
        return BENCHFS_MINI_ERROR;
    }

    ep = g_client.endpoints[server_rank];
    if (ep == NULL) {
        fprintf(stderr, "[Client] No connection to server %d\n", server_rank);
        return BENCHFS_MINI_ERROR;
    }

    uint32_t path_len = (uint32_t)strlen(path);
    if (path_len > MAX_PATH_LEN) {
        fprintf(stderr, "[Client] Path length %u exceeds MAX_PATH_LEN %d\n", path_len, MAX_PATH_LEN);
        return BENCHFS_MINI_ERROR;
    }

    fprintf(stderr, "[Client] rpc_read_am: path=%s, path_len=%u, buffer_len=%zu, server_rank=%d\n",
            path, path_len, buffer_len, server_rank);

    // Build header: [path_len(4)] [buffer_len(4)] [path(variable)]
    size_t header_size = 8 + path_len;
    uint8_t *header = (uint8_t *)malloc(header_size);
    if (header == NULL) {
        fprintf(stderr, "[Client] Failed to allocate header buffer\n");
        return BENCHFS_MINI_ERROR;
    }

    // path_len (little-endian)
    header[0] = (path_len >> 0) & 0xFF;
    header[1] = (path_len >> 8) & 0xFF;
    header[2] = (path_len >> 16) & 0xFF;
    header[3] = (path_len >> 24) & 0xFF;

    // buffer_len (little-endian)
    uint32_t buffer_len_u32 = (uint32_t)buffer_len;
    header[4] = (buffer_len_u32 >> 0) & 0xFF;
    header[5] = (buffer_len_u32 >> 8) & 0xFF;
    header[6] = (buffer_len_u32 >> 16) & 0xFF;
    header[7] = (buffer_len_u32 >> 24) & 0xFF;

    // path
    memcpy(header + 8, path, path_len);

    // Reset reply context
    reset_reply_context(&g_client.reply_ctx);

    // Send Active Message with reply flag (no data payload for READ)
    memset(&send_param, 0, sizeof(send_param));
    send_param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                              UCP_OP_ATTR_FIELD_FLAGS;
    send_param.cb.send = am_send_callback;
    send_param.flags = UCP_AM_SEND_FLAG_REPLY;  // Request reply

    fprintf(stderr, "[Client] Sending READ AM (ID=%d) with header_len=%zu\n",
            RPC_READ, header_size);

    status_ptr = ucp_am_send_nbx(ep, RPC_READ, header, header_size,
                                 NULL, 0, &send_param);  // No data for READ

    free(header);

    if (status_ptr == NULL) {
        fprintf(stderr, "[Client] AM send completed immediately\n");
    } else if (UCS_PTR_IS_ERR(status_ptr)) {
        status = UCS_PTR_STATUS(status_ptr);
        fprintf(stderr, "[Client] AM send failed: %s\n", ucs_status_string(status));
        return BENCHFS_MINI_ERROR;
    } else {
        fprintf(stderr, "[Client] Waiting for AM send completion...\n");
        do {
            ucp_worker_progress(g_client.ucp_worker);
            status = ucp_request_check_status(status_ptr);
        } while (status == UCS_INPROGRESS);

        if (status != UCS_OK) {
            fprintf(stderr, "[Client] AM send failed after wait: %s\n", ucs_status_string(status));
            ucp_request_free(status_ptr);
            return BENCHFS_MINI_ERROR;
        }
        ucp_request_free(status_ptr);
        fprintf(stderr, "[Client] AM send completed\n");
    }

    // Wait for reply
    fprintf(stderr, "[Client] Waiting for reply...\n");
    ret = wait_for_reply(&g_client.reply_ctx, 10000);  // 10 second timeout
    if (ret != 0) {
        fprintf(stderr, "[Client] Failed to receive reply\n");
        return BENCHFS_MINI_ERROR;
    }

    // Parse reply header: [status(4)] [data_len(4)]
    if (g_client.reply_ctx.header_len < 8) {
        fprintf(stderr, "[Client] Reply header too short: %zu bytes\n", g_client.reply_ctx.header_len);
        return BENCHFS_MINI_ERROR;
    }

    int32_t status_response =
        (int32_t)g_client.reply_ctx.header[0] |
        ((int32_t)g_client.reply_ctx.header[1] << 8) |
        ((int32_t)g_client.reply_ctx.header[2] << 16) |
        ((int32_t)g_client.reply_ctx.header[3] << 24);

    uint32_t data_len_response =
        (uint32_t)g_client.reply_ctx.header[4] |
        ((uint32_t)g_client.reply_ctx.header[5] << 8) |
        ((uint32_t)g_client.reply_ctx.header[6] << 16) |
        ((uint32_t)g_client.reply_ctx.header[7] << 24);

    if (status_response != BENCHFS_MINI_SUCCESS) {
        fprintf(stderr, "[Client] Read failed with status: %d\n", status_response);
        return BENCHFS_MINI_ERROR;
    }

    // Copy data from reply context to user buffer
    if (data_len_response > 0) {
        if (data_len_response > buffer_len) {
            fprintf(stderr, "[Client] Response data (%u bytes) exceeds buffer size (%zu bytes)\n",
                    data_len_response, buffer_len);
            return BENCHFS_MINI_ERROR;
        }

        if (g_client.reply_ctx.data && g_client.reply_ctx.data_len == data_len_response) {
            memcpy(buffer, g_client.reply_ctx.data, data_len_response);
            fprintf(stderr, "[Client] Copied %u bytes to user buffer\n", data_len_response);
        } else {
            fprintf(stderr, "[Client] Reply data mismatch: expected %u, got %zu\n",
                    data_len_response, g_client.reply_ctx.data_len);
            return BENCHFS_MINI_ERROR;
        }
    }

    fprintf(stderr, "[Client] Read %u bytes successfully\n", data_len_response);
    return (int)data_len_response;
}

// ============================================================================
// Cleanup
// ============================================================================

/**
 * Finalize and cleanup
 */
void benchfs_mini_client_finalize(void) {
    int i;

    if (!g_client.initialized) {
        return;
    }

    fprintf(stderr, "[Client] Finalizing BenchFS Mini C client (AM version)\n");

    // Close all endpoints
    for (i = 0; i < g_client.num_servers; i++) {
        if (g_client.endpoints[i] != NULL) {
            ucp_ep_destroy(g_client.endpoints[i]);
            g_client.endpoints[i] = NULL;
        }
    }

    // Destroy worker
    if (g_client.ucp_worker != NULL) {
        ucp_worker_destroy(g_client.ucp_worker);
        g_client.ucp_worker = NULL;
    }

    // Cleanup context
    if (g_client.ucp_context != NULL) {
        ucp_cleanup(g_client.ucp_context);
        g_client.ucp_context = NULL;
    }

    // Cleanup reply context
    if (g_client.reply_ctx.data) {
        free(g_client.reply_ctx.data);
    }
    pthread_mutex_destroy(&g_client.reply_ctx.mutex);
    pthread_cond_destroy(&g_client.reply_ctx.cond);

    g_client.initialized = 0;
    fprintf(stderr, "[Client] Finalization complete\n");
}
