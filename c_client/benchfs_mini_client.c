/**
 * BenchFS Mini C Client
 *
 * Pure C implementation of BenchFS Mini client using UCX API directly.
 * This client connects to BenchFS Mini servers and performs read/write operations
 * using UCX stream API, matching the server-side implementation.
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

#include <ucp/api/ucp.h>

// ============================================================================
// Constants
// ============================================================================

#define BENCHFS_MINI_SUCCESS 0
#define BENCHFS_MINI_ERROR -1

#define RPC_WRITE 1
#define RPC_READ 2

#define MAX_SERVERS 16
#define MAX_PATH_LEN 256
#define REGISTRY_CHECK_INTERVAL_SEC 1
#define MAX_REGISTRY_WAIT_SEC 30

// ============================================================================
// Global State
// ============================================================================

typedef struct {
    ucp_context_h ucp_context;
    ucp_worker_h ucp_worker;
    ucp_ep_h endpoints[MAX_SERVERS];
    int num_servers;
    char registry_dir[256];
    int initialized;
} benchfs_client_t;

static benchfs_client_t g_client = {0};

// ============================================================================
// UCX Helper Functions
// ============================================================================

/**
 * Stream send - sends exactly len bytes
 */
static int stream_send(ucp_ep_h ep, const void *buf, size_t len) {
    ucs_status_ptr_t status_ptr;
    ucs_status_t status;

    fprintf(stderr, "[stream_send] Sending %zu bytes\n", len);

    // Note: We use ucp_stream_send_nbx which is safer for request handling
    // But for UCX 1.x compatibility, we use the older API with proper request handling
    status_ptr = ucp_stream_send_nb(
        ep,
        buf,
        len,
        ucp_dt_make_contig(1),
        (ucp_send_callback_t)ucp_request_free,  // Free request on completion
        0
    );

    if (status_ptr == NULL) {
        // Completed immediately
        fprintf(stderr, "[stream_send] Send completed immediately\n");
        return 0;
    } else if (UCS_PTR_IS_ERR(status_ptr)) {
        status = UCS_PTR_STATUS(status_ptr);
        fprintf(stderr, "[stream_send] Send failed: %s\n", ucs_status_string(status));
        return -1;
    } else {
        // Need to wait for completion
        do {
            ucp_worker_progress(g_client.ucp_worker);
            status = ucp_request_check_status(status_ptr);
        } while (status == UCS_INPROGRESS);

        // Request will be freed by callback
        if (status != UCS_OK) {
            fprintf(stderr, "[stream_send] Send failed: %s\n", ucs_status_string(status));
            return -1;
        }
        fprintf(stderr, "[stream_send] Send completed after wait\n");
    }

    return 0;
}

/**
 * Callback for stream receive
 */
static void stream_recv_callback(void *request, ucs_status_t status, size_t length) {
    // This callback is called when the receive operation completes
    // We don't need to do anything special here as we'll poll for completion
    (void)request;
    (void)status;
    (void)length;
}

/**
 * Stream receive - receives exactly len bytes
 * May require multiple recv calls as UCX stream_recv can return partial data
 */
static int stream_recv(ucp_ep_h ep, void *buf, size_t len) {
    size_t total_received = 0;
    size_t length;
    ucs_status_ptr_t status_ptr;
    ucs_status_t status;

    fprintf(stderr, "[stream_recv] Receiving %zu bytes\n", len);

    while (total_received < len) {
        size_t remaining = len - total_received;
        void *current_buf = (char *)buf + total_received;

        status_ptr = ucp_stream_recv_nb(
            ep,
            current_buf,
            remaining,
            ucp_dt_make_contig(1),
            stream_recv_callback,
            &length,
            0
        );

        if (status_ptr == NULL) {
            // Completed immediately
            fprintf(stderr, "[stream_recv] Received %zu bytes immediately (total: %zu/%zu)\n",
                    length, total_received + length, len);
            total_received += length;

            if (length == 0) {
                // No data available yet, progress worker and retry
                ucp_worker_progress(g_client.ucp_worker);
                usleep(100);  // Small delay to avoid busy-wait
                continue;
            }
        } else if (UCS_PTR_IS_ERR(status_ptr)) {
            status = UCS_PTR_STATUS(status_ptr);
            fprintf(stderr, "[stream_recv] Receive failed: %s\n", ucs_status_string(status));
            return -1;
        } else {
            // Need to wait for completion
            do {
                ucp_worker_progress(g_client.ucp_worker);
                status = ucp_stream_recv_request_test(status_ptr, &length);
            } while (status == UCS_INPROGRESS);

            ucp_request_free(status_ptr);

            if (status != UCS_OK) {
                fprintf(stderr, "[stream_recv] Receive failed: %s\n", ucs_status_string(status));
                return -1;
            }

            fprintf(stderr, "[stream_recv] Received %zu bytes after wait (total: %zu/%zu)\n",
                    length, total_received + length, len);
            total_received += length;
        }
    }

    fprintf(stderr, "[stream_recv] Receive complete: %zu bytes\n", total_received);
    return 0;
}

// ============================================================================
// Registry Functions
// ============================================================================

/**
 * Resolve hostname to IP address
 */
static int resolve_hostname(const char *hostname, uint16_t port, struct sockaddr_in *addr) {
    struct hostent *he;

    memset(addr, 0, sizeof(*addr));
    addr->sin_family = AF_INET;
    addr->sin_port = htons(port);

    he = gethostbyname(hostname);
    if (he == NULL) {
        fprintf(stderr, "[Registry] Failed to resolve hostname %s: %s\n",
                hostname, hstrerror(h_errno));
        return -1;
    }

    memcpy(&addr->sin_addr, he->h_addr_list[0], he->h_length);
    return 0;
}

/**
 * Lookup server address from registry
 */
static int lookup_server(const char *registry_dir, int server_rank, struct sockaddr_in *addr) {
    char path[512];
    FILE *fp;
    char content[128];
    char hostname[64];
    int port;
    int attempts;

    snprintf(path, sizeof(path), "%s/server_%d.txt", registry_dir, server_rank);

    // Wait for registry file to appear
    for (attempts = 0; attempts < MAX_REGISTRY_WAIT_SEC; attempts++) {
        fp = fopen(path, "r");
        if (fp != NULL) {
            if (fgets(content, sizeof(content), fp) != NULL) {
                fclose(fp);

                // Parse "hostname:port" format
                if (sscanf(content, "%63[^:]:%d", hostname, &port) == 2) {
                    fprintf(stderr, "[Registry] Found server %d at %s:%d\n",
                            server_rank, hostname, port);
                    return resolve_hostname(hostname, port, addr);
                } else {
                    fprintf(stderr, "[Registry] Invalid format in %s: %s\n", path, content);
                    return -1;
                }
            }
            fclose(fp);
        }

        fprintf(stderr, "[Registry] Waiting for server %d registration (attempt %d/%d)...\n",
                server_rank, attempts + 1, MAX_REGISTRY_WAIT_SEC);
        sleep(REGISTRY_CHECK_INTERVAL_SEC);
    }

    fprintf(stderr, "[Registry] Server %d not found in registry after %d seconds\n",
            server_rank, MAX_REGISTRY_WAIT_SEC);
    return -1;
}

// ============================================================================
// UCX Initialization
// ============================================================================

/**
 * Initialize UCX context and worker
 */
int benchfs_mini_client_init(const char *registry_dir) {
    ucp_params_t ucp_params;
    ucp_worker_params_t worker_params;
    ucp_config_t *config;
    ucs_status_t status;

    if (g_client.initialized) {
        fprintf(stderr, "[Client] Already initialized\n");
        return BENCHFS_MINI_SUCCESS;
    }

    fprintf(stderr, "[Client] Initializing BenchFS Mini C client\n");
    fprintf(stderr, "[Client] Registry directory: %s\n", registry_dir);

    // Save registry directory
    strncpy(g_client.registry_dir, registry_dir, sizeof(g_client.registry_dir) - 1);

    // Read UCX configuration
    status = ucp_config_read(NULL, NULL, &config);
    if (status != UCS_OK) {
        fprintf(stderr, "[Client] Failed to read UCX config: %s\n", ucs_status_string(status));
        return BENCHFS_MINI_ERROR;
    }

    // Initialize UCP context
    memset(&ucp_params, 0, sizeof(ucp_params));
    ucp_params.field_mask = UCP_PARAM_FIELD_FEATURES;
    ucp_params.features = UCP_FEATURE_STREAM;

    status = ucp_init(&ucp_params, config, &g_client.ucp_context);
    ucp_config_release(config);

    if (status != UCS_OK) {
        fprintf(stderr, "[Client] Failed to initialize UCP context: %s\n", ucs_status_string(status));
        return BENCHFS_MINI_ERROR;
    }

    fprintf(stderr, "[Client] UCP context created\n");

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

    g_client.num_servers = 0;
    g_client.initialized = 1;

    fprintf(stderr, "[Client] Initialization complete\n");
    return BENCHFS_MINI_SUCCESS;
}

// ============================================================================
// Connection Management
// ============================================================================

/**
 * Connect to a server
 */
int benchfs_mini_client_connect(int server_rank) {
    struct sockaddr_in server_addr;
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

    fprintf(stderr, "[Client] Connecting to server rank %d\n", server_rank);

    // Lookup server address from registry
    if (lookup_server(g_client.registry_dir, server_rank, &server_addr) != 0) {
        return BENCHFS_MINI_ERROR;
    }

    char addr_str[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &server_addr.sin_addr, addr_str, sizeof(addr_str));
    fprintf(stderr, "[Client] Connecting to %s:%d\n", addr_str, ntohs(server_addr.sin_port));

    // Create endpoint
    memset(&ep_params, 0, sizeof(ep_params));
    ep_params.field_mask = UCP_EP_PARAM_FIELD_FLAGS |
                           UCP_EP_PARAM_FIELD_SOCK_ADDR;
    ep_params.flags = UCP_EP_PARAMS_FLAGS_CLIENT_SERVER;
    ep_params.sockaddr.addr = (const struct sockaddr *)&server_addr;
    ep_params.sockaddr.addrlen = sizeof(server_addr);

    status = ucp_ep_create(g_client.ucp_worker, &ep_params, &g_client.endpoints[server_rank]);
    if (status != UCS_OK) {
        fprintf(stderr, "[Client] Failed to create endpoint: %s\n", ucs_status_string(status));
        return BENCHFS_MINI_ERROR;
    }

    fprintf(stderr, "[Client] Connected to server rank %d\n", server_rank);

    if (server_rank >= g_client.num_servers) {
        g_client.num_servers = server_rank + 1;
    }

    return BENCHFS_MINI_SUCCESS;
}

// ============================================================================
// RPC Operations
// ============================================================================

/**
 * Write operation
 */
int benchfs_mini_client_write(const char *path, const void *data, size_t data_len, int server_rank) {
    ucp_ep_h ep;
    uint16_t rpc_id = RPC_WRITE;
    uint32_t path_len;
    uint32_t data_len_u32;
    int32_t status_response;
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

    fprintf(stderr, "[Client] rpc_write: path=%s, data_len=%zu, server_rank=%d\n",
            path, data_len, server_rank);

    path_len = (uint32_t)strlen(path);
    data_len_u32 = (uint32_t)data_len;

    // Send RPC ID
    ret = stream_send(ep, &rpc_id, sizeof(rpc_id));
    if (ret != 0) return BENCHFS_MINI_ERROR;

    // Send header (path_len + data_len)
    ret = stream_send(ep, &path_len, sizeof(path_len));
    if (ret != 0) return BENCHFS_MINI_ERROR;

    ret = stream_send(ep, &data_len_u32, sizeof(data_len_u32));
    if (ret != 0) return BENCHFS_MINI_ERROR;

    // Send path
    if (path_len > 0) {
        ret = stream_send(ep, path, path_len);
        if (ret != 0) return BENCHFS_MINI_ERROR;
    }

    // Send data
    if (data_len > 0) {
        ret = stream_send(ep, data, data_len);
        if (ret != 0) return BENCHFS_MINI_ERROR;
    }

    // Receive response
    ret = stream_recv(ep, &status_response, sizeof(status_response));
    if (ret != 0) return BENCHFS_MINI_ERROR;

    if (status_response != BENCHFS_MINI_SUCCESS) {
        fprintf(stderr, "[Client] Write failed with status: %d\n", status_response);
        return BENCHFS_MINI_ERROR;
    }

    fprintf(stderr, "[Client] Write completed successfully\n");
    return BENCHFS_MINI_SUCCESS;
}

/**
 * Read operation
 */
int benchfs_mini_client_read(const char *path, void *buffer, size_t buffer_len, int server_rank) {
    ucp_ep_h ep;
    uint16_t rpc_id = RPC_READ;
    uint32_t path_len;
    uint32_t buffer_len_u32;
    int32_t status_response;
    uint32_t data_len_response;
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

    fprintf(stderr, "[Client] rpc_read: path=%s, buffer_len=%zu, server_rank=%d\n",
            path, buffer_len, server_rank);

    path_len = (uint32_t)strlen(path);
    buffer_len_u32 = (uint32_t)buffer_len;

    // Send RPC ID
    ret = stream_send(ep, &rpc_id, sizeof(rpc_id));
    if (ret != 0) return BENCHFS_MINI_ERROR;

    // Send header (path_len + buffer_len)
    ret = stream_send(ep, &path_len, sizeof(path_len));
    if (ret != 0) return BENCHFS_MINI_ERROR;

    ret = stream_send(ep, &buffer_len_u32, sizeof(buffer_len_u32));
    if (ret != 0) return BENCHFS_MINI_ERROR;

    // Send path
    if (path_len > 0) {
        ret = stream_send(ep, path, path_len);
        if (ret != 0) return BENCHFS_MINI_ERROR;
    }

    // Receive response status
    ret = stream_recv(ep, &status_response, sizeof(status_response));
    if (ret != 0) return BENCHFS_MINI_ERROR;

    // Receive data length
    ret = stream_recv(ep, &data_len_response, sizeof(data_len_response));
    if (ret != 0) return BENCHFS_MINI_ERROR;

    if (status_response != BENCHFS_MINI_SUCCESS) {
        fprintf(stderr, "[Client] Read failed with status: %d\n", status_response);
        return BENCHFS_MINI_ERROR;
    }

    // Receive data
    if (data_len_response > 0) {
        if (data_len_response > buffer_len) {
            fprintf(stderr, "[Client] Response data (%u bytes) exceeds buffer size (%zu bytes)\n",
                    data_len_response, buffer_len);
            return BENCHFS_MINI_ERROR;
        }
        ret = stream_recv(ep, buffer, data_len_response);
        if (ret != 0) return BENCHFS_MINI_ERROR;
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

    fprintf(stderr, "[Client] Finalizing BenchFS Mini C client\n");

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

    g_client.initialized = 0;
    fprintf(stderr, "[Client] Finalization complete\n");
}
