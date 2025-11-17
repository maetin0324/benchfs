#include "benchfs_client.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <errno.h>

/* Client structure */
struct benchfs_client {
    ucp_context_h  ucp_context;
    ucp_worker_h   ucp_worker;
    ucp_ep_h       ucp_ep;
    size_t         worker_addr_len;
    void          *worker_addr;
    int            connected;
    int            am_handlers_registered;  /* Flag to track AM handler registration */
};

/* AM handler IDs for reply streams */
static ucs_status_t am_reply_handler(void *arg, const void *header, size_t header_length,
                                     void *data, size_t data_length,
                                     const ucp_am_recv_param_t *param);

/* Helper function to wait for AM reply */
static int wait_for_am_reply(benchfs_client_t *client, uint32_t reply_stream_id,
                             void *response_header, size_t header_size,
                             void *response_data, size_t *data_size);

/* ========================================================================== */
/* Client Initialization and Cleanup                                         */
/* ========================================================================== */

benchfs_client_t *benchfs_client_create(void) {
    benchfs_client_t *client = NULL;
    ucp_config_t *config = NULL;
    ucp_params_t ucp_params;
    ucp_worker_params_t worker_params;
    ucs_status_t status;

    /* Enable UCX debug logging if UCX_DEBUG env var is set */
    const char *ucx_debug = getenv("UCX_DEBUG");
    if (ucx_debug && atoi(ucx_debug) > 0) {
        setenv("UCX_LOG_LEVEL", "debug", 1);
        printf("[DEBUG] UCX debug logging enabled\n");
    }

    /* Allocate client structure */
    client = (benchfs_client_t *)calloc(1, sizeof(benchfs_client_t));
    if (!client) {
        fprintf(stderr, "Failed to allocate client structure\n");
        return NULL;
    }

    /* Read UCP configuration */
    status = ucp_config_read(NULL, NULL, &config);
    if (status != UCS_OK) {
        fprintf(stderr, "Failed to read UCP config: %s\n", ucs_status_string(status));
        free(client);
        return NULL;
    }

    /* Initialize UCP context */
    memset(&ucp_params, 0, sizeof(ucp_params));
    ucp_params.field_mask = UCP_PARAM_FIELD_FEATURES |
                            UCP_PARAM_FIELD_REQUEST_SIZE |
                            UCP_PARAM_FIELD_REQUEST_INIT;
    ucp_params.features = UCP_FEATURE_AM;  /* Active Message support */
    ucp_params.request_size = 0;
    ucp_params.request_init = NULL;

    status = ucp_init(&ucp_params, config, &client->ucp_context);
    ucp_config_release(config);

    if (status != UCS_OK) {
        fprintf(stderr, "Failed to initialize UCP context: %s\n", ucs_status_string(status));
        free(client);
        return NULL;
    }

    /* Create UCP worker */
    memset(&worker_params, 0, sizeof(worker_params));
    worker_params.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
    worker_params.thread_mode = UCS_THREAD_MODE_SINGLE;

    status = ucp_worker_create(client->ucp_context, &worker_params, &client->ucp_worker);
    if (status != UCS_OK) {
        fprintf(stderr, "Failed to create UCP worker: %s\n", ucs_status_string(status));
        ucp_cleanup(client->ucp_context);
        free(client);
        return NULL;
    }

    /* Get worker address for replies */
    status = ucp_worker_get_address(client->ucp_worker, (ucp_address_t **)&client->worker_addr,
                                    &client->worker_addr_len);
    if (status != UCS_OK) {
        fprintf(stderr, "Failed to get worker address: %s\n", ucs_status_string(status));
        ucp_worker_destroy(client->ucp_worker);
        ucp_cleanup(client->ucp_context);
        free(client);
        return NULL;
    }

    client->connected = 0;

    printf("BenchFS client created successfully\n");
    return client;
}

int benchfs_client_connect(benchfs_client_t *client, const char *server_addr) {
    if (!client || !server_addr) {
        return BENCHFS_INVALID_ARG;
    }

    /* Parse server address (IP:port) */
    char *addr_copy = strdup(server_addr);
    char *colon = strchr(addr_copy, ':');
    if (!colon) {
        fprintf(stderr, "Invalid server address format (expected IP:port)\n");
        free(addr_copy);
        return BENCHFS_INVALID_ARG;
    }

    *colon = '\0';
    const char *ip = addr_copy;
    const char *port_str = colon + 1;

    /* Resolve server address (supports both hostname and IP) */
    struct sockaddr_in server_sockaddr;
    memset(&server_sockaddr, 0, sizeof(server_sockaddr));
    server_sockaddr.sin_family = AF_INET;
    server_sockaddr.sin_port = htons(atoi(port_str));

    /* Try as IP address first */
    if (inet_pton(AF_INET, ip, &server_sockaddr.sin_addr) <= 0) {
        /* Not a valid IP, try DNS resolution */
        struct hostent *he = gethostbyname(ip);
        if (he == NULL || he->h_addr_list[0] == NULL) {
            fprintf(stderr, "Failed to resolve hostname: %s\n", ip);
            free(addr_copy);
            return BENCHFS_INVALID_ARG;
        }
        memcpy(&server_sockaddr.sin_addr, he->h_addr_list[0], he->h_length);
    }

    /* Create endpoint to server */
    ucp_ep_params_t ep_params;
    memset(&ep_params, 0, sizeof(ep_params));
    ep_params.field_mask = UCP_EP_PARAM_FIELD_FLAGS |
                           UCP_EP_PARAM_FIELD_SOCK_ADDR;
    ep_params.flags = UCP_EP_PARAMS_FLAGS_CLIENT_SERVER;
    ep_params.sockaddr.addr = (const struct sockaddr *)&server_sockaddr;
    ep_params.sockaddr.addrlen = sizeof(server_sockaddr);

    ucs_status_t status = ucp_ep_create(client->ucp_worker, &ep_params, &client->ucp_ep);
    free(addr_copy);

    if (status != UCS_OK) {
        fprintf(stderr, "Failed to create endpoint: %s\n", ucs_status_string(status));
        return BENCHFS_ERROR;
    }

    /* Register AM handlers for reply streams BEFORE sending any requests */
    if (!client->am_handlers_registered) {
        printf("[DEBUG] Registering AM handlers for reply streams\n");

        /* Register handler for reply stream ID 120 (used by metadata operations) */
        ucp_am_handler_param_t am_param;
        memset(&am_param, 0, sizeof(am_param));
        am_param.field_mask = UCP_AM_HANDLER_PARAM_FIELD_ID |
                              UCP_AM_HANDLER_PARAM_FIELD_CB |
                              UCP_AM_HANDLER_PARAM_FIELD_ARG;
        am_param.id = 120;  /* Reply stream ID for metadata ops */
        am_param.cb = am_reply_handler;
        am_param.arg = NULL;

        status = ucp_worker_set_am_recv_handler(client->ucp_worker, &am_param);
        if (status != UCS_OK) {
            fprintf(stderr, "[ERROR] Failed to set AM handler for stream 120: %s\n", ucs_status_string(status));
            ucp_ep_destroy(client->ucp_ep);
            client->ucp_ep = NULL;
            client->connected = 0;
            return BENCHFS_ERROR;
        }

        printf("[DEBUG] Successfully registered AM handler for stream 120\n");
        client->am_handlers_registered = 1;
    }

    client->connected = 1;
    printf("Connected to BenchFS server at %s\n", server_addr);

    return BENCHFS_SUCCESS;
}

int benchfs_client_connect_worker_addr(benchfs_client_t *client, const unsigned char *worker_addr_bytes, size_t addr_len) {
    if (!client || !worker_addr_bytes || addr_len == 0) {
        fprintf(stderr, "[ERROR] Invalid arguments to benchfs_client_connect_worker_addr\n");
        return BENCHFS_INVALID_ARG;
    }

    printf("[DEBUG] benchfs_client_connect_worker_addr: Creating endpoint with %zu byte worker address\n", addr_len);

    /* Register AM handlers BEFORE creating endpoint */
    if (!client->am_handlers_registered) {
        printf("[DEBUG] Registering AM handlers for reply streams\n");

        /* Register handler for reply stream ID 120 (used by metadata operations) */
        ucp_am_handler_param_t am_param;
        memset(&am_param, 0, sizeof(am_param));
        am_param.field_mask = UCP_AM_HANDLER_PARAM_FIELD_ID |
                              UCP_AM_HANDLER_PARAM_FIELD_CB |
                              UCP_AM_HANDLER_PARAM_FIELD_ARG;
        am_param.id = 120;  /* Reply stream ID for metadata ops */
        am_param.cb = am_reply_handler;
        am_param.arg = NULL;

        ucs_status_t status = ucp_worker_set_am_recv_handler(client->ucp_worker, &am_param);
        if (status != UCS_OK) {
            fprintf(stderr, "[ERROR] Failed to set AM handler for stream 120: %s\n", ucs_status_string(status));
            return BENCHFS_ERROR;
        }

        printf("[DEBUG] Successfully registered AM handler for stream 120\n");
        client->am_handlers_registered = 1;
    }

    /* Create endpoint using worker address */
    ucp_ep_params_t ep_params;
    memset(&ep_params, 0, sizeof(ep_params));
    ep_params.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS;
    ep_params.address = (const ucp_address_t *)worker_addr_bytes;

    printf("[DEBUG] Calling ucp_ep_create...\n");
    ucs_status_t status = ucp_ep_create(client->ucp_worker, &ep_params, &client->ucp_ep);

    if (status != UCS_OK) {
        fprintf(stderr, "[ERROR] Failed to create endpoint from worker address: %s\n", ucs_status_string(status));
        return BENCHFS_ERROR;
    }

    printf("[DEBUG] ucp_ep_create succeeded\n");

    client->connected = 1;
    printf("Connected to BenchFS server using worker address\n");

    return BENCHFS_SUCCESS;
}

void benchfs_client_destroy(benchfs_client_t *client) {
    if (!client) {
        return;
    }

    /* Close endpoint if connected */
    if (client->ucp_ep) {
        ucp_ep_destroy(client->ucp_ep);
    }

    /* Release worker address */
    if (client->worker_addr) {
        ucp_worker_release_address(client->ucp_worker, (ucp_address_t *)client->worker_addr);
    }

    /* Destroy worker */
    if (client->ucp_worker) {
        ucp_worker_destroy(client->ucp_worker);
    }

    /* Cleanup context */
    if (client->ucp_context) {
        ucp_cleanup(client->ucp_context);
    }

    free(client);
    printf("BenchFS client destroyed\n");
}

/* ========================================================================== */
/* Helper Functions                                                           */
/* ========================================================================== */

/* Structure to hold AM reply data */
typedef struct {
    void   *header;
    size_t  header_len;
    void   *data;
    size_t  data_len;
    int     received;
} am_reply_t;

static am_reply_t g_reply; /* Global reply storage (simplified for single-threaded) */

static ucs_status_t am_reply_handler(void *arg, const void *header, size_t header_length,
                                     void *data, size_t data_length,
                                     const ucp_am_recv_param_t *param) {
    (void)arg;
    (void)param;

    printf("[DEBUG] am_reply_handler called: header_len=%zu, data_len=%zu\n",
           header_length, data_length);

    /* Copy header */
    if (g_reply.header && header_length <= g_reply.header_len) {
        memcpy(g_reply.header, header, header_length);
        g_reply.header_len = header_length;
        printf("[DEBUG] Copied %zu bytes of header\n", header_length);
    } else if (header_length > 0) {
        printf("[DEBUG] Warning: header buffer too small or NULL (need %zu, have %zu)\n",
               header_length, g_reply.header_len);
    }

    /* Copy data if present */
    if (g_reply.data && data && data_length <= g_reply.data_len) {
        memcpy(g_reply.data, data, data_length);
        g_reply.data_len = data_length;
        printf("[DEBUG] Copied %zu bytes of data\n", data_length);
    } else if (data && data_length > 0) {
        g_reply.data_len = data_length;
        printf("[DEBUG] Warning: data buffer too small or NULL (need %zu, have %zu)\n",
               data_length, g_reply.data_len);
    }

    g_reply.received = 1;
    printf("[DEBUG] Reply marked as received\n");

    return UCS_OK;
}

static int wait_for_am_reply(benchfs_client_t *client, uint32_t reply_stream_id,
                             void *response_header, size_t header_size,
                             void *response_data, size_t *data_size) {
    ucp_worker_h worker = client->ucp_worker;
    int timeout_ms = 5000;  /* 5 second timeout */
    int elapsed_ms = 0;

    printf("[DEBUG] wait_for_am_reply: Waiting for reply on stream %u\n", reply_stream_id);

    /* Setup global reply structure */
    memset(&g_reply, 0, sizeof(g_reply));
    g_reply.header = response_header;
    g_reply.header_len = header_size;
    g_reply.data = response_data;
    g_reply.data_len = data_size ? *data_size : 0;
    g_reply.received = 0;

    /* AM handler was already registered during connection - just poll for reply */
    while (!g_reply.received && elapsed_ms < timeout_ms) {
        ucp_worker_progress(worker);
        usleep(1000);  /* 1ms sleep */
        elapsed_ms += 1;

        if (elapsed_ms % 1000 == 0) {
            printf("[DEBUG] Still waiting for reply... (%d ms elapsed)\n", elapsed_ms);
        }
    }

    if (!g_reply.received) {
        fprintf(stderr, "[ERROR] Timeout waiting for reply on stream %u after %d ms\n",
                reply_stream_id, elapsed_ms);
        return BENCHFS_TIMEOUT;
    }

    printf("[DEBUG] Reply received successfully on stream %u\n", reply_stream_id);

    /* Copy back data size if requested */
    if (data_size) {
        *data_size = g_reply.data_len;
    }

    return BENCHFS_SUCCESS;
}

int send_am_request(benchfs_client_t *client, uint32_t rpc_id, uint32_t reply_stream_id,
                   const void *request_header, size_t header_size,
                   const void *request_data, size_t data_size,
                   void *response_header, size_t response_header_size,
                   void *response_data, size_t *response_data_size) {
    if (!client || !client->connected) {
        fprintf(stderr, "[ERROR] send_am_request: client not connected\n");
        return BENCHFS_ERROR;
    }

    printf("[DEBUG] send_am_request: Sending AM on RPC ID %u, expecting reply on stream %u\n",
           rpc_id, reply_stream_id);
    printf("[DEBUG] send_am_request: header_size=%zu, data_size=%zu\n", header_size, data_size);
    printf("[DEBUG] send_am_request: endpoint=%p, worker=%p\n",
           (void*)client->ucp_ep, (void*)client->ucp_worker);

    /* Prepare send parameters for UCX 1.18+ */
    ucp_request_param_t send_param;
    memset(&send_param, 0, sizeof(send_param));
    send_param.op_attr_mask = UCP_OP_ATTR_FIELD_DATATYPE;  /* No callback - using synchronous polling */
    send_param.datatype = ucp_dt_make_contig(1);  /* Byte datatype */

    printf("[DEBUG] send_am_request: Calling ucp_am_send_nbx...\n");

    /* Send AM request with header and data
     * UCX 1.18+ separates header and payload */
    ucs_status_ptr_t request = ucp_am_send_nbx(
        client->ucp_ep,
        rpc_id,
        request_header,
        header_size,
        request_data,
        data_size,
        &send_param
    );

    printf("[DEBUG] send_am_request: ucp_am_send_nbx returned request=%p\n", request);

    /* Wait for send completion */
    if (UCS_PTR_IS_PTR(request)) {
        printf("[DEBUG] send_am_request: Request is pointer, waiting for completion...\n");
        ucs_status_t status;
        int progress_count = 0;
        do {
            ucp_worker_progress(client->ucp_worker);
            status = ucp_request_check_status(request);
            progress_count++;
            if (progress_count % 1000 == 0) {
                printf("[DEBUG] send_am_request: Still waiting... (progress_count=%d)\n", progress_count);
            }
        } while (status == UCS_INPROGRESS);

        printf("[DEBUG] send_am_request: Send completed with status: %s\n", ucs_status_string(status));
        ucp_request_free(request);

        if (status != UCS_OK) {
            fprintf(stderr, "[ERROR] AM send failed: %s\n", ucs_status_string(status));
            return BENCHFS_ERROR;
        }
    } else if (UCS_PTR_STATUS(request) != UCS_OK) {
        fprintf(stderr, "[ERROR] AM send failed immediately: %s\n",
                ucs_status_string(UCS_PTR_STATUS(request)));
        return BENCHFS_ERROR;
    } else {
        printf("[DEBUG] send_am_request: Send completed immediately (status=OK)\n");
    }

    /* Wait for reply */
    int ret = wait_for_am_reply(client, reply_stream_id, response_header,
                                response_header_size, response_data, response_data_size);

    return ret;
}

/* Implementations of benchfs operations in benchfs_ops.c */
