/*
 * IOR BENCHFS Backend Implementation
 *
 * This is the AIORI (Abstract I/O Interface) implementation for BenchFS.
 * It allows IOR benchmark to run on BenchFS distributed filesystem.
 *
 * Based on aiori-DUMMY.c
 */

#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <mpi.h>

#include "ior.h"
#include "aiori.h"
#include "utilities.h"
#include "benchfs_c_api.h"

/************************** O P T I O N S *****************************/

typedef struct {
  char *registry_dir;     /* Shared directory for service discovery */
  char *data_dir;         /* Data directory for server nodes */
  int use_mpi_rank;       /* Use MPI rank for node ID */
} benchfs_options_t;

/* Global state */
static int benchfs_rank = -1;
static int benchfs_size = -1;
static int benchfs_initialized = 0;
static benchfs_context_t *benchfs_ctx = NULL;

/************************** O P T I O N S *****************************/

static option_help * BENCHFS_options(
    aiori_mod_opt_t **init_backend_options,
    aiori_mod_opt_t *init_values
) {
  benchfs_options_t *o = malloc(sizeof(benchfs_options_t));

  if (init_values != NULL) {
    memcpy(o, init_values, sizeof(benchfs_options_t));
  } else {
    memset(o, 0, sizeof(benchfs_options_t));
    /* Set defaults */
    o->registry_dir = strdup("/tmp/benchfs_registry");
    o->data_dir = strdup("/tmp/benchfs_data");
    o->use_mpi_rank = 1;
  }

  *init_backend_options = (aiori_mod_opt_t*)o;

  option_help h[] = {
      {0, "benchfs.registry", "Registry directory path",
       OPTION_OPTIONAL_ARGUMENT, 's', &o->registry_dir},
      {0, "benchfs.datadir", "Data directory path (for server)",
       OPTION_OPTIONAL_ARGUMENT, 's', &o->data_dir},
      {0, "benchfs.use-mpi-rank", "Use MPI rank for node ID",
       OPTION_FLAG, 'd', &o->use_mpi_rank},
      LAST_OPTION
  };

  option_help *help = malloc(sizeof(h));
  memcpy(help, h, sizeof(h));
  return help;
}

/************************** I N I T I A L I Z E *****************************/

static void BENCHFS_Initialize(aiori_mod_opt_t *options) {
  if (benchfs_initialized) {
    return;
  }

  benchfs_options_t *o = (benchfs_options_t*)options;

  /* Get MPI rank and size */
  MPI_Comm_rank(MPI_COMM_WORLD, &benchfs_rank);
  MPI_Comm_size(MPI_COMM_WORLD, &benchfs_size);

  /* Initialize BenchFS C API */
  if (benchfs_rank == 0) {
    WARNF("BENCHFS initialized (rank %d/%d) - SERVER MODE", benchfs_rank, benchfs_size);
    fprintf(out_logfile, "BENCHFS: Registry dir: %s\n", o->registry_dir);
    fprintf(out_logfile, "BENCHFS: Data dir: %s\n", o->data_dir);

    /* Start server */
    benchfs_ctx = benchfs_init("server", o->registry_dir, o->data_dir, 1);
    if (benchfs_ctx == NULL) {
      ERRF("BENCHFS server initialization failed: %s", benchfs_get_error());
    }
  }

  /* Wait for server to be ready before clients connect */
  MPI_Barrier(MPI_COMM_WORLD);

  if (benchfs_rank != 0) {
    WARNF("BENCHFS initialized (rank %d/%d) - CLIENT MODE", benchfs_rank, benchfs_size);

    /* Start client */
    char node_id[64];
    snprintf(node_id, sizeof(node_id), "client_%d", benchfs_rank);
    benchfs_ctx = benchfs_init(node_id, o->registry_dir, NULL, 0);
    if (benchfs_ctx == NULL) {
      ERRF("BENCHFS client initialization failed: %s", benchfs_get_error());
    }
  }

  benchfs_initialized = 1;
}

/************************** F I N A L I Z E *****************************/

static void BENCHFS_Finalize(aiori_mod_opt_t *options) {
  if (!benchfs_initialized) {
    return;
  }

  WARNF("BENCHFS finalized (rank %d)", benchfs_rank);

  /* Finalize BenchFS C API */
  if (benchfs_ctx != NULL) {
    benchfs_finalize(benchfs_ctx);
    benchfs_ctx = NULL;
  }

  benchfs_initialized = 0;
}

/************************** C R E A T E *****************************/

static aiori_fd_t *BENCHFS_Create(
    char *testFileName,
    int iorflags,
    aiori_mod_opt_t *options
) {
  if (!benchfs_initialized) {
    ERR("BENCHFS not initialized in create\n");
  }

  if (verbose > 4) {
    fprintf(out_logfile, "BENCHFS create: rank=%d, file=%s\n",
            benchfs_rank, testFileName);
  }

  /* Convert IOR flags to BenchFS flags */
  int flags = BENCHFS_O_CREAT | BENCHFS_O_WRONLY;
  if (iorflags & IOR_TRUNC) flags |= BENCHFS_O_TRUNC;
  if (iorflags & IOR_EXCL)  flags |= BENCHFS_O_EXCL;

  /* Call BenchFS C API */
  benchfs_file_t *file = benchfs_create(benchfs_ctx, testFileName, flags, 0644);
  if (file == NULL) {
    ERRF("BENCHFS create failed: %s", benchfs_get_error());
  }

  return (aiori_fd_t*)file;
}

/************************** O P E N *****************************/

static aiori_fd_t *BENCHFS_Open(
    char *testFileName,
    int iorflags,
    aiori_mod_opt_t *options
) {
  if (!benchfs_initialized) {
    ERR("BENCHFS not initialized in open\n");
  }

  if (verbose > 4) {
    fprintf(out_logfile, "BENCHFS open: rank=%d, file=%s\n",
            benchfs_rank, testFileName);
  }

  /* Convert IOR flags to BenchFS flags */
  int flags = 0;
  if (iorflags & IOR_RDONLY) flags = BENCHFS_O_RDONLY;
  if (iorflags & IOR_WRONLY) flags = BENCHFS_O_WRONLY;
  if (iorflags & IOR_RDWR)   flags = BENCHFS_O_RDWR;

  /* Call BenchFS C API */
  benchfs_file_t *file = benchfs_open(benchfs_ctx, testFileName, flags);
  if (file == NULL) {
    ERRF("BENCHFS open failed: %s", benchfs_get_error());
  }

  return (aiori_fd_t*)file;
}

/************************** X F E R *****************************/

static IOR_offset_t BENCHFS_Xfer(
    int access,
    aiori_fd_t *file,
    IOR_size_t *buffer,
    IOR_offset_t length,
    IOR_offset_t offset,
    aiori_mod_opt_t *options
) {
  if (verbose > 4) {
    fprintf(out_logfile, "BENCHFS xfer: rank=%d, handle=%p, %s, offset=%lld, length=%lld\n",
            benchfs_rank, file,
            (access == WRITE) ? "WRITE" : "READ",
            (long long)offset, (long long)length);
  }

  /* Call BenchFS C API */
  ssize_t ret;
  if (access == WRITE) {
    ret = benchfs_write((benchfs_file_t*)file, buffer, length, offset);
  } else {
    ret = benchfs_read((benchfs_file_t*)file, buffer, length, offset);
  }

  if (ret < 0) {
    ERRF("BENCHFS xfer failed: %s", benchfs_get_error());
  }

  return ret;
}

/************************** C L O S E *****************************/

static void BENCHFS_Close(aiori_fd_t *fd, aiori_mod_opt_t *options) {
  if (verbose > 4) {
    fprintf(out_logfile, "BENCHFS close: rank=%d, handle=%p\n", benchfs_rank, fd);
  }

  /* Call BenchFS C API */
  int ret = benchfs_close((benchfs_file_t*)fd);
  if (ret != BENCHFS_SUCCESS) {
    ERRF("BENCHFS close failed: %s", benchfs_get_error());
  }
}

/************************** D E L E T E *****************************/

static void BENCHFS_Delete(char *testFileName, aiori_mod_opt_t *options) {
  if (verbose > 4) {
    fprintf(out_logfile, "BENCHFS delete: rank=%d, file=%s\n",
            benchfs_rank, testFileName);
  }

  /* Call BenchFS C API */
  int ret = benchfs_remove(benchfs_ctx, testFileName);
  if (ret != BENCHFS_SUCCESS) {
    /* File may not exist - this is not necessarily an error in IOR context */
    /* IOR sometimes tries to delete files before creating them */
    const char *err_msg = benchfs_get_error();
    if (strstr(err_msg, "File not found") || strstr(err_msg, "not found")) {
      if (verbose >= 3) {
        WARNF("BENCHFS remove: file not found (this may be expected): %s", testFileName);
      }
    } else {
      WARNF("BENCHFS remove failed: %s", err_msg);
    }
  }
}

/************************** F S Y N C *****************************/

static void BENCHFS_Fsync(aiori_fd_t *fd, aiori_mod_opt_t *options) {
  if (verbose > 4) {
    fprintf(out_logfile, "BENCHFS fsync: rank=%d, handle=%p\n", benchfs_rank, fd);
  }

  /* Call BenchFS C API */
  int ret = benchfs_fsync((benchfs_file_t*)fd);
  if (ret != BENCHFS_SUCCESS) {
    ERRF("BENCHFS fsync failed: %s", benchfs_get_error());
  }
}

/************************** S Y N C *****************************/

static void BENCHFS_Sync(aiori_mod_opt_t *options) {
  if (verbose > 4) {
    fprintf(out_logfile, "BENCHFS sync: rank=%d\n", benchfs_rank);
  }
}

/************************** G E T _ F I L E _ S I Z E *****************************/

static IOR_offset_t BENCHFS_GetFileSize(
    aiori_mod_opt_t *options,
    char *testFileName
) {
  if (verbose > 4) {
    fprintf(out_logfile, "BENCHFS get_file_size: rank=%d, file=%s\n",
            benchfs_rank, testFileName);
  }

  /* Call BenchFS C API */
  off_t size = benchfs_get_file_size(benchfs_ctx, testFileName);
  if (size < 0) {
    ERRF("BENCHFS get_file_size failed: %s", benchfs_get_error());
  }

  return (IOR_offset_t)size;
}

/************************** S T A T F S *****************************/

static int BENCHFS_statfs(
    const char *path,
    ior_aiori_statfs_t *stat,
    aiori_mod_opt_t *options
) {
  /* Return dummy filesystem statistics */
  stat->f_bsize = 4096;
  stat->f_blocks = 1000000;
  stat->f_bfree = 900000;
  stat->f_bavail = 900000;
  stat->f_files = 1000000;
  stat->f_ffree = 999000;
  return 0;
}

/************************** M K D I R *****************************/

static int BENCHFS_mkdir(
    const char *path,
    mode_t mode,
    aiori_mod_opt_t *options
) {
  if (verbose > 4) {
    fprintf(out_logfile, "BENCHFS mkdir: rank=%d, path=%s\n", benchfs_rank, path);
  }
  return 0;
}

/************************** R M D I R *****************************/

static int BENCHFS_rmdir(const char *path, aiori_mod_opt_t *options) {
  if (verbose > 4) {
    fprintf(out_logfile, "BENCHFS rmdir: rank=%d, path=%s\n", benchfs_rank, path);
  }
  return 0;
}

/************************** A C C E S S *****************************/

static int BENCHFS_access(const char *path, int mode, aiori_mod_opt_t *options) {
  return 0;  /* Always accessible */
}

/************************** S T A T *****************************/

static int BENCHFS_stat(
    const char *path,
    struct stat *buf,
    aiori_mod_opt_t *options
) {
  (void)options;
  memset(buf, 0, sizeof(*buf));
  off_t sz = benchfs_get_file_size(benchfs_ctx, (char*)path);
  if (sz < 0) {
    errno = ENOENT;
    return -1;
  }
  buf->st_mode = S_IFREG | 0644;
  buf->st_nlink = 1;
  buf->st_size = (off_t)sz;
  return 0;
}

/************************** R E N A M E *****************************/

static int BENCHFS_rename(
    const char *oldpath,
    const char *newpath,
    aiori_mod_opt_t *options
) {
  if (verbose > 4) {
    fprintf(out_logfile, "BENCHFS rename: rank=%d, %s -> %s\n",
            benchfs_rank, oldpath, newpath);
  }
  return 0;
}

/************************** V E R S I O N *****************************/

static char *BENCHFS_get_version(void) {
  return "BenchFS-IOR-0.1.0";
}

/************************** C H E C K _ P A R A M S *****************************/

static int BENCHFS_check_params(aiori_mod_opt_t *options) {
  benchfs_options_t *o = (benchfs_options_t*)options;

  if (o->registry_dir == NULL || strlen(o->registry_dir) == 0) {
    ERR("BENCHFS: registry_dir must be specified\n");
    return 1;
  }

  return 0;
}

/************************** A I O R I   S T R U C T *****************************/

ior_aiori_t benchfs_aiori = {
    .name = "BENCHFS",
    .name_legacy = NULL,
    .create = BENCHFS_Create,
    .open = BENCHFS_Open,
    .xfer = BENCHFS_Xfer,
    .close = BENCHFS_Close,
    .remove = BENCHFS_Delete,
    .get_version = BENCHFS_get_version,
    .fsync = BENCHFS_Fsync,
    .get_file_size = BENCHFS_GetFileSize,
    .statfs = BENCHFS_statfs,
    .mkdir = BENCHFS_mkdir,
    .rmdir = BENCHFS_rmdir,
    .rename = BENCHFS_rename,
    .access = BENCHFS_access,
    .stat = BENCHFS_stat,
    .initialize = BENCHFS_Initialize,
    .finalize = BENCHFS_Finalize,
    .get_options = BENCHFS_options,
    .check_params = BENCHFS_check_params,
    .sync = BENCHFS_Sync,
    .enable_mdtest = false  /* BenchFS doesn't support mdtest yet */
};
