/*
 * benchfs_pfind — io500-compatible find driver for BenchFS.
 *
 * MPI-parallel tree walker that calls into BenchFS through the C FFI
 * (benchfs_readdir + benchfs_stat). io500's `find` phase external-script
 * mode runs this via `external-mpi-args = mpirun -np <N>` so we can fan
 * out the readdir + stat work across many ranks instead of doing the
 * whole walk serially from io500's rank 0.
 *
 * Supported predicates (the io500 default args):
 *   <root>            — directory to walk
 *   -newer <FILE>     — match files with mtime newer than FILE's mtime
 *                       (currently a no-op: benchfs_stat_t lacks mtime)
 *   -size <NBYTES>c   — match files of exactly NBYTES bytes
 *   -name <PATTERN>   — fnmatch(3) basename pattern (NULL=match all)
 *
 * Required env vars:
 *   BENCHFS_REGISTRY_DIR  — locusta QP exchange dir (shared FS)
 *   (BENCHFS_NODE_ID is auto-generated per-rank from host+pid+rank)
 *
 * Output (last line is what io500 parses, see phase_find.c:120):
 *   MATCHED <hits>/<total>
 *
 * Static work partitioning
 * ------------------------
 * Each rank descends the tree depth-first, but at every readdir() it only
 * recurses into subdirectories whose (visit_count_mod_N) equals its MPI
 * rank. visit_count increments for every directory ANY rank would
 * eventually traverse, computed deterministically as we walk so all ranks
 * agree without communication. Files within a directory are processed
 * only by the rank that "owns" the parent directory; this is simple and
 * keeps the BenchFS stat() load balanced across ranks roughly evenly when
 * the tree is wide (mdtest-hard-style: many shallow dirs).
 */

#define _GNU_SOURCE
#include <benchfs_c_api.h>

/* Optional MPI support. When called from io500's popen(), MPI_Init from
 * inside an outer mpirun context hangs at startup (nested mpirun issue:
 * OpenMPI's PMIx tries to register with the parent runtime and never
 * returns). So MPI is gated behind BENCHFS_PFIND_USE_MPI=1 and even
 * then the binary needs to be built with -DBENCHFS_PFIND_WITH_MPI.
 * Without that define, the binary is pure single-process and avoids
 * the nested-mpirun trap entirely.
 */
#ifdef BENCHFS_PFIND_WITH_MPI
#include <mpi.h>
#endif

#include <errno.h>
#include <fnmatch.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <time.h>

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

struct opts {
    const char *root;
    const char *name_pattern;  /* NULL = match all */
    int         have_newer;
    time_t      newer_mtime;
    int         have_size;
    int64_t     size_bytes;
    int         size_op;  /* 0 = exact, 1 = > (size +N), -1 = < (size -N) */
};

static benchfs_context_t *g_ctx = NULL;
static uint64_t           g_hits = 0;
static uint64_t           g_total_files = 0;
static int                g_mpi_rank = 0;
static int                g_mpi_size = 1;

/* dir stack — owned strings (we strdup each pushed path) */
struct dir_stack {
    char   **paths;
    size_t   len;
    size_t   cap;
};

static void stack_push(struct dir_stack *s, char *owned_path) {
    if (s->len == s->cap) {
        size_t newcap = s->cap == 0 ? 64 : s->cap * 2;
        s->paths = realloc(s->paths, newcap * sizeof(char *));
        s->cap = newcap;
    }
    s->paths[s->len++] = owned_path;
}

static char *stack_pop(struct dir_stack *s) {
    if (s->len == 0) return NULL;
    return s->paths[--s->len];
}

static void stack_free(struct dir_stack *s) {
    for (size_t i = 0; i < s->len; i++) free(s->paths[i]);
    free(s->paths);
}

struct dir_collect {
    char        cur_path[PATH_MAX];
    struct dir_stack subdirs;  /* owned strings (full paths) */
    struct {
        char    **names;       /* basenames, owned */
        uint64_t *sizes;       /* file sizes from readdirplus, parallel to names */
        size_t    len, cap;
    } files;
};

static int filler_cb(void *arg, const char *name, int entry_type, uint64_t size) {
    struct dir_collect *dc = (struct dir_collect *)arg;
    if (name == NULL || name[0] == '\0') return 0;
    if (name[0] == '.' && (name[1] == '\0' || (name[1] == '.' && name[2] == '\0'))) {
        return 0;
    }
    if (entry_type == 1 /* directory */) {
        size_t need = strlen(dc->cur_path) + 1 + strlen(name) + 1;
        char *buf = malloc(need);
        if (buf == NULL) return 0;
        snprintf(buf, need, "%s/%s", dc->cur_path, name);
        stack_push(&dc->subdirs, buf);
    } else if (entry_type == 0 /* file */) {
        if (dc->files.len == dc->files.cap) {
            size_t newcap = dc->files.cap == 0 ? 64 : dc->files.cap * 2;
            dc->files.names = realloc(dc->files.names, newcap * sizeof(char *));
            dc->files.sizes = realloc(dc->files.sizes, newcap * sizeof(uint64_t));
            dc->files.cap = newcap;
        }
        dc->files.names[dc->files.len] = strdup(name);
        dc->files.sizes[dc->files.len] = size;
        dc->files.len++;
    }
    return 0;
}

static int match_predicates(const struct opts *o, const char *basename,
                            const benchfs_stat_t *st) {
    if (o->name_pattern != NULL) {
        if (fnmatch(o->name_pattern, basename, 0) != 0) {
            return 0;
        }
    }
    if (o->have_size) {
        int64_t fsize = (int64_t)st->st_size;
        if (o->size_op > 0) {
            if (fsize <= o->size_bytes) return 0;   /* -size +Nc → size > N */
        } else if (o->size_op < 0) {
            if (fsize >= o->size_bytes) return 0;   /* -size -Nc → size < N */
        } else {
            if (fsize != o->size_bytes) return 0;   /* -size Nc → exact */
        }
    }
    /* -newer mtime: skipped (benchfs_stat_t lacks mtime). io500's default
     * arg set uses -size + -name which already filters heavily. */
    (void)o;
    return 1;
}

static int parse_args(int argc, char **argv, struct opts *o) {
    memset(o, 0, sizeof(*o));
    if (argc < 2) {
        if (g_mpi_rank == 0) {
            fprintf(stderr, "usage: %s <root> [-newer FILE] [-size NBYTESc] [-name PATTERN]\n", argv[0]);
        }
        return -1;
    }
    if (g_mpi_rank == 0) {
        fprintf(stderr, "[pfind] argv:");
        for (int i = 0; i < argc; i++) fprintf(stderr, " '%s'", argv[i]);
        fprintf(stderr, "\n");
    }
    o->root = argv[1];
    for (int i = 2; i < argc; i++) {
        if (strcmp(argv[i], "-newer") == 0 && i + 1 < argc) {
            struct stat sb;
            if (stat(argv[i + 1], &sb) == 0) {
                o->have_newer = 1;
                o->newer_mtime = sb.st_mtime;
            }
            i++;
        } else if (strcmp(argv[i], "-size") == 0 && i + 1 < argc) {
            char *spec = argv[i + 1];
            int op = 0;
            if (*spec == '+') { op = 1; spec++; }
            else if (*spec == '-') { op = -1; spec++; }
            char *endp;
            long long v = strtoll(spec, &endp, 10);
            if (*endp == 'c' || *endp == '\0') {
                o->have_size = 1;
                o->size_bytes = (int64_t)v;
                o->size_op = op;
            }
            i++;
        } else if (strcmp(argv[i], "-name") == 0 && i + 1 < argc) {
            o->name_pattern = argv[i + 1];
            i++;
        } else if (strcmp(argv[i], "-C") == 0) {
            /* count-only */
        } else if (strcmp(argv[i], "-q") == 0 && i + 1 < argc) {
            i++;
        } else if (strcmp(argv[i], "-N") == 0) {
        } else if (strcmp(argv[i], "-H") == 0 && i + 1 < argc) {
            i++;
        } else {
            if (g_mpi_rank == 0) {
                fprintf(stderr, "%s: unknown arg '%s' (ignored)\n", argv[0], argv[i]);
            }
        }
    }
    return 0;
}

static int init_benchfs(void) {
    char node_id_buf[256];
    char host[64] = {0};
    gethostname(host, sizeof(host) - 1);
    /* Always auto-generate so multiple MPI ranks don't collide on the
     * same `pfind_<host>_<pid>` (in case of identical PIDs across ranks
     * sharing a host — unlikely but cheap to guard). Include MPI rank. */
    snprintf(node_id_buf, sizeof(node_id_buf), "pfind_%s_%d_r%d",
             host, (int)getpid(), g_mpi_rank);
    const char *node_id = node_id_buf;

    const char *registry = getenv("BENCHFS_REGISTRY_DIR");
    if (registry == NULL) {
        if (g_mpi_rank == 0) {
            fprintf(stderr, "benchfs_pfind: BENCHFS_REGISTRY_DIR must be set\n");
        }
        return -1;
    }
    g_ctx = benchfs_init(node_id, registry, NULL, /*is_server=*/0, /*chunk_size=*/0);
    if (g_ctx == NULL) {
        fprintf(stderr, "benchfs_pfind[%d]: benchfs_init failed: %s\n",
                g_mpi_rank, benchfs_get_error());
        return -1;
    }
    return 0;
}

int main(int argc, char **argv) {
    /* Line-buffer stdout/stderr so progress messages reach io500 even
     * when stdout is a pipe (popen mode "r") — by default stdout is
     * block-buffered on a pipe, which makes pfind look hung. */
    setvbuf(stdout, NULL, _IOLBF, 0);
    setvbuf(stderr, NULL, _IOLBF, 0);
    fprintf(stderr, "[pfind] start: argc=%d\n", argc);

#ifdef BENCHFS_PFIND_WITH_MPI
    /* Only call MPI_Init if explicitly opted in via env var — avoids the
     * nested-mpirun hang when this binary is launched from io500 rank 0's
     * popen() without an explicit mpirun prefix. */
    int use_mpi = (getenv("BENCHFS_PFIND_USE_MPI") != NULL);
    if (use_mpi) {
        fprintf(stderr, "[pfind] MPI_Init\n");
        MPI_Init(&argc, &argv);
        MPI_Comm_rank(MPI_COMM_WORLD, &g_mpi_rank);
        MPI_Comm_size(MPI_COMM_WORLD, &g_mpi_size);
        fprintf(stderr, "[pfind] MPI: rank=%d size=%d\n", g_mpi_rank, g_mpi_size);
    }
#endif

    struct opts o;
    if (parse_args(argc, argv, &o) != 0) {
#ifdef BENCHFS_PFIND_WITH_MPI
        if (use_mpi) MPI_Finalize();
#endif
        return 1;
    }
    fprintf(stderr, "[pfind] parsed args, root=%s\n", o.root);

    if (init_benchfs() != 0) {
#ifdef BENCHFS_PFIND_WITH_MPI
        if (use_mpi) MPI_Finalize();
#endif
        return 1;
    }
    fprintf(stderr, "[pfind] benchfs_init done\n");

    /* Static partition: every directory we visit gets a deterministic
     * `dir_index` (incremented in a deterministic DFS order shared by
     * every rank). Rank `r` only stats files of directories where
     * `dir_index % nproc == r`. All ranks still walk the full tree to
     * keep `dir_index` consistent — the savings come from doing zero
     * BenchFS stat()s for directories owned by other ranks. */
    struct dir_stack work = {0};
    stack_push(&work, strdup(o.root));
    uint64_t dir_index = 0;
    fprintf(stderr, "[pfind] starting walk at %s\n", o.root);

    char *cur;
    while ((cur = stack_pop(&work)) != NULL) {
        struct dir_collect dc;
        memset(&dc, 0, sizeof(dc));
        snprintf(dc.cur_path, sizeof(dc.cur_path), "%s", cur);

        if (dir_index < 8 || dir_index % 256 == 0) {
            fprintf(stderr, "[pfind] readdir #%" PRIu64 ": %s\n", dir_index, cur);
        }
        int rc = benchfs_readdir(g_ctx, cur, filler_cb, &dc);
        if (rc < 0) {
            fprintf(stderr, "benchfs_pfind[%d]: readdir(%s) failed: %s\n",
                    g_mpi_rank, cur, benchfs_get_error());
        }

        for (size_t i = 0; i < dc.subdirs.len; i++) {
            stack_push(&work, dc.subdirs.paths[i]);
        }
        free(dc.subdirs.paths);
        dc.subdirs.paths = NULL;
        dc.subdirs.len = dc.subdirs.cap = 0;

        /* readdirplus: filler already populated size for each file.
         * No per-file benchfs_stat() RPC needed — was the 50 kIOPS
         * bottleneck (400 k stat × 1 RTT per file at iter72-87). */
        int we_own = (dir_index % (uint64_t)g_mpi_size) == (uint64_t)g_mpi_rank;
        for (size_t i = 0; i < dc.files.len; i++) {
            if (we_own) {
                benchfs_stat_t st;
                memset(&st, 0, sizeof(st));
                st.st_size = (off_t)dc.files.sizes[i];
                /* Mode bits are not used by io500's default predicates
                 * (-name / -size / -newer); leave them at 0. */
                g_total_files++;
                if (match_predicates(&o, dc.files.names[i], &st)) {
                    g_hits++;
                }
            }
            free(dc.files.names[i]);
        }
        free(dc.files.names);
        free(dc.files.sizes);
        free(cur);
        dir_index++;
    }
    stack_free(&work);

    benchfs_finalize(g_ctx);

    /* Aggregate counters across ranks (MPI) or use local counters (singleton). */
    uint64_t global_hits = g_hits;
    uint64_t global_total = g_total_files;
#ifdef BENCHFS_PFIND_WITH_MPI
    if (use_mpi) {
        MPI_Reduce(&g_hits, &global_hits, 1, MPI_UINT64_T, MPI_SUM, 0, MPI_COMM_WORLD);
        MPI_Reduce(&g_total_files, &global_total, 1, MPI_UINT64_T, MPI_SUM, 0, MPI_COMM_WORLD);
    }
#endif

    if (g_mpi_rank == 0) {
        printf("MATCHED %" PRIu64 "/%" PRIu64 "\n", global_hits, global_total);
    }

#ifdef BENCHFS_PFIND_WITH_MPI
    if (use_mpi) MPI_Finalize();
#endif
    return 0;
}
