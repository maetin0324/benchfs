/*
 * Minimal reproducer for the mdtest-hard heap corruption in benchfs_write.
 *
 * Mimics mdtest-hard's tight loop:
 *   for i in 0..N:
 *     fd = benchfs_create(path/i, O_CREAT|O_WRONLY)
 *     benchfs_write(fd, 3901-byte buffer, 3901, 0)
 *     benchfs_close(fd)
 *
 * Built and run standalone (no MPI, no IOR, no mdtest), so we get a clean
 * heap and don't have to wait for io500 phase setup.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include "benchfs_c_api.h"

#define WRITE_SIZE 3901

int main(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "usage: %s <registry_dir> <iterations>\n", argv[0]);
        return 1;
    }
    const char *registry = argv[1];
    int iterations = atoi(argv[2]);
    if (iterations <= 0) iterations = 1000;

    fprintf(stderr, "[repro] init: registry=%s iterations=%d write_size=%d\n",
            registry, iterations, WRITE_SIZE);

    benchfs_context_t *ctx = benchfs_init("repro_client", registry, NULL, 0, 0);
    if (!ctx) {
        fprintf(stderr, "[repro] benchfs_init failed: %s\n", benchfs_get_error());
        return 1;
    }
    fprintf(stderr, "[repro] init OK, ctx=%p\n", (void*)ctx);

    /* Mimic mdtest-hard's setup: nested per-rank directory tree.
     * mdtest creates: /<test_dir>/test-dir.0-0/mdtest_tree.0/ then files inside.
     */
    int pid = getpid();
    char rank_dir[128];
    snprintf(rank_dir, sizeof(rank_dir), "/repro_%d", pid);
    benchfs_mkdir(ctx, rank_dir, 0755);

    char tree_dir[160];
    snprintf(tree_dir, sizeof(tree_dir), "%s/mdtest_tree.0", rank_dir);
    benchfs_mkdir(ctx, tree_dir, 0755);

    char *buf = malloc(WRITE_SIZE);
    memset(buf, 0xa5, WRITE_SIZE);

    char path[256];
    int errors = 0;
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    /* Phase 1: mdtest-hard create+write+close loop */
    for (int i = 0; i < iterations; i++) {
        snprintf(path, sizeof(path), "%s/file.mdtest.%d.%d",
                 tree_dir, pid, i);

        if (i % 200 == 0) {
            fprintf(stderr, "[repro %d] iter=%d path=%s\n", pid, i, path);
        }

        benchfs_file_t *fd = benchfs_create(ctx, path,
                BENCHFS_O_CREAT | BENCHFS_O_WRONLY, 0644);
        if (!fd) {
            fprintf(stderr, "[repro %d] iter=%d create failed: %s\n",
                    pid, i, benchfs_get_error());
            errors++;
            continue;
        }

        ssize_t n = benchfs_write(fd, buf, WRITE_SIZE, 0);
        if (n != WRITE_SIZE) {
            fprintf(stderr, "[repro %d] iter=%d write returned %zd: %s\n",
                    pid, i, n, benchfs_get_error());
            errors++;
        }

        int rc = benchfs_close(fd);
        if (rc != BENCHFS_SUCCESS) {
            fprintf(stderr, "[repro %d] iter=%d close failed: %d %s\n",
                    pid, i, rc, benchfs_get_error());
            errors++;
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &t1);
    double elapsed = (t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) / 1e9;
    fprintf(stderr, "[repro] DONE: %d iterations in %.2fs (%.0f ops/s), errors=%d\n",
            iterations, elapsed, iterations / elapsed, errors);

    free(buf);
    benchfs_finalize(ctx);
    return errors ? 2 : 0;
}
