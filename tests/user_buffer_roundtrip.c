/* Hardware correctness test for ordinary buffers. No BenchFS allocator or IOR
 * changes. Run multiple MPI ranks; >chunk transfers also overlap chunk RPCs.
 * mmap MAP_FIXED here replaces only the test's own mapping, never other memory. */
#define _GNU_SOURCE
#include "benchfs_c_api.h"
#include <mpi.h>
#include <sys/mman.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int rank;
#define REQUIRE(x) do { if (!(x)) { fprintf(stderr, "rank=%d line=%d failed: %s\n", rank, __LINE__, #x); MPI_Abort(MPI_COMM_WORLD, 2); } } while (0)
static unsigned char value(size_t i, unsigned phase) {
    return (unsigned char)((i * 131 + (i >> 12) * 17 + phase * 53 + rank * 29) % 251);
}
/* Releasing a range that still has a cached registration invalidates its
 * device translations; report how long the application-side release takes. */
static void timed_release(const char *what, double t0) {
    printf("RELEASE rank=%d %s us=%.0f\n", rank, what, (MPI_Wtime() - t0) * 1e6);
}
static void roundtrip(benchfs_file_t *file, unsigned char *p, size_t size, unsigned phase) {
    for (size_t i=0; i<size; i++) p[i] = value(i, phase);
    double t0 = MPI_Wtime();
    REQUIRE(benchfs_write(file, p, size, 0) == (ssize_t)size);
    double t1 = MPI_Wtime();
    REQUIRE(benchfs_fsync(file) == 0);
    double tf = MPI_Wtime();
    memset(p, 0xa5, size);
    double t2 = MPI_Wtime();
    REQUIRE(benchfs_read(file, p, size, 0) == (ssize_t)size);
    /* Diagnostic only: exposes stalls such as HCA page-fault retries. */
    printf("PHASE rank=%d phase=%u size=%zu write_us=%.0f fsync_us=%.0f read_us=%.0f\n", rank, phase, size,
           (t1 - t0) * 1e6, (tf - t1) * 1e6, (MPI_Wtime() - t2) * 1e6);
    for (size_t i=0; i<size; i++) REQUIRE(p[i] == value(i, phase));
}
int main(int argc, char **argv) {
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    REQUIRE(argc == 2);
    char id[80], path[100];
    snprintf(id, sizeof id, "ordinary_buffer_%d", rank);
    snprintf(path, sizeof path, "/ordinary_buffer_%d", rank);
    benchfs_context_t *ctx = benchfs_init(id, argv[1], NULL, 0, 4*1024*1024);
    REQUIRE(ctx != NULL);
    printf("CLIENT_CONNECTED_IDLE rank=%d\n", rank); fflush(stdout);
    sleep(2);
    double ts = MPI_Wtime();
    benchfs_file_t *file = benchfs_create(ctx, path, BENCHFS_O_CREAT | BENCHFS_O_RDWR, 0644);
    REQUIRE(file != NULL);
    timed_release("create", ts); ts = MPI_Wtime();
    const size_t size = 8*1024*1024 + 37;
    const size_t mapped = (size + 4095) & ~(size_t)4095;
    unsigned char *p = mmap(NULL, mapped, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
    REQUIRE(p != MAP_FAILED);
    roundtrip(file, p, size, 1);
    roundtrip(file, p, size, 2); /* cache reuse */
    double tr = MPI_Wtime();
    REQUIRE(munmap(p, mapped) == 0);
    timed_release("munmap", tr);
    void *again = mmap(p, mapped, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS|MAP_FIXED_NOREPLACE, -1, 0);
    REQUIRE(again == p); /* same VA, different physical pages: stale pinned MR fails */
    roundtrip(file, p, size, 3);
    /* Small and unaligned subranges must not touch sentinels. */
    p[0] = 0x5a; p[65538] = 0x5a;
    roundtrip(file, p+1, 65537, 4);
    REQUIRE(p[0] == 0x5a && p[65538] == 0x5a);
    roundtrip(file, p+1, 4096, 5);
    tr = MPI_Wtime();
    REQUIRE(munmap(p, mapped) == 0);
    timed_release("munmap", tr);
    for (unsigned phase=6; phase<10; phase++) {
        unsigned char *q = malloc(size);
        REQUIRE(q != NULL);
        roundtrip(file, q, size, phase); /* free/reallocate and eviction */
        tr = MPI_Wtime();
        free(q);
        timed_release("free", tr);
    }
    timed_release("roundtrips", ts); ts = MPI_Wtime();
    REQUIRE(benchfs_close(file) == 0);
    timed_release("close", ts); ts = MPI_Wtime();
    MPI_Barrier(MPI_COMM_WORLD);
    timed_release("barrier", ts); ts = MPI_Wtime();
    benchfs_finalize(ctx);
    timed_release("finalize", ts);
    printf("CLIENT_DISCONNECTED_IDLE rank=%d\n", rank); fflush(stdout);
    sleep(2);
    /* Re-init reopens the logical connection while retaining the QPs. */
    ctx = benchfs_init(id, argv[1], NULL, 0, 4*1024*1024);
    REQUIRE(ctx != NULL);
    file = benchfs_open(ctx, path, BENCHFS_O_RDONLY);
    REQUIRE(file != NULL);
    unsigned char check[4096];
    REQUIRE(benchfs_read(file, check, sizeof check, 0) == sizeof check);
    for (size_t i=0; i<sizeof check; i++) REQUIRE(check[i] == value(i, 9));
    REQUIRE(benchfs_close(file) == 0);
    benchfs_finalize(ctx);
    printf("USER_BUFFER_ROUNDTRIP_PASS rank=%d\n", rank);
    MPI_Finalize();
    return 0;
}
