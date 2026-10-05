# Locusta user buffers and dispatch idle policy

`benchfs_write` and `benchfs_read` can use ordinary caller-owned buffers directly
for bulk RDMA. The C API, allocator contract, and IOR backend calls are unchanged.
The caller must keep the buffer valid until the synchronous call returns, and a
read destination must be exclusively writable during the call.

The embedded relay needs the external MR extension in
`patches/locusta-external-mr.patch` (base Locusta `0b5b83dbc3a271d8e04175b5004b2f8f5ba5e239`).
Apply it to an initialized Locusta checkout before building. The extension
adds a client/relay descriptor; the network Put grant and Get reply protocols
are unchanged. Both client and server must include the BenchFS logical-session
RPC added with this version. The standalone shared-daemon client uses copies.

```toml
[locusta]
arena_size = 268435456          # 256 MiB per runtime/rank
user_mr_cache = true
user_mr_min_bytes = 65536       # smaller calls retain the copy path
user_mr_cache_entries = 64
user_mr_cache_bytes = 67108864  # 64 MiB of page-rounded MR coverage per runtime
user_mr_cache_idle_ms = 30000
user_mr_prefetch_on_reuse = true # re-warm read destinations on every reuse
dispatch_idle_mode = "sleep"   # default; alternative: "spin_while_clients"
dispatch_idle_threshold = 16
dispatch_idle_sleep_us = 20
```

The MR cache uses **explicit on-demand paging (ODP)**. Kernel MMU notifiers
invalidate device translations when memory is unmapped or remapped. This is
necessary because BenchFS cannot observe `free`/`munmap` on arbitrary user
buffers. A conventional pinned MR cached solely by virtual address would be
unsafe. If ODP registration fails, a limit is reached, or the request is too
small, BenchFS uses the existing copy path. No registration failure turns a
valid request into an I/O failure. Very large RPC headers also use the copy path.

The cache reuses matching page ranges and access modes, evicts idle entries in
LRU order, and expires idle entries on the next registration attempt. The last
finalize clears unused registrations. In-flight entries remain charged to both
limits and cannot be evicted. Slice lookup checks the exact active application
range (not the rounded MR range), access direction, and length, including chunk
subranges. Each runtime has its own cache and registrations in its relay PD.

Registration prefetches the device translations of the whole range
(`ibv_advise_mr`, synchronous). Without it the first RDMA WRITE into a read
destination faults at the HCA and the server waits out an RNR NAK; on the
login-node HCA this cost about 650 ms per remapped 8 MiB read. A buffer that was
freed and reallocated at the same address hits the cache but has lost its
translations, so `user_mr_prefetch_on_reuse = true` (default) repeats the
prefetch for every read destination on reuse; its cost is a syscall per
`benchfs_read` of at least `user_mr_min_bytes`. Put sources fault locally
without that wait and are warmed only once. Releasing (munmap/free) a range that
still has a cached registration costs about 1 ms per 8 MiB for the invalidation.
`USER_MR_CACHE` reports `register_us`, `prefetch_us`, and `release_us`.

`user_mr_cache_bytes` bounds registered virtual coverage and conservatively
bounds its resident device translations; ODP does not pin the whole range up
front. Add this budget to the separately registered arena: the example permits
up to 320 MiB per client rank, or 5 GiB at ppn 16. Server fixed buffers, QP rings,
and other runtime memory are additional. `USER_MR_CACHE` at finalize reports
registrations, reuse hits, limit/registration fallbacks, and direct Put/Get counts.

Until the transport implements cancellation with a confirmed QP flush, an
external-DMA RPC timeout or cancellation aborts the process. Returning the user
buffer while the NIC might still access it is unsafe. Ordinary RPC errors after
DMA completion retain the normal error path.

`spin_while_clients` uses `YieldOnce` while at least one logical client session
is open. The first FFI initialization opens a session at the servers; the last
finalize closes it. Reinitialization reopens the session and reuses the existing
QPs. Server-to-server peers do not count. When the last client disconnects,
the dispatch loop returns to the configured idle sleep. A crashed client cannot
send close, so its session persists until server shutdown; this mode is intended
for jobs whose daemon lifetime is bounded by the job.

Locusta clients connect to every server and retain their sessions throughout
the run. Consequently **spin mode keeps one server runtime core busy for the
entire connected interval, including gaps between requests**. The default
remains `sleep`. A zero sleep duration always uses a cooperative yield; it never
calls the immediately-ready `sleep(0)`.

Measured CPU (Sirius job 43223, 2 server + 2 client hosts, ppn 1, per-thread
jiffies of `benchfsd_mpi` sampled every 250 ms): the server runtime thread used
0.97 core in `spin_while_clients` **and** 0.97 core in the default `sleep` mode
(UCX transport: 0.99). With `reactor_mode = true` the pluvio executor already
busy-polls the io_uring and Locusta reactors, so the dispatch task's 20 µs
sleep only adds completion latency; it does not free the core. The mode is
therefore a pure latency win in that configuration (4 KiB: +33 % write /
+46 % read), and only becomes a latency-versus-CPU tradeoff in configurations
where the executor otherwise idles. Other threads (`async`, `iou-wrk-*`) stayed
below 0.01 core in both modes.

The hardware correctness test is `tests/user_buffer_roundtrip.c`. Launch it on
allocated client nodes with MPI, passing the running servers' registry path.
It exercises data equality, repeated malloc/free, same-address mmap replacement,
unaligned ranges with sentinels, concurrent client ranks, multi-chunk RPCs, fsync,
and finalize/re-init. Policy tests under `rpc::user_buffer::tests` exercise cache
bounds and expiration without requiring an HCA.
