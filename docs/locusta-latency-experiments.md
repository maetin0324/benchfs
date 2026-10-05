# Locusta latency residual: experimental probes (C)

These probes exist to attribute the residual difference between the Locusta
and UCX transports at 2 server + 2 client, ppn 1 after the user-buffer MR cache
(A) and the dispatch idle policy (B). They are **experiments, not
improvements**: both are compiled out unless the Cargo features are enabled,
their TOML knobs default to off, and neither changes how the main reactor is
polled or the Locusta protocol concepts for the default build.

```sh
cargo build --release --features mpi-support,transport-locusta,exp-uring-reap,exp-inline-get \
  --lib --bin benchfsd_mpi
```

## C1: io_uring completion reaping cadence

Default path: the executor polls the `IoUringReactor` every
`[scheduling] reactor_poll_interval` iterations, its `status()` is cached for
`status_cache_iters` iterations, and `status()` only reports `Running` after
`[iouring] submit_timeout_us` / `complete_timeout_us` have elapsed since the
last submit or when `submit_depth` is reached. A storage completion is therefore
noticed with a delay that depends on these three cadences.

- Config-only variant: `reactor_poll_interval = 1`, `status_cache_iters = 1`,
  `submit_timeout_us = 0`, `complete_timeout_us = 0`.
- Feature `exp-uring-reap` with `[locusta] exp_dispatch_reap_uring = true`:
  the server dispatch loop calls `poll_submit_and_completions()` on the
  io_uring reactor on every iteration, independent of the reactor cadence.
  Cost: one `io_uring_enter`-free CQ scan per iteration while the loop spins;
  with `dispatch_idle_mode = "spin_while_clients"` this is continuous.

## C2: Get reply protocol

Default Locusta Get: the server posts an RDMA WRITE into the client's buffer
and then the small reply on the same QP; the client completes on the reply.
UCX active-message replies carry small data inside the message.

Feature `exp-inline-get` with `[locusta] exp_inline_get_max_bytes = N`: Get
replies of at most `N` bytes carry the data inside the small reply and post no
RDMA WRITE (`RoundtripGetHandle::reply_inline`, added by
`patches/locusta-external-mr.patch`). The client copies the bytes from the
reply into the destination. Costs: one server-side copy of `n` bytes into the
reply, one client-side copy out of it, and the reply travels through the
32 KiB small-response path (`N` must stay well below that). Large transfers are
unaffected; this probe targets the 4 KiB read residual only.

Result (Sirius, 2026-10-05, 2 server + 2 client, ppn 1, MR cache + spin enabled,
3 repetitions): the config-only cadence variant raised 4 MiB write by 8 %,
4 MiB read by 6 %, 4 KiB write by 60 % and 4 KiB read by 24 %; the dispatch-loop
reap variant gave 3 % / 6 % / 72 % / 28 %. The inline Get reply changed nothing
(4 KiB read 0.98×). The residual against the UCX transport is therefore the
io_uring completion-reaping cadence, not the Get reply protocol. Neither probe
is enabled by default; the cadence change has not been evaluated at high
parallelism, where losing submit batching may cost bandwidth.
