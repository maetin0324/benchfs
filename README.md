# BenchFS

BenchFS is an experimental distributed file system built for benchmarking modern storage and transport stacks. It runs on the in-tree Pluvio runtime, speaks UCX active messages end-to-end, and exposes a path-centric API so researchers can iterate quickly on metadata, placement, and I/O ideas.

## Highlights
- **Distributed metadata without a directory tree** powered by consistent hashing, cached lookups, and path-based keys, keeping placement logic in `src/metadata` and `src/cache` easy to reason about.
- **Chunk stores backed by io_uring** that default to 4 MiB blocks and plug into the server runtime via the shared allocator, giving reproducible latency numbers from a single process.
- **UCX-first RPC paths** with Active Messages, zero-copy buffers, and RDMA-aware call types so data operations and metadata RPCs share one transport implementation.
- **Async API and C FFI** layers that wrap the same core logic, letting Rust clients use the safe POSIX-like interface while MPI or IOR runners bind against the exported C symbols.

## Repository layout
- `src/` hosts the Rust library modules; the crate root re-exports API, cache, config, metadata, RPC, server helpers, and storage layers for reuse in binaries and FFI code.
- `src/bin/` contains runnable targets:
  - `benchfsd`: single-node server that wires up the Pluvio runtime, UCX worker, metadata manager, and io_uring-backed chunk store.
  - `benchfsd_mpi`: MPI-enabled variant that derives node IDs from ranks, coordinates registry exchange, and spins up the same reactors on every process.
  - `benchfs_rpc_bench`: RPC-only MPI harness for latency/IOPS sweeps without touching storage.
- `benchfs.toml` / `benchfs.toml.example` document configurable chunk sizes, RDMA thresholds, and cache sizing for experiments.
- `docs/` holds deeper background including architecture notes, MPI usage, and environment setup guides for UCX and IOR runs.

## Building and testing
```bash
cargo build
cargo test
```
The default build targets the pure Rust stack; enable the MPI pathway when needed:
```bash
cargo build --release --features mpi-support --bin benchfsd_mpi
```

## Running a server
1. Create or adjust a configuration file using the provided example.
2. Launch the standalone daemon:
   ```bash
   cargo run --bin benchfsd --release -- benchfs.toml
   ```
3. For multi-node experiments, use MPI to start `benchfsd_mpi` after preparing a shared registry directory for UCX worker addresses.

## Documentation quick links
- Architecture overview and historical notes: `docs/ARCHITECTURE_COMPARISON.md`
- MPI deployment walkthrough: `docs/MPI_USAGE.md`
- IOR integration steps: `docs/SETUP_IOR.md`
