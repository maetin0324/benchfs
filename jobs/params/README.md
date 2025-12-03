# Unified Benchmark Parameters

This directory contains unified parameter configuration files for all benchmark file systems (BenchFS, BeeOND, CHFS).

## Parameter Files

| File | Purpose | Duration |
|------|---------|----------|
| `standard.conf` | Default comprehensive benchmark | 10-15 min |
| `minimal.conf` | Quick sanity check | ~1 min |
| `debug.conf` | Minimal parameters for debugging | ~30 sec |
| `debug_small.conf` | Debug with small transfers | Variable |
| `debug_large.conf` | Debug with large transfers | Variable |
| `scalability.conf` | Scalability test with increasing ppn | 20-30 min |
| `throughput.conf` | Maximum throughput test | 5-10 min |
| `chunk_comparison.conf` | Chunk size comparison | ~15 min |
| `shared_file.conf` | Shared file mode testing | ~10 min |
| `small_scale.conf` | Small transfer/high concurrency | Variable |
| `large_scale.conf` | Large scale (16+ nodes) | Variable |

## Usage

```bash
# Use default (standard.conf)
cd jobs/benchfs && ./benchfs.sh

# Use specific parameter file
PARAM_FILE=/path/to/jobs/params/minimal.conf ./benchfs.sh

# Or with relative path
PARAM_FILE=../params/scalability.conf ./benchfs.sh
```

## Parameter Structure

Each file contains three sections:

1. **Common IOR Parameters** - Used by all file systems
   - `transfer_size_list`
   - `block_size_list`
   - `ppn_list`
   - `ior_flags_list`

2. **BenchFS-specific Parameters**
   - `server_ppn_list`
   - `benchfs_chunk_size_list`

3. **CHFS-specific Parameters**
   - `CHFS_CHUNK_SIZE`
   - `CHFS_PROTOCOL`
   - `CHFS_DB_SIZE`
