# BenchFS Parameter Templates

Parameter configuration files for BenchFS IOR benchmarks.

## Usage

```bash
# Use a specific parameter file
PARAM_FILE=./jobs/benchfs/params/minimal.conf ./jobs/benchfs/benchfs.sh

# Or with a label
PARAM_FILE=./jobs/benchfs/params/throughput.conf LABEL=throughput-test ./jobs/benchfs/benchfs.sh
```

## Available Templates

| File | Purpose | Duration (4 nodes) |
|------|---------|-------------------|
| `minimal.conf` | Quick sanity check | ~1 min |
| `debug.conf` | Troubleshooting | ~30 sec |
| `debug_large.conf` | Large-scale debugging | ~5 min |
| `standard.conf` | Comprehensive benchmark | ~10-15 min |
| `scalability.conf` | Scalability testing | ~20-30 min |
| `throughput.conf` | Maximum throughput | ~5-10 min |
| `chunk_comparison.conf` | Chunk size impact | ~15 min |
| `shared_file.conf` | Shared file mode | ~10 min |
| `large_scale.conf` | Large cluster (16+ nodes) | Variable |

## Parameter Reference

### transfer_size_list
- Size of each I/O transfer operation
- **Must use 4m** (larger sizes may cause UCX deadlock)

### block_size_list
- Total data size per process
- Must be a multiple of transfer_size
- Larger values = better sequential throughput

### ppn_list
- Processes per node
- Higher values = more parallelism, but also more contention

### ior_flags_list
- `-w -r -F`: Write, read, file-per-process (independent I/O)
- `-w -r`: Write, read, shared file (collective I/O)

### benchfs_chunk_size_list
- BenchFS internal chunk size (bytes)
- Larger = fewer RPCs, but more memory per chunk
- Recommended: 4MiB-32MiB

## Known Limitations

### CRITICAL: transfer_size must be 4m or smaller

**transfer_size >= 16m causes UCX Active Message deadlock** regardless of block_size or node count.

The deadlock occurs in the UCX transport layer when sending large Active Messages:
- 100% of runs with transfer_size=16m fail with deadlock
- 0% of runs with transfer_size=4m fail

This is due to UCX Rendezvous protocol issues when the message size exceeds certain thresholds.

**Always use transfer_size=4m or smaller.**

### Path handling

When specifying PARAM_FILE, use absolute paths or paths relative to the project root:
```bash
# Good - absolute path
PARAM_FILE=/path/to/params/large_scale.conf ./jobs/benchfs/benchfs.sh

# Good - relative from project root, will be converted to absolute
PARAM_FILE=./jobs/benchfs/params/large_scale.conf ./jobs/benchfs/benchfs.sh
```
