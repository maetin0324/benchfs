#!/bin/bash
#------- qsub option -----------
#PBS -A NBB
#PBS -l elapstim_req=01:00:00
#PBS -T openmpi
#PBS -v NQSV_MPI_VER=4.1.8/gcc11.4.0-cuda12.8.1
#------- Program execution -----------
set -euo pipefail

# Perf profiling job for BenchFS Read performance analysis
# This job runs a simplified benchmark with perf recording to identify bottlenecks

ulimit -n 1048576 2>/dev/null || ulimit -n 524288 2>/dev/null || ulimit -n 262144 2>/dev/null || ulimit -n 65536
echo "File descriptor limit: $(ulimit -n)"

module purge
module load "openmpi/$NQSV_MPI_VER"

source "$SCRIPT_DIR/common.sh"

JOB_START=$(timestamp)
NNODES=$(wc --lines "${PBS_NODEFILE}" | awk '{print $1}')
JOBID=$(echo "$PBS_JOBID" | cut -d : -f 2)
JOB_OUTPUT_DIR="${OUTPUT_DIR}/${JOB_START}-${JOBID}-${NNODES}-perf"
JOB_BACKEND_DIR="${BACKEND_DIR}/$(basename -- "${JOB_OUTPUT_DIR}")"
BENCHFS_REGISTRY_DIR="${JOB_BACKEND_DIR}/registry"
BENCHFS_DATA_DIR="/scr"
BENCHFSD_LOG_BASE_DIR="${JOB_OUTPUT_DIR}/benchfsd_logs"
IOR_OUTPUT_DIR="${JOB_OUTPUT_DIR}/ior_results"
PERF_OUTPUT_DIR="${JOB_OUTPUT_DIR}/perf_results"

PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
export LD_LIBRARY_PATH="${PROJECT_ROOT}/target/release:${LD_LIBRARY_PATH:-}"

IFS=" " read -r -a nqsii_mpiopts_array <<<"$NQSII_MPIOPTS"

echo "=========================================="
echo "Perf Profiling Job for BenchFS Read Analysis"
echo "=========================================="
echo "Job ID: $JOBID"
echo "Nodes: $NNODES"
echo "Output Dir: $JOB_OUTPUT_DIR"
echo "=========================================="

mkdir -p "${JOB_OUTPUT_DIR}"
mkdir -p "${JOB_BACKEND_DIR}"
mkdir -p "${BENCHFS_REGISTRY_DIR}"
mkdir -p "${BENCHFSD_LOG_BASE_DIR}"
mkdir -p "${IOR_OUTPUT_DIR}"
mkdir -p "${PERF_OUTPUT_DIR}"

cp "$0" "${JOB_OUTPUT_DIR}"
cp "${PBS_NODEFILE}" "${JOB_OUTPUT_DIR}"
printenv >"${JOB_OUTPUT_DIR}/env.txt"

trap 'rm -rf "${JOB_BACKEND_DIR}" ; exit 1' 1 2 3 15
trap 'rm -rf "${JOB_BACKEND_DIR}" ; exit 0' EXIT

# MPI settings
export OMPI_MCA_mpi_yield_when_idle=1
export OMPI_MCA_btl_base_warn_component_unused=0
export OMPI_MCA_mpi_show_handle_leaks=0
export RUST_LOG=warn
export RUST_BACKTRACE=full

supports_ucx_pml() {
  command -v ompi_info >/dev/null 2>&1 || return 1
  ompi_info --param pml all --level 9 2>/dev/null | grep -q "mca:pml:.*ucx"
}

if supports_ucx_pml; then
  USE_UCX_PML=1
  echo "UCX PML detected"
else
  USE_UCX_PML=0
  echo "Falling back to ob1/tcp"
fi

if [[ "${USE_UCX_PML}" -eq 1 ]]; then
  cmd_mpirun_common=(
    mpirun
    "${nqsii_mpiopts_array[@]}"
    --mca pml ucx
    --mca btl self
    --mca osc ucx
    -x PATH
    -x LD_LIBRARY_PATH
  )
else
  cmd_mpirun_common=(
    mpirun
    "${nqsii_mpiopts_array[@]}"
    --mca pml ob1
    --mca btl tcp,vader,self
    --mca btl_openib_allow_ib 0
    -x PATH
    -x LD_LIBRARY_PATH
  )
fi

# Kill any previous instances
"${cmd_mpirun_common[@]}" -np "$NNODES" -map-by ppr:1:node pkill -9 benchfsd_mpi || true

check_server_ready() {
  local expected_count=$1
  local max_attempts=60
  local attempt=0
  while [ $attempt -lt $max_attempts ]; do
    local ready_count=$(find "${BENCHFS_REGISTRY_DIR}" -name "node_*.addr" -type f 2>/dev/null | wc -l)
    if [ "$ready_count" -eq "$expected_count" ]; then
      echo "BenchFS servers registered: $ready_count/$expected_count"
      sleep 10
      return 0
    fi
    echo "Waiting for servers: $ready_count/$expected_count (attempt $((attempt+1))/$max_attempts)"
    sleep 1
    attempt=$((attempt + 1))
  done
  return 1
}

# Fixed parameters for profiling (matching the problematic scenario)
transfer_size="4m"
block_size="16g"
ppn=16
server_ppn=1
benchfs_chunk_size=33554432  # 32MB

server_np=$((NNODES * server_ppn))
np=$((NNODES * ppn))

echo "=========================================="
echo "Profiling Configuration:"
echo "  Transfer size: $transfer_size"
echo "  Block size: $block_size"
echo "  Client PPN: $ppn (total: $np)"
echo "  Server PPN: $server_ppn (total: $server_np)"
echo "  Chunk size: $benchfs_chunk_size bytes"
echo "=========================================="

rm -rf "${BENCHFS_REGISTRY_DIR}"/*
rm -rf "${BENCHFS_DATA_DIR}"/*

# Create config
config_file="${JOB_OUTPUT_DIR}/benchfs_perf.toml"
cat > "${config_file}" <<EOF
[node]
node_id = "node0"
data_dir = "${BENCHFS_DATA_DIR}"
log_level = "info"

[storage]
chunk_size = ${benchfs_chunk_size}
use_iouring = true
max_storage_gb = 0

[network]
bind_addr = "0.0.0.0:50051"
timeout_secs = 30
rdma_threshold_bytes = 32768
registry_dir = "${BENCHFS_REGISTRY_DIR}"

[cache]
metadata_cache_entries = 10000
chunk_cache_mb = 1024
cache_ttl_secs = 0
EOF

# ============================================================
# Start server with perf recording (for both Write and Read)
# ============================================================
echo ""
echo "=========================================="
echo "Starting BenchFS Server with Perf Recording"
echo "=========================================="

# Get first node for perf
FIRST_NODE=$(head -1 "${PBS_NODEFILE}")
echo "Perf will record on node: $FIRST_NODE"

# Create wrapper script to conditionally run perf on rank 0
PERF_WRAPPER="${JOB_OUTPUT_DIR}/perf_wrapper.sh"
cat > "${PERF_WRAPPER}" <<'WRAPPER_EOF'
#!/bin/bash
RANK=${OMPI_COMM_WORLD_RANK:-0}
PERF_OUTPUT_DIR="$1"
shift
BENCHFSD_BIN="$1"
shift

if [ "$RANK" -eq 0 ]; then
  echo "Rank 0: Running with perf record"
  # Record with call graph for flame graph generation
  perf record -g -F 99 -o "${PERF_OUTPUT_DIR}/perf_server_rank0.data" -- "$BENCHFSD_BIN" "$@"
else
  echo "Rank $RANK: Running without perf"
  exec "$BENCHFSD_BIN" "$@"
fi
WRAPPER_EOF
chmod +x "${PERF_WRAPPER}"

cmd_benchfsd_perf=(
  "${cmd_mpirun_common[@]}"
  -np "$server_np"
  --bind-to none
  -map-by "ppr:${server_ppn}:node"
  -x RUST_LOG
  -x RUST_BACKTRACE
  "${PERF_WRAPPER}"
  "${PERF_OUTPUT_DIR}"
  "${BENCHFS_PREFIX}/benchfsd_mpi"
  "${BENCHFS_REGISTRY_DIR}"
  "${config_file}"
)

echo "Starting BenchFS servers with perf..."
echo "Command: ${cmd_benchfsd_perf[*]}"
"${cmd_benchfsd_perf[@]}" > "${BENCHFSD_LOG_BASE_DIR}/benchfsd_perf.log" 2>&1 &
BENCHFSD_PID=$!

if ! check_server_ready "$server_np"; then
  echo "ERROR: Servers failed to start"
  cat "${BENCHFSD_LOG_BASE_DIR}/benchfsd_perf.log"
  exit 1
fi
sleep 5

# ============================================================
# Run Write+Read benchmark with perf on client
# ============================================================
echo ""
echo "=========================================="
echo "Running Write+Read Benchmark with Perf on Client"
echo "=========================================="

# Create client perf wrapper
CLIENT_PERF_WRAPPER="${JOB_OUTPUT_DIR}/client_perf_wrapper.sh"
cat > "${CLIENT_PERF_WRAPPER}" <<'CLIENT_WRAPPER_EOF'
#!/bin/bash
RANK=${OMPI_COMM_WORLD_RANK:-0}
PERF_OUTPUT_DIR="$1"
shift
IOR_BIN="$1"
shift

if [ "$RANK" -eq 0 ]; then
  echo "Client Rank 0: Running with perf record"
  perf record -g -F 99 -o "${PERF_OUTPUT_DIR}/perf_client_rank0.data" -- "$IOR_BIN" "$@"
else
  exec "$IOR_BIN" "$@"
fi
CLIENT_WRAPPER_EOF
chmod +x "${CLIENT_PERF_WRAPPER}"

cmd_ior_perf=(
  "${cmd_mpirun_common[@]}"
  -np "$np"
  --bind-to none
  --map-by "ppr:${ppn}:node"
  -x RUST_LOG
  -x RUST_BACKTRACE
  "${CLIENT_PERF_WRAPPER}"
  "${PERF_OUTPUT_DIR}"
  "${IOR_PREFIX}/src/ior"
  -vvv
  -a BENCHFS
  -t "$transfer_size"
  -b "$block_size"
  -w -r
  --benchfs.registry="${BENCHFS_REGISTRY_DIR}"
  --benchfs.datadir="${BENCHFS_DATA_DIR}"
  -o "${BENCHFS_DATA_DIR}/testfile"
  -O summaryFormat=JSON
  -O summaryFile="${IOR_OUTPUT_DIR}/ior_result.json"
)

echo "Running Write+Read benchmark with perf..."
echo "Command: ${cmd_ior_perf[*]}"
"${cmd_ior_perf[@]}" > "${IOR_OUTPUT_DIR}/ior_stdout.log" 2>&1

echo "Write+Read benchmark completed"

# ============================================================
# Phase 3: Stop server and collect perf data
# ============================================================
echo ""
echo "=========================================="
echo "Phase 3: Stopping Server and Collecting Perf Data"
echo "=========================================="

echo "Stopping server (perf data will be written)..."
"${cmd_mpirun_common[@]}" -np "$NNODES" -map-by ppr:1:node pkill -TERM benchfsd_mpi || true
sleep 5
kill $BENCHFSD_PID 2>/dev/null || true
wait $BENCHFSD_PID 2>/dev/null || true
"${cmd_mpirun_common[@]}" -np "$NNODES" -map-by ppr:1:node pkill -9 benchfsd_mpi || true

# ============================================================
# Phase 4: Generate perf reports
# ============================================================
echo ""
echo "=========================================="
echo "Phase 3: Generating Perf Reports"
echo "=========================================="

# Generate text reports
if [ -f "${PERF_OUTPUT_DIR}/perf_server_rank0.data" ]; then
  echo "Generating server perf report..."
  perf report -i "${PERF_OUTPUT_DIR}/perf_server_rank0.data" --stdio > "${PERF_OUTPUT_DIR}/perf_server_report.txt" 2>&1 || true
  perf report -i "${PERF_OUTPUT_DIR}/perf_server_rank0.data" --stdio --sort=dso,symbol > "${PERF_OUTPUT_DIR}/perf_server_by_dso.txt" 2>&1 || true

  # Generate collapsed stack for flame graph (if stackcollapse-perf.pl is available)
  if command -v stackcollapse-perf.pl >/dev/null 2>&1; then
    perf script -i "${PERF_OUTPUT_DIR}/perf_server_rank0.data" | stackcollapse-perf.pl > "${PERF_OUTPUT_DIR}/server_collapsed.txt" 2>/dev/null || true
  else
    # Alternative: just generate perf script output
    perf script -i "${PERF_OUTPUT_DIR}/perf_server_rank0.data" > "${PERF_OUTPUT_DIR}/perf_server_script.txt" 2>&1 || true
  fi
else
  echo "WARNING: Server perf data not found"
fi

if [ -f "${PERF_OUTPUT_DIR}/perf_client_rank0.data" ]; then
  echo "Generating client perf report..."
  perf report -i "${PERF_OUTPUT_DIR}/perf_client_rank0.data" --stdio > "${PERF_OUTPUT_DIR}/perf_client_report.txt" 2>&1 || true
  perf report -i "${PERF_OUTPUT_DIR}/perf_client_rank0.data" --stdio --sort=dso,symbol > "${PERF_OUTPUT_DIR}/perf_client_by_dso.txt" 2>&1 || true
  perf script -i "${PERF_OUTPUT_DIR}/perf_client_rank0.data" > "${PERF_OUTPUT_DIR}/perf_client_script.txt" 2>&1 || true
else
  echo "WARNING: Client perf data not found"
fi

# ============================================================
# Phase 4: Summary
# ============================================================
echo ""
echo "=========================================="
echo "Summary"
echo "=========================================="

echo "IOR result:"
if [ -f "${IOR_OUTPUT_DIR}/ior_result.json" ]; then
  cat "${IOR_OUTPUT_DIR}/ior_result.json" | grep -E '"bwMiB"|"access"' | head -8
fi

echo ""
echo "Perf output files:"
ls -la "${PERF_OUTPUT_DIR}/"

echo ""
echo "Top functions in server (read operation):"
if [ -f "${PERF_OUTPUT_DIR}/perf_server_report.txt" ]; then
  head -50 "${PERF_OUTPUT_DIR}/perf_server_report.txt"
fi

echo ""
echo "Top functions in client (read operation):"
if [ -f "${PERF_OUTPUT_DIR}/perf_client_report.txt" ]; then
  head -50 "${PERF_OUTPUT_DIR}/perf_client_report.txt"
fi

echo ""
echo "=========================================="
echo "Perf profiling job completed"
echo "Results saved to: ${JOB_OUTPUT_DIR}"
echo "=========================================="
