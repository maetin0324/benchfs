#!/bin/bash
#------- qsub option -----------
#PBS -A NBB
#PBS -l elapstim_req=01:00:00
#PBS -T openmpi
#PBS -v NQSV_MPI_VER=4.1.8/gcc11.4.0-cuda12.8.1
#------- Program execution -----------
set -euo pipefail

# Increase file descriptor limit for large-scale MPI jobs
ulimit -n 65536

module purge
module load "openmpi/$NQSV_MPI_VER"

# Requires
# - SCRIPT_DIR
# - OUTPUT_DIR
# - BACKEND_DIR
# - BENCHFS_PREFIX
# - IOR_PREFIX

source "$SCRIPT_DIR/common.sh"

JOB_START=$(timestamp)
NNODES=$(wc --lines "${PBS_NODEFILE}" | awk '{print $1}')
JOBID=$(echo "$PBS_JOBID" | cut -d : -f 2)
JOB_OUTPUT_DIR="${OUTPUT_DIR}/${JOB_START}-${JOBID}-${NNODES}"
JOB_BACKEND_DIR="${BACKEND_DIR}/$(basename -- "${JOB_OUTPUT_DIR}")"
BENCHFS_REGISTRY_DIR="${JOB_BACKEND_DIR}/registry"
BENCHFS_DATA_DIR="/scr"
BENCHFSD_LOG_BASE_DIR="${JOB_OUTPUT_DIR}/benchfsd_logs"
IOR_OUTPUT_DIR="${JOB_OUTPUT_DIR}/ior_results"
PERF_OUTPUT_DIR="${JOB_OUTPUT_DIR}/perf_results"

# Calculate project root from SCRIPT_DIR and set LD_LIBRARY_PATH dynamically
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
export LD_LIBRARY_PATH="${PROJECT_ROOT}/target/release:${LD_LIBRARY_PATH:-}"

IFS=" " read -r -a nqsii_mpiopts_array <<<"$NQSII_MPIOPTS"

echo "prepare the output directory: ${JOB_OUTPUT_DIR}"
mkdir -p "${JOB_OUTPUT_DIR}"
cp "$0" "${JOB_OUTPUT_DIR}"
cp "${PBS_NODEFILE}" "${JOB_OUTPUT_DIR}"
cp "${SCRIPT_DIR}/common.sh" "${JOB_OUTPUT_DIR}"
printenv >"${JOB_OUTPUT_DIR}/env.txt"

# Debug: Print key variables
echo "=========================================="
echo "PERF DEBUG Job Configuration"
echo "=========================================="
echo "BENCHFS_PREFIX: ${BENCHFS_PREFIX}"
echo "IOR_PREFIX: ${IOR_PREFIX}"
echo "BACKEND_DIR: ${BACKEND_DIR}"
echo "Registry: ${BENCHFS_REGISTRY_DIR}"
echo "Data: ${BENCHFS_DATA_DIR}"
echo ""
echo "Checking binary:"
ls -la "${BENCHFS_PREFIX}/benchfsd_mpi" || echo "ERROR: Binary not found at ${BENCHFS_PREFIX}/benchfsd_mpi"
echo ""
echo "Checking IOR:"
ls -la "${IOR_PREFIX}/src/ior" || echo "ERROR: IOR not found at ${IOR_PREFIX}/src/ior"
echo ""
echo "Checking perf:"
which perf || echo "ERROR: perf not found"
perf --version || true
echo "=========================================="
echo ""

echo "prepare backend dir: ${JOB_BACKEND_DIR}"
mkdir -p "${JOB_BACKEND_DIR}"
trap 'rm -rf "${JOB_BACKEND_DIR}" ; exit 1' 1 2 3 15
trap 'rm -rf "${JOB_BACKEND_DIR}" ; exit 0' EXIT

echo "prepare benchfs registry dir: ${BENCHFS_REGISTRY_DIR}"
mkdir -p "${BENCHFS_REGISTRY_DIR}"

echo "prepare benchfs data dir: ${BENCHFS_DATA_DIR}"
mkdir -p "${BENCHFS_DATA_DIR}"

echo "prepare benchfsd log dir: ${BENCHFSD_LOG_BASE_DIR}"
mkdir -p "${BENCHFSD_LOG_BASE_DIR}"

echo "prepare ior output dir: ${IOR_OUTPUT_DIR}"
mkdir -p "${IOR_OUTPUT_DIR}"

echo "prepare perf output dir: ${PERF_OUTPUT_DIR}"
mkdir -p "${PERF_OUTPUT_DIR}"

# Network Configuration
export OMPI_MCA_mpi_yield_when_idle=1
export OMPI_MCA_btl_base_warn_component_unused=0
export OMPI_MCA_mpi_show_handle_leaks=0

export RUST_LOG=info
export RUST_BACKTRACE=full

# MPI Configuration
supports_ucx_pml() {
  command -v ompi_info >/dev/null 2>&1 || return 1
  ompi_info --param pml all --level 9 2>/dev/null | grep -q "mca:pml:.*ucx"
}

if supports_ucx_pml; then
  USE_UCX_PML=1
  echo "UCX PML detected – using --mca pml ucx configuration"
else
  USE_UCX_PML=0
  echo "WARNING: UCX PML not available – falling back to ob1/tcp configuration"
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

# Kill any previous benchfsd instances
cmd_mpirun_kill=(
  "${cmd_mpirun_common[@]}"
  -np "$NNODES"
  -map-by ppr:1:node
  pkill -9 benchfsd_mpi
)

echo "Kill any previous benchfsd instances"
"${cmd_mpirun_kill[@]}" || true

check_server_ready() {
  local max_attempts=60
  local attempt=0

  while [ $attempt -lt $max_attempts ]; do
    local ready_count=$(find "${BENCHFS_REGISTRY_DIR}" -name "node_*.addr" -type f 2>/dev/null | wc -l)

    if [ "$ready_count" -eq "$NNODES" ]; then
      echo "BenchFS servers registered: $ready_count/$NNODES nodes"
      local rpc_wait_time=10
      echo "Waiting ${rpc_wait_time}s for RPC handler initialization..."
      sleep $rpc_wait_time
      echo "BenchFS servers are fully ready"
      return 0
    fi

    echo "Waiting for BenchFS servers: $ready_count/$NNODES nodes (attempt $((attempt+1))/$max_attempts)"
    sleep 1
    attempt=$((attempt + 1))
  done

  echo "ERROR: BenchFS servers failed to start after $max_attempts seconds"
  return 1
}

# Simple debug parameters - single run only
benchfs_chunk_size=4194304
ppn=1
np=$((NNODES * ppn))
transfer_size="4m"
block_size="64m"
ior_flags="-w -r -F"
runid=0

# Perf recording duration (seconds) - collect samples while IOR is running
PERF_DURATION=60

echo "=========================================="
echo "PERF DEBUG Run"
echo "Nodes: $NNODES, PPN: $ppn, NP: $np"
echo "Transfer size: $transfer_size, Block size: $block_size"
echo "IOR flags: $ior_flags"
echo "BenchFS chunk size: $benchfs_chunk_size bytes"
echo "Perf duration: ${PERF_DURATION}s"
echo "=========================================="

# Clean up previous run
rm -rf "${BENCHFS_REGISTRY_DIR}"/*
rm -rf "${BENCHFS_DATA_DIR}"/*

run_log_dir="${BENCHFSD_LOG_BASE_DIR}/run_${runid}"
mkdir -p "${run_log_dir}"

# Create BenchFS config file for this run
config_file="${JOB_OUTPUT_DIR}/benchfs_${runid}.toml"
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

# Launch BenchFS servers
echo "Launching BenchFS servers..."
echo "Registry dir: ${BENCHFS_REGISTRY_DIR}"
echo "Config file: ${config_file}"
echo "Binary: ${BENCHFS_PREFIX}/benchfsd_mpi"

# Verify files exist
ls -la "${BENCHFS_REGISTRY_DIR}" || echo "WARNING: Registry dir not accessible"
ls -la "${config_file}" || echo "WARNING: Config file not found"
ls -la "${BENCHFS_PREFIX}/benchfsd_mpi" || echo "WARNING: Binary not found"

cmd_benchfsd=(
  "${cmd_mpirun_common[@]}"
  -np "$NNODES"
  --bind-to none
  -map-by ppr:1:node
  -x RUST_LOG
  -x RUST_BACKTRACE
  "${BENCHFS_PREFIX}/benchfsd_mpi"
  "${BENCHFS_REGISTRY_DIR}"
  "${config_file}"
)

echo "${cmd_benchfsd[@]}"
"${cmd_benchfsd[@]}" > "${run_log_dir}/benchfsd_stdout.log" 2> "${run_log_dir}/benchfsd_stderr.log" &
BENCHFSD_PID=$!

# Wait for servers to be ready
if ! check_server_ready; then
  echo "ERROR: BenchFS servers failed to start"
  echo "=========================================="
  echo "BenchFS Server STDOUT:"
  echo "=========================================="
  cat "${run_log_dir}/benchfsd_stdout.log" || echo "No stdout log"
  echo ""
  echo "=========================================="
  echo "BenchFS Server STDERR:"
  echo "=========================================="
  cat "${run_log_dir}/benchfsd_stderr.log" || echo "No stderr log"
  echo ""
  kill $BENCHFSD_PID 2>/dev/null || true
  wait $BENCHFSD_PID 2>/dev/null || true
  exit 1
fi

# Give servers a bit more time to fully initialize
sleep 5

# MPI Debug: Testing MPI communication before IOR
echo "MPI Debug: Testing MPI communication before IOR"
"${cmd_mpirun_common[@]}" -np "$np" --map-by "ppr:${ppn}:node" hostname > "${IOR_OUTPUT_DIR}/mpi_test_${runid}.txt" 2>&1
echo "MPI Debug: Communication test completed"

# ==============================================================================
# PERF PROFILING SECTION
# ==============================================================================
echo ""
echo "=========================================="
echo "Starting perf profiling..."
echo "=========================================="

# Create a wrapper script that will be run on each node with perf
PERF_WRAPPER="${JOB_OUTPUT_DIR}/perf_wrapper.sh"
cat > "${PERF_WRAPPER}" <<'WRAPPER_EOF'
#!/bin/bash
# This script runs on each node and attaches perf to the local benchfsd_mpi process

PERF_OUTPUT_DIR="$1"
PERF_DURATION="$2"
HOSTNAME=$(hostname)

echo "[${HOSTNAME}] Starting perf profiling..."

# Find the local benchfsd_mpi PID
BENCHFSD_PID=$(pgrep -x benchfsd_mpi | head -1)

if [ -z "$BENCHFSD_PID" ]; then
    echo "[${HOSTNAME}] ERROR: Could not find benchfsd_mpi process"
    exit 1
fi

echo "[${HOSTNAME}] Found benchfsd_mpi PID: ${BENCHFSD_PID}"

# Start perf record with call graph
# -g: enable call graph
# -F 99: sample at 99 Hz
# --call-graph dwarf: use DWARF for call graph (better for Rust)
perf record \
    -g \
    -F 99 \
    --call-graph dwarf,32768 \
    -p "${BENCHFSD_PID}" \
    -o "${PERF_OUTPUT_DIR}/perf_server_${HOSTNAME}.data" \
    -- sleep "${PERF_DURATION}" &
PERF_SERVER_PID=$!

echo "[${HOSTNAME}] Started perf recording (PID: ${PERF_SERVER_PID}) for ${PERF_DURATION}s"

# Wait for perf to finish
wait $PERF_SERVER_PID 2>/dev/null || true

echo "[${HOSTNAME}] Perf recording completed"

# Generate report
echo "[${HOSTNAME}] Generating perf report..."
perf report \
    -i "${PERF_OUTPUT_DIR}/perf_server_${HOSTNAME}.data" \
    --stdio \
    --no-children \
    -g fractal,0.5,caller \
    > "${PERF_OUTPUT_DIR}/perf_server_${HOSTNAME}_report.txt" 2>&1 || true

# Generate collapsed stack for flamegraph
perf script \
    -i "${PERF_OUTPUT_DIR}/perf_server_${HOSTNAME}.data" \
    > "${PERF_OUTPUT_DIR}/perf_server_${HOSTNAME}_script.txt" 2>&1 || true

echo "[${HOSTNAME}] Perf analysis completed"
WRAPPER_EOF

chmod +x "${PERF_WRAPPER}"

# Run perf wrapper on all nodes
echo "Running perf profiling on all nodes..."
"${cmd_mpirun_common[@]}" \
  -np "$NNODES" \
  --bind-to none \
  -map-by ppr:1:node \
  "${PERF_WRAPPER}" "${PERF_OUTPUT_DIR}" "${PERF_DURATION}" &
PERF_WRAPPER_PID=$!

# Wait a bit for perf to attach
sleep 5

# Run IOR benchmark while perf is recording
echo "Running IOR benchmark (perf is recording in background)..."
ior_json_file="${IOR_OUTPUT_DIR}/ior_result_${runid}.json"
ior_stdout_file="${IOR_OUTPUT_DIR}/ior_stdout_${runid}.log"

# Also profile IOR client on rank 0
IOR_PERF_WRAPPER="${JOB_OUTPUT_DIR}/ior_perf_wrapper.sh"
cat > "${IOR_PERF_WRAPPER}" <<'IOR_WRAPPER_EOF'
#!/bin/bash
# Wrapper to run IOR with perf on rank 0

PERF_OUTPUT_DIR="$1"
shift
IOR_BINARY="$1"
shift

HOSTNAME=$(hostname)
OMPI_RANK="${OMPI_COMM_WORLD_RANK:-${PMI_RANK:-0}}"

if [ "$OMPI_RANK" -eq 0 ]; then
    echo "[${HOSTNAME}] Rank 0: Running IOR with perf profiling..."
    perf record \
        -g \
        -F 99 \
        --call-graph dwarf,32768 \
        -o "${PERF_OUTPUT_DIR}/perf_ior_rank0.data" \
        -- "${IOR_BINARY}" "$@"

    # Generate report
    perf report \
        -i "${PERF_OUTPUT_DIR}/perf_ior_rank0.data" \
        --stdio \
        --no-children \
        -g fractal,0.5,caller \
        > "${PERF_OUTPUT_DIR}/perf_ior_rank0_report.txt" 2>&1 || true

    perf script \
        -i "${PERF_OUTPUT_DIR}/perf_ior_rank0.data" \
        > "${PERF_OUTPUT_DIR}/perf_ior_rank0_script.txt" 2>&1 || true
else
    # Other ranks run IOR normally
    "${IOR_BINARY}" "$@"
fi
IOR_WRAPPER_EOF

chmod +x "${IOR_PERF_WRAPPER}"

cmd_ior=(
  "${cmd_mpirun_common[@]}"
  -np "$np"
  --bind-to none
  --map-by "ppr:${ppn}:node"
  -x RUST_LOG=warn
  -x RUST_BACKTRACE
  -x OMPI_COMM_WORLD_RANK
  "${IOR_PERF_WRAPPER}"
  "${PERF_OUTPUT_DIR}"
  "${IOR_PREFIX}/src/ior"
  -vvv
  -a BENCHFS
  -t "$transfer_size"
  -b "$block_size"
  $ior_flags
  --benchfs.registry="${BENCHFS_REGISTRY_DIR}"
  --benchfs.datadir="${BENCHFS_DATA_DIR}"
  -o "${BENCHFS_DATA_DIR}/testfile"
  -O summaryFormat=JSON
  -O summaryFile="${ior_json_file}"
)

echo "${cmd_ior[@]}"
timeout 300 "${cmd_ior[@]}" \
  > "${ior_stdout_file}" \
  2> "${IOR_OUTPUT_DIR}/ior_stderr_${runid}.log" &
IOR_PID=$!

echo "IOR started with PID: ${IOR_PID}"
echo "Waiting for IOR to complete (timeout: 300s)..."

# Wait for IOR to complete or timeout
wait $IOR_PID 2>/dev/null
IOR_EXIT_CODE=$?

echo "IOR completed with exit code: ${IOR_EXIT_CODE}"

# Wait for perf wrapper to complete
echo "Waiting for perf profiling to complete..."
wait $PERF_WRAPPER_PID 2>/dev/null || true

# ==============================================================================
# ADDITIONAL DIAGNOSTICS IF IOR HUNG
# ==============================================================================
if [ $IOR_EXIT_CODE -ne 0 ]; then
    echo ""
    echo "=========================================="
    echo "IOR did not complete successfully (exit code: ${IOR_EXIT_CODE})"
    echo "Running additional diagnostics..."
    echo "=========================================="

    # Collect stack traces from all processes
    echo "Collecting stack traces from benchfsd_mpi processes..."
    "${cmd_mpirun_common[@]}" \
      -np "$NNODES" \
      --bind-to none \
      -map-by ppr:1:node \
      bash -c '
        HOSTNAME=$(hostname)
        BENCHFSD_PID=$(pgrep -x benchfsd_mpi | head -1)
        if [ -n "$BENCHFSD_PID" ]; then
            echo "=========================================="
            echo "[$HOSTNAME] Stack trace for PID $BENCHFSD_PID:"
            echo "=========================================="
            cat /proc/$BENCHFSD_PID/stack 2>/dev/null || echo "Could not read kernel stack"
            echo ""
            echo "User-space stack (via gdb):"
            gdb -batch -ex "thread apply all bt" -p $BENCHFSD_PID 2>/dev/null || echo "gdb not available"
        else
            echo "[$HOSTNAME] No benchfsd_mpi process found"
        fi
      ' > "${PERF_OUTPUT_DIR}/stack_traces.txt" 2>&1 || true

    # Get /proc/pid/status for blocking info
    echo "Collecting process status..."
    "${cmd_mpirun_common[@]}" \
      -np "$NNODES" \
      --bind-to none \
      -map-by ppr:1:node \
      bash -c '
        HOSTNAME=$(hostname)
        BENCHFSD_PID=$(pgrep -x benchfsd_mpi | head -1)
        if [ -n "$BENCHFSD_PID" ]; then
            echo "=========================================="
            echo "[$HOSTNAME] Process status for PID $BENCHFSD_PID:"
            echo "=========================================="
            cat /proc/$BENCHFSD_PID/status 2>/dev/null || echo "Could not read status"
            echo ""
            echo "wchan (waiting channel):"
            cat /proc/$BENCHFSD_PID/wchan 2>/dev/null || echo "Could not read wchan"
            echo ""
            echo "File descriptors:"
            ls -la /proc/$BENCHFSD_PID/fd 2>/dev/null | head -20 || echo "Could not read fd"
        fi
      ' > "${PERF_OUTPUT_DIR}/process_status.txt" 2>&1 || true
fi

# Stop BenchFS servers
echo "Stopping BenchFS servers..."
kill $BENCHFSD_PID 2>/dev/null || true
wait $BENCHFSD_PID 2>/dev/null || true

# Force cleanup of any orphaned processes
echo "Force cleanup of orphaned processes..."
pkill -9 benchfsd_mpi || true

# Wait for cleanup
sleep 5

# ==============================================================================
# SUMMARY
# ==============================================================================
echo ""
echo "=========================================="
echo "PERF DEBUG SUMMARY"
echo "=========================================="
echo "Output directory: ${JOB_OUTPUT_DIR}"
echo ""
echo "Perf results:"
ls -la "${PERF_OUTPUT_DIR}"/*.txt 2>/dev/null || echo "No perf text reports found"
echo ""
echo "Perf data files:"
ls -la "${PERF_OUTPUT_DIR}"/*.data 2>/dev/null || echo "No perf data files found"
echo ""
echo "IOR results:"
ls -la "${IOR_OUTPUT_DIR}/" 2>/dev/null || echo "No IOR results found"
echo ""
echo "To analyze perf reports, check:"
echo "  ${PERF_OUTPUT_DIR}/perf_server_*_report.txt  - Server side profiles"
echo "  ${PERF_OUTPUT_DIR}/perf_ior_rank0_report.txt - Client side profile"
echo ""
echo "If IOR hung, check:"
echo "  ${PERF_OUTPUT_DIR}/stack_traces.txt   - Stack traces at hang time"
echo "  ${PERF_OUTPUT_DIR}/process_status.txt - Process status info"
echo "=========================================="

echo "PERF DEBUG job completed"
