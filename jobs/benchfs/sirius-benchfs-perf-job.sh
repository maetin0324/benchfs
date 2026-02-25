#!/bin/bash
#------- qsub option -----------
#PBS -q gold
#PBS -A NBB
#------- Program execution -----------
# NOTE: DO NOT use "set -e" — IOR/perf may fail and we still want to collect data
set +e

# Perf profiling job for BenchFS on Sirius
# Profiles both server and client to identify performance bottlenecks

ulimit -n 1048576 2>/dev/null || ulimit -n 524288 2>/dev/null || ulimit -n 262144 2>/dev/null || ulimit -n 65536
echo "File descriptor limit: $(ulimit -n)"

# Clean up exported bash functions from -V to avoid /bin/sh errors on remote nodes
cleanup_exported_bash_functions() {
  unset -f module ml _module_raw 2>/dev/null || true
  local vars_to_unset=()
  while IFS= read -r line; do
    local var_name="${line%%=*}"
    if [[ "$var_name" == BASH_FUNC_* ]]; then
      vars_to_unset+=("$var_name")
    fi
  done < <(env)
  for var in "${vars_to_unset[@]}"; do
    unset "$var" 2>/dev/null || true
  done
  for func_name in module ml _module_raw; do
    unset "BASH_FUNC_${func_name}%%" 2>/dev/null || true
    unset "BASH_FUNC_${func_name}()" 2>/dev/null || true
  done
}

# Load modules for Sirius
module purge
module load openmpi/5.0.9/gcc11.5.0
cleanup_exported_bash_functions

# ==============================================================================
# Path Configuration (passed via qsub -V from sirius-benchfs-perf.sh)
# ==============================================================================
SCRIPT_DIR="${SCRIPT_DIR:-/work/NBB/rmaeda/workspace/rust/benchfs/jobs/benchfs}"
PROJECT_ROOT="${PROJECT_ROOT:-/work/NBB/rmaeda/workspace/rust/benchfs}"
: ${OUTPUT_DIR:="$PROJECT_ROOT/results/benchfs/$(date +%Y.%m.%d-%H.%M.%S)-sirius-perf"}
BACKEND_DIR="${BACKEND_DIR:-$PROJECT_ROOT/backend/benchfs}"
BENCHFS_PREFIX="${BENCHFS_PREFIX:-${PROJECT_ROOT}/target/release}"
IOR_PREFIX="${IOR_PREFIX:-${PROJECT_ROOT}/ior_integration/ior}"

source "$SCRIPT_DIR/common.sh"

JOB_START=$(timestamp)
NNODES=$(wc -l < "${PBS_NODEFILE}")
JOBID=$(echo "$PBS_JOBID" | cut -d . -f 1)
JOB_OUTPUT_DIR="${OUTPUT_DIR}/${JOB_START}-${JOBID}-${NNODES}n-perf"
JOB_BACKEND_DIR="${BACKEND_DIR}/$(basename -- "${JOB_OUTPUT_DIR}")"
BENCHFS_REGISTRY_DIR="${JOB_BACKEND_DIR}/registry"

# Sirius scratch directory detection
detect_scratch_dir() {
  if [ -d "/scr/${PBS_JOBID}" ]; then
    echo "/scr/${PBS_JOBID}"
    return 0
  fi
  for n in 0 1 2 3; do
    local candidate="/scr${n}/${PBS_JOBID}"
    if [ -d "$candidate" ]; then
      echo "$candidate"
      return 0
    fi
  done
  echo "ERROR: No scratch directory found for job ${PBS_JOBID}" >&2
  return 1
}
BENCHFS_DATA_DIR="$(detect_scratch_dir)"

BENCHFSD_LOG_BASE_DIR="${JOB_OUTPUT_DIR}/benchfsd_logs"
IOR_OUTPUT_DIR="${JOB_OUTPUT_DIR}/ior_results"
PERF_OUTPUT_DIR="${JOB_OUTPUT_DIR}/perf_results"

export LD_LIBRARY_PATH="${PROJECT_ROOT}/target/release:/home/NBB/rmaeda/.local/lib:${LD_LIBRARY_PATH:-}"

echo "=========================================="
echo "BenchFS Perf Profiling Job (Sirius)"
echo "=========================================="
echo "Job ID: $JOBID"
echo "Nodes: $NNODES"
echo "Output Dir: $JOB_OUTPUT_DIR"
echo "Scratch Dir: $BENCHFS_DATA_DIR"
echo "=========================================="

mkdir -p "${JOB_OUTPUT_DIR}"
mkdir -p "${JOB_BACKEND_DIR}"
mkdir -p "${BENCHFS_REGISTRY_DIR}"
mkdir -p "${BENCHFSD_LOG_BASE_DIR}"
mkdir -p "${IOR_OUTPUT_DIR}"
mkdir -p "${PERF_OUTPUT_DIR}"

cp "$0" "${JOB_OUTPUT_DIR}"
cp "${PBS_NODEFILE}" "${JOB_OUTPUT_DIR}"
cp "${SCRIPT_DIR}/common.sh" "${JOB_OUTPUT_DIR}"
printenv > "${JOB_OUTPUT_DIR}/env.txt"

# Check debug symbols in binaries
echo "Checking debug symbols..."
file "${BENCHFS_PREFIX}/benchfsd_mpi" 2>/dev/null | grep -q "not stripped" \
  && echo "  benchfsd_mpi: has debug symbols" \
  || echo "  WARNING: benchfsd_mpi may be stripped"
file "${BENCHFS_PREFIX}/libbenchfs.so" 2>/dev/null | grep -q "not stripped" \
  && echo "  libbenchfs.so: has debug symbols" \
  || echo "  WARNING: libbenchfs.so may be stripped"

echo ""
echo "Checking perf:"
which perf || echo "ERROR: perf not found"
perf --version || true

# Cleanup traps
cleanup_and_exit() {
  local exit_code=${1:-1}
  local signal_name=${2:-"unknown"}
  echo ""
  echo "=========================================="
  echo "Job interrupted by signal: $signal_name"
  echo "Cleaning up..."
  echo "=========================================="
  pkill -TERM benchfsd_mpi 2>/dev/null || true
  sleep 3
  pkill -9 benchfsd_mpi 2>/dev/null || true
  rm -rf "${JOB_BACKEND_DIR}" 2>/dev/null || true
  exit "$exit_code"
}
trap 'cleanup_and_exit 1 "SIGHUP"' 1
trap 'cleanup_and_exit 1 "SIGINT"' 2
trap 'cleanup_and_exit 1 "SIGQUIT"' 3
trap 'cleanup_and_exit 1 "SIGTERM"' 15
trap 'rm -rf "${JOB_BACKEND_DIR}" 2>/dev/null || true; exit 0' EXIT

# ==============================================================================
# MPI Configuration for Sirius (OpenMPI 5.0.9)
# ==============================================================================
export OMPI_MCA_mpi_yield_when_idle=1
export OMPI_MCA_btl_base_warn_component_unused=0
export OMPI_MCA_mpi_show_handle_leaks=0
export RUST_BACKTRACE=full

supports_ucx_pml() {
  command -v ompi_info >/dev/null 2>&1 || return 1
  ompi_info 2>/dev/null | grep -q "pml.*ucx"
}

if supports_ucx_pml; then
  USE_UCX_PML=1
  echo "UCX PML detected"
else
  USE_UCX_PML=0
  echo "WARNING: UCX PML not available – falling back to ob1/tcp"
fi

if [[ "${USE_UCX_PML}" -eq 1 ]]; then
  cmd_mpirun_common=(
    mpirun
    --mca pml ucx
    --mca btl self
    --mca osc ucx
    -x PATH
    -x LD_LIBRARY_PATH
  )
else
  cmd_mpirun_common=(
    mpirun
    --mca pml ob1
    --mca btl tcp,sm,self
    -x PATH
    -x LD_LIBRARY_PATH
  )
fi

# Kill any previous benchfsd instances
echo "Kill any previous benchfsd instances"
pkill -9 benchfsd_mpi 2>/dev/null || true

check_server_ready() {
  local expected_count=$1
  local max_attempts=180
  local attempt=0
  while [ $attempt -lt $max_attempts ]; do
    local ready_count=$(find "${BENCHFS_REGISTRY_DIR}" -name "node_*.addr" -type f 2>/dev/null | wc -l)
    if [ "$ready_count" -eq "$expected_count" ]; then
      echo "BenchFS servers registered: $ready_count/$expected_count"
      echo "Waiting 10s for RPC handler initialization..."
      sleep 10
      echo "BenchFS servers are fully ready"
      return 0
    fi
    echo "Waiting for servers: $ready_count/$expected_count (attempt $((attempt+1))/$max_attempts)"
    sleep 1
    attempt=$((attempt + 1))
  done
  echo "ERROR: BenchFS servers failed to start after $max_attempts seconds"
  return 1
}

# ==============================================================================
# Benchmark parameters (matching debug_large.conf for reproducibility)
# ==============================================================================
benchfs_chunk_size=4194304  # 4 MiB
ppn=${PERF_PPN:-23}
server_ppn=1
transfer_size="4m"
block_size="${PERF_BLOCK_SIZE:-16g}"
ior_flags="${PERF_IOR_FLAGS:--w -r}"

server_np=$((NNODES * server_ppn))
np=$((NNODES * ppn))

echo "=========================================="
echo "Profiling Configuration:"
echo "  Transfer size: $transfer_size"
echo "  Block size: $block_size"
echo "  Client PPN: $ppn (total: $np)"
echo "  Server PPN: $server_ppn (total: $server_np)"
echo "  Chunk size: $benchfs_chunk_size bytes"
echo "  IOR flags: $ior_flags"
echo "=========================================="

# Clean up
rm -rf "${BENCHFS_REGISTRY_DIR}"/*
rm -rf "${BENCHFS_DATA_DIR}"/* 2>/dev/null || true

run_log_dir="${BENCHFSD_LOG_BASE_DIR}/run_0"
mkdir -p "${run_log_dir}"

# Create BenchFS config
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

# ==============================================================================
# Phase 1: Start BenchFS servers
# ==============================================================================
echo ""
echo "=========================================="
echo "Phase 1: Starting BenchFS Servers"
echo "=========================================="

cmd_benchfsd=(
  "${cmd_mpirun_common[@]}"
  -np "$server_np"
  --bind-to none
  -x RUST_LOG=info
  -x RUST_BACKTRACE
  "${BENCHFS_PREFIX}/benchfsd_mpi"
  "${BENCHFS_REGISTRY_DIR}"
  "${config_file}"
)

echo "${cmd_benchfsd[@]}"
"${cmd_benchfsd[@]}" > "${run_log_dir}/benchfsd_stdout.log" 2> "${run_log_dir}/benchfsd_stderr.log" &
BENCHFSD_PID=$!

if ! check_server_ready "$server_np"; then
  echo "ERROR: BenchFS servers failed to start"
  echo "STDOUT:"
  cat "${run_log_dir}/benchfsd_stdout.log" || true
  echo "STDERR:"
  cat "${run_log_dir}/benchfsd_stderr.log" || true
  kill $BENCHFSD_PID 2>/dev/null || true
  wait $BENCHFSD_PID 2>/dev/null || true
  exit 1
fi
sleep 5

# ==============================================================================
# Phase 2: Start perf recording on ALL server nodes
# ==============================================================================
echo ""
echo "=========================================="
echo "Phase 2: Starting Perf Recording on Server Nodes"
echo "=========================================="

PERF_SERVER_WRAPPER="${JOB_OUTPUT_DIR}/perf_server_wrapper.sh"
cat > "${PERF_SERVER_WRAPPER}" <<'WRAPPER_EOF'
#!/bin/bash
# Runs on each node and attaches perf to the local benchfsd_mpi process
PERF_OUTPUT_DIR="$1"
HOSTNAME=$(hostname)

echo "[${HOSTNAME}] Starting server perf profiling..."

# Find local benchfsd_mpi PID
BENCHFSD_PID=$(pgrep -x benchfsd_mpi | head -1)
if [ -z "$BENCHFSD_PID" ]; then
    echo "[${HOSTNAME}] ERROR: Could not find benchfsd_mpi process"
    exit 1
fi
echo "[${HOSTNAME}] Found benchfsd_mpi PID: ${BENCHFSD_PID}"

# Attach perf record with DWARF call graph (best for Rust)
echo "[${HOSTNAME}] Attaching perf record..."
perf record \
    -g \
    -F 99 \
    --call-graph dwarf,32768 \
    -p "${BENCHFSD_PID}" \
    -o "${PERF_OUTPUT_DIR}/perf_server_${HOSTNAME}.data" &
PERF_PID=$!

echo "[${HOSTNAME}] perf recording started (PID: ${PERF_PID})"
echo "$PERF_PID" > "${PERF_OUTPUT_DIR}/perf_server_${HOSTNAME}.pid"

# Wait for perf to be stopped externally (SIGINT from parent)
wait $PERF_PID 2>/dev/null || true

echo "[${HOSTNAME}] Server perf recording completed"

# Generate text report
echo "[${HOSTNAME}] Generating perf report..."
perf report \
    -i "${PERF_OUTPUT_DIR}/perf_server_${HOSTNAME}.data" \
    --stdio \
    --no-children \
    -g fractal,0.5,caller \
    > "${PERF_OUTPUT_DIR}/perf_server_${HOSTNAME}_report.txt" 2>&1 || true

# Generate collapsed stacks for flamegraph
perf script \
    -i "${PERF_OUTPUT_DIR}/perf_server_${HOSTNAME}.data" \
    > "${PERF_OUTPUT_DIR}/perf_server_${HOSTNAME}_script.txt" 2>&1 || true

# Generate DSO-level report
perf report \
    -i "${PERF_OUTPUT_DIR}/perf_server_${HOSTNAME}.data" \
    --stdio \
    --sort=dso,symbol \
    > "${PERF_OUTPUT_DIR}/perf_server_${HOSTNAME}_by_dso.txt" 2>&1 || true

echo "[${HOSTNAME}] Server perf analysis completed"
WRAPPER_EOF
chmod +x "${PERF_SERVER_WRAPPER}"

# Launch perf wrapper on all nodes (runs in background, recording until stopped)
"${cmd_mpirun_common[@]}" \
  -np "$NNODES" \
  --bind-to none \
  "${PERF_SERVER_WRAPPER}" "${PERF_OUTPUT_DIR}" &
PERF_SERVER_MPI_PID=$!
echo "Server perf wrappers started (MPI PID: $PERF_SERVER_MPI_PID)"

# Wait for perf to attach
sleep 5

# ==============================================================================
# Phase 3: Run IOR benchmark with perf on client rank 0
# ==============================================================================
echo ""
echo "=========================================="
echo "Phase 3: Running IOR Benchmark with Client Perf"
echo "=========================================="

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
    echo "[${HOSTNAME}] Client Rank 0: Running IOR with perf profiling..."
    perf record \
        -g \
        -F 99 \
        --call-graph dwarf,32768 \
        -o "${PERF_OUTPUT_DIR}/perf_client_rank0.data" \
        -- "${IOR_BINARY}" "$@"
    IOR_EXIT=$?

    # Generate report
    echo "[${HOSTNAME}] Generating client perf report..."
    perf report \
        -i "${PERF_OUTPUT_DIR}/perf_client_rank0.data" \
        --stdio \
        --no-children \
        -g fractal,0.5,caller \
        > "${PERF_OUTPUT_DIR}/perf_client_rank0_report.txt" 2>&1 || true

    perf script \
        -i "${PERF_OUTPUT_DIR}/perf_client_rank0.data" \
        > "${PERF_OUTPUT_DIR}/perf_client_rank0_script.txt" 2>&1 || true

    perf report \
        -i "${PERF_OUTPUT_DIR}/perf_client_rank0.data" \
        --stdio \
        --sort=dso,symbol \
        > "${PERF_OUTPUT_DIR}/perf_client_rank0_by_dso.txt" 2>&1 || true

    exit $IOR_EXIT
else
    exec "${IOR_BINARY}" "$@"
fi
IOR_WRAPPER_EOF
chmod +x "${IOR_PERF_WRAPPER}"

# MPI Debug: test communication
echo "MPI Debug: Testing MPI communication..."
"${cmd_mpirun_common[@]}" -np "$np" --bind-to none --oversubscribe hostname \
  > "${IOR_OUTPUT_DIR}/mpi_test.txt" 2>&1
echo "MPI Debug: Communication test completed"

ior_json_file="${IOR_OUTPUT_DIR}/ior_result_0.json"
ior_stdout_file="${IOR_OUTPUT_DIR}/ior_stdout_0.log"

cmd_ior=(
  time_json -o "${JOB_OUTPUT_DIR}/time_0.json"
  "${cmd_mpirun_common[@]}"
  -np "$np"
  --bind-to none
  --oversubscribe
  -x RUST_LOG=warn
  -x RUST_BACKTRACE
  -x BENCHFS_EXPECTED_NODES="${server_np}"
  "${IOR_PERF_WRAPPER}"
  "${PERF_OUTPUT_DIR}"
  "${IOR_PREFIX}/src/ior"
  -vvv
  -a BENCHFS
  -t "$transfer_size"
  -b "$block_size"
  -e
  $ior_flags
  --benchfs.registry="${BENCHFS_REGISTRY_DIR}"
  --benchfs.datadir="${BENCHFS_DATA_DIR}"
  --benchfs.chunk-size="${benchfs_chunk_size}"
  -o "${BENCHFS_DATA_DIR}/testfile"
  -O summaryFormat=JSON
  -O summaryFile="${ior_json_file}"
)

echo "Running IOR benchmark (perf recording in background)..."
echo "${cmd_ior[@]}"
"${cmd_ior[@]}" \
  > "${ior_stdout_file}" \
  2> "${IOR_OUTPUT_DIR}/ior_stderr_0.log"
IOR_EXIT_CODE=$?

echo "IOR completed with exit code: ${IOR_EXIT_CODE}"

# ==============================================================================
# Phase 4: Stop perf and collect data
# ==============================================================================
echo ""
echo "=========================================="
echo "Phase 4: Stopping Perf and Collecting Data"
echo "=========================================="

# Stop server perf by sending SIGINT to the perf processes on each node
echo "Stopping server perf recordings..."
# Use pkill to send SIGINT to perf processes on all nodes
"${cmd_mpirun_common[@]}" \
  -np "$NNODES" \
  --bind-to none \
  bash -c 'pkill -INT -f "perf record.*perf_server" 2>/dev/null; echo "[$(hostname)] perf stopped"' \
  2>/dev/null || true

# Wait for perf wrapper MPI job to finish
echo "Waiting for perf data to flush (max 60s)..."
PERF_WAIT=0
while kill -0 $PERF_SERVER_MPI_PID 2>/dev/null && [ $PERF_WAIT -lt 60 ]; do
  sleep 2
  PERF_WAIT=$((PERF_WAIT + 2))
  if [ $((PERF_WAIT % 10)) -eq 0 ]; then
    echo "  Still waiting... ${PERF_WAIT}s"
  fi
done

if kill -0 $PERF_SERVER_MPI_PID 2>/dev/null; then
  echo "WARNING: perf wrapper did not stop in 60s, force killing"
  kill -9 $PERF_SERVER_MPI_PID 2>/dev/null || true
fi
wait $PERF_SERVER_MPI_PID 2>/dev/null || true

# ==============================================================================
# Phase 5: Additional diagnostics if IOR failed
# ==============================================================================
if [ $IOR_EXIT_CODE -ne 0 ]; then
  echo ""
  echo "=========================================="
  echo "IOR did not complete successfully (exit code: ${IOR_EXIT_CODE})"
  echo "Running additional diagnostics..."
  echo "=========================================="

  # Collect stack traces from benchfsd_mpi processes
  echo "Collecting stack traces..."
  "${cmd_mpirun_common[@]}" \
    -np "$NNODES" \
    --bind-to none \
    bash -c '
      HOSTNAME=$(hostname)
      BENCHFSD_PID=$(pgrep -x benchfsd_mpi | head -1)
      if [ -n "$BENCHFSD_PID" ]; then
        echo "=========================================="
        echo "[$HOSTNAME] Stack trace for PID $BENCHFSD_PID:"
        echo "=========================================="
        cat /proc/$BENCHFSD_PID/stack 2>/dev/null || echo "Could not read kernel stack"
        echo ""
        echo "wchan (waiting channel):"
        cat /proc/$BENCHFSD_PID/wchan 2>/dev/null || echo "Could not read wchan"
        echo ""
        echo "File descriptors (first 20):"
        ls -la /proc/$BENCHFSD_PID/fd 2>/dev/null | head -20 || echo "Could not read fd"
      else
        echo "[$HOSTNAME] No benchfsd_mpi process found"
      fi
    ' > "${PERF_OUTPUT_DIR}/stack_traces.txt" 2>&1 || true
fi

# ==============================================================================
# Phase 6: Stop servers
# ==============================================================================
echo ""
echo "=========================================="
echo "Phase 6: Stopping BenchFS Servers"
echo "=========================================="

echo "Sending SIGTERM..."
pkill -TERM benchfsd_mpi 2>/dev/null || true

SHUTDOWN_TIMEOUT=30
ELAPSED=0
while kill -0 $BENCHFSD_PID 2>/dev/null && [ $ELAPSED -lt $SHUTDOWN_TIMEOUT ]; do
  sleep 1
  ELAPSED=$((ELAPSED + 1))
done

if kill -0 $BENCHFSD_PID 2>/dev/null; then
  echo "WARNING: Server did not stop in ${SHUTDOWN_TIMEOUT}s, force killing"
  pkill -9 benchfsd_mpi 2>/dev/null || true
  kill -9 $BENCHFSD_PID 2>/dev/null || true
else
  echo "Server stopped gracefully after ${ELAPSED}s"
fi
wait $BENCHFSD_PID 2>/dev/null || true

# Final perf process cleanup
pkill -9 -f "perf record" 2>/dev/null || true
sleep 2

# ==============================================================================
# Phase 7: Summary
# ==============================================================================
echo ""
echo "=========================================="
echo "SUMMARY"
echo "=========================================="

echo "IOR result:"
if [ -f "${ior_json_file}" ]; then
  grep -E '"bwMiB"|"access"|"writeMiB"|"readMiB"' "${ior_json_file}" | head -12
fi

echo ""
echo "Perf output files:"
ls -lh "${PERF_OUTPUT_DIR}/" 2>/dev/null || echo "No perf output"

echo ""
echo "=== Top functions in server perf reports ==="
for report in "${PERF_OUTPUT_DIR}"/perf_server_*_report.txt; do
  if [ -f "$report" ]; then
    echo ""
    echo "--- $(basename "$report") ---"
    head -80 "$report"
  fi
done

echo ""
echo "=== Top functions in client perf report ==="
if [ -f "${PERF_OUTPUT_DIR}/perf_client_rank0_report.txt" ]; then
  head -80 "${PERF_OUTPUT_DIR}/perf_client_rank0_report.txt"
else
  echo "WARNING: Client perf report not found"
fi

echo ""
echo "=========================================="
echo "Perf profiling job completed"
echo "Results saved to: ${JOB_OUTPUT_DIR}"
echo ""
echo "To generate flamegraphs:"
echo "  cd ${PERF_OUTPUT_DIR}"
echo "  stackcollapse-perf.pl perf_server_*_script.txt | flamegraph.pl > server_flamegraph.svg"
echo "  stackcollapse-perf.pl perf_client_rank0_script.txt | flamegraph.pl > client_flamegraph.svg"
echo "=========================================="
