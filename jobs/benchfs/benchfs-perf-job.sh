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
# Use debug build for perf profiling with full debug symbols
export LD_LIBRARY_PATH="${PROJECT_ROOT}/target/debug:${LD_LIBRARY_PATH:-}"

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

# Check debug symbols in binaries
echo "Checking debug symbols in ${BENCHFS_PREFIX}..."
file "${BENCHFS_PREFIX}/benchfsd_mpi" 2>/dev/null | grep -q "not stripped" && echo "  benchfsd_mpi: has debug symbols" || echo "  WARNING: benchfsd_mpi may be stripped"
file "${BENCHFS_PREFIX}/libbenchfs.so" 2>/dev/null | grep -q "not stripped" && echo "  libbenchfs.so: has debug symbols" || echo "  WARNING: libbenchfs.so may be stripped"

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
ppn=4
server_ppn=1
benchfs_chunk_size=4194304  # 4MB

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
# Using attach mode to avoid perf data corruption on process termination
# ============================================================
echo ""
echo "=========================================="
echo "Starting BenchFS Server with Perf Recording (Attach Mode)"
echo "=========================================="

# Get first node for perf
FIRST_NODE=$(head -1 "${PBS_NODEFILE}")
echo "Perf will record on node: $FIRST_NODE"

# Create wrapper script that starts benchfsd and then attaches perf on rank 0
# IMPORTANT: The wrapper MUST exit with 0 to prevent MPI from aborting the job
# and killing perf before it can flush data
PERF_WRAPPER="${JOB_OUTPUT_DIR}/perf_wrapper.sh"
cat > "${PERF_WRAPPER}" <<'WRAPPER_EOF'
#!/bin/bash
RANK=${OMPI_COMM_WORLD_RANK:-0}
PERF_OUTPUT_DIR="$1"
shift
BENCHFSD_BIN="$1"
shift

if [ "$RANK" -eq 0 ]; then
  echo "Rank 0: Starting benchfsd_mpi and attaching perf"
  # Start benchfsd_mpi in background
  "$BENCHFSD_BIN" "$@" &
  BENCHFSD_PID=$!
  echo "Rank 0: benchfsd_mpi started with PID $BENCHFSD_PID"

  # Give process time to initialize
  sleep 2

  # Attach perf to the running process
  echo "Rank 0: Attaching perf record to PID $BENCHFSD_PID"
  perf record -g -F 1000 --call-graph fp -o "${PERF_OUTPUT_DIR}/perf_server_rank0.data" -p $BENCHFSD_PID &
  PERF_PID=$!
  echo "Rank 0: perf record started with PID $PERF_PID"

  # Save PIDs for later cleanup
  echo "$BENCHFSD_PID" > "${PERF_OUTPUT_DIR}/benchfsd_rank0.pid"
  echo "$PERF_PID" > "${PERF_OUTPUT_DIR}/perf_server.pid"

  # Wait for benchfsd to finish
  wait $BENCHFSD_PID
  BENCHFSD_EXIT=$?
  echo "Rank 0: benchfsd_mpi exited with code $BENCHFSD_EXIT"

  # Stop perf gracefully with SIGINT to ensure data is flushed
  if kill -0 $PERF_PID 2>/dev/null; then
    echo "Rank 0: Stopping perf gracefully (SIGINT)"
    kill -INT $PERF_PID
    # Wait for perf with timeout (max 30 seconds)
    PERF_WAIT=0
    while kill -0 $PERF_PID 2>/dev/null && [ $PERF_WAIT -lt 30 ]; do
      sleep 1
      PERF_WAIT=$((PERF_WAIT + 1))
    done
    if kill -0 $PERF_PID 2>/dev/null; then
      echo "Rank 0: perf did not stop after 30s, forcing"
      kill -9 $PERF_PID 2>/dev/null || true
    else
      echo "Rank 0: perf stopped after ${PERF_WAIT}s"
    fi
  fi

  # IMPORTANT: Always exit 0 to prevent MPI from aborting and killing other processes
  # The actual benchfsd exit code is logged above for debugging
  exit 0
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

echo "Starting BenchFS servers with perf (attach mode)..."
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

# Create client perf wrapper (using attach mode for proper data flushing)
# For client, IOR completes normally and exit code should be passed through
CLIENT_PERF_WRAPPER="${JOB_OUTPUT_DIR}/client_perf_wrapper.sh"
cat > "${CLIENT_PERF_WRAPPER}" <<'CLIENT_WRAPPER_EOF'
#!/bin/bash
RANK=${OMPI_COMM_WORLD_RANK:-0}
PERF_OUTPUT_DIR="$1"
shift
IOR_BIN="$1"
shift

if [ "$RANK" -eq 0 ]; then
  echo "Client Rank 0: Starting IOR and attaching perf"
  # Start IOR in background
  "$IOR_BIN" "$@" &
  IOR_PID=$!
  echo "Client Rank 0: IOR started with PID $IOR_PID"

  # Give process time to initialize
  sleep 1

  # Attach perf to the running process
  echo "Client Rank 0: Attaching perf record to PID $IOR_PID"
  perf record -g -F 1000 --call-graph fp -o "${PERF_OUTPUT_DIR}/perf_client_rank0.data" -p $IOR_PID &
  PERF_PID=$!
  echo "Client Rank 0: perf record started with PID $PERF_PID"

  # Save PIDs
  echo "$IOR_PID" > "${PERF_OUTPUT_DIR}/ior_rank0.pid"
  echo "$PERF_PID" > "${PERF_OUTPUT_DIR}/perf_client.pid"

  # Wait for IOR to finish
  wait $IOR_PID
  IOR_EXIT=$?
  echo "Client Rank 0: IOR exited with code $IOR_EXIT"

  # Stop perf gracefully with SIGINT and timeout
  if kill -0 $PERF_PID 2>/dev/null; then
    echo "Client Rank 0: Stopping perf gracefully (SIGINT)"
    kill -INT $PERF_PID
    # Wait for perf with timeout (max 30 seconds)
    PERF_WAIT=0
    while kill -0 $PERF_PID 2>/dev/null && [ $PERF_WAIT -lt 30 ]; do
      sleep 1
      PERF_WAIT=$((PERF_WAIT + 1))
    done
    if kill -0 $PERF_PID 2>/dev/null; then
      echo "Client Rank 0: perf did not stop after 30s, forcing"
      kill -9 $PERF_PID 2>/dev/null || true
    else
      echo "Client Rank 0: perf stopped after ${PERF_WAIT}s"
    fi
  fi

  # Pass through IOR exit code (IOR success/failure matters)
  exit $IOR_EXIT
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
  -x RUST_LOG=warn
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

# Function to stop perf gracefully with extended timeout for data flushing
stop_perf_gracefully() {
    local pid_file="$1"
    local name="$2"
    local timeout="${3:-30}"  # Default 30 seconds
    if [ -f "$pid_file" ]; then
        local perf_pid=$(cat "$pid_file")
        if kill -0 "$perf_pid" 2>/dev/null; then
            echo "Stopping $name perf (PID: $perf_pid) gracefully..."
            kill -INT "$perf_pid" 2>/dev/null || true
            # Wait for perf to finish flushing data
            local elapsed=0
            while kill -0 "$perf_pid" 2>/dev/null && [ $elapsed -lt $timeout ]; do
                sleep 1
                elapsed=$((elapsed + 1))
                if [ $((elapsed % 10)) -eq 0 ]; then
                    echo "  $name perf still flushing data... ${elapsed}s"
                fi
            done
            if kill -0 "$perf_pid" 2>/dev/null; then
                echo "WARNING: $name perf did not stop after ${timeout}s, forcing..."
                kill -9 "$perf_pid" 2>/dev/null || true
            else
                echo "$name perf stopped and data flushed after ${elapsed}s"
            fi
        else
            echo "$name perf already stopped"
        fi
    else
        echo "$name perf PID file not found"
    fi
}

# IMPORTANT: Stop perf BEFORE killing the server!
# This allows perf to flush its data buffer while the target process is still alive
echo "Step 1: Stopping server perf first (while server is still running)..."
stop_perf_gracefully "${PERF_OUTPUT_DIR}/perf_server.pid" "server" 60

echo ""
echo "Step 2: Sending SIGTERM to benchfsd_mpi for graceful shutdown..."
"${cmd_mpirun_common[@]}" -np "$NNODES" -map-by ppr:1:node pkill -TERM benchfsd_mpi || true

echo "Waiting for server to shutdown gracefully (max 60 seconds)..."
SHUTDOWN_TIMEOUT=60
ELAPSED=0
while kill -0 $BENCHFSD_PID 2>/dev/null && [ $ELAPSED -lt $SHUTDOWN_TIMEOUT ]; do
    sleep 1
    ELAPSED=$((ELAPSED + 1))
    if [ $((ELAPSED % 10)) -eq 0 ]; then
        echo "  Still waiting... ${ELAPSED}s elapsed"
    fi
done

if kill -0 $BENCHFSD_PID 2>/dev/null; then
    echo "WARNING: Server did not shutdown within ${SHUTDOWN_TIMEOUT}s, sending SIGKILL..."
    "${cmd_mpirun_common[@]}" -np "$NNODES" -map-by ppr:1:node pkill -9 benchfsd_mpi || true
    kill -9 $BENCHFSD_PID 2>/dev/null || true
else
    echo "Server shutdown completed gracefully after ${ELAPSED}s"
fi
wait $BENCHFSD_PID 2>/dev/null || true

# Final cleanup: ensure all perf processes are stopped (should already be stopped)
echo ""
echo "Step 3: Final cleanup..."
stop_perf_gracefully "${PERF_OUTPUT_DIR}/perf_server.pid" "server" 10
stop_perf_gracefully "${PERF_OUTPUT_DIR}/perf_client.pid" "client" 10

echo "Waiting for filesystem to sync..."
sync
sleep 2

# ============================================================
# Phase 4: Generate perf reports
# ============================================================
echo ""
echo "=========================================="
echo "Phase 4: Generating Perf Reports"
echo "=========================================="

# Generate text reports
if [ -f "${PERF_OUTPUT_DIR}/perf_server_rank0.data" ]; then
  echo "Generating server perf report..."
  perf report -i "${PERF_OUTPUT_DIR}/perf_server_rank0.data" --stdio > "${PERF_OUTPUT_DIR}/perf_server_report.txt" 2>&1 || true
  perf report -i "${PERF_OUTPUT_DIR}/perf_server_rank0.data" --stdio --sort=dso,symbol > "${PERF_OUTPUT_DIR}/perf_server_by_dso.txt" 2>&1 || true
  perf script -i "${PERF_OUTPUT_DIR}/perf_server_rank0.data" > "${PERF_OUTPUT_DIR}/perf_server_script.txt" 2>&1 || true
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
# Phase 5: Summary
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
