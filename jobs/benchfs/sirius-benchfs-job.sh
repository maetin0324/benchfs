#!/bin/bash
#------- qsub option -----------
#PBS -q gold
#PBS -A NBB
#------- Program execution -----------
# NOTE: DO NOT use "set -e" or "set -u" or "set -o pipefail" here!
# IOR benchmark may fail for various reasons (timeout, resource exhaustion, etc.)
# and we want to continue running other parameter combinations.
set +e

# Increase file descriptor limit for large-scale MPI jobs
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
# NOTE: compute nodes start with no modules loaded
module purge
module load openmpi/5.0.9/gcc11.5.0
cleanup_exported_bash_functions

# ==============================================================================
# Path Configuration
# ==============================================================================
# Variables are passed via qsub -v from sirius-benchfs.sh

SCRIPT_DIR="${SCRIPT_DIR:-/work/NBB/rmaeda/workspace/rust/benchfs/jobs/benchfs}"
PROJECT_ROOT="${PROJECT_ROOT:-/work/NBB/rmaeda/workspace/rust/benchfs}"
: ${OUTPUT_DIR:="$PROJECT_ROOT/results/benchfs/$(date +%Y.%m.%d-%H.%M.%S)-sirius"}
BACKEND_DIR="${BACKEND_DIR:-$PROJECT_ROOT/backend/benchfs}"
BENCHFS_PREFIX="${BENCHFS_PREFIX:-${PROJECT_ROOT}/target/release}"
IOR_PREFIX="${IOR_PREFIX:-${PROJECT_ROOT}/ior_integration/ior}"

source "$SCRIPT_DIR/common.sh"

JOB_START=$(timestamp)
# On Sirius: select=1 gives 1 chunk per node, PBS_NODEFILE has 1 line per node
NNODES=$(wc -l < "${PBS_NODEFILE}")
JOBID=$(echo "$PBS_JOBID" | cut -d . -f 1)
JOB_OUTPUT_DIR="${OUTPUT_DIR}/${JOB_START}-${JOBID}-${NNODES}n"
JOB_BACKEND_DIR="${BACKEND_DIR}/$(basename -- "${JOB_OUTPUT_DIR}")"
BENCHFS_REGISTRY_DIR="${JOB_BACKEND_DIR}/registry"

# Sirius scratch directory detection:
#   Each vnode gets 1 of 4 NVMe SSDs (/scr0-/scr3).
#   PBS creates /scr{n}/${PBS_JOBID} on each allocated SSD.
#   For multi-vnode jobs, we detect ALL available scratch dirs so each
#   server rank can use its own local NVMe for maximum I/O throughput.
detect_all_scratch_dirs() {
  BENCHFS_ALL_SCRATCH_DIRS=()
  for n in 0 1 2 3; do
    local candidate="/scr${n}/${PBS_JOBID}"
    if [ -d "$candidate" ]; then
      BENCHFS_ALL_SCRATCH_DIRS+=("$candidate")
    fi
  done
  if [ ${#BENCHFS_ALL_SCRATCH_DIRS[@]} -eq 0 ]; then
    echo "ERROR: No scratch directories found for job ${PBS_JOBID}" >&2
    return 1
  fi
  echo "Detected ${#BENCHFS_ALL_SCRATCH_DIRS[@]} scratch directories: ${BENCHFS_ALL_SCRATCH_DIRS[*]}"
}
detect_all_scratch_dirs
BENCHFS_DATA_DIR="${BENCHFS_ALL_SCRATCH_DIRS[0]}"  # Primary (for IOR client and general use)
BENCHFS_SCRATCH_DIRS_CSV=$(IFS=','; echo "${BENCHFS_ALL_SCRATCH_DIRS[*]}")

BENCHFSD_LOG_BASE_DIR="${JOB_OUTPUT_DIR}/benchfsd_logs"
IOR_OUTPUT_DIR="${JOB_OUTPUT_DIR}/ior_results"
STATS_OUTPUT_DIR="${JOB_OUTPUT_DIR}/stats"
PERFETTO_OUTPUT_DIR="${JOB_OUTPUT_DIR}/perfetto"

# Default configurable variables (may be overridden by qsub -v)
: ${ENABLE_PERFETTO:=0}
: ${ENABLE_CHROME:=0}
: ${RUST_LOG_S:=info}
: ${RUST_LOG_C:=warn}
: ${TASKSET:=0}
: ${TASKSET_CORES:=0,1}
: ${ENABLE_NODE_DIAGNOSTICS:=0}
: ${ENABLE_STATS:=0}

# Set LD_LIBRARY_PATH for BenchFS shared library
export LD_LIBRARY_PATH="${PROJECT_ROOT}/target/release:/home/NBB/rmaeda/.local/lib:${LD_LIBRARY_PATH:-}"

echo "prepare the output directory: ${JOB_OUTPUT_DIR}"
mkdir -p "${JOB_OUTPUT_DIR}"
cp "$0" "${JOB_OUTPUT_DIR}"
cp "${PBS_NODEFILE}" "${JOB_OUTPUT_DIR}"
cp "${SCRIPT_DIR}/common.sh" "${JOB_OUTPUT_DIR}"
printenv >"${JOB_OUTPUT_DIR}/env.txt"

# Debug: Print key variables
echo "=========================================="
echo "Job Configuration (Sirius)"
echo "=========================================="
echo "NNODES: ${NNODES}"
echo "PBS_JOBID: ${PBS_JOBID}"
echo "BENCHFS_PREFIX: ${BENCHFS_PREFIX}"
echo "IOR_PREFIX: ${IOR_PREFIX}"
echo "BACKEND_DIR: ${BACKEND_DIR}"
echo "Registry: ${BENCHFS_REGISTRY_DIR}"
echo "Data dirs: ${BENCHFS_ALL_SCRATCH_DIRS[*]}"
echo "Tracing: ENABLE_PERFETTO=${ENABLE_PERFETTO}, ENABLE_CHROME=${ENABLE_CHROME}"
echo "RUST_LOG (server): ${RUST_LOG_S}"
echo "RUST_LOG (client): ${RUST_LOG_C}"
echo "TASKSET: ${TASKSET} (cores: ${TASKSET_CORES})"
echo "ENABLE_STATS: ${ENABLE_STATS}"
echo "ENABLE_NODE_DIAGNOSTICS: ${ENABLE_NODE_DIAGNOSTICS}"
echo ""
echo "PBS_NODEFILE contents:"
cat "${PBS_NODEFILE}"
echo ""
echo "Checking binary:"
ls -la "${BENCHFS_PREFIX}/benchfsd_mpi" || echo "ERROR: Binary not found at ${BENCHFS_PREFIX}/benchfsd_mpi"
echo ""
echo "Checking IOR:"
ls -la "${IOR_PREFIX}/src/ior" || echo "ERROR: IOR not found at ${IOR_PREFIX}/src/ior"
echo ""
echo "Scratch directories:"
for _scratch_dir in "${BENCHFS_ALL_SCRATCH_DIRS[@]}"; do
  ls -la "${_scratch_dir}" 2>/dev/null || echo "WARNING: ${_scratch_dir} not accessible yet"
done
echo "=========================================="
echo ""

echo "prepare backend dir: ${JOB_BACKEND_DIR}"
mkdir -p "${JOB_BACKEND_DIR}"

# Cleanup function for graceful shutdown
cleanup_and_exit() {
  local exit_code=${1:-1}
  local signal_name=${2:-"unknown"}
  echo ""
  echo "=========================================="
  echo "Job interrupted by signal: $signal_name"
  echo "Current Run ID: ${runid:-N/A}"
  echo "Cleaning up backend and data directories..."
  echo "=========================================="
  stop_benchfsd 2>/dev/null || true
  rm -rf "${JOB_BACKEND_DIR}" 2>/dev/null || true
  exit "$exit_code"
}

# Trap signals for cleanup
trap 'cleanup_and_exit 1 "SIGHUP (walltime or session end)"' 1
trap 'cleanup_and_exit 1 "SIGINT (user interrupt)"' 2
trap 'cleanup_and_exit 1 "SIGQUIT"' 3
trap 'cleanup_and_exit 1 "SIGTERM (PBS walltime reached)"' 15
trap 'rm -rf "${JOB_BACKEND_DIR}" 2>/dev/null || true; exit 0' EXIT

echo "prepare benchfs registry dir: ${BENCHFS_REGISTRY_DIR}"
mkdir -p "${BENCHFS_REGISTRY_DIR}"

echo "prepare benchfs data dirs: ${BENCHFS_ALL_SCRATCH_DIRS[*]}"
# Scratch dirs are pre-created by PBS; ensure they all exist
for _scratch_dir in "${BENCHFS_ALL_SCRATCH_DIRS[@]}"; do
  if [ ! -d "${_scratch_dir}" ]; then
    echo "ERROR: Scratch directory not found: ${_scratch_dir}"
    exit 1
  fi
done

echo "prepare benchfsd log dir: ${BENCHFSD_LOG_BASE_DIR}"
mkdir -p "${BENCHFSD_LOG_BASE_DIR}"

echo "prepare ior output dir: ${IOR_OUTPUT_DIR}"
mkdir -p "${IOR_OUTPUT_DIR}"

echo "prepare stats output dir: ${STATS_OUTPUT_DIR}"
mkdir -p "${STATS_OUTPUT_DIR}"

if [ "${ENABLE_PERFETTO}" -eq 1 ] || [ "${ENABLE_CHROME}" -eq 1 ]; then
  echo "prepare trace output dir: ${PERFETTO_OUTPUT_DIR}"
  mkdir -p "${PERFETTO_OUTPUT_DIR}"
fi

save_job_metadata() {
  local file_per_proc=0
  [[ "$ior_flags" == *"-F"* ]] && file_per_proc=1
  cat <<EOS >"${JOB_OUTPUT_DIR}"/job_metadata_${runid}.json
{
  "jobid": "$JOBID",
  "runid": ${runid},
  "nnodes": ${NNODES},
  "system": "sirius",
  "server_ppn": ${server_ppn},
  "server_np": ${server_np},
  "client_ppn": ${ppn},
  "client_np": ${np},
  "transfer_size": "${transfer_size}",
  "block_size": "${block_size}",
  "benchfs_chunk_size": ${benchfs_chunk_size},
  "file_per_proc": ${file_per_proc},
  "ior_flags": "${ior_flags}",
  "job_start_time": "${JOB_START}"
}
EOS
}

# ==============================================================================
# MPI Configuration for Sirius (OpenMPI 5.0.9)
# ==============================================================================

# Immediate mitigation environment variables
export OMPI_MCA_mpi_yield_when_idle=1
export OMPI_MCA_btl_base_warn_component_unused=0
export OMPI_MCA_mpi_show_handle_leaks=0

export RUST_BACKTRACE=full

# MCA pml detection
supports_ucx_pml() {
  command -v ompi_info >/dev/null 2>&1 || return 1
  ompi_info 2>/dev/null | grep -q "pml.*ucx"
}

if supports_ucx_pml; then
  USE_UCX_PML=1
  echo "UCX PML detected – using --mca pml ucx configuration"
else
  USE_UCX_PML=0
  echo "WARNING: UCX PML not available – falling back to ob1/tcp configuration"
fi

# OpenMPI 5.0.9 on Sirius: use UCX for point-to-point and one-sided
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

# Kill any previous benchfsd instances (run on each unique physical node)
echo "Kill any previous benchfsd instances"
pkill -9 benchfsd_mpi 2>/dev/null || true

# Load benchmark parameters from configuration file
PARAM_FILE="${PARAM_FILE:-${SCRIPT_DIR}/../params/debug.conf}"
if [ -f "$PARAM_FILE" ]; then
    echo "Loading parameters from: $PARAM_FILE"
    source "$PARAM_FILE"
else
    echo "WARNING: Parameter file not found: $PARAM_FILE"
    echo "Using built-in default parameters"
    transfer_size_list=(4m)
    block_size_list=(64m)
    ppn_list=(1)
    server_ppn_list=(1)
    ior_flags_list=("-w -r -F")
    benchfs_chunk_size_list=(4194304)
fi

# Default server_ppn_list if not defined in param file
if [ -z "${server_ppn_list+x}" ]; then
    server_ppn_list=(1)
fi

# Convert size string (e.g., 4m, 16m, 1g) to bytes
parse_size_to_bytes() {
    local size_str="${1,,}"  # Convert to lowercase
    local number="${size_str%[kmgt]}"
    local suffix="${size_str: -1}"

    case "$suffix" in
        k) echo $((number * 1024)) ;;
        m) echo $((number * 1024 * 1024)) ;;
        g) echo $((number * 1024 * 1024 * 1024)) ;;
        t) echo $((number * 1024 * 1024 * 1024 * 1024)) ;;
        [0-9]) echo "$size_str" ;;  # Already a number
        *) echo "$size_str" ;;  # Return as-is if unknown
    esac
}

# Save parameter configuration for reproducibility
cat > "${JOB_OUTPUT_DIR}/parameters.json" <<EOF
{
  "parameter_file": "$PARAM_FILE",
  "system": "sirius",
  "nnodes": ${NNODES},
  "transfer_sizes": [$(printf '"%s",' "${transfer_size_list[@]}" | sed 's/,$//; s/,$//')],
  "block_sizes": [$(printf '"%s",' "${block_size_list[@]}" | sed 's/,$//; s/,$//')],
  "client_ppn_values": [$(printf '%s,' "${ppn_list[@]}" | sed 's/,$//; s/,$//')],
  "server_ppn_values": [$(printf '%s,' "${server_ppn_list[@]}" | sed 's/,$//; s/,$//')],
  "ior_flags": [$(printf '"%s",' "${ior_flags_list[@]}" | sed 's/,$//; s/,$//')],
  "chunk_sizes": [$(printf '%s,' "${benchfs_chunk_size_list[@]}" | sed 's/,$//; s/,$//')]
}
EOF

check_server_ready() {
  local expected_count=$1
  local max_attempts=180
  local attempt=0

  while [ $attempt -lt $max_attempts ]; do
    local ready_count=$(find "${BENCHFS_REGISTRY_DIR}" -name "node_*.addr" -type f 2>/dev/null | wc -l)

    if [ "$ready_count" -eq "$expected_count" ]; then
      echo "BenchFS servers registered: $ready_count/$expected_count processes"

      # Additional wait for RPC handler initialization
      local rpc_wait_time=10
      echo "Waiting ${rpc_wait_time}s for RPC handler initialization..."
      sleep $rpc_wait_time

      echo "BenchFS servers are fully ready"
      return 0
    fi

    echo "Waiting for BenchFS servers: $ready_count/$expected_count processes (attempt $((attempt+1))/$max_attempts)"
    sleep 1
    attempt=$((attempt + 1))
  done

  echo "ERROR: BenchFS servers failed to start after $max_attempts seconds"
  return 1
}

# ==============================================================================
# Benchmark Loop
# ==============================================================================
# Optimized loop structure: benchfsd is only restarted when its parameters change

stop_benchfsd() {
  if [ -n "${BENCHFSD_PID:-}" ]; then
    echo "Stopping BenchFS servers (graceful shutdown)..."

    local graceful_timeout=${BENCHFS_SHUTDOWN_TIMEOUT:-30}

    echo "Sending SIGTERM to benchfsd_mpi processes..."
    pkill -TERM benchfsd_mpi 2>/dev/null || true

    echo "Waiting up to ${graceful_timeout}s for graceful shutdown..."
    local elapsed=0
    while [ $elapsed -lt $graceful_timeout ]; do
      if ! kill -0 $BENCHFSD_PID 2>/dev/null; then
        echo "BenchFS servers stopped gracefully after ${elapsed}s"
        wait $BENCHFSD_PID 2>/dev/null || true
        unset BENCHFSD_PID
        return 0
      fi
      sleep 1
      elapsed=$((elapsed + 1))

      if [ $((elapsed % 5)) -eq 0 ]; then
        echo "  Still waiting for graceful shutdown... (${elapsed}s/${graceful_timeout}s)"
      fi
    done

    echo "WARNING: Graceful shutdown timed out after ${graceful_timeout}s"

    kill $BENCHFSD_PID 2>/dev/null || true
    wait $BENCHFSD_PID 2>/dev/null || true

    pkill -9 benchfsd_mpi 2>/dev/null || true

    sleep 3
    unset BENCHFSD_PID
  fi
}

start_benchfsd() {
  local config_id=$1

  # Clean up registry and data directories
  rm -rf "${BENCHFS_REGISTRY_DIR}"/*
  for _scratch_dir in "${BENCHFS_ALL_SCRATCH_DIRS[@]}"; do
    rm -rf "${_scratch_dir}"/* 2>/dev/null || true
  done

  server_log_dir="${BENCHFSD_LOG_BASE_DIR}/server_${config_id}"
  mkdir -p "${server_log_dir}"

  # Create BenchFS config file
  config_file="${JOB_OUTPUT_DIR}/benchfs_${config_id}.toml"
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

  echo "=========================================="
  echo "Starting BenchFS servers"
  echo "  Config ID: ${config_id}"
  echo "  Server PPN: ${server_ppn}, NP: ${server_np}"
  echo "  Chunk size: ${benchfs_chunk_size_str} (${benchfs_chunk_size} bytes)"
  echo "  Config file: ${config_file}"
  echo "  Data dirs: ${BENCHFS_ALL_SCRATCH_DIRS[*]}"
  echo "=========================================="

  # Network debug info (only on first start)
  if [ "$config_id" == "0" ]; then
    echo ""
    echo "=========================================="
    echo "Network Configuration Debug Information"
    echo "=========================================="
    echo "UCX Configuration:"
    echo "  UCX_TLS=${UCX_TLS:-not set}"
    echo "  UCX_NET_DEVICES=${UCX_NET_DEVICES:-not set}"
    echo ""
    echo "Available network interfaces:"
    ip -o -4 addr show | awk '{print "  " $2 " : " $4}' || echo "  Unable to list interfaces"
    echo ""
    echo "InfiniBand status:"
    which ibstat >/dev/null 2>&1 && ibstat 2>/dev/null | grep -E "State:|Physical state:" | head -4 || echo "  InfiniBand not available or ibstat not found"
    echo ""
    echo "UCX info (if available):"
    which ucx_info >/dev/null 2>&1 && ucx_info -v 2>/dev/null | head -3 || echo "  ucx_info not available"
    echo "=========================================="
    echo ""
  fi

  # Determine the binary to use (with optional taskset wrapper)
  BENCHFSD_BINARY="${BENCHFS_PREFIX}/benchfsd_mpi"
  if [ "${TASKSET}" = "1" ]; then
    echo "TASKSET mode enabled: limiting benchfsd_mpi to cores ${TASKSET_CORES}"
    TASKSET_WRAPPER="${server_log_dir}/benchfsd_taskset_wrapper.sh"
    cat > "${TASKSET_WRAPPER}" <<EOF2
#!/bin/bash
exec taskset -c ${TASKSET_CORES} ${BENCHFS_PREFIX}/benchfsd_mpi "\$@"
EOF2
    chmod +x "${TASKSET_WRAPPER}"
    BENCHFSD_BINARY="${TASKSET_WRAPPER}"
    echo "Created taskset wrapper: ${TASKSET_WRAPPER}"
  fi

  # Create per-rank data_dir wrapper to distribute server ranks across NVMe SSDs.
  # Each MPI rank selects its own local NVMe based on OMPI_COMM_WORLD_LOCAL_RANK.
  DATADIR_WRAPPER="${server_log_dir}/benchfsd_datadir_wrapper.sh"
  export BENCHFS_INNER_BINARY="${BENCHFSD_BINARY}"
  cat > "${DATADIR_WRAPPER}" <<'WRAPPER_EOF'
#!/bin/bash
LOCAL_RANK=${OMPI_COMM_WORLD_LOCAL_RANK:-0}
IFS=',' read -ra SCRATCH_DIRS <<< "${BENCHFS_SCRATCH_DIRS}"
NUM_DIRS=${#SCRATCH_DIRS[@]}
DIR_INDEX=$((LOCAL_RANK % NUM_DIRS))
RANK_DATA_DIR="${SCRATCH_DIRS[$DIR_INDEX]}"

# Create rank-specific config with correct data_dir
CONFIG_FILE="$2"
RANK_CONFIG="${CONFIG_FILE%.toml}_rank${LOCAL_RANK}.toml"
sed "s|^data_dir = .*|data_dir = \"${RANK_DATA_DIR}\"|" "${CONFIG_FILE}" > "${RANK_CONFIG}"

echo "Rank ${LOCAL_RANK}: data_dir=${RANK_DATA_DIR}, config=${RANK_CONFIG}"
exec "${BENCHFS_INNER_BINARY}" "$1" "${RANK_CONFIG}" "${@:3}"
WRAPPER_EOF
  chmod +x "${DATADIR_WRAPPER}"
  BENCHFSD_BINARY="${DATADIR_WRAPPER}"
  echo "Created per-rank data_dir wrapper: ${DATADIR_WRAPPER}"

  stats_file="${STATS_OUTPUT_DIR}/stats_server${config_id}.csv"
  cmd_benchfsd=(
    "${cmd_mpirun_common[@]}"
    -np "$server_np"
    --bind-to none
    --oversubscribe
    -x RUST_LOG="${RUST_LOG_S}"
    -x RUST_BACKTRACE
    -x BENCHFS_SCRATCH_DIRS="${BENCHFS_SCRATCH_DIRS_CSV}"
    -x BENCHFS_INNER_BINARY
    "${BENCHFSD_BINARY}"
    "${BENCHFS_REGISTRY_DIR}"
    "${config_file}"
    --stats-output "${stats_file}"
  )

  # Add tracing option if enabled
  if [ "${ENABLE_PERFETTO}" -eq 1 ]; then
    trace_file="${PERFETTO_OUTPUT_DIR}/trace_server${config_id}.pftrace"
    binary_idx=$((${#cmd_benchfsd[@]} - 5))
    cmd_benchfsd=("${cmd_benchfsd[@]:0:binary_idx}" -x ENABLE_PERFETTO=1 "${cmd_benchfsd[@]:binary_idx}")
    cmd_benchfsd+=(--trace-output "${trace_file}")
    echo "Perfetto tracing enabled: ${trace_file}"
  elif [ "${ENABLE_CHROME}" -eq 1 ]; then
    trace_file="${PERFETTO_OUTPUT_DIR}/trace_server${config_id}.json"
    binary_idx=$((${#cmd_benchfsd[@]} - 5))
    cmd_benchfsd=("${cmd_benchfsd[@]:0:binary_idx}" -x ENABLE_CHROME=1 "${cmd_benchfsd[@]:binary_idx}")
    cmd_benchfsd+=(--trace-output "${trace_file}")
    echo "Chrome tracing enabled: ${trace_file}"
  fi

  # Add --enable-stats option if detailed timing statistics are requested
  if [ "${ENABLE_STATS:-0}" -eq 1 ]; then
    cmd_benchfsd+=(--enable-stats)
    echo "Detailed timing statistics collection enabled"
  fi

  echo "${cmd_benchfsd[@]}"
  "${cmd_benchfsd[@]}" > "${server_log_dir}/benchfsd_stdout.log" 2> "${server_log_dir}/benchfsd_stderr.log" &
  BENCHFSD_PID=$!

  # Wait for servers to be ready
  if ! check_server_ready "$server_np"; then
    echo "ERROR: BenchFS servers failed to start"
    echo "=========================================="
    echo "BenchFS Server STDOUT:"
    cat "${server_log_dir}/benchfsd_stdout.log" || echo "No stdout log"
    echo ""
    echo "BenchFS Server STDERR:"
    cat "${server_log_dir}/benchfsd_stderr.log" || echo "No stderr log"
    echo ""
    echo "Registry Directory Contents:"
    ls -la "${BENCHFS_REGISTRY_DIR}/" || echo "Cannot access registry"
    echo ""
    echo "Scratch Directory:"
    ls -la "${BENCHFS_DATA_DIR}/" || echo "Cannot access data dir"
    echo "=========================================="
    stop_benchfsd
    exit 1
  fi

  # Give servers time to fully initialize
  sleep 5
  echo "BenchFS servers started successfully"
}

runid=0
server_config_id=0

for benchfs_chunk_size_str in "${benchfs_chunk_size_list[@]}"; do
  # Convert chunk size string to bytes
  benchfs_chunk_size=$(parse_size_to_bytes "$benchfs_chunk_size_str")

  for server_ppn in "${server_ppn_list[@]}"; do
    server_np=$((NNODES * server_ppn))

    # Start benchfsd with current server configuration
    stop_benchfsd
    start_benchfsd "$server_config_id"

    # Run all IOR parameter combinations with this server configuration
    for ppn in "${ppn_list[@]}"; do
      np=$((NNODES * ppn))

      for transfer_size in "${transfer_size_list[@]}"; do
        for block_size in "${block_size_list[@]}"; do
          for ior_flags in "${ior_flags_list[@]}"; do
            echo "=========================================="
            echo "Run ID: $runid"
            echo "Nodes: $NNODES"
            echo "Server: PPN=$server_ppn, NP=$server_np (config_id=$server_config_id)"
            echo "Client: PPN=$ppn, NP=$np"
            echo "Transfer size: $transfer_size, Block size: $block_size"
            echo "IOR flags: $ior_flags"
            echo "BenchFS chunk size: $benchfs_chunk_size_str ($benchfs_chunk_size bytes)"
            echo "=========================================="

            # Clean data directories between IOR runs (but keep registry)
            for _scratch_dir in "${BENCHFS_ALL_SCRATCH_DIRS[@]}"; do
              rm -rf "${_scratch_dir}"/* 2>/dev/null || true
            done

            # Create run_${runid} symlink in benchfsd_logs/ for extract script compatibility
            # Extract scripts expect: benchfsd_logs/run_*/
            ln -sfn "server_${server_config_id}" "${BENCHFSD_LOG_BASE_DIR}/run_${runid}"

            # Start iostat capture for disk utilization analysis
            # Place in diagnostics/<hostname>/iostat_during_run*.txt for extract_iostat_csv.sh
            DIAGNOSTICS_DIR="${JOB_OUTPUT_DIR}/diagnostics"
            IOSTAT_NODE_DIR="${DIAGNOSTICS_DIR}/$(hostname)"
            mkdir -p "${IOSTAT_NODE_DIR}"
            IOSTAT_LOG="${IOSTAT_NODE_DIR}/iostat_during_run${runid}.txt"
            if command -v iostat >/dev/null 2>&1; then
              iostat -xt 1 > "${IOSTAT_LOG}" 2>/dev/null &
              IOSTAT_PID=$!
              echo "Started iostat capture (PID: $IOSTAT_PID) -> ${IOSTAT_LOG}"
            else
              IOSTAT_PID=""
              echo "WARNING: iostat not available, skipping disk utilization capture"
            fi

            # MPI Debug: Testing MPI communication before IOR
            echo "MPI Debug: Testing MPI communication before IOR"
            "${cmd_mpirun_common[@]}" -np "$np" --bind-to none --oversubscribe hostname > "${IOR_OUTPUT_DIR}/mpi_test_${runid}.txt" 2>&1
            echo "MPI Debug: Communication test completed"

            # Run IOR benchmark
            echo "Running IOR benchmark..."
            ior_json_file="${IOR_OUTPUT_DIR}/ior_result_${runid}.json"
            ior_stdout_file="${IOR_OUTPUT_DIR}/ior_stdout_${runid}.log"

            # Create directory for retry stats from IOR clients
            retry_stats_dir="${STATS_OUTPUT_DIR}/retry_stats_run${runid}"
            mkdir -p "${retry_stats_dir}"

            cmd_ior=(
              time_json -o "${JOB_OUTPUT_DIR}/time_${runid}.json"
              "${cmd_mpirun_common[@]}"
              -np "$np"
              --bind-to none
              --oversubscribe
              -x RUST_LOG="${RUST_LOG_C}"
              -x RUST_BACKTRACE
              -x BENCHFS_RETRY_STATS_OUTPUT="${retry_stats_dir}/"
              -x BENCHFS_EXPECTED_NODES="${server_np}"
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

            save_job_metadata

            echo "${cmd_ior[@]}"
            ior_exit_code=0
            "${cmd_ior[@]}" \
              > "${ior_stdout_file}" \
              2> "${IOR_OUTPUT_DIR}/ior_stderr_${runid}.log" || ior_exit_code=$?

            # Stop iostat capture
            if [ -n "${IOSTAT_PID:-}" ]; then
              kill "$IOSTAT_PID" 2>/dev/null || true
              wait "$IOSTAT_PID" 2>/dev/null || true
              echo "Stopped iostat capture -> ${IOSTAT_LOG}"
              unset IOSTAT_PID
            fi

            if [ "$ior_exit_code" -ne 0 ]; then
              echo "WARNING: IOR run $runid failed with exit code $ior_exit_code"
              echo "  Stderr log: ${IOR_OUTPUT_DIR}/ior_stderr_${runid}.log"
              echo "  Continuing with next parameter combination..."
            fi

            # Merge client retry stats into a single CSV file
            merged_retry_stats="${STATS_OUTPUT_DIR}/retry_stats_run${runid}.csv"
            if [ -d "${retry_stats_dir}" ]; then
              echo "Merging client retry stats into ${merged_retry_stats}..."
              echo "node_id,total_requests,total_retries,retry_successes,retry_failures,retry_rate" > "${merged_retry_stats}"
              for csv_file in "${retry_stats_dir}"/*.csv; do
                if [ -f "$csv_file" ]; then
                  tail -n +2 "$csv_file" >> "${merged_retry_stats}"
                fi
              done
              rm -rf "${retry_stats_dir}"
            fi

            runid=$((runid + 1))
          done
        done
      done
    done

    server_config_id=$((server_config_id + 1))
  done
done

# Final cleanup
stop_benchfsd

echo "All benchmarks completed"
