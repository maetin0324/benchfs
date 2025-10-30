#!/bin/bash
#------- qsub option -----------
#PBS -A NBB
#PBS -l elapstim_req=12:00:00
#PBS -T openmpi
#PBS -v NQSV_MPI_VER=4.1.8/gcc11.4.0-cuda12.8.1
#------- Program execution -----------
set -euo pipefail

# Increase file descriptor limit for large-scale MPI jobs
# This prevents FD exhaustion when running with high ppn values
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
# NOTE: Disabled process substitution to avoid FD leak
# exec 1> >(addtimestamp)
# exec 2> >(addtimestamp >&2)

JOB_START=$(timestamp)
NNODES=$(wc --lines "${PBS_NODEFILE}" | awk '{print $1}')
JOBID=$(echo "$PBS_JOBID" | cut -d : -f 2)
JOB_OUTPUT_DIR="${OUTPUT_DIR}/${JOB_START}-${JOBID}-${NNODES}"
JOB_BACKEND_DIR="${BACKEND_DIR}/$(basename -- "${JOB_OUTPUT_DIR}")"
BENCHFS_REGISTRY_DIR="${JOB_BACKEND_DIR}/registry"
BENCHFS_DATA_DIR="/scr"
BENCHFSD_LOG_BASE_DIR="${JOB_OUTPUT_DIR}/benchfsd_logs"
IOR_OUTPUT_DIR="${JOB_OUTPUT_DIR}/ior_results"

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
echo "Job Configuration"
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
echo "=========================================="
echo ""
echo "=========================================="
echo "MPI Configuration Diagnostics"
echo "=========================================="
echo "NQSII_MPIOPTS: ${NQSII_MPIOPTS:-<not set>}"
echo "NQSII_MPIOPTS_ARRAY (${#nqsii_mpiopts_array[@]} elements):"
for i in "${!nqsii_mpiopts_array[@]}"; do
  echo "  [$i] = ${nqsii_mpiopts_array[$i]}"
done
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

save_job_metadata() {
  cat <<EOS >"${JOB_OUTPUT_DIR}"/job_metadata_${runid}.json
{
  "jobid": "$JOBID",
  "runid": ${runid},
  "benchfs_chunk_size": ${benchfs_chunk_size},
  "job_start_time": "${JOB_START}",
  "nnodes": ${NNODES}
}
EOS
}

# Network Configuration
# ==================================================
# FIX for IOR JSON output hang issue:
#
# ISSUE: Open MPI 4.1.8 has a conflict between openib BTL and UCX when both
# are enabled simultaneously. This causes OpenFabrics initialization errors
# and leads to MPI communication deadlocks during IOR result gathering.
#
# ERROR: "WARNING: There was an error initializing an OpenFabrics device."
# SYMPTOM: Hangs during JSON output (MPI_Gather/MPI_Allreduce)
#
# SOLUTION: Use optimized MPI settings with immediate mitigation variables.

# Immediate mitigation environment variables
export OMPI_MCA_mpi_yield_when_idle=1
export OMPI_MCA_btl_base_warn_component_unused=0
export OMPI_MCA_mpi_show_handle_leaks=0

# Optimized MPI configuration for IOR JSON hang fix
cmd_mpirun_common=(
  mpirun
  "${nqsii_mpiopts_array[@]}"
  --mca pml ucx                           # UCX for MPI communication
  --mca btl ^openib,^tcp                  # Disable openib and TCP BTL
  --mca osc ucx                            # OSC also uses UCX
  -x "UCX_TLS=rc_mlx5,sm,self"           # RDMA RC + shared memory
  -x "UCX_NET_DEVICES=mlx5_0:1"          # InfiniBand device
  -x "UCX_RC_TIMEOUT=10s"                 # Set timeout to prevent infinite hang
  -x "UCX_RC_RETRY_COUNT=7"                # Retry count for RC transport
  -x "UCX_LOG_LEVEL=error"                # Only show error logs
  -x "UCX_WARN_UNUSED_ENV_VARS=n"
  -x PATH
  -x LD_LIBRARY_PATH
)

# Fallback: TCP-only transport if UCX continues to have issues
# cmd_mpirun_common=(
#   mpirun
#   "${nqsii_mpiopts_array[@]}"
#   --mca pml ob1                         # Use standard ob1 PML
#   --mca btl tcp,vader,self              # TCP + shared memory + loopback
#   --mca btl_tcp_if_include eno1         # Use TCP interface explicitly
#   --mca btl_openib_allow_ib 0           # Disable openib BTL
#   -x PATH
#   -x LD_LIBRARY_PATH
# )

# Kill any previous benchfsd instances
cmd_mpirun_kill=(
  "${cmd_mpirun_common[@]}"
  -np "$NNODES"
  -map-by ppr:1:node
  pkill -9 benchfsd_mpi
)

echo "Kill any previous benchfsd instances"
"${cmd_mpirun_kill[@]}" || true

# Load benchmark parameters from configuration file
PARAM_FILE="${PARAM_FILE:-${SCRIPT_DIR}/default_params.conf}"
if [ -f "$PARAM_FILE" ]; then
    echo "Loading parameters from: $PARAM_FILE"
    source "$PARAM_FILE"
else
    echo "WARNING: Parameter file not found: $PARAM_FILE"
    echo "Using built-in default parameters"
    # Fallback default values
    # NOTE: block_size must be a multiple of transfer_size
    transfer_size_list=(4m 16m)
    block_size_list=(64m 128m 256m)
    ppn_list=(1 2 4)
    ior_flags_list=("-w -r -F" "-w -r")
    benchfs_chunk_size_list=(4194304)
fi

# Save parameter configuration for reproducibility
cat > "${JOB_OUTPUT_DIR}/parameters.json" <<EOF
{
  "parameter_file": "$PARAM_FILE",
  "transfer_sizes": [$(printf '"%s",' "${transfer_size_list[@]}" | sed 's/,$//; s/,$//')],
  "block_sizes": [$(printf '"%s",' "${block_size_list[@]}" | sed 's/,$//; s/,$//')],
  "ppn_values": [$(printf '%s,' "${ppn_list[@]}" | sed 's/,$//; s/,$//')],
  "ior_flags": [$(printf '"%s",' "${ior_flags_list[@]}" | sed 's/,$//; s/,$//')],
  "chunk_sizes": [$(printf '%s,' "${benchfs_chunk_size_list[@]}" | sed 's/,$//; s/,$//')]
}
EOF

check_server_ready() {
  local max_attempts=60
  local attempt=0

  while [ $attempt -lt $max_attempts ]; do
    local ready_count=$(find "${BENCHFS_REGISTRY_DIR}" -name "node_*.addr" -type f 2>/dev/null | wc -l)

    if [ "$ready_count" -eq "$NNODES" ]; then
      echo "BenchFS servers registered: $ready_count/$NNODES nodes"

      # Additional wait for RPC handler initialization
      # All nodes have registered, but their RPC handlers may still be initializing
      # This prevents "connection refused" errors when 256 clients connect simultaneously
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

runid=0
for benchfs_chunk_size in "${benchfs_chunk_size_list[@]}"; do
  for ppn in "${ppn_list[@]}"; do
    np=$((NNODES * ppn))

    for transfer_size in "${transfer_size_list[@]}"; do
      for block_size in "${block_size_list[@]}"; do
        for ior_flags in "${ior_flags_list[@]}"; do
          echo "=========================================="
          echo "Run ID: $runid"
          echo "Nodes: $NNODES, PPN: $ppn, NP: $np"
          echo "Transfer size: $transfer_size, Block size: $block_size"
          echo "IOR flags: $ior_flags"
          echo "BenchFS chunk size: $benchfs_chunk_size bytes"
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
            -x "RUST_LOG=debug"
            -x "RUST_BACKTRACE=1"
            # Note: PATH and LD_LIBRARY_PATH are already set in cmd_mpirun_common
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
            echo "=========================================="
            echo "Registry Directory Contents:"
            echo "=========================================="
            ls -la "${BENCHFS_REGISTRY_DIR}/" || echo "Cannot access registry"
            echo ""
            echo "=========================================="
            echo "Data Directory Contents:"
            echo "=========================================="
            ls -la "${BENCHFS_DATA_DIR}/" || echo "Cannot access data dir"
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

          # Run IOR benchmark
          echo "Running IOR benchmark..."
          ior_json_file="${IOR_OUTPUT_DIR}/ior_result_${runid}.json"
          ior_stdout_file="${IOR_OUTPUT_DIR}/ior_stdout_${runid}.txt"

          cmd_ior=(
            time_json -o "${JOB_OUTPUT_DIR}/time_${runid}.json"
            "${cmd_mpirun_common[@]}"
            -np "$np"
            --bind-to none
            --map-by "ppr:${ppn}:node"
            -x "UCX_LOG_LEVEL=error"              # Suppress verbose UCX debug logs
            # Note: PATH and LD_LIBRARY_PATH are already set in cmd_mpirun_common
            "${IOR_PREFIX}/src/ior"
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

          save_job_metadata

          echo "${cmd_ior[@]}"
          # NOTE: Use simple redirection instead of process substitution to avoid FD leak
          "${cmd_ior[@]}" \
            > "${ior_stdout_file}" \
            2> "${IOR_OUTPUT_DIR}/ior_stderr_${runid}.txt" || true

          # Stop BenchFS servers
          echo "Stopping BenchFS servers..."
          kill $BENCHFSD_PID 2>/dev/null || true
          wait $BENCHFSD_PID 2>/dev/null || true

          # Force cleanup of any orphaned processes
          # This is critical when running with high ppn values to prevent
          # resource exhaustion across multiple benchmark runs
          echo "Force cleanup of orphaned processes..."
          pkill -9 benchfsd_mpi || true

          # Wait for cleanup and FD release
          sleep 5

          runid=$((runid + 1))
        done
      done
    done
  done
done

echo "All benchmarks completed"
