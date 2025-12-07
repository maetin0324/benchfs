#!/bin/bash
# CHFS Benchmark Job Script
#
# This script is executed by the job scheduler to run CHFS benchmarks.
# It starts CHFS servers, runs IOR benchmarks, and collects results.

#PBS -A NBBG
#PBS -l elapstim_req=00:30:00
#PBS -T openmpi
#PBS -v NQSV_MPI_VER=4.1.8/gcc11.4.0-cuda12.8.1

set -euo pipefail

# Try to increase file descriptor limit (may fail on some job schedulers)
ulimit -n 1048576 2>/dev/null || true

# Load common utilities
source "${SCRIPT_DIR}/../benchfs/common.sh"
source "$HOME/.bashrc"

# Load OpenMPI module
module purge
module load "openmpi/$NQSV_MPI_VER"

# ============================================================
# Job Environment Setup
# ============================================================

# Get job information
NNODES=$(wc --lines "${PBS_NODEFILE}" | awk '{print $1}')
JOBID=$(echo "$PBS_JOBID" | cut -d : -f 2)
JOB_START=$(timestamp)
JOB_OUTPUT_DIR="${OUTPUT_DIR}/${JOB_START}-${JOBID}-${NNODES}"
JOB_BACKEND_DIR="${BACKEND_DIR}/$(basename -- "${JOB_OUTPUT_DIR}")"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# CHFS directories
CHFS_SCRATCH_DIR="/scr/chfs"           # Node-local NVMe SSD
CHFS_LOG_DIR="${JOB_OUTPUT_DIR}/chfs_logs"
IOR_OUTPUT_DIR="${JOB_OUTPUT_DIR}/ior_results"

# Create output directories
mkdir -p "${JOB_OUTPUT_DIR}" "${CHFS_LOG_DIR}" "${IOR_OUTPUT_DIR}" "${JOB_BACKEND_DIR}"

# Save OpenMPI mpirun path before loading spack (spack may override PATH)
MPIRUN_CMD="$(which mpirun)"
echo "OpenMPI mpirun: ${MPIRUN_CMD}"

# Load CHFS and its dependencies via spack
SPACK_SETUP="/work/NBB/rmaeda/spack/share/spack/setup-env.sh"
echo "Loading spack from: ${SPACK_SETUP}"
if [ -f "$SPACK_SETUP" ]; then
  source "$SPACK_SETUP"
  echo "Spack loaded, loading chfs..."
  spack load chfs || { echo "ERROR: Failed to load chfs"; exit 1; }
else
  echo "ERROR: Spack setup file not found at ${SPACK_SETUP}"
  exit 1
fi

# Verify chfsctl is accessible
echo "PATH: ${PATH}"
echo "LD_LIBRARY_PATH: ${LD_LIBRARY_PATH:-}"
if ! command -v chfsctl &> /dev/null; then
  echo "ERROR: chfsctl not found in PATH"
  exit 1
fi
echo "chfsctl found at: $(which chfsctl)"

# Copy job artifacts
cp "$0" "${JOB_OUTPUT_DIR}/"
cp "${PBS_NODEFILE}" "${JOB_OUTPUT_DIR}/nodelist"
cp "${SCRIPT_DIR}/../benchfs/common.sh" "${JOB_OUTPUT_DIR}/"
printenv > "${JOB_OUTPUT_DIR}/env.txt"

echo "=========================================="
echo "CHFS Benchmark Job Started"
echo "=========================================="
echo "Job ID: ${JOBID}"
echo "Nodes: ${NNODES}"
echo "Start Time: ${JOB_START}"
echo "Output Directory: ${JOB_OUTPUT_DIR}"
echo "CHFS Prefix: ${CHFS_PREFIX}"
echo "IOR Prefix: ${IOR_PREFIX}"
echo "=========================================="

# ============================================================
# MPI Configuration
# ============================================================

# Parse NQS-II MPI options
if [ -n "${NQSII_MPIOPTS:-}" ]; then
  IFS=' ' read -r -a nqsii_mpiopts_array <<< "${NQSII_MPIOPTS}"
else
  nqsii_mpiopts_array=()
fi

# Build mpirun common command (use TCP for compatibility)
cmd_mpirun_common=(
  "${MPIRUN_CMD}"
  "${nqsii_mpiopts_array[@]}"
  --mca pml ob1
  --mca btl tcp,vader,self
  --mca btl_openib_allow_ib 0
  -x PATH
  -x LD_LIBRARY_PATH
)

# ============================================================
# Parameter Loading
# ============================================================

PARAM_FILE="${PARAM_FILE:-${SCRIPT_DIR}/../params/standard.conf}"
if [ -f "$PARAM_FILE" ]; then
    echo "Loading parameters from: $PARAM_FILE"
    source "$PARAM_FILE"
else
    echo "ERROR: Parameter file not found: $PARAM_FILE"
    exit 1
fi

# Set defaults for optional parameters
: ${CHFS_CHUNK_SIZE:=1048576}
: ${CHFS_PROTOCOL:=verbs}
: ${CHFS_DB_SIZE:=10GB}

echo "CHFS Parameters:"
echo "  CHFS_PROTOCOL: ${CHFS_PROTOCOL}"
echo "  CHFS_CHUNK_SIZE: ${CHFS_CHUNK_SIZE}"
echo "  CHFS_DB_SIZE: ${CHFS_DB_SIZE}"

# Save parameters as JSON
cat > "${JOB_OUTPUT_DIR}/parameters.json" <<EOF
{
  "parameter_file": "$PARAM_FILE",
  "nnodes": ${NNODES},
  "transfer_sizes": $(printf '%s\n' "${transfer_size_list[@]}" | jq -R . | jq -s .),
  "block_sizes": $(printf '%s\n' "${block_size_list[@]}" | jq -R . | jq -s .),
  "ppn_list": $(printf '%s\n' "${ppn_list[@]}" | jq -R . | jq -s .),
  "ior_flags_list": $(printf '%s\n' "${ior_flags_list[@]}" | jq -R . | jq -s .),
  "chfs_chunk_size": ${CHFS_CHUNK_SIZE},
  "chfs_protocol": "${CHFS_PROTOCOL}",
  "chfs_db_size": "${CHFS_DB_SIZE}"
}
EOF

# ============================================================
# CHFS Server Management Functions
# ============================================================

start_chfs_servers() {
  local protocol=${1:-sockets}
  local db_size=${2:-10GB}

  echo ""
  echo "=========================================="
  echo "Starting CHFS Servers"
  echo "=========================================="
  echo "Protocol: ${protocol}"
  echo "DB Size: ${db_size}"
  echo "Scratch Directory: ${CHFS_SCRATCH_DIR}"
  echo "=========================================="

  # Create hostfile
  cp "${PBS_NODEFILE}" "${JOB_OUTPUT_DIR}/hostfile"

  # Create scratch directory on all nodes
  echo "Creating scratch directory ${CHFS_SCRATCH_DIR} on all nodes..."
  local CHFSD_REAL=$(which chfsd)
  echo "Real chfsd path: ${CHFSD_REAL}"

  # Check if TASKSET is enabled
  : ${TASKSET:=0}
  : ${TASKSET_CORES:=0,1}

  if [ "${TASKSET}" = "1" ]; then
    echo "TASKSET mode enabled: limiting chfsd to cores ${TASKSET_CORES}"

    local WRAPPER_DIR="${CHFS_SCRATCH_DIR}/bin"

    # Create wrapper script locally first
    local WRAPPER_SCRIPT="${JOB_OUTPUT_DIR}/chfsd_wrapper.sh"
    cat > "${WRAPPER_SCRIPT}" <<EOF
#!/bin/bash
# Wrapper to limit chfsd to specified CPU cores
exec taskset -c ${TASKSET_CORES} ${CHFSD_REAL} "\$@"
EOF
    chmod +x "${WRAPPER_SCRIPT}"
    echo "Created wrapper script: ${WRAPPER_SCRIPT}"
    cat "${WRAPPER_SCRIPT}"

    # Deploy to all nodes
    while IFS= read -r host; do
      ssh -o StrictHostKeyChecking=no "$host" "mkdir -p ${CHFS_SCRATCH_DIR} && mkdir -p ${WRAPPER_DIR}" &
    done < "${JOB_OUTPUT_DIR}/hostfile"
    wait

    while IFS= read -r host; do
      scp -o StrictHostKeyChecking=no "${WRAPPER_SCRIPT}" "${host}:${WRAPPER_DIR}/chfsd" &
    done < "${JOB_OUTPUT_DIR}/hostfile"
    wait

    echo "Scratch directories and taskset wrappers created"
    echo "chfsd wrapper: ${WRAPPER_DIR}/chfsd (limits to cores ${TASKSET_CORES})"

    # Prepend wrapper directory to PATH so chfsctl finds it first
    export PATH="${WRAPPER_DIR}:${PATH}"
    echo "Updated PATH to use taskset wrapper: ${WRAPPER_DIR}"
  else
    echo "TASKSET mode disabled: chfsd will use all available cores"

    # Just create scratch directory on all nodes (no wrapper)
    while IFS= read -r host; do
      ssh -o StrictHostKeyChecking=no "$host" "mkdir -p ${CHFS_SCRATCH_DIR}" &
    done < "${JOB_OUTPUT_DIR}/hostfile"
    wait

    echo "Scratch directories created"
  fi

  # Start CHFS servers (no FUSE mount, using native API)
  echo "Running chfsctl start..."
  echo "chfsctl command: chfsctl -h ${JOB_OUTPUT_DIR}/hostfile -c ${CHFS_SCRATCH_DIR} -s ${db_size} -p ${protocol} -M -f 2 -n 32 -L ${CHFS_LOG_DIR} start"

  # Run chfsctl and capture output
  # Use -x to export environment variables to remote nodes (variable name only)
  local chfsctl_output
  chfsctl_output=$(chfsctl \
    -h "${JOB_OUTPUT_DIR}/hostfile" \
    -c "${CHFS_SCRATCH_DIR}" \
    -s "${db_size}" \
    -p "${protocol}" \
    -M \
    -f 2 \
    -n 32 \
    -L "${CHFS_LOG_DIR}" \
    -x PATH \
    -x LD_LIBRARY_PATH \
    start 2>&1) || {
      echo "ERROR: chfsctl start failed with exit code $?"
      echo "Output: ${chfsctl_output}"
      echo ""
      echo "=== CHFS Log Files ==="
      ls -la "${CHFS_LOG_DIR}/" 2>&1 || true
      echo ""
      echo "=== chfsd log content (first 50 lines each) ==="
      for logfile in "${CHFS_LOG_DIR}"/*.log; do
        if [ -f "$logfile" ]; then
          echo "--- $logfile ---"
          head -50 "$logfile" 2>&1 || true
        fi
      done
      return 1
    }

  echo "${chfsctl_output}" | tee "${CHFS_LOG_DIR}/chfsctl_start.log"

  # Evaluate the output to set environment variables
  eval "${chfsctl_output}"

  # Verify environment variables are set
  if [ -z "${CHFS_SERVER:-}" ]; then
    echo "ERROR: CHFS_SERVER not set after chfsctl start"
    cat "${CHFS_LOG_DIR}/chfsctl_start.log"
    return 1
  fi

  echo "CHFS_SERVER=${CHFS_SERVER}"
  export CHFS_SERVER

  # Wait for servers to be ready
  echo "Waiting for CHFS servers to be ready..."
  sleep 5

  # Verify servers are running
  echo "Checking server status..."
  chlist > "${CHFS_LOG_DIR}/chlist.log" 2>&1 || {
    echo "ERROR: chlist failed"
    cat "${CHFS_LOG_DIR}/chlist.log"
    return 1
  }
  cat "${CHFS_LOG_DIR}/chlist.log"

  local server_count=$(wc -l < "${CHFS_LOG_DIR}/chlist.log")
  echo "CHFS servers ready: ${server_count} servers"

  return 0
}

stop_chfs_servers() {
  echo ""
  echo "=========================================="
  echo "Stopping CHFS Servers"
  echo "=========================================="

  # Stop servers
  chfsctl \
    -h "${JOB_OUTPUT_DIR}/hostfile" \
    -M \
    stop > "${CHFS_LOG_DIR}/chfsctl_stop.log" 2>&1 || true

  # Clean up scratch directories (also removes the taskset wrapper)
  chfsctl \
    -h "${JOB_OUTPUT_DIR}/hostfile" \
    -c "${CHFS_SCRATCH_DIR}" \
    clean > "${CHFS_LOG_DIR}/chfsctl_clean.log" 2>&1 || true

  echo "CHFS servers stopped"
}

# Cleanup on exit
cleanup() {
  echo ""
  echo "=========================================="
  echo "Cleanup"
  echo "=========================================="
  stop_chfs_servers
  rm -rf "${JOB_BACKEND_DIR}" 2>/dev/null || true
  echo "Cleanup complete"
}
trap cleanup EXIT

# ============================================================
# Benchmark Functions
# ============================================================

save_job_metadata() {
  local runid=$1
  local ppn=$2
  local transfer_size=$3
  local block_size=$4
  local ior_flags=$5

  local np=$((NNODES * ppn))
  local file_per_proc=0
  [[ "$ior_flags" == *"-F"* ]] && file_per_proc=1

  cat > "${JOB_OUTPUT_DIR}/job_metadata_${runid}.json" <<EOF
{
  "jobid": "${JOBID}",
  "runid": ${runid},
  "nnodes": ${NNODES},
  "ppn": ${ppn},
  "np": ${np},
  "transfer_size": "${transfer_size}",
  "block_size": "${block_size}",
  "chfs_chunk_size": ${CHFS_CHUNK_SIZE},
  "file_per_proc": ${file_per_proc},
  "ior_flags": "${ior_flags}",
  "job_start_time": "${JOB_START}"
}
EOF
}

run_ior_benchmark() {
  local runid=$1
  local ppn=$2
  local transfer_size=$3
  local block_size=$4
  local ior_flags=$5

  local np=$((NNODES * ppn))
  local ior_json="${IOR_OUTPUT_DIR}/ior_result_${runid}.json"
  local ior_stdout="${IOR_OUTPUT_DIR}/ior_stdout_${runid}.log"
  local ior_stderr="${IOR_OUTPUT_DIR}/ior_stderr_${runid}.log"

  echo ""
  echo "=========================================="
  echo "IOR Benchmark Run ${runid}"
  echo "=========================================="
  echo "Processes: ${np} (${ppn} per node)"
  echo "Transfer Size: ${transfer_size}"
  echo "Block Size: ${block_size}"
  echo "IOR Flags: ${ior_flags}"
  echo "=========================================="

  # Build IOR command
  cmd_ior=(
    time_json -o "${JOB_OUTPUT_DIR}/time_${runid}.json"
    "${cmd_mpirun_common[@]}"
    -np "$np"
    --bind-to none
    --map-by "ppr:${ppn}:node"
    -x CHFS_SERVER
    -x CHFS_CHUNK_SIZE="${CHFS_CHUNK_SIZE}"
    "${IOR_PREFIX}/src/ior"
    -vvv
    -a CHFS
    --chfs.chunk_size="${CHFS_CHUNK_SIZE}"
    -t "$transfer_size"
    -b "$block_size"
    $ior_flags
    -o "/testfile_${runid}"
    -O summaryFormat=JSON
    -O summaryFile="${ior_json}"
  )

  echo "Running: ${cmd_ior[*]}"
  "${cmd_ior[@]}" > "${ior_stdout}" 2> "${ior_stderr}" || {
    echo "WARNING: IOR command failed (see logs for details)"
    cat "${ior_stderr}"
  }

  # Check if results were produced
  if [ -f "${ior_json}" ]; then
    echo "IOR results saved to: ${ior_json}"
    # Extract and display key metrics
    if command -v jq >/dev/null 2>&1; then
      jq -r '.tests[0].Results[] | "\(.operation): \(.bwMiB) MiB/s"' "${ior_json}" 2>/dev/null || true
    fi
  else
    echo "WARNING: IOR result file not created"
  fi

  return 0
}

# ============================================================
# Main Benchmark Loop
# ============================================================

echo ""
echo "=========================================="
echo "Starting Benchmark Sweep"
echo "=========================================="

# Start CHFS servers
if ! start_chfs_servers "${CHFS_PROTOCOL}" "${CHFS_DB_SIZE}"; then
  echo "ERROR: Failed to start CHFS servers"
  exit 1
fi

runid=0

for ppn in "${ppn_list[@]}"; do
  for transfer_size in "${transfer_size_list[@]}"; do
    for block_size in "${block_size_list[@]}"; do
      for ior_flags in "${ior_flags_list[@]}"; do

        # Run benchmark
        run_ior_benchmark "$runid" "$ppn" "$transfer_size" "$block_size" "$ior_flags"

        # Save metadata
        save_job_metadata "$runid" "$ppn" "$transfer_size" "$block_size" "$ior_flags"

        runid=$((runid + 1))

      done
    done
  done
done

echo ""
echo "=========================================="
echo "Benchmark Complete"
echo "=========================================="
echo "Total runs: ${runid}"
echo "Results saved to: ${JOB_OUTPUT_DIR}"
echo "=========================================="

# Cleanup is handled by trap
exit 0
