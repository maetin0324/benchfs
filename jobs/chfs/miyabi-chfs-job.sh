#!/bin/bash
# CHFS Benchmark Job Script (Miyabi)
#
# This script is executed by the job scheduler to run CHFS benchmarks.
# It starts CHFS servers, runs IOR benchmarks, and collects results.

#------- qsub option -----------
#PBS -W group_list="xg24i002"
#PBS -q debug-g
#PBS -V
#------- Program execution -----------
# NOTE: DO NOT use "set -e" or "set -u" or "set -o pipefail" here!
# IOR benchmark may fail for various reasons (timeout, resource exhaustion, etc.)
# and we want to continue running other parameter combinations.
set +e

cleanup_exported_bash_functions() {
  # PBS -V exports bash functions (module/ml) as environment variables.
  # Open MPI spawns /bin/sh on remote nodes, which fails to import them.

  # Explicitly unset known problematic functions
  unset -f module ml _module_raw 2>/dev/null || true

  # Remove all BASH_FUNC_* environment variables
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

  # Also try the %% suffix format used by bash 4.4+
  for func_name in module ml _module_raw; do
    unset "BASH_FUNC_${func_name}%%" 2>/dev/null || true
    unset "BASH_FUNC_${func_name}()" 2>/dev/null || true
  done
}

# Increase file descriptor limit for large-scale MPI jobs
ulimit -n 1048576 2>/dev/null || ulimit -n 524288 2>/dev/null || ulimit -n 262144 2>/dev/null || ulimit -n 65536 2>/dev/null || true

# Set gcc and OpenMPI modules
module purge
module load gcc-toolset/14
module load ompi-cuda/4.1.6-12.6
cleanup_exported_bash_functions

unset OMPI_MCA_mca_base_env_list

SCRIPT_DIR="${SCRIPT_DIR:-/work/xg24i002/x10043/workspace/rust/benchfs/jobs/chfs}"
PROJECT_ROOT="${PROJECT_ROOT:-/work/xg24i002/x10043/workspace/rust/benchfs}"
# OUTPUT_DIR is exported from miyabi-chfs.sh via -V
: ${OUTPUT_DIR:="$PROJECT_ROOT/results/chfs/${TIMESTAMP:-$(date +%Y.%m.%d-%H.%M.%S)}-${PBS_JOBID}"}
BACKEND_DIR="${BACKEND_DIR:-$PROJECT_ROOT/backend/chfs}"
IOR_PREFIX="${IOR_PREFIX:-${PROJECT_ROOT}/ior_integration/ior}"

# Load common utilities
source "${SCRIPT_DIR}/../benchfs/common.sh"

# ============================================================
# Job Environment Setup
# ============================================================

JOB_START=$(timestamp)
NNODES=$(cat "${PBS_NODEFILE}" | sort -u | wc -l)
JOBID=$(echo "$PBS_JOBID" | cut -d . -f 1)
JOB_OUTPUT_DIR="${OUTPUT_DIR}/${JOB_START}-${JOBID}-${NNODES}"
JOB_BACKEND_DIR="${BACKEND_DIR}/$(basename -- "${JOB_OUTPUT_DIR}")"

# CHFS directories
CHFS_SCRATCH_DIR="/local/chfs_${JOBID}"  # Node-local storage
CHFS_LOG_DIR="${JOB_OUTPUT_DIR}/chfs_logs"
IOR_OUTPUT_DIR="${JOB_OUTPUT_DIR}/ior_results"

# Default configurable variables (may be overridden by miyabi-chfs.sh via -V)
: ${TASKSET:=0}
: ${TASKSET_CORES:=0,1}
: ${CHUNK_SIZE_MATCH_XFER:=0}

# Create output directories
mkdir -p "${JOB_OUTPUT_DIR}" "${CHFS_LOG_DIR}" "${IOR_OUTPUT_DIR}" "${JOB_BACKEND_DIR}"

# Save OpenMPI mpirun path before loading spack (spack may override PATH)
MPIRUN_CMD="$(which mpirun)"
echo "OpenMPI mpirun: ${MPIRUN_CMD}"

# Load CHFS dependencies via spack and add local bin to PATH
SPACK_SETUP="$HOME/spack/share/spack/setup-env.sh"
echo "Loading spack from: ${SPACK_SETUP}"
if [ -f "$SPACK_SETUP" ]; then
  source "$SPACK_SETUP"
  echo "Spack loaded, loading mochi-margo..."
  spack load mochi-margo || { echo "ERROR: Failed to load mochi-margo"; exit 1; }
else
  echo "ERROR: Spack setup file not found at ${SPACK_SETUP}"
  exit 1
fi

# Add locally installed CHFS binaries to PATH
export PATH="$HOME/.local/bin:$PATH"

# Re-cleanup after spack load
cleanup_exported_bash_functions

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
echo "CHFS Benchmark Job Started (Miyabi)"
echo "=========================================="
echo "Job ID: ${JOBID}"
echo "Nodes: ${NNODES}"
echo "Start Time: ${JOB_START}"
echo "Output Directory: ${JOB_OUTPUT_DIR}"
echo "IOR Prefix: ${IOR_PREFIX}"
echo "=========================================="

# ============================================================
# MPI Configuration
# ============================================================

# Immediate mitigation environment variables
export OMPI_MCA_mpi_yield_when_idle=1
export OMPI_MCA_btl_base_warn_component_unused=0
export OMPI_MCA_mpi_show_handle_leaks=0

# Re-run cleanup to ensure bash functions are not passed to mpirun
cleanup_exported_bash_functions
unset -f module ml 2>/dev/null || true
unset BASH_ENV 2>/dev/null || true
unset 'BASH_FUNC_module%%' 'BASH_FUNC_ml%%' 2>/dev/null || true
unset 'BASH_FUNC_module' 'BASH_FUNC_ml' 2>/dev/null || true
export BASH_ENV=

# Build mpirun common command (use TCP for compatibility)
cmd_mpirun_common=(
  "${MPIRUN_CMD}"
  --mca routed direct
  --mca plm_rsh_no_tree_spawn 1
  --mca pml ob1
  --mca btl tcp,vader,self
  --mca btl_openib_allow_ib 0
  -x PATH
  -x LD_LIBRARY_PATH
)

# ============================================================
# mpirun-based SSH replacement for chfsctl
# ============================================================
# Miyabi does not allow inter-node SSH. chfsctl uses ssh by default
# to launch chfsd on remote nodes. We use chfsctl's -ssh option to
# provide a wrapper that uses mpirun instead.
MPIRUN_SSH_WRAPPER="${JOB_OUTPUT_DIR}/mpirun_ssh.sh"
cat > "${MPIRUN_SSH_WRAPPER}" <<'SSHWRAPPER_EOF'
#!/bin/bash
# mpirun-based SSH replacement for chfsctl
#
# chfsctl calls this as:  ssh_cmd hostname command [args...]
# where command may include "VAR=val ... executable args" or
# shell constructs like glob patterns (rm -rf dir/*).
#
# ssh would run a remote shell that handles both env assignments and globs.
# mpirun does NOT run a shell, so we must use "bash -c" to emulate ssh.
HOST="$1"
shift

# Clean up ALL exported bash functions that break /bin/sh on remote nodes.
# BASH_FUNC_name%% variables can't be unset by name (% is not valid in
# variable names), but "unset -f name" removes both the function AND its
# environment export.
while IFS= read -r _func; do
  _fname="${_func##* }"
  unset -f "$_fname" 2>/dev/null || true
done < <(declare -Fx 2>/dev/null)
export BASH_ENV=
SSHWRAPPER_EOF
# Append mpirun command with resolved paths (not quoted, so they expand now)
# Use "bash -c" so that env assignments (VAR=val cmd) and globs (*) work
# just like they would through ssh's remote shell.
cat >> "${MPIRUN_SSH_WRAPPER}" <<SSHWRAPPER_EOF2
# Build a single command string for bash -c (preserves env assignments and globs)
_cmd=""
for _arg in "\$@"; do
  _cmd="\${_cmd:+\$_cmd }\$_arg"
done
exec ${MPIRUN_CMD} --host "\$HOST" -np 1 \\
  --mca routed direct \\
  --mca plm_rsh_no_tree_spawn 1 \\
  --mca pml ob1 \\
  --mca btl tcp,vader,self \\
  --mca btl_openib_allow_ib 0 \\
  bash -c "\$_cmd"
SSHWRAPPER_EOF2
chmod +x "${MPIRUN_SSH_WRAPPER}"
echo "Created mpirun SSH wrapper: ${MPIRUN_SSH_WRAPPER}"

# ============================================================
# Parameter Loading
# ============================================================

PARAM_FILE="${PARAM_FILE:-${SCRIPT_DIR}/../params/debug.conf}"
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

if [ "${CHUNK_SIZE_MATCH_XFER}" -eq 1 ]; then
  echo "CHUNK_SIZE_MATCH_XFER=1: CHFS_CHUNK_SIZE will be set to match transfer_size"
fi

echo "CHFS Parameters:"
echo "  CHFS_PROTOCOL: ${CHFS_PROTOCOL}"
echo "  CHFS_CHUNK_SIZE: ${CHFS_CHUNK_SIZE}$([ "${CHUNK_SIZE_MATCH_XFER}" -eq 1 ] && echo ' (will be overridden by transfer_size)')"
echo "  CHFS_DB_SIZE: ${CHFS_DB_SIZE}"

# Save parameters as JSON
cat > "${JOB_OUTPUT_DIR}/parameters.json" <<EOF
{
  "parameter_file": "$PARAM_FILE",
  "system": "miyabi",
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

  # Create scratch directory on all nodes via mpirun (ssh is not available on miyabi)
  echo "Creating scratch directory ${CHFS_SCRATCH_DIR} on all nodes..."
  "${cmd_mpirun_common[@]}" -np "$NNODES" --map-by ppr:1:node \
    bash -c "mkdir -p '${CHFS_SCRATCH_DIR}' && echo \$(hostname): scratch dir created" || true

  local CHFSD_REAL=$(which chfsd 2>/dev/null || true)
  echo "Real chfsd path: ${CHFSD_REAL:-not found}"

  if [ "${TASKSET}" = "1" ] && [ -n "${CHFSD_REAL}" ]; then
    echo "TASKSET mode enabled: limiting chfsd to cores ${TASKSET_CORES}"

    # Use shared filesystem path for the wrapper (accessible from all nodes)
    local WRAPPER_SCRIPT="${JOB_OUTPUT_DIR}/chfsd_taskset_wrapper.sh"
    cat > "${WRAPPER_SCRIPT}" <<WRAPPER_EOF
#!/bin/bash
# Wrapper to limit chfsd to specified CPU cores
exec taskset -c ${TASKSET_CORES} ${CHFSD_REAL} "\$@"
WRAPPER_EOF
    chmod +x "${WRAPPER_SCRIPT}"
    echo "Created wrapper script: ${WRAPPER_SCRIPT}"
    cat "${WRAPPER_SCRIPT}"

    # Prepend wrapper directory to PATH so chfsctl finds it first
    export PATH="$(dirname "${WRAPPER_SCRIPT}"):${PATH}"
    echo "Updated PATH to use taskset wrapper"
  else
    if [ "${TASKSET}" = "1" ]; then
      echo "WARNING: TASKSET=1 but chfsd not found in PATH, skipping taskset wrapper"
    fi
    echo "TASKSET mode disabled: chfsd will use all available cores"
  fi

  # Start CHFS servers (no FUSE mount, using native API)
  # Use -ssh to launch chfsd via mpirun instead of ssh (ssh is blocked on miyabi)
  echo "Running chfsctl start..."
  echo "chfsctl command: chfsctl -ssh ${MPIRUN_SSH_WRAPPER} -h ${JOB_OUTPUT_DIR}/hostfile -c ${CHFS_SCRATCH_DIR} -b ${JOB_BACKEND_DIR} -s ${db_size} -p ${protocol} -M -f 2 -n 32 -L ${CHFS_LOG_DIR} start"

  # Run chfsctl and capture output
  # IMPORTANT: Do NOT merge stderr into stdout (2>&1).
  # The mpirun SSH wrapper produces stderr messages like "Primary job terminated
  # normally, but 1 process returned a non-zero exit code" which are benign
  # (chfsd daemonizes, causing mpirun to see a non-zero exit). Mixing these
  # into chfsctl_output would break "eval" and could trigger false error detection.
  local chfsctl_output
  chfsctl_output=$(chfsctl \
    -ssh "${MPIRUN_SSH_WRAPPER}" \
    -h "${JOB_OUTPUT_DIR}/hostfile" \
    -c "${CHFS_SCRATCH_DIR}" \
    -b "${JOB_BACKEND_DIR}" \
    -s "${db_size}" \
    -p "${protocol}" \
    -M \
    -f 2 \
    -n 32 \
    -L "${CHFS_LOG_DIR}" \
    -x PATH \
    -x LD_LIBRARY_PATH \
    start 2>"${CHFS_LOG_DIR}/chfsctl_start_stderr.log") || {
      echo "ERROR: chfsctl start failed with exit code $?"
      echo "Stdout: ${chfsctl_output}"
      echo ""
      echo "=== chfsctl stderr ==="
      cat "${CHFS_LOG_DIR}/chfsctl_start_stderr.log" 2>/dev/null || true
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
  echo "chfsctl stderr (mpirun messages, usually benign):"
  cat "${CHFS_LOG_DIR}/chfsctl_start_stderr.log" 2>/dev/null || true

  # Evaluate the output to set environment variables (stdout only, no mpirun noise)
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

  # Stop servers (use -ssh wrapper since ssh is blocked on miyabi)
  chfsctl \
    -ssh "${MPIRUN_SSH_WRAPPER}" \
    -h "${JOB_OUTPUT_DIR}/hostfile" \
    -M \
    stop > "${CHFS_LOG_DIR}/chfsctl_stop.log" 2>"${CHFS_LOG_DIR}/chfsctl_stop_stderr.log" || true

  # Clean up scratch directories (also removes the taskset wrapper)
  chfsctl \
    -ssh "${MPIRUN_SSH_WRAPPER}" \
    -h "${JOB_OUTPUT_DIR}/hostfile" \
    -c "${CHFS_SCRATCH_DIR}" \
    clean > "${CHFS_LOG_DIR}/chfsctl_clean.log" 2>"${CHFS_LOG_DIR}/chfsctl_clean_stderr.log" || true

  echo "CHFS servers stopped"
}

# Cleanup on exit
cleanup_and_exit() {
  local exit_code=${1:-1}
  local signal_name=${2:-"unknown"}
  echo ""
  echo "=========================================="
  echo "Job interrupted by signal: $signal_name"
  echo "Cleaning up..."
  echo "=========================================="
  stop_chfs_servers
  rm -rf "${JOB_BACKEND_DIR}" 2>/dev/null || true
  exit "$exit_code"
}

# Trap signals for cleanup
trap 'cleanup_and_exit 1 "SIGHUP (walltime or session end)"' 1
trap 'cleanup_and_exit 1 "SIGINT (user interrupt)"' 2
trap 'cleanup_and_exit 1 "SIGQUIT"' 3
trap 'cleanup_and_exit 1 "SIGTERM (PBS walltime reached)"' 15
trap 'stop_chfs_servers; rm -rf "${JOB_BACKEND_DIR}" 2>/dev/null || true; exit 0' EXIT

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

  local effective_chunk_size="${CHFS_CHUNK_SIZE}"
  if [ "${CHUNK_SIZE_MATCH_XFER}" -eq 1 ]; then
    effective_chunk_size=$(parse_size_to_bytes "$transfer_size")
  fi

  cat > "${JOB_OUTPUT_DIR}/job_metadata_${runid}.json" <<METADATA_EOF
{
  "jobid": "${JOBID}",
  "runid": ${runid},
  "system": "miyabi",
  "nnodes": ${NNODES},
  "ppn": ${ppn},
  "np": ${np},
  "transfer_size": "${transfer_size}",
  "block_size": "${block_size}",
  "chfs_chunk_size": ${effective_chunk_size},
  "file_per_proc": ${file_per_proc},
  "ior_flags": "${ior_flags}",
  "job_start_time": "${JOB_START}"
}
METADATA_EOF
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

  # Override CHFS_CHUNK_SIZE to match transfer_size when requested
  local effective_chunk_size="${CHFS_CHUNK_SIZE}"
  if [ "${CHUNK_SIZE_MATCH_XFER}" -eq 1 ]; then
    effective_chunk_size=$(parse_size_to_bytes "$transfer_size")
    echo "CHUNK_SIZE_MATCH_XFER: using chunk_size=${effective_chunk_size} (from transfer_size=${transfer_size})"
  fi

  echo ""
  echo "=========================================="
  echo "IOR Benchmark Run ${runid}"
  echo "=========================================="
  echo "Processes: ${np} (${ppn} per node)"
  echo "Transfer Size: ${transfer_size}"
  echo "Block Size: ${block_size}"
  echo "CHFS Chunk Size: ${effective_chunk_size}"
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
    -x CHFS_CHUNK_SIZE="${effective_chunk_size}"
    "${IOR_PREFIX}/src/ior"
    -vvv
    -g
    -D 120
    -O stoneWallingWearOut=1
    -a CHFS
    --chfs.chunk_size="${effective_chunk_size}"
    -t "$transfer_size"
    -b "$block_size"
    $ior_flags
    -o "/testfile_${runid}"
    -O summaryFormat=JSON
    -O summaryFile="${ior_json}"
  )

  echo "Running: ${cmd_ior[*]}"
  local ior_exit_code=0
  "${cmd_ior[@]}" > "${ior_stdout}" 2> "${ior_stderr}" || ior_exit_code=$?

  if [ "$ior_exit_code" -ne 0 ]; then
    echo "WARNING: IOR run $runid failed with exit code $ior_exit_code"
    echo "  Stderr log: ${ior_stderr}"
    echo "  Continuing with next parameter combination..."
    cat "${ior_stderr}"
  fi

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
