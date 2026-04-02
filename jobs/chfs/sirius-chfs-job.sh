#!/bin/bash
# CHFS Benchmark Job Script (Sirius)
#
# This script is executed by the job scheduler to run CHFS benchmarks.
# It starts CHFS servers with NUMA-aware NVMe SSD assignment,
# runs IOR benchmarks, and collects results.
#
# Sirius topology (per physical node):
#   APU 0 (NUMA 0, cores  0-23) → /scr0
#   APU 1 (NUMA 1, cores 24-47) → /scr1
#   APU 2 (NUMA 2, cores 48-71) → /scr2
#   APU 3 (NUMA 3, cores 72-95) → /scr3

#------- qsub option -----------
#PBS -q gen
#PBS -A NBB
#------- Program execution -----------
# NOTE: DO NOT use "set -e" or "set -u" or "set -o pipefail" here!
set +e

# Increase file descriptor limit for large-scale MPI jobs
ulimit -n 1048576 2>/dev/null || ulimit -n 524288 2>/dev/null || ulimit -n 262144 2>/dev/null || ulimit -n 65536 2>/dev/null || true
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
# Path Configuration
# ==============================================================================

SCRIPT_DIR="${SCRIPT_DIR:-/work/NBB/rmaeda/workspace/rust/benchfs/jobs/chfs}"
PROJECT_ROOT="${PROJECT_ROOT:-/work/NBB/rmaeda/workspace/rust/benchfs}"
: ${OUTPUT_DIR:="$PROJECT_ROOT/results/chfs/$(date +%Y.%m.%d-%H.%M.%S)-sirius"}
BACKEND_DIR="${BACKEND_DIR:-$PROJECT_ROOT/backend/chfs}"
IOR_PREFIX="${IOR_PREFIX:-${PROJECT_ROOT}/ior_integration/ior}"

# Load common utilities
source "${SCRIPT_DIR}/../benchfs/common.sh"

# ==============================================================================
# Job Environment Setup
# ==============================================================================

JOB_START=$(timestamp)
NNODES=$(wc -l < "${PBS_NODEFILE}")
JOBID=$(echo "$PBS_JOBID" | cut -d . -f 1)
JOB_OUTPUT_DIR="${OUTPUT_DIR}/${JOB_START}-${JOBID}-${NNODES}n"
JOB_BACKEND_DIR="${BACKEND_DIR}/$(basename -- "${JOB_OUTPUT_DIR}")"

# CHFS directories
CHFS_LOG_DIR="${JOB_OUTPUT_DIR}/chfs_logs"
IOR_OUTPUT_DIR="${JOB_OUTPUT_DIR}/ior_results"

# Default configurable variables (may be overridden by qsub -V)
: ${TASKSET:=0}
: ${TASKSET_CORES:=0}
: ${CHUNK_SIZE_MATCH_XFER:=0}

# Sirius scratch directory detection:
#   Each vnode gets 1 of 4 NVMe SSDs (/scr0-/scr3).
#   PBS creates /scrN/${PBS_JOBID} on each allocated SSD.
detect_all_scratch_dirs() {
  CHFS_ALL_SCRATCH_DIRS=()
  for n in 0 1 2 3; do
    local candidate="/scr${n}/${PBS_JOBID}"
    if [ -d "$candidate" ]; then
      CHFS_ALL_SCRATCH_DIRS+=("$candidate")
    fi
  done
  if [ ${#CHFS_ALL_SCRATCH_DIRS[@]} -eq 0 ]; then
    echo "ERROR: No scratch directories found for job ${PBS_JOBID}" >&2
    return 1
  fi
  echo "Detected ${#CHFS_ALL_SCRATCH_DIRS[@]} scratch directories: ${CHFS_ALL_SCRATCH_DIRS[*]}"
}
detect_all_scratch_dirs
CHFS_PRIMARY_SCRATCH="${CHFS_ALL_SCRATCH_DIRS[0]}"

# Create output directories
mkdir -p "${JOB_OUTPUT_DIR}" "${CHFS_LOG_DIR}" "${IOR_OUTPUT_DIR}" "${JOB_BACKEND_DIR}"

# Create unique node list (PBS_NODEFILE may have duplicate entries for multi-vnode physical nodes)
UNIQUE_HOSTFILE="${JOB_OUTPUT_DIR}/unique_nodes"
sort -u "${PBS_NODEFILE}" > "${UNIQUE_HOSTFILE}"
UNIQUE_NNODES=$(wc -l < "${UNIQUE_HOSTFILE}")
echo "Unique physical nodes: ${UNIQUE_NNODES} (from ${NNODES} vnodes)"

# Add locally installed CHFS binaries to PATH
export PATH="$HOME/.local/bin:$HOME/.local/sbin:$PATH"

# Save OpenMPI mpirun path before loading spack (spack may override PATH)
MPIRUN_CMD="$(which mpirun)"
echo "OpenMPI mpirun: ${MPIRUN_CMD}"

# Load mochi-margo (CHFS dependency) via spack
SPACK_SETUP="/work/NBB/rmaeda/spack/share/spack/setup-env.sh"
echo "Loading spack from: ${SPACK_SETUP}"
if [ -f "$SPACK_SETUP" ]; then
  source "$SPACK_SETUP"
  echo "Spack loaded, loading mochi-margo..."
  spack load mochi-margo || { echo "ERROR: Failed to load mochi-margo"; exit 1; }
else
  echo "ERROR: Spack setup file not found at ${SPACK_SETUP}"
  exit 1
fi

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
echo "CHFS Benchmark Job Started (Sirius)"
echo "=========================================="
echo "Job ID: ${JOBID}"
echo "Nodes: ${NNODES}"
echo "Start Time: ${JOB_START}"
echo "Output Directory: ${JOB_OUTPUT_DIR}"
echo "IOR Prefix: ${IOR_PREFIX}"
echo "Scratch dirs: ${CHFS_ALL_SCRATCH_DIRS[*]}"
echo "=========================================="

# ==============================================================================
# MPI Configuration for Sirius (OpenMPI 5.0.9)
# ==============================================================================

export OMPI_MCA_mpi_yield_when_idle=1
export OMPI_MCA_btl_base_warn_component_unused=0
export OMPI_MCA_mpi_show_handle_leaks=0
export RUST_BACKTRACE=full

# Re-run cleanup to ensure bash functions are not passed to mpirun
cleanup_exported_bash_functions
unset -f module ml 2>/dev/null || true
unset BASH_ENV 2>/dev/null || true
unset 'BASH_FUNC_module%%' 'BASH_FUNC_ml%%' 2>/dev/null || true
unset 'BASH_FUNC_module' 'BASH_FUNC_ml' 2>/dev/null || true
export BASH_ENV=

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

# Build mpirun common command for IOR
# --mca routed direct: avoid DVM tree routing issues with multi-vnode PBS jobs
# --mca plm_rsh_no_tree_spawn 1: linear process spawning
if [[ "${USE_UCX_PML}" -eq 1 ]]; then
  cmd_mpirun_common=(
    "${MPIRUN_CMD}"
    --mca routed direct
    --mca plm_rsh_no_tree_spawn 1
    --mca pml ucx
    --mca btl self
    --mca osc ucx
    -x PATH
    -x LD_LIBRARY_PATH
  )
else
  cmd_mpirun_common=(
    "${MPIRUN_CMD}"
    --mca routed direct
    --mca plm_rsh_no_tree_spawn 1
    --mca pml ob1
    --mca btl tcp,sm,self
    -x PATH
    -x LD_LIBRARY_PATH
  )
fi

# Build mpirun command for utility tasks (bash scripts, cleanup, etc.)
# Uses simple TCP transport to avoid UCX/PMIX issues with non-MPI programs
cmd_mpirun_util=(
  "${MPIRUN_CMD}"
  --mca routed direct
  --mca plm_rsh_no_tree_spawn 1
  --mca pml ob1
  --mca btl tcp,sm,self
)

# Build mpirun command for chfsctl SSH wrapper (same as util)
cmd_mpirun_ssh=("${cmd_mpirun_util[@]}")

# ==============================================================================
# mpirun-based SSH replacement for chfsctl
# ==============================================================================
# chfsctl uses ssh to launch chfsd on remote nodes.
# On Sirius we provide a wrapper that uses mpirun instead.

MPIRUN_SSH_WRAPPER="${JOB_OUTPUT_DIR}/mpirun_ssh.sh"
cat > "${MPIRUN_SSH_WRAPPER}" <<'SSHWRAPPER_EOF'
#!/bin/bash
# mpirun-based SSH replacement for chfsctl
#
# chfsctl calls this as:  ssh_cmd hostname command [args...]
# ssh would run a remote shell, so we use "bash -c" to emulate it.
HOST="$1"
shift

# Clean up ALL exported bash functions that break /bin/sh on remote nodes.
while IFS= read -r _func; do
  _fname="${_func##* }"
  unset -f "$_fname" 2>/dev/null || true
done < <(declare -Fx 2>/dev/null)
export BASH_ENV=
SSHWRAPPER_EOF
# Append mpirun command (expand paths now, escape runtime variables)
cat >> "${MPIRUN_SSH_WRAPPER}" <<SSHWRAPPER_EOF2
# Build a single command string for bash -c (preserves env assignments and globs)
_cmd=""
for _arg in "\$@"; do
  _cmd="\${_cmd:+\$_cmd }\$_arg"
done
exec ${cmd_mpirun_ssh[@]} --host "\$HOST" -np 1 \
  -x PBS_JOBID \
  bash -c "\$_cmd"
SSHWRAPPER_EOF2
chmod +x "${MPIRUN_SSH_WRAPPER}"
echo "Created mpirun SSH wrapper: ${MPIRUN_SSH_WRAPPER}"

# ==============================================================================
# NUMA-aware chfsd wrapper
# ==============================================================================
# Each chfsd process detects the local /scrN on its node and uses the
# NVMe SSD closest to its assigned NUMA node as the cache directory.
# A file-based counter ensures multiple chfsd processes on the same
# physical node get different /scrN directories.

REAL_CHFSD="$(which chfsd 2>/dev/null || true)"
echo "Real chfsd path: ${REAL_CHFSD:-not found}"

if [ -z "${REAL_CHFSD}" ]; then
  echo "ERROR: chfsd not found in PATH"
  exit 1
fi

CHFSD_WRAPPER_DIR="${JOB_OUTPUT_DIR}/chfsd_wrapper"
mkdir -p "${CHFSD_WRAPPER_DIR}"

# Create the NUMA-aware chfsd wrapper
# NOTE: Use double-quoted heredoc so REAL_CHFSD and TASKSET values are embedded.
cat > "${CHFSD_WRAPPER_DIR}/chfsd" <<CHFSD_WRAPPER_EOF
#!/bin/bash
# NUMA-aware chfsd wrapper for Sirius
# Detects local /scrN and binds chfsd to the corresponding NUMA node.

REAL_CHFSD="${REAL_CHFSD}"
TASKSET_ENABLED=${TASKSET}
TASKSET_CORES_VAL="${TASKSET_CORES}"

# Detect NVMe scratch directories on THIS node
LOCAL_SCRATCH_DIRS=()
LOCAL_NUMA_NODES=()
for n in 0 1 2 3; do
    if [ -d "/scr\${n}/\${PBS_JOBID}" ]; then
        LOCAL_SCRATCH_DIRS+=("/scr\${n}/\${PBS_JOBID}")
        LOCAL_NUMA_NODES+=("\${n}")
    fi
done

SELECTED_DIR=""
SELECTED_NUMA=""

if [ \${#LOCAL_SCRATCH_DIRS[@]} -gt 0 ]; then
    # Use atomic file-based counter to assign different /scrN
    # to each chfsd process on the same node
    COUNTER_FILE="/tmp/chfsd_numa_counter_\${PBS_JOBID}"
    # flock + read/write for atomicity
    RANK=\$(flock "\${COUNTER_FILE}" bash -c '
        n=\$(cat "'\${COUNTER_FILE}'" 2>/dev/null || echo 0)
        echo \$((n + 1)) > "'\${COUNTER_FILE}'"
        echo \$n
    ')
    IDX=\$((RANK % \${#LOCAL_SCRATCH_DIRS[@]}))
    SELECTED_DIR="\${LOCAL_SCRATCH_DIRS[\$IDX]}"
    SELECTED_NUMA="\${LOCAL_NUMA_NODES[\$IDX]}"
    echo "chfsd wrapper: rank=\${RANK}, NUMA \${SELECTED_NUMA}, cache=\${SELECTED_DIR}/chfs (from \${#LOCAL_SCRATCH_DIRS[@]} local SSDs)" >&2
fi

# Override the -c (cache dir) argument if we detected a local /scrN
ARGS=()
if [ -n "\${SELECTED_DIR}" ]; then
    CACHE_DIR="\${SELECTED_DIR}/chfs"
    mkdir -p "\${CACHE_DIR}"
    SKIP_NEXT=0
    for arg in "\$@"; do
        if [ \${SKIP_NEXT} -eq 1 ]; then
            ARGS+=("\${CACHE_DIR}")
            SKIP_NEXT=0
        elif [ "\$arg" = "-c" ]; then
            ARGS+=("\$arg")
            SKIP_NEXT=1
        else
            ARGS+=("\$arg")
        fi
    done
else
    ARGS=("\$@")
fi

# Launch chfsd with NUMA binding (and optional taskset)
if [ -n "\${SELECTED_NUMA}" ] && command -v numactl >/dev/null 2>&1; then
    if [ "\${TASKSET_ENABLED}" = "1" ]; then
        echo "chfsd wrapper: numactl --cpunodebind=\${SELECTED_NUMA} --membind=\${SELECTED_NUMA} + taskset -c \${TASKSET_CORES_VAL}" >&2
        exec numactl --cpunodebind="\${SELECTED_NUMA}" --membind="\${SELECTED_NUMA}" \\
            taskset -c "\${TASKSET_CORES_VAL}" "\${REAL_CHFSD}" "\${ARGS[@]}"
    else
        echo "chfsd wrapper: numactl --cpunodebind=\${SELECTED_NUMA} --membind=\${SELECTED_NUMA}" >&2
        exec numactl --cpunodebind="\${SELECTED_NUMA}" --membind="\${SELECTED_NUMA}" \\
            "\${REAL_CHFSD}" "\${ARGS[@]}"
    fi
else
    if [ "\${TASKSET_ENABLED}" = "1" ]; then
        echo "chfsd wrapper: taskset -c \${TASKSET_CORES_VAL} (no NUMA binding)" >&2
        exec taskset -c "\${TASKSET_CORES_VAL}" "\${REAL_CHFSD}" "\${ARGS[@]}"
    else
        echo "chfsd wrapper: no NUMA binding, no taskset" >&2
        exec "\${REAL_CHFSD}" "\${ARGS[@]}"
    fi
fi
CHFSD_WRAPPER_EOF
chmod +x "${CHFSD_WRAPPER_DIR}/chfsd"
echo "Created NUMA-aware chfsd wrapper: ${CHFSD_WRAPPER_DIR}/chfsd"

# Prepend wrapper directory to PATH so chfsctl finds it first
export PATH="${CHFSD_WRAPPER_DIR}:${PATH}"
echo "Updated PATH to use NUMA-aware chfsd wrapper"

# ==============================================================================
# Parameter Loading
# ==============================================================================

PARAM_FILE="${PARAM_FILE:-${SCRIPT_DIR}/../params/debug.conf}"
if [ -f "$PARAM_FILE" ]; then
    echo "Loading parameters from: $PARAM_FILE"
    source "$PARAM_FILE"
else
    echo "ERROR: Parameter file not found: $PARAM_FILE"
    exit 1
fi

# Set defaults for optional parameters
: ${CHFS_CHUNK_SIZE:=4194304}
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
        [0-9]) echo "$size_str" ;;
        *) echo "$size_str" ;;
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
  "system": "sirius",
  "nnodes": ${NNODES},
  "transfer_sizes": [$(printf '"%s",' "${transfer_size_list[@]}" | sed 's/,$//; s/,$//')],
  "block_sizes": [$(printf '"%s",' "${block_size_list[@]}" | sed 's/,$//; s/,$//')],
  "client_ppn_values": [$(printf '%s,' "${ppn_list[@]}" | sed 's/,$//; s/,$//')],
  "ior_flags": [$(printf '"%s",' "${ior_flags_list[@]}" | sed 's/,$//; s/,$//')],
  "chfs_chunk_size": ${CHFS_CHUNK_SIZE},
  "chfs_protocol": "${CHFS_PROTOCOL}",
  "chfs_db_size": "${CHFS_DB_SIZE}"
}
EOF

# ==============================================================================
# CHFS Server Management Functions
# ==============================================================================

start_chfs_servers() {
  local protocol=${1:-sockets}
  local db_size=${2:-10GB}

  echo ""
  echo "=========================================="
  echo "Starting CHFS Servers"
  echo "=========================================="
  echo "Protocol: ${protocol}"
  echo "DB Size: ${db_size}"
  echo "Scratch dirs: ${CHFS_ALL_SCRATCH_DIRS[*]}"
  echo "=========================================="

  # Create hostfile from PBS_NODEFILE
  cp "${PBS_NODEFILE}" "${JOB_OUTPUT_DIR}/hostfile"

  # Reset the NUMA counter on all nodes before starting chfsd
  # This ensures counter starts at 0 for each start_chfs_servers call
  # Use unique hostfile to run exactly one process per physical node
  "${cmd_mpirun_util[@]}" --hostfile "${UNIQUE_HOSTFILE}" -np "$UNIQUE_NNODES" \
    --bind-to none --oversubscribe \
    -x PBS_JOBID \
    bash -c "rm -f /tmp/chfsd_numa_counter_\${PBS_JOBID} && echo \$(hostname): NUMA counter reset" || true

  # Create scratch directories on all nodes
  echo "Creating scratch directories on all nodes..."
  "${cmd_mpirun_util[@]}" --hostfile "${UNIQUE_HOSTFILE}" -np "$UNIQUE_NNODES" \
    --bind-to none --oversubscribe \
    -x PBS_JOBID \
    bash -c '
      for n in 0 1 2 3; do
        d="/scr${n}/${PBS_JOBID}/chfs"
        if [ -d "/scr${n}/${PBS_JOBID}" ]; then
          mkdir -p "$d"
          echo "$(hostname): created $d"
        fi
      done
    ' || true

  # Start CHFS servers via chfsctl
  # Use -ssh to launch chfsd via mpirun instead of ssh
  # The NUMA-aware chfsd wrapper (in PATH) handles /scrN selection and NUMA binding
  echo "Running chfsctl start..."
  echo "chfsctl command: chfsctl -ssh ${MPIRUN_SSH_WRAPPER} -h ${JOB_OUTPUT_DIR}/hostfile -c ${CHFS_PRIMARY_SCRATCH}/chfs -b ${JOB_BACKEND_DIR} -s ${db_size} -p ${protocol} -M -f 2 -n 32 -L ${CHFS_LOG_DIR} start"

  local chfsctl_output
  chfsctl_output=$(chfsctl \
    -ssh "${MPIRUN_SSH_WRAPPER}" \
    -h "${JOB_OUTPUT_DIR}/hostfile" \
    -c "${CHFS_PRIMARY_SCRATCH}/chfs" \
    -b "${JOB_BACKEND_DIR}" \
    -s "${db_size}" \
    -p "${protocol}" \
    -M \
    -f 2 \
    -n 32 \
    -L "${CHFS_LOG_DIR}" \
    -x PATH \
    -x LD_LIBRARY_PATH \
    -x PBS_JOBID \
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

  # Evaluate the output to set environment variables (stdout only)
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
    -ssh "${MPIRUN_SSH_WRAPPER}" \
    -h "${JOB_OUTPUT_DIR}/hostfile" \
    -M \
    stop > "${CHFS_LOG_DIR}/chfsctl_stop.log" 2>"${CHFS_LOG_DIR}/chfsctl_stop_stderr.log" || true

  # Clean up scratch directories and NUMA counter
  chfsctl \
    -ssh "${MPIRUN_SSH_WRAPPER}" \
    -h "${JOB_OUTPUT_DIR}/hostfile" \
    -c "${CHFS_PRIMARY_SCRATCH}/chfs" \
    clean > "${CHFS_LOG_DIR}/chfsctl_clean.log" 2>"${CHFS_LOG_DIR}/chfsctl_clean_stderr.log" || true

  # Clean up NUMA counters on all nodes
  "${cmd_mpirun_util[@]}" --hostfile "${UNIQUE_HOSTFILE}" -np "$UNIQUE_NNODES" \
    --bind-to none --oversubscribe \
    -x PBS_JOBID \
    bash -c "rm -f /tmp/chfsd_numa_counter_\${PBS_JOBID}" 2>/dev/null || true

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

# ==============================================================================
# Benchmark Functions
# ==============================================================================

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
  "system": "sirius",
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
    --oversubscribe
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

  # NOTE: Do NOT clean chfsd cache dirs while chfsd is running.
  # Deleting cache contents corrupts chfsd's internal state (chfs_create fails).
  # IOR removes its own test files by default (keepFile=0).

  # Check if results were produced
  if [ -f "${ior_json}" ]; then
    echo "IOR results saved to: ${ior_json}"
    if command -v jq >/dev/null 2>&1; then
      jq -r '.tests[0].Results[] | "\(.operation): \(.bwMiB) MiB/s"' "${ior_json}" 2>/dev/null || true
    fi
  else
    echo "WARNING: IOR result file not created"
  fi

  return 0
}

# ==============================================================================
# Main Benchmark Loop
# ==============================================================================

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
