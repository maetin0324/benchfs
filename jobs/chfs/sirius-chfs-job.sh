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

# Save OpenMPI mpirun path and the clean environment BEFORE loading spack.
# spack load pushes libfabric-2.4.0 into LD_LIBRARY_PATH which is ABI-incompatible
# with OpenMPI 5.0.9's PRTE/orted and causes mpirun to SIGSEGV with
# "PMIX_ERR_NOT_FOUND / NO PATH TO TARGET". We run mpirun with the original
# LD_LIBRARY_PATH and only expose the spack env to chfsd/chlist/IOR explicitly.
MPIRUN_CMD="$(which mpirun)"
echo "OpenMPI mpirun: ${MPIRUN_CMD}"

# Sanitize LD_LIBRARY_PATH / PATH: strip any spack paths that leaked in via
# `qsub -V` (user's shell may already have `spack load` state). Without this,
# ORIG_LD_LIBRARY_PATH contains libfabric-2.4.0 and mpirun/PRTE still segfaults.
strip_spack_paths() {
  local in="$1"
  local IFS=:
  local out=""
  for p in $in; do
    case "$p" in
      */spack/*|*/.spack/*|*libfabric-2.4.0*) ;;
      *) out="${out:+$out:}$p" ;;
    esac
  done
  printf '%s' "$out"
}
ORIG_LD_LIBRARY_PATH="$(strip_spack_paths "${LD_LIBRARY_PATH:-}")"
ORIG_PATH="$(strip_spack_paths "${PATH}")"
echo "ORIG_PATH (sanitized): ${ORIG_PATH}"
echo "ORIG_LD_LIBRARY_PATH (sanitized): ${ORIG_LD_LIBRARY_PATH}"

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

# Capture the spack-loaded environment for CHFS binaries (chfsd, chlist, IOR).
CHFS_LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-}"
CHFS_PATH="${PATH}"
echo "ORIG_LD_LIBRARY_PATH: ${ORIG_LD_LIBRARY_PATH}"
echo "CHFS_LD_LIBRARY_PATH: ${CHFS_LD_LIBRARY_PATH}"

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
# All mpirun invocations are prefixed with `env LD_LIBRARY_PATH=$ORIG_LD_LIBRARY_PATH`
# so that OpenMPI/PRTE/orted load the system libfabric they were built with,
# NOT the spack libfabric-2.4.0 (which causes PMIx segfaults).
# Remote processes that need the spack env (chfsd, IOR) set LD_LIBRARY_PATH
# themselves via the chfsd wrapper or via explicit -x LD_LIBRARY_PATH=... below.
MPIRUN_ENV_PREFIX=(env "LD_LIBRARY_PATH=${ORIG_LD_LIBRARY_PATH}" "PATH=${ORIG_PATH}")

if [[ "${USE_UCX_PML}" -eq 1 ]]; then
  cmd_mpirun_common=(
    "${MPIRUN_ENV_PREFIX[@]}"
    "${MPIRUN_CMD}"
    --mca routed direct
    --mca plm_rsh_no_tree_spawn 1
    --mca pml ucx
    --mca btl self
    --mca osc ucx
  )
else
  cmd_mpirun_common=(
    "${MPIRUN_ENV_PREFIX[@]}"
    "${MPIRUN_CMD}"
    --mca routed direct
    --mca plm_rsh_no_tree_spawn 1
    --mca pml ob1
    --mca btl tcp,sm,self
  )
fi

# Build mpirun command for utility tasks (bash scripts, cleanup, etc.)
# Uses simple TCP transport to avoid UCX/PMIX issues with non-MPI programs.
# Remote processes here are just bash; they don't need the spack env.
cmd_mpirun_util=(
  "${MPIRUN_ENV_PREFIX[@]}"
  "${MPIRUN_CMD}"
  --mca routed direct
  --mca plm_rsh_no_tree_spawn 1
  --mca pml ob1
  --mca btl tcp,sm,self
)

# Build mpirun command for chfsctl SSH wrapper (same as util)
cmd_mpirun_ssh=("${cmd_mpirun_util[@]}")

# ==============================================================================
# chfsd-sirius wrapper (installed in $HOME/.local/sbin)
# ==============================================================================
# The official Sirius chfsd wrapper handles /scrN selection, NUMA binding and
# per-HCA IB device (mlx5_$i) assignment. We invoke it via
#   chfsctl -wrapper chfsd-sirius -ssh <mpirun_ssh_wrapper> ...
# chfsctl-side expansion: CHFSD = dirname($CHFSD)/chfsd-sirius, so it resolves
# to $HOME/.local/sbin/chfsd-sirius (absolute path).
# NOTE: pbs_tmrsh is NOT installed on Sirius (only the man page), so we still
# need the mpirun-based SSH replacement for chfsctl to reach remote nodes.
CHFSD_SIRIUS="$(dirname "$(command -v chfsd)")/chfsd-sirius"
if [ ! -x "${CHFSD_SIRIUS}" ]; then
  echo "ERROR: chfsd-sirius not found at ${CHFSD_SIRIUS}"
  exit 1
fi
echo "chfsd-sirius wrapper: ${CHFSD_SIRIUS}"

CHFSCTL="$(command -v chfsctl)"

# IOR wrapper script: sets the spack-loaded env (mercury/margo/libfabric 2.4.0)
# on each rank before execing IOR. mpirun itself runs with ORIG env to avoid
# loading spack libfabric in OpenMPI's PRTE/orted (which segfaults).
# This replaces the fragile `-x LD_LIBRARY_PATH=<very-long-spack-path>` which
# hits PRTE argv/env handling issues.
IOR_WRAPPER="${JOB_OUTPUT_DIR}/ior_wrapper.sh"
cat > "${IOR_WRAPPER}" <<IOR_WRAPPER_EOF
#!/bin/bash
export LD_LIBRARY_PATH="${CHFS_LD_LIBRARY_PATH}"
export PATH="${CHFS_PATH}"
exec "${IOR_PREFIX}/src/ior" "\$@"
IOR_WRAPPER_EOF
chmod +x "${IOR_WRAPPER}"
echo "Created IOR wrapper: ${IOR_WRAPPER}"

# Create mpirun-based SSH replacement for chfsctl's -ssh option.
MPIRUN_SSH_WRAPPER="${JOB_OUTPUT_DIR}/mpirun_ssh.sh"
cat > "${MPIRUN_SSH_WRAPPER}" <<'SSHWRAPPER_EOF'
#!/bin/bash
# mpirun-based SSH replacement for chfsctl.
# chfsctl calls this as: ssh_cmd hostname command [args...]
HOST="$1"
shift

# Clean up exported bash functions that break /bin/sh on remote nodes.
while IFS= read -r _func; do
  _fname="${_func##* }"
  unset -f "$_fname" 2>/dev/null || true
done < <(declare -Fx 2>/dev/null)
export BASH_ENV=

# Join remaining args into a single command string for `bash -c`.
_cmd=""
for _arg in "$@"; do
  _cmd="${_cmd:+$_cmd }$_arg"
done
SSHWRAPPER_EOF
# Append the mpirun invocation (expand paths at wrapper generation time).
cat >> "${MPIRUN_SSH_WRAPPER}" <<SSHWRAPPER_EOF2
exec ${cmd_mpirun_ssh[@]} --host "\$HOST" -np 1 -x PBS_JOBID bash -c "\$_cmd"
SSHWRAPPER_EOF2
chmod +x "${MPIRUN_SSH_WRAPPER}"
echo "Created mpirun SSH wrapper: ${MPIRUN_SSH_WRAPPER}"

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
  echo "[$(date +%T)] Invoking chfsctl start with chfsd-sirius wrapper..."

  # Start CHFS servers via chfsctl
  # Use -ssh to launch chfsd via mpirun instead of ssh
  # The NUMA-aware chfsd wrapper (in PATH) handles /scrN selection and NUMA binding
  # Per CHFS author feedback:
  # - Remove -b (backend dir causes flushing, hurts performance)
  # - Remove -f (flush threads unnecessary without -b)
  # - Add -O "-H 0 -T $NIOT" to disable heartbeat and set I/O thread count
  local NIOT="${CHFS_NIOT:-1}"
  echo "Running chfsctl start..."
  echo "chfsctl command: ${CHFSCTL} -wrapper chfsd-sirius -ssh ${MPIRUN_SSH_WRAPPER} -h ${JOB_OUTPUT_DIR}/hostfile -s ${db_size} -p ${protocol} -M -n 32 -L ${CHFS_LOG_DIR} -O \"-H 0 -T ${NIOT}\" start"

  local chfsctl_output
  chfsctl_output=$("${CHFSCTL}" \
    -wrapper chfsd-sirius \
    -ssh "${MPIRUN_SSH_WRAPPER}" \
    -h "${JOB_OUTPUT_DIR}/hostfile" \
    -s "${db_size}" \
    -p "${protocol}" \
    -M \
    -n 32 \
    -L "${CHFS_LOG_DIR}" \
    -O "-H 0 -T ${NIOT}" \
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
  echo "=== chlist output ==="
  cat "${CHFS_LOG_DIR}/chlist.log"

  local server_count=$(wc -l < "${CHFS_LOG_DIR}/chlist.log")
  echo "CHFS servers ready: ${server_count} servers"

  # Check chfsd status on each node via chfsctl status
  echo "=== chfsctl status ==="
  local status_hostfile="${CHFS_LOG_DIR}/status_hostfile"
  uniq "${PBS_NODEFILE}" > "${status_hostfile}"
  chfsctl -ssh pbs_tmrsh -h "${status_hostfile}" status \
    > "${CHFS_LOG_DIR}/chfsctl_status.log" 2>&1 || true
  cat "${CHFS_LOG_DIR}/chfsctl_status.log"

  return 0
}

stop_chfs_servers() {
  echo ""
  echo "=========================================="
  echo "Stopping CHFS Servers"
  echo "=========================================="

  # Stop servers
  "${CHFSCTL}" \
    -ssh "${MPIRUN_SSH_WRAPPER}" \
    -h "${JOB_OUTPUT_DIR}/hostfile" \
    -M \
    stop > "${CHFS_LOG_DIR}/chfsctl_stop.log" 2>"${CHFS_LOG_DIR}/chfsctl_stop_stderr.log" || true

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

  # Build IOR command (without time_json wrapper - it's a bash function
  # and cannot be called via timeout which spawns a new process)
  cmd_ior=(
    "${cmd_mpirun_common[@]}"
    -np "$np"
    --bind-to none
    --oversubscribe
    -x CHFS_SERVER
    -x CHFS_CHUNK_SIZE="${effective_chunk_size}"
    "${IOR_WRAPPER}"
    -vvv
    -g
    -D 120
    -a CHFS
    --chfs.chunk_size="${effective_chunk_size}"
    -t "$transfer_size"
    -b "$block_size"
    $ior_flags
    -o "/testfile_${runid}"
    -O summaryFormat=JSON
    -O summaryFile="${ior_json}"
  )

  # Timeout: IOR uses -D 120 (120s per test phase).
  # A full run (write + read + overhead) should complete within 600s.
  # If it exceeds this, it's hung (e.g. MPI barrier deadlock from CHFS timeouts).
  local IOR_TIMEOUT=600

  echo "Running (timeout=${IOR_TIMEOUT}s): ${cmd_ior[*]}"
  local ior_exit_code=0
  local start_seconds=$SECONDS
  timeout --signal=TERM --kill-after=30 "${IOR_TIMEOUT}" \
    "${cmd_ior[@]}" > "${ior_stdout}" 2> "${ior_stderr}" || ior_exit_code=$?
  local elapsed_seconds=$((SECONDS - start_seconds))

  # Save elapsed time as simple JSON (replaces time_json)
  cat > "${JOB_OUTPUT_DIR}/time_${runid}.json" <<TIME_EOF
{"ElapsedSeconds": ${elapsed_seconds}, "ExitCode": ${ior_exit_code}}
TIME_EOF

  if [ "$ior_exit_code" -eq 124 ]; then
    echo "WARNING: IOR run $runid killed by timeout after ${IOR_TIMEOUT}s (likely hung)"
    echo "  Stderr log: ${ior_stderr}"
  elif [ "$ior_exit_code" -ne 0 ]; then
    echo "WARNING: IOR run $runid failed with exit code $ior_exit_code"
    echo "  Stderr log: ${ior_stderr}"
    echo "  Continuing with next parameter combination..."
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

runid=0
first_iter=1

# Restart CHFS servers at the ppn boundary to prevent state accumulation.
# chfsd's internal state (metadata DB, connections, buffers) degrades over many
# IOR runs, causing escalating HG_TIMEOUT errors until servers become unresponsive.
for ppn in "${ppn_list[@]}"; do

  echo ""
  echo "=========================================="
  echo "(Re)starting CHFS servers for ppn=${ppn}"
  echo "=========================================="
  # Skip stop on the very first iteration; no servers / hostfile yet.
  if [ "${first_iter}" -eq 0 ]; then
    stop_chfs_servers
  fi
  first_iter=0
  if ! start_chfs_servers "${CHFS_PROTOCOL}" "${CHFS_DB_SIZE}"; then
    echo "ERROR: Failed to start CHFS servers"
    exit 1
  fi

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
