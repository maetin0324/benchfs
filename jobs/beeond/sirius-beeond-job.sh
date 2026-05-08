#!/bin/bash
# BeeOND IOR Benchmark Job Script (Sirius)
#
# BeeOND provides a shared parallel filesystem mounted at /beeond,
# aggregating local NVMe storage across all allocated nodes.
# IOR uses POSIX API with O_DIRECT to bypass page cache.
#
# Sirius topology (per physical node):
#   APU 0 (NUMA 0, cores  0-23) → /scr0
#   APU 1 (NUMA 1, cores 24-47) → /scr1
#   APU 2 (NUMA 2, cores 48-71) → /scr2
#   APU 3 (NUMA 3, cores 72-95) → /scr3

#------- qsub option -----------
#PBS -q mcrp
#PBS -A NBB
#PBS -l place=exclhost
#PBS -v USE_BEEOND=1
#------- Program execution -----------
# NOTE: DO NOT use "set -e" or "set -u" or "set -o pipefail" here!
# IOR benchmark may fail for various reasons (timeout, resource exhaustion, etc.)
# and we want to continue running other parameter combinations.
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

SCRIPT_DIR="${SCRIPT_DIR:-/work/NBB/rmaeda/workspace/rust/benchfs/jobs/beeond}"
PROJECT_ROOT="${PROJECT_ROOT:-/work/NBB/rmaeda/workspace/rust/benchfs}"
: ${OUTPUT_DIR:="$PROJECT_ROOT/results/beeond/$(date +%Y.%m.%d-%H.%M.%S)-sirius"}
IOR_PREFIX="${IOR_PREFIX:-${PROJECT_ROOT}/ior_integration/ior}"

source "${SCRIPT_DIR}/../benchfs/common.sh"

# ==============================================================================
# Job Environment Setup
# ==============================================================================

JOB_START=$(timestamp)
VNODES=$(wc -l < "${PBS_NODEFILE}")
JOBID=$(echo "$PBS_JOBID" | cut -d . -f 1)

# On Sirius with place=exclhost, PBS_NODEFILE contains 4 vnodes per physical node.
# Use unique node count (physical nodes) for directory naming and process calculation.
NNODES=$(sort -u "${PBS_NODEFILE}" | wc -l)

JOB_OUTPUT_DIR="${OUTPUT_DIR}/${JOB_START}-${JOBID}-${NNODES}n"
IOR_OUTPUT_DIR="${JOB_OUTPUT_DIR}/ior_results"

# BeeOND mount point (created automatically when USE_BEEOND=1)
BEEOND_MOUNT="/beeond"

echo "prepare the output directory: ${JOB_OUTPUT_DIR}"
mkdir -p "${JOB_OUTPUT_DIR}"

# Duplicate stdout/stderr to log files in JOB_OUTPUT_DIR.
# PBS stdout capture can fail when cgroup resources are exhausted,
# losing all job output. These log files are preserved regardless.
exec > >(tee "${JOB_OUTPUT_DIR}/job_stdout.log") 2> >(tee "${JOB_OUTPUT_DIR}/job_stderr.log" >&2)

# Save unique node list for reference
UNIQUE_HOSTFILE="${JOB_OUTPUT_DIR}/unique_nodes"
sort -u "${PBS_NODEFILE}" > "${UNIQUE_HOSTFILE}"
echo "Physical nodes: ${NNODES} (from ${VNODES} vnodes)"

cp "$0" "${JOB_OUTPUT_DIR}"
cp "${PBS_NODEFILE}" "${JOB_OUTPUT_DIR}"
cp "${SCRIPT_DIR}/../benchfs/common.sh" "${JOB_OUTPUT_DIR}"
printenv > "${JOB_OUTPUT_DIR}/env.txt"

# Debug: Print key variables
echo "=========================================="
echo "Job Configuration (Sirius - BeeOND)"
echo "=========================================="
echo "NNODES (physical): ${NNODES}"
echo "VNODES (PBS): ${VNODES}"
echo "PBS_JOBID: ${PBS_JOBID}"
echo "IOR_PREFIX: ${IOR_PREFIX}"
echo "BEEOND_MOUNT: ${BEEOND_MOUNT}"
echo ""
echo "PBS_NODEFILE contents:"
cat "${PBS_NODEFILE}"
echo ""
echo "Checking IOR:"
ls -la "${IOR_PREFIX}/src/ior" || echo "ERROR: IOR not found at ${IOR_PREFIX}/src/ior"
echo "=========================================="
echo ""

# ==============================================================================
# BeeOND Verification
# ==============================================================================

echo "Checking BeeOND mount..."
if [ -d "${BEEOND_MOUNT}" ]; then
  echo "BeeOND mount point exists: ${BEEOND_MOUNT}"
  df -h "${BEEOND_MOUNT}" || echo "WARNING: df failed on BeeOND"
  echo ""
  echo "BeeOND filesystem info:"
  mount | grep -i bee || echo "WARNING: BeeOND not in mount table"
else
  echo "ERROR: BeeOND mount point does not exist: ${BEEOND_MOUNT}"
  echo "Make sure USE_BEEOND=1 is set in PBS job options"
  exit 1
fi

# Create test directory on BeeOND
BEEOND_TEST_DIR="${BEEOND_MOUNT}/ior_${JOBID}"
mkdir -p "${BEEOND_TEST_DIR}"
echo "Test directory created: ${BEEOND_TEST_DIR}"

echo "prepare ior output dir: ${IOR_OUTPUT_DIR}"
mkdir -p "${IOR_OUTPUT_DIR}"

# Cleanup function for graceful shutdown
cleanup_and_exit() {
  local exit_code=${1:-1}
  local signal_name=${2:-"unknown"}
  echo ""
  echo "=========================================="
  echo "Job interrupted by signal: $signal_name"
  echo "Cleaning up BeeOND test directory..."
  echo "=========================================="
  rm -rf "${BEEOND_TEST_DIR}" 2>/dev/null || true
  exit "$exit_code"
}

# Trap signals for cleanup
trap 'cleanup_and_exit 1 "SIGHUP (walltime or session end)"' 1
trap 'cleanup_and_exit 1 "SIGINT (user interrupt)"' 2
trap 'cleanup_and_exit 1 "SIGQUIT"' 3
trap 'cleanup_and_exit 1 "SIGTERM (PBS walltime reached)"' 15
trap 'rm -rf "${BEEOND_TEST_DIR}" 2>/dev/null || true; exit 0' EXIT

# ==============================================================================
# Job Metadata
# ==============================================================================

save_job_metadata() {
  local file_per_proc=0
  [[ "$ior_flags" == *"-F"* ]] && file_per_proc=1
  cat <<EOS >"${JOB_OUTPUT_DIR}/job_metadata_${runid}.json"
{
  "jobid": "$JOBID",
  "runid": ${runid},
  "nnodes": ${NNODES},
  "system": "sirius",
  "client_ppn": ${ppn},
  "client_np": ${np},
  "transfer_size": "${transfer_size}",
  "block_size": "${block_size}",
  "file_per_proc": ${file_per_proc},
  "ior_flags": "${ior_flags}",
  "filesystem": "beeond",
  "mount_point": "${BEEOND_MOUNT}",
  "job_start_time": "${JOB_START}"
}
EOS
}

# ==============================================================================
# MPI Configuration for Sirius (OpenMPI 5.0.9)
# ==============================================================================

export OMPI_MCA_mpi_yield_when_idle=1
export OMPI_MCA_btl_base_warn_component_unused=0
export OMPI_MCA_mpi_show_handle_leaks=0

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

if [[ "${USE_UCX_PML}" -eq 1 ]]; then
  cmd_mpirun=(
    mpirun
    --mca routed direct
    --mca plm_rsh_no_tree_spawn 1
    --mca pml ucx
    --mca btl self
    --mca osc ucx
    -x PATH
    -x LD_LIBRARY_PATH
  )
else
  cmd_mpirun=(
    mpirun
    --mca routed direct
    --mca plm_rsh_no_tree_spawn 1
    --mca pml ob1
    --mca btl tcp,sm,self
    -x PATH
    -x LD_LIBRARY_PATH
  )
fi

# ==============================================================================
# Load Benchmark Parameters
# ==============================================================================

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
    ior_flags_list=("-w -r -F")
fi

# Save parameter configuration for reproducibility
cat > "${JOB_OUTPUT_DIR}/parameters.json" <<EOF
{
  "parameter_file": "$PARAM_FILE",
  "system": "sirius",
  "nnodes": ${NNODES},
  "filesystem": "beeond",
  "transfer_sizes": [$(printf '"%s",' "${transfer_size_list[@]}" | sed 's/,$//; s/,$//')],
  "block_sizes": [$(printf '"%s",' "${block_size_list[@]}" | sed 's/,$//; s/,$//')],
  "client_ppn_values": [$(printf '%s,' "${ppn_list[@]}" | sed 's/,$//; s/,$//')],
  "ior_flags": [$(printf '"%s",' "${ior_flags_list[@]}" | sed 's/,$//; s/,$//')]
}
EOF

# ==============================================================================
# Benchmark Loop
# ==============================================================================

runid=0
for ppn in "${ppn_list[@]}"; do
  np=$((NNODES * ppn))

  for transfer_size in "${transfer_size_list[@]}"; do
    for block_size in "${block_size_list[@]}"; do
      for ior_flags in "${ior_flags_list[@]}"; do
        echo "=========================================="
        echo "Run ID: $runid"
        echo "Nodes: $NNODES"
        echo "Client: PPN=$ppn, NP=$np"
        echo "Transfer size: $transfer_size, Block size: $block_size"
        echo "IOR flags: $ior_flags"
        echo "Filesystem: BeeOND (${BEEOND_MOUNT})"
        echo "=========================================="

        # Clean test directory between runs
        rm -rf "${BEEOND_TEST_DIR}"/*

        # Run IOR benchmark on BeeOND
        echo "Running IOR benchmark on BeeOND..."
        ior_json_file="${IOR_OUTPUT_DIR}/ior_result_${runid}.json"
        ior_stdout_file="${IOR_OUTPUT_DIR}/ior_stdout_${runid}.log"

        cmd_ior=(
          "${cmd_mpirun[@]}"
          -np "$np"
          --bind-to none
          --oversubscribe
          "${IOR_PREFIX}/src/ior"
          -vvv
          -D 120
          -a POSIX
          --posix.odirect
          -t "$transfer_size"
          -b "$block_size"
          -e
          $ior_flags
          -o "${BEEOND_TEST_DIR}/testfile"
          -O summaryFormat=JSON
          -O summaryFile="${ior_json_file}"
        )

        save_job_metadata

        # Timeout: IOR uses -D 120 (120s per test phase).
        # A full run (write + read + overhead) should complete within 600s.
        IOR_TIMEOUT=600

        echo "Running (timeout=${IOR_TIMEOUT}s): ${cmd_ior[*]}"
        ior_exit_code=0
        start_seconds=$SECONDS
        timeout --signal=TERM --kill-after=30 "${IOR_TIMEOUT}" \
          "${cmd_ior[@]}" \
          > "${ior_stdout_file}" \
          2> "${IOR_OUTPUT_DIR}/ior_stderr_${runid}.log" || ior_exit_code=$?
        elapsed_seconds=$((SECONDS - start_seconds))

        cat > "${JOB_OUTPUT_DIR}/time_${runid}.json" <<TIME_EOF
{"ElapsedSeconds": ${elapsed_seconds}, "ExitCode": ${ior_exit_code}}
TIME_EOF

        if [ "$ior_exit_code" -eq 124 ]; then
          echo "WARNING: IOR run $runid killed by timeout after ${IOR_TIMEOUT}s (likely hung)"
          echo "  Stderr log: ${IOR_OUTPUT_DIR}/ior_stderr_${runid}.log"
        elif [ "$ior_exit_code" -ne 0 ]; then
          echo "WARNING: IOR run $runid failed with exit code $ior_exit_code"
          echo "  Stderr log: ${IOR_OUTPUT_DIR}/ior_stderr_${runid}.log"
          echo "  Continuing with next parameter combination..."
        fi

        # Clean up test files immediately after each run to prevent BeeOND storage full
        echo "Cleaning up test files from run $runid..."
        rm -rf "${BEEOND_TEST_DIR}"/*

        runid=$((runid + 1))
      done
    done
  done
done

echo ""
echo "=========================================="
echo "All BeeOND benchmarks completed"
echo "=========================================="
echo "Total runs: $runid"
echo "Results directory: ${JOB_OUTPUT_DIR}"
echo "=========================================="
