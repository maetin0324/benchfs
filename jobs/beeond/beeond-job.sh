#!/bin/bash
#------- qsub option -----------
#PBS -A NBB
#PBS -l elapstim_req=12:00:00
#PBS -T openmpi
#PBS -v NQSV_MPI_VER=4.1.8/gcc11.4.0-cuda12.8.1
#PBS -v USE_BEEOND=1
#------- Program execution -----------
set -euo pipefail

# Increase file descriptor limit for large-scale MPI jobs
ulimit -n 1048576 2>/dev/null || ulimit -n 524288 2>/dev/null || ulimit -n 262144 2>/dev/null || ulimit -n 65536
echo "File descriptor limit: $(ulimit -n)"

module purge
module load "openmpi/$NQSV_MPI_VER"

# Requires
# - SCRIPT_DIR
# - OUTPUT_DIR
# - IOR_PREFIX

source "$SCRIPT_DIR/common.sh"

JOB_START=$(timestamp)
NNODES=$(wc --lines "${PBS_NODEFILE}" | awk '{print $1}')
JOBID=$(echo "$PBS_JOBID" | cut -d : -f 2)
JOB_OUTPUT_DIR="${OUTPUT_DIR}/${JOB_START}-${JOBID}-${NNODES}"
IOR_OUTPUT_DIR="${JOB_OUTPUT_DIR}/ior_results"

# BeeOND mount point (created automatically when USE_BEEOND=1)
BEEOND_MOUNT="/beeond"

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
echo "BeeOND IOR Benchmark Job Configuration"
echo "=========================================="
echo "JOBID: $JOBID"
echo "NNODES: $NNODES"
echo "IOR_PREFIX: ${IOR_PREFIX}"
echo "BEEOND_MOUNT: ${BEEOND_MOUNT}"
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

# Verify BeeOND is mounted
echo "Checking BeeOND mount..."
if [ -d "${BEEOND_MOUNT}" ]; then
  echo "BeeOND mount point exists: ${BEEOND_MOUNT}"
  df -h "${BEEOND_MOUNT}" || echo "WARNING: df failed on BeeOND"
  echo ""
  echo "BeeOND filesystem info:"
  mount | grep beeond || echo "WARNING: BeeOND not in mount table"
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

trap 'rm -rf "${BEEOND_TEST_DIR}" ; exit 1' 1 2 3 15
trap 'rm -rf "${BEEOND_TEST_DIR}" ; exit 0' EXIT

save_job_metadata() {
  local file_per_proc=0
  [[ "$ior_flags" == *"-F"* ]] && file_per_proc=1
  cat <<EOS >"${JOB_OUTPUT_DIR}"/job_metadata_${runid}.json
{
  "jobid": "$JOBID",
  "runid": ${runid},
  "nnodes": ${NNODES},
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

# Immediate mitigation environment variables for MPI
export OMPI_MCA_mpi_yield_when_idle=1
export OMPI_MCA_btl_base_warn_component_unused=0
export OMPI_MCA_mpi_show_handle_leaks=0

# MPI Configuration Fix for UCX Transport Layer Issues
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

# Load benchmark parameters from configuration file
PARAM_FILE="${PARAM_FILE:-${SCRIPT_DIR}/default_params.conf}"
if [ -f "$PARAM_FILE" ]; then
    echo "Loading parameters from: $PARAM_FILE"
    source "$PARAM_FILE"
else
    echo "WARNING: Parameter file not found: $PARAM_FILE"
    echo "Using built-in default parameters"
    # Fallback default values
    transfer_size_list=(1m 4m)
    block_size_list=(64m 256m 1g)
    ppn_list=(1 2 4 8)
    ior_flags_list=("-w -r -F")
fi

# Save parameter configuration for reproducibility
cat > "${JOB_OUTPUT_DIR}/parameters.json" <<EOF
{
  "parameter_file": "$PARAM_FILE",
  "nnodes": ${NNODES},
  "filesystem": "beeond",
  "transfer_sizes": [$(printf '"%s",' "${transfer_size_list[@]}" | sed 's/,$//; s/,$//')],
  "block_sizes": [$(printf '"%s",' "${block_size_list[@]}" | sed 's/,$//; s/,$//')],
  "client_ppn_values": [$(printf '%s,' "${ppn_list[@]}" | sed 's/,$//; s/,$//')],
  "ior_flags": [$(printf '"%s",' "${ior_flags_list[@]}" | sed 's/,$//; s/,$//')]
}
EOF

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

        # MPI Debug: Testing MPI communication before IOR
        echo "MPI Debug: Testing MPI communication before IOR"
        "${cmd_mpirun_common[@]}" -np "$np" --map-by "ppr:${ppn}:node" hostname > "${IOR_OUTPUT_DIR}/mpi_test_${runid}.txt" 2>&1
        echo "MPI Debug: Communication test completed"

        # Run IOR benchmark on BeeOND
        echo "Running IOR benchmark on BeeOND..."
        ior_json_file="${IOR_OUTPUT_DIR}/ior_result_${runid}.json"
        ior_stdout_file="${IOR_OUTPUT_DIR}/ior_stdout_${runid}.log"

        cmd_ior=(
          time_json -o "${JOB_OUTPUT_DIR}/time_${runid}.json"
          "${cmd_mpirun_common[@]}"
          -np "$np"
          --bind-to none
          --map-by "ppr:${ppn}:node"
          "${IOR_PREFIX}/src/ior"
          -vvv
          -a POSIX
          --posix.odirect
          -t "$transfer_size"
          -b "$block_size"
          $ior_flags
          -o "${BEEOND_TEST_DIR}/testfile"
          -O summaryFormat=JSON
          -O summaryFile="${ior_json_file}"
        )

        save_job_metadata

        echo "${cmd_ior[@]}"
        # NOTE: Use simple redirection instead of process substitution to avoid FD leak
        "${cmd_ior[@]}" \
          > "${ior_stdout_file}" \
          2> "${IOR_OUTPUT_DIR}/ior_stderr_${runid}.log" || true

        runid=$((runid + 1))
      done
    done
  done
done

echo "All BeeOND benchmarks completed"
echo "Total runs: $runid"
echo "Results directory: ${JOB_OUTPUT_DIR}"
