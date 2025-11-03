#!/bin/bash
#------- qsub option -----------
#PBS -A NBBG
#PBS -l elapstim_req=0:30:00
#PBS -T openmpi
#PBS -v NQSV_MPI_VER=4.1.8/gcc11.4.0-cuda12.8.1
#------- Program execution -----------
set -euo pipefail

# Increase file descriptor limit for large-scale MPI jobs
ulimit -n 65536

module purge
module load "openmpi/$NQSV_MPI_VER"

# Requires
# - SCRIPT_DIR
# - OUTPUT_DIR
# - BENCHFS_PREFIX
# - PING_ITERATIONS

source "$SCRIPT_DIR/common.sh"

JOB_START=$(timestamp)
NNODES=$(wc --lines "${PBS_NODEFILE}" | awk '{print $1}')
JOBID=$(echo "$PBS_JOBID" | cut -d : -f 2)
JOB_OUTPUT_DIR="${OUTPUT_DIR}/${JOB_START}-${JOBID}-${NNODES}"
BENCHFS_REGISTRY_DIR="${JOB_OUTPUT_DIR}/registry"
RESULTS_DIR="${JOB_OUTPUT_DIR}/results"

# Calculate project root from SCRIPT_DIR and set LD_LIBRARY_PATH dynamically
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
export LD_LIBRARY_PATH="${PROJECT_ROOT}/target/release:${LD_LIBRARY_PATH:-}"

# Default ping iterations if not set
: ${PING_ITERATIONS:=10000}

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
echo "Registry: ${BENCHFS_REGISTRY_DIR}"
echo "Results: ${RESULTS_DIR}"
echo "Ping Iterations: ${PING_ITERATIONS}"
echo "Nodes: ${NNODES}"
echo ""
echo "Checking binary:"
ls -la "${BENCHFS_PREFIX}/benchfs_rpc_bench" || echo "ERROR: Binary not found at ${BENCHFS_PREFIX}/benchfs_rpc_bench"
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

echo "prepare registry dir: ${BENCHFS_REGISTRY_DIR}"
mkdir -p "${BENCHFS_REGISTRY_DIR}"

echo "prepare results dir: ${RESULTS_DIR}"
mkdir -p "${RESULTS_DIR}"

# Cleanup on exit
trap 'rm -rf "${BENCHFS_REGISTRY_DIR}"; exit 1' 1 2 3 15
trap 'rm -rf "${BENCHFS_REGISTRY_DIR}"; exit 0' EXIT

save_job_metadata() {
  cat <<EOS >"${JOB_OUTPUT_DIR}"/job_metadata.json
{
  "jobid": "$JOBID",
  "nnodes": $NNODES,
  "ping_iterations": $PING_ITERATIONS,
  "start_time": "$JOB_START",
  "mpi_version": "$NQSV_MPI_VER",
  "binary": "${BENCHFS_PREFIX}/benchfs_rpc_bench"
}
EOS
}

run_rpc_benchmark() {
  local runid=$1
  local output_file="${RESULTS_DIR}/rpc_bench_${runid}.log"
  local json_file="${RESULTS_DIR}/rpc_bench_${runid}.json"

  echo "=========================================="
  echo "Running RPC Benchmark - Run ${runid}"
  echo "=========================================="
  echo "Output: ${output_file}"
  echo "JSON: ${json_file}"
  echo "Registry: ${BENCHFS_REGISTRY_DIR}"
  echo "Ping Iterations: ${PING_ITERATIONS}"
  echo ""

  # Clean registry before each run
  rm -rf "${BENCHFS_REGISTRY_DIR}"/*

  # Build mpirun command
  local mpirun_cmd=(
    mpirun
    "${nqsii_mpiopts_array[@]}"
    "${BENCHFS_PREFIX}/benchfs_rpc_bench"
    "${BENCHFS_REGISTRY_DIR}"
    --ping-iterations "${PING_ITERATIONS}"
    --output "${json_file}"
  )

  echo "Command: ${mpirun_cmd[*]}"
  echo ""

  # Run benchmark with time measurement
  if time_json "${mpirun_cmd[@]}" 2>&1 | tee "${output_file}"; then
    echo "RPC benchmark completed successfully"
  else
    echo "ERROR: RPC benchmark failed"
    return 1
  fi

  echo ""
}

# Save job metadata
save_job_metadata

# Run benchmark (can add multiple iterations if needed)
echo "=========================================="
echo "Starting RPC Benchmark Runs"
echo "=========================================="
echo ""

for runid in 1; do
  if ! run_rpc_benchmark "$runid"; then
    echo "ERROR: Run $runid failed, continuing..."
  fi

  # Optional: Add delay between runs
  # sleep 5
done

echo ""
echo "=========================================

"
echo "All RPC Benchmark Runs Complete"
echo "=========================================="
echo "Results saved to: ${RESULTS_DIR}"
echo ""

# Print summary
echo "=========================================="
echo "Results Summary"
echo "=========================================="
if [ -f "${RESULTS_DIR}/rpc_bench_1.json" ]; then
  echo "JSON results available:"
  ls -lh "${RESULTS_DIR}"/*.json
else
  echo "No JSON results found (--output may not be implemented yet)"
fi
echo ""
echo "Log files:"
ls -lh "${RESULTS_DIR}"/*.log
echo "=========================================="
