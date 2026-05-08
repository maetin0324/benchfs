#!/bin/bash
# IO500 (BenchFS) submission script for Sirius
#
# Workflow inside the job:
#   1. Start benchfsd_mpi servers on 10 physical nodes (40 vnodes).
#   2. Sweep ior-easy parameters (transferSize, blockSize, ppn, filePerProc,
#      benchfs chunk_size); pick the configuration with the best ior-easy
#      bandwidth (write+read average).
#   3. Run io500 once more at the best configuration with ior-hard enabled.
#
# Each parameter combination uses a short stonewall window for the sweep so
# that the search completes in one walltime. The final best+ior-hard run uses
# a longer stonewall for stable numbers.

set -euo pipefail
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE:-$0}" )" &> /dev/null && pwd )"
source "${SCRIPT_DIR}/../benchfs/common.sh"
TIMESTAMP="$(timestamp)"

: ${ELAPSTIM_REQ:="01:30:00"}      # 90 minutes
: ${RUST_LOG_S:=warn}
: ${RUST_LOG_C:=warn}
: ${SWEEP_STONEWALL:=30}           # seconds per ior-easy phase during sweep
: ${FINAL_STONEWALL:=60}           # seconds per phase for final ior-hard run

JOB_FILE="$(remove_ext "$(this_file)")-job.sh"
PROJECT_ROOT="$(to_fullpath "${SCRIPT_DIR}/../..")"
BACKEND_DIR="$PROJECT_ROOT/backend/io500"
BENCHFS_PREFIX="${PROJECT_ROOT}/target/release"
IO500_DIR="${PROJECT_ROOT}/ior_integration/io500"

LABEL="${LABEL:-ior_easy_sweep}"
OUTPUT_DIR="$PROJECT_ROOT/results/io500/${TIMESTAMP}-sirius-${LABEL}"

export JOB_FILE
export PROJECT_ROOT
export OUTPUT_DIR
export BACKEND_DIR
export BENCHFS_PREFIX
export IO500_DIR
export TIMESTAMP
export LABEL
export RUST_LOG_S
export RUST_LOG_C
export SWEEP_STONEWALL
export FINAL_STONEWALL

echo "=========================================="
echo "IO500 Job Submission (Sirius)"
echo "=========================================="
echo "PROJECT_ROOT: $PROJECT_ROOT"
echo "BENCHFS_PREFIX: $BENCHFS_PREFIX"
echo "IO500_DIR: $IO500_DIR"
echo "LABEL: $LABEL"
echo "SWEEP_STONEWALL: ${SWEEP_STONEWALL}s, FINAL_STONEWALL: ${FINAL_STONEWALL}s"
echo ""
echo "Checking binaries:"
ls -la "${BENCHFS_PREFIX}/benchfsd_mpi" || echo "ERROR: benchfsd_mpi not found"
ls -la "${IO500_DIR}/io500" || echo "ERROR: io500 binary not found"
echo "=========================================="

mkdir -p "${OUTPUT_DIR}"
cd "${OUTPUT_DIR}"
mkdir -p "${BACKEND_DIR}"

# 10 physical nodes × 4 chunks/node = 40 PBS chunks (vnodes).
# qsub `select=N` is in chunks; exclhost requires N to be a multiple of 4.
nnodes_list=(40)
niter=1

for nnodes in "${nnodes_list[@]}"; do
  for ((iter=0; iter<niter; iter++)); do
    cmd_qsub=(
      qsub
      -q mcrp
      -A NBB
      -l select="${nnodes}"
      -l place=exclhost
      -l walltime="${ELAPSTIM_REQ}"
      -v SCRRAID=no
      -V
      "${JOB_FILE}"
    )
    echo "${cmd_qsub[@]}"
    "${cmd_qsub[@]}"
  done
done
