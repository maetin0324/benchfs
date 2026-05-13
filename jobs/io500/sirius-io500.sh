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

# Default workflow is single-run with the empirical best ior-easy config
# (transfer=4m block=4g ppn=4 fpp=TRUE chunk=4m). Set SKIP_SWEEP=0 to fall
# back to the original 24-config sweep + final-best workflow.
: ${SKIP_SWEEP:=1}
: ${ELAPSTIM_REQ:="00:30:00"}      # 30 minutes (single-run mode); raise to 01:30:00 for SKIP_SWEEP=0
: ${RUST_LOG_S:=warn}
: ${RUST_LOG_C:=warn}
: ${SWEEP_STONEWALL:=30}           # seconds per ior-easy phase during sweep (only used when SKIP_SWEEP=0)
: ${FINAL_STONEWALL:=60}           # seconds per phase for final ior-hard run

# Empirically-best ior-easy config (passed through to job script).
: ${BEST_TRANSFER:=4m}
: ${BEST_BLOCK:=4g}
: ${BEST_PPN:=4}
: ${BEST_FPP:=TRUE}
: ${BEST_CHUNK_BYTES:=4194304}

JOB_FILE="$(remove_ext "$(this_file)")-job.sh"
PROJECT_ROOT="$(to_fullpath "${SCRIPT_DIR}/../..")"
BACKEND_DIR="$PROJECT_ROOT/backend/io500"
BENCHFS_PREFIX="${PROJECT_ROOT}/target/release"
IO500_DIR="${PROJECT_ROOT}/ior_integration/io500"

if [ "${SKIP_SWEEP}" = "1" ]; then
  LABEL="${LABEL:-single_${BEST_TRANSFER}_b${BEST_BLOCK}_p${BEST_PPN}_fpp${BEST_FPP}}"
else
  LABEL="${LABEL:-ior_easy_sweep}"
fi

# Allow override of select=N for small reproductions on a single node
: ${SELECT_NODES:=40}
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
export SKIP_SWEEP
export SKIP_MDTEST_HARD
export SKIP_IOR
export FORCE_UCX_TCP
export IO500_FINAL_HARD
export IO500_FINAL_MDTEST
export IO500_ASAN_LIB
export IO500_ASAN_OPTIONS
export IO500_MALLOC_CHECK
export BEST_TRANSFER
export BEST_BLOCK
export BEST_PPN
export BEST_FPP
export BEST_CHUNK_BYTES

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
nnodes_list=("${SELECT_NODES}")
niter=1

for nnodes in "${nnodes_list[@]}"; do
  for ((iter=0; iter<niter; iter++)); do
    cmd_qsub=(
      qsub
      -q mcrp
      -A NBB
      -l select="${nnodes}:ncpus=24:mem=124gb:ngpus=1"
      -l place="${PBS_PLACE:-exclhost}"
      -l walltime="${ELAPSTIM_REQ}"
      -v SCRRAID=no
      -V
      "${JOB_FILE}"
    )
    echo "${cmd_qsub[@]}"
    "${cmd_qsub[@]}"
  done
done
