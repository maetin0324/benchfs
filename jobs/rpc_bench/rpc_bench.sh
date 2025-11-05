#!/bin/bash
set -euo pipefail
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE:-$0}" )" &> /dev/null && pwd )"
source "${SCRIPT_DIR}/common.sh"
TIMESTAMP="$(timestamp)"

# default params
: ${ELAPSTIM_REQ:="2:00:00"}
: ${LABEL:=default}
: ${PING_ITERATIONS:=1000000}

JOB_FILE="$(remove_ext "$(this_file)")-job.sh"
PROJECT_ROOT="$(to_fullpath "$(this_directory)/../..")"
OUTPUT_DIR="$PROJECT_ROOT/results/rpc_bench/${TIMESTAMP}-${LABEL}"
BENCHFS_PREFIX="${PROJECT_ROOT}/target/release"

# Debug: Print paths
echo "=========================================="
echo "BenchFS RPC Benchmark Job Submission"
echo "=========================================="
echo "PROJECT_ROOT: $PROJECT_ROOT"
echo "BENCHFS_PREFIX: $BENCHFS_PREFIX"
echo "PING_ITERATIONS: $PING_ITERATIONS"
echo ""
echo "Checking binary:"
ls -la "${BENCHFS_PREFIX}/benchfs_rpc_bench" || echo "ERROR: Binary not found at ${BENCHFS_PREFIX}/benchfs_rpc_bench"
echo "=========================================="

mkdir -p "${OUTPUT_DIR}"
cd "${OUTPUT_DIR}"

nnodes_list=(
  # 4
  # 4 8 16 32
  32 64
)
niter=1

param_set_list=(
  "
  NQSV_MPI_VER=4.1.8/gcc11.4.0-cuda12.8.1
  "
)

for nnodes in "${nnodes_list[@]}"; do
  for ((iter=0; iter<niter; iter++)); do
    for param_set in "${param_set_list[@]}"; do
      eval "$param_set"

      cmd_qsub=(
        qsub
        -A NBBG
        # -q gen_S
        -q gen_M
        -l elapstim_req="${ELAPSTIM_REQ}"
        -T openmpi
        -v NQSV_MPI_VER="${NQSV_MPI_VER}"
        -b "$nnodes"
        -v OUTPUT_DIR="$OUTPUT_DIR"
        -v SCRIPT_DIR="$SCRIPT_DIR"
        -v LABEL="$LABEL"
        -v BENCHFS_PREFIX="$BENCHFS_PREFIX"
        -v PING_ITERATIONS="$PING_ITERATIONS"
        "${JOB_FILE}"
      )
      echo "${cmd_qsub[@]}"
      "${cmd_qsub[@]}"
    done
  done
done
