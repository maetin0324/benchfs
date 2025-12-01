#!/bin/bash
set -euo pipefail
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE:-$0}" )" &> /dev/null && pwd )"
source "${SCRIPT_DIR}/common.sh"
TIMESTAMP="$(timestamp)"

# default params
: ${ELAPSTIM_REQ:="0:05:00"}
: ${LABEL:=default}

JOB_FILE="$(remove_ext "$(this_file)")-job.sh"
PROJECT_ROOT="$(to_fullpath "$(this_directory)/../..")"
OUTPUT_DIR="$PROJECT_ROOT/results/benchfs/${TIMESTAMP}-${LABEL}"
BACKEND_DIR="$PROJECT_ROOT/backend/benchfs"
BENCHFS_PREFIX="${PROJECT_ROOT}/target/release"
IOR_PREFIX="${PROJECT_ROOT}/ior_integration/ior"

# Resolve PARAM_FILE to absolute path BEFORE cd (relative paths won't work after cd)
PARAM_FILE="$(readlink -f "${PARAM_FILE:-${SCRIPT_DIR}/default_params.conf}")"

export JOB_FILE
export PROJECT_ROOT
export OUTPUT_DIR
export BACKEND_DIR
export BENCHFS_PREFIX
export IOR_PREFIX
export TIMESTAMP
export PARAM_FILE

# Debug: Print paths
echo "=========================================="
echo "BenchFS Job Submission"
echo "=========================================="
echo "PROJECT_ROOT: $PROJECT_ROOT"
echo "BENCHFS_PREFIX: $BENCHFS_PREFIX"
echo "IOR_PREFIX: $IOR_PREFIX"
echo "PARAM_FILE: $PARAM_FILE"
echo ""
echo "Checking binary:"
ls -la "${BENCHFS_PREFIX}/benchfsd_mpi" || echo "ERROR: Binary not found at ${BENCHFS_PREFIX}/benchfsd_mpi"
echo ""
echo "Checking IOR:"
ls -la "${IOR_PREFIX}/src/ior" || echo "ERROR: IOR not found at ${IOR_PREFIX}/src/ior"
echo "=========================================="

mkdir -p "${OUTPUT_DIR}"
cd "${OUTPUT_DIR}"
mkdir -p "${BACKEND_DIR}"

nnodes_list=(
  # 1 2 4 8
  # 4
  2 4 8 16
  # 2 4 8 16 32
  # 32
  # 64
  # 128
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
        -W group_list="xg24i002"
        -q regular-g
        -l select="$nnodes"
        -l walltime="${ELAPSTIM_REQ}"
        -V
        "${JOB_FILE}"
      )
      echo "${cmd_qsub[@]}"
      "${cmd_qsub[@]}"
    done
  done
done
