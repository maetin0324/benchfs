#!/bin/bash
set -euo pipefail
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE:-$0}" )" &> /dev/null && pwd )"
source "${SCRIPT_DIR}/common.sh"
TIMESTAMP="$(timestamp)"

# default params
: ${ELAPSTIM_REQ:="1:00:00"}
: ${LABEL:=perf-debug}

JOB_FILE="${SCRIPT_DIR}/perf-debug-job.sh"
PROJECT_ROOT="$(to_fullpath "$(this_directory)/../..")"
OUTPUT_DIR="$PROJECT_ROOT/results/benchfs/${TIMESTAMP}-${LABEL}"
BACKEND_DIR="$PROJECT_ROOT/backend/benchfs"
BENCHFS_PREFIX="${PROJECT_ROOT}/target/release"
IOR_PREFIX="${PROJECT_ROOT}/ior_integration/ior"

# Debug: Print paths
echo "=========================================="
echo "BenchFS Perf Debug Job Submission"
echo "=========================================="
echo "PROJECT_ROOT: $PROJECT_ROOT"
echo "BENCHFS_PREFIX: $BENCHFS_PREFIX"
echo "IOR_PREFIX: $IOR_PREFIX"
echo ""
echo "Checking binary:"
ls -la "${BENCHFS_PREFIX}/benchfsd_mpi" || echo "ERROR: Binary not found at ${BENCHFS_PREFIX}/benchfsd_mpi"
echo ""
echo "Checking IOR:"
ls -la "${IOR_PREFIX}/src/ior" || echo "ERROR: IOR not found at ${IOR_PREFIX}/src/ior"
echo ""
echo "Checking job file:"
ls -la "${JOB_FILE}" || echo "ERROR: Job file not found"
echo "=========================================="

mkdir -p "${OUTPUT_DIR}"
cd "${OUTPUT_DIR}"
mkdir -p "${BACKEND_DIR}"

# Use a small number of nodes for debugging
nnodes=4

param_set="NQSV_MPI_VER=4.1.8/gcc11.4.0-cuda12.8.1"
eval "$param_set"

cmd_qsub=(
  qsub
  -A NBBG
  -q gen_S
  -l elapstim_req="${ELAPSTIM_REQ}"
  -T openmpi
  -v NQSV_MPI_VER="${NQSV_MPI_VER}"
  -b "$nnodes"
  -v OUTPUT_DIR="$OUTPUT_DIR"
  -v SCRIPT_DIR="$SCRIPT_DIR"
  -v BACKEND_DIR="$BACKEND_DIR"
  -v LABEL="$LABEL"
  -v BENCHFS_PREFIX="$BENCHFS_PREFIX"
  -v IOR_PREFIX="$IOR_PREFIX"
  "${JOB_FILE}"
)

echo "Submitting perf debug job..."
echo "${cmd_qsub[@]}"
"${cmd_qsub[@]}"
