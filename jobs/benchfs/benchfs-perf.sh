#!/bin/bash
set -euo pipefail
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE:-$0}" )" &> /dev/null && pwd )"
source "${SCRIPT_DIR}/common.sh"
TIMESTAMP="$(timestamp)"

# Perf profiling job submission script
: ${LABEL:=perf_debug}

JOB_FILE="${SCRIPT_DIR}/benchfs-perf-job.sh"
PROJECT_ROOT="$(to_fullpath "$(this_directory)/../..")"
OUTPUT_DIR="$PROJECT_ROOT/results/benchfs/${TIMESTAMP}-${LABEL}"
BACKEND_DIR="$PROJECT_ROOT/backend/benchfs"
# Use release build with debug symbols for perf profiling
BENCHFS_PREFIX="${PROJECT_ROOT}/target/release"
IOR_PREFIX="${PROJECT_ROOT}/ior_integration/ior"

echo "=========================================="
echo "BenchFS Perf Profiling Job Submission"
echo "=========================================="
echo "PROJECT_ROOT: $PROJECT_ROOT"
echo "BENCHFS_PREFIX: $BENCHFS_PREFIX"
echo "IOR_PREFIX: $IOR_PREFIX"
echo "OUTPUT_DIR: $OUTPUT_DIR"
echo ""
echo "Checking binary:"
ls -la "${BENCHFS_PREFIX}/benchfsd_mpi" || echo "ERROR: Binary not found"
echo ""
echo "Checking IOR:"
ls -la "${IOR_PREFIX}/src/ior" || echo "ERROR: IOR not found"
echo "=========================================="

mkdir -p "${OUTPUT_DIR}"
cd "${OUTPUT_DIR}"
mkdir -p "${BACKEND_DIR}"

nnodes=4

cmd_qsub=(
  qsub
  -A NBBG
  -q gen_S
  -l elapstim_req="01:00:00"
  -T openmpi
  -v NQSV_MPI_VER="4.1.8/gcc11.4.0-cuda12.8.1"
  -b "$nnodes"
  -v OUTPUT_DIR="$OUTPUT_DIR"
  -v SCRIPT_DIR="$SCRIPT_DIR"
  -v BACKEND_DIR="$BACKEND_DIR"
  -v LABEL="$LABEL"
  -v BENCHFS_PREFIX="$BENCHFS_PREFIX"
  -v IOR_PREFIX="$IOR_PREFIX"
  "${JOB_FILE}"
)

echo "Submitting job:"
echo "${cmd_qsub[@]}"
"${cmd_qsub[@]}"
