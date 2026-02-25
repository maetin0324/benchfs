#!/bin/bash
# BenchFS perf profiling submission script for Sirius supercomputer
#
# Usage:
#   ./sirius-benchfs-perf.sh                      # Default: 2 nodes, 23 ppn, 16g block
#   PERF_PPN=4 ./sirius-benchfs-perf.sh           # Fewer client processes
#   PERF_BLOCK_SIZE=4g ./sirius-benchfs-perf.sh    # Smaller block size (faster)
#   PERF_IOR_FLAGS="-w" ./sirius-benchfs-perf.sh   # Write only
set -euo pipefail
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE:-$0}" )" &> /dev/null && pwd )"
source "${SCRIPT_DIR}/common.sh"
TIMESTAMP="$(timestamp)"

# Default params
: ${ELAPSTIM_REQ:="01:00:00"}
: ${PERF_PPN:=23}
: ${PERF_BLOCK_SIZE:=16g}
: ${PERF_IOR_FLAGS:="-w -r"}

JOB_FILE="${SCRIPT_DIR}/sirius-benchfs-perf-job.sh"
PROJECT_ROOT="$(to_fullpath "$(this_directory)/../..")"
BACKEND_DIR="$PROJECT_ROOT/backend/benchfs"
BENCHFS_PREFIX="${PROJECT_ROOT}/target/release"
IOR_PREFIX="${PROJECT_ROOT}/ior_integration/ior"
OUTPUT_DIR="$PROJECT_ROOT/results/benchfs/${TIMESTAMP}-sirius-perf"

# Export for qsub -V
export JOB_FILE
export PROJECT_ROOT
export OUTPUT_DIR
export BACKEND_DIR
export BENCHFS_PREFIX
export IOR_PREFIX
export TIMESTAMP
export SCRIPT_DIR
export PERF_PPN
export PERF_BLOCK_SIZE
export PERF_IOR_FLAGS

echo "=========================================="
echo "BenchFS Perf Profiling Job Submission (Sirius)"
echo "=========================================="
echo "PROJECT_ROOT: $PROJECT_ROOT"
echo "BENCHFS_PREFIX: $BENCHFS_PREFIX"
echo "IOR_PREFIX: $IOR_PREFIX"
echo "OUTPUT_DIR: $OUTPUT_DIR"
echo ""
echo "Perf parameters:"
echo "  Client PPN: $PERF_PPN"
echo "  Block size: $PERF_BLOCK_SIZE"
echo "  IOR flags: $PERF_IOR_FLAGS"
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

# Default: 2 nodes (matching the benchmark that showed ~40% HW performance)
nnodes_list=(
  2
)

for nnodes in "${nnodes_list[@]}"; do
  cmd_qsub=(
    qsub
    -q gold
    -A NBB
    -l select="${nnodes}"
    -l walltime="${ELAPSTIM_REQ}"
    -V
    "${JOB_FILE}"
  )
  echo "Submitting job:"
  echo "${cmd_qsub[@]}"
  "${cmd_qsub[@]}"
done
