#!/bin/bash
# CHFS Benchmark Submission Script
#
# This script submits CHFS benchmark jobs using IOR.
# CHFS is a caching hierarchical file system for node-local storage.

set -euo pipefail
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE:-$0}" )" &> /dev/null && pwd )"
source "${SCRIPT_DIR}/../benchfs/common.sh"
TIMESTAMP="$(timestamp)"

# Default parameters
: ${ELAPSTIM_REQ:="0:30:00"}
: ${LABEL:=default}

JOB_FILE="${SCRIPT_DIR}/chfs-job.sh"
PROJECT_ROOT="$(to_fullpath "${SCRIPT_DIR}/../..")"
OUTPUT_DIR="$PROJECT_ROOT/results/chfs/${TIMESTAMP}-${LABEL}"
BACKEND_DIR="$PROJECT_ROOT/backend/chfs"
CHFS_PREFIX="/work/NBB/rmaeda/.local"  # CHFS installation directory
IOR_PREFIX="${PROJECT_ROOT}/ior_integration/ior"  # IOR with CHFS support

# Resolve PARAM_FILE to absolute path BEFORE cd
PARAM_FILE_RESOLVED="$(readlink -f "${PARAM_FILE:-${SCRIPT_DIR}/../params/standard.conf}")"

echo "=========================================="
echo "CHFS Job Submission"
echo "=========================================="
echo "PROJECT_ROOT: $PROJECT_ROOT"
echo "CHFS_PREFIX: $CHFS_PREFIX"
echo "IOR_PREFIX: $IOR_PREFIX"
echo "PARAM_FILE: $PARAM_FILE_RESOLVED"
echo ""
echo "Checking CHFS binaries:"
ls -la "${CHFS_PREFIX}/bin/chfsctl" 2>/dev/null || echo "ERROR: chfsctl not found"
ls -la "${CHFS_PREFIX}/bin/chfsd" 2>/dev/null || echo "ERROR: chfsd not found"
echo ""
echo "Checking CHFS-enabled IOR:"
ls -la "${IOR_PREFIX}/src/ior" 2>/dev/null || echo "WARNING: CHFS-enabled IOR not found (needs to be built)"
echo "=========================================="

mkdir -p "${OUTPUT_DIR}"
cd "${OUTPUT_DIR}"
mkdir -p "${BACKEND_DIR}"

# Node count list for benchmarks
nnodes_list=(
  # 1 2 4 8
  16
  # 32
)
niter=1

param_set_list=(
  "
  NQSV_MPI_VER=4.1.8/gcc11.4.0-cuda12.8.1
  "
)

# Calculate total nodes and select appropriate queue
select_queue_for_nodes() {
  local total_nodes=0
  for n in "${nnodes_list[@]}"; do
    total_nodes=$((total_nodes + n))
  done

  local queue
  if [ "$total_nodes" -le 31 ]; then
    queue="gen_S"
  elif [ "$total_nodes" -le 63 ]; then
    queue="gen_M"
  else
    queue="gen_L"
  fi

  echo "Total nodes in nnodes_list: $total_nodes -> Queue: $queue"
  echo "$queue"
}

# Get the appropriate queue based on total nodes
SELECTED_QUEUE=$(select_queue_for_nodes | tail -1)
echo "=========================================="
echo "Queue Selection"
echo "=========================================="
select_queue_for_nodes | head -1
echo "Selected queue: $SELECTED_QUEUE"
echo "=========================================="

for nnodes in "${nnodes_list[@]}"; do
  for ((iter=0; iter<niter; iter++)); do
    for param_set in "${param_set_list[@]}"; do
      eval "$param_set"

      cmd_qsub=(
        qsub
        -A NBBG
        -q "$SELECTED_QUEUE"
        -l elapstim_req="${ELAPSTIM_REQ}"
        -T openmpi
        -v NQSV_MPI_VER="${NQSV_MPI_VER}"
        -b "$nnodes"
        -v OUTPUT_DIR="$OUTPUT_DIR"
        -v SCRIPT_DIR="$SCRIPT_DIR"
        -v BACKEND_DIR="$BACKEND_DIR"
        -v LABEL="$LABEL"
        -v CHFS_PREFIX="$CHFS_PREFIX"
        -v IOR_PREFIX="$IOR_PREFIX"
        -v PARAM_FILE="$PARAM_FILE_RESOLVED"
        "${JOB_FILE}"
      )
      echo "${cmd_qsub[@]}"
      "${cmd_qsub[@]}"
    done
  done
done
