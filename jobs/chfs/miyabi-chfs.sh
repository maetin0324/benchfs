#!/bin/bash
# CHFS Benchmark Submission Script (Miyabi)
#
# This script submits CHFS benchmark jobs on the Miyabi supercomputer.
# CHFS is a caching hierarchical file system for node-local storage.

set -euo pipefail
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE:-$0}" )" &> /dev/null && pwd )"
source "${SCRIPT_DIR}/../benchfs/common.sh"
TIMESTAMP="$(timestamp)"

# Default parameters
: ${ELAPSTIM_REQ:="0:30:00"}
: ${TASKSET:=0}         # Set to 1 to enable taskset CPU pinning for chfsd
: ${TASKSET_CORES:=0} # CPU cores to pin chfsd to when TASKSET=1
: ${CHUNK_SIZE_MATCH_XFER:=0}  # Set to 1 to ignore CHFS_CHUNK_SIZE and use transfer_size as chunk_size

JOB_FILE="$(remove_ext "$(this_file)")-job.sh"
PROJECT_ROOT="$(to_fullpath "${SCRIPT_DIR}/../..")"
BACKEND_DIR="$PROJECT_ROOT/backend/chfs"
CHFS_PREFIX="spack"  # CHFS is loaded via 'spack load mochi-margo' + $HOME/.local/bin
IOR_PREFIX="${PROJECT_ROOT}/ior_integration/ior"

# Resolve PARAM_FILE to absolute path BEFORE cd
PARAM_FILE="$(readlink -f "${PARAM_FILE:-${SCRIPT_DIR}/../params/debug.conf}")"

# Set LABEL: use provided value, or derive from param file name (without .conf extension)
if [ -z "${LABEL:-}" ]; then
  LABEL="$(basename "${PARAM_FILE}" .conf)"
fi

OUTPUT_DIR="$PROJECT_ROOT/results/chfs/${TIMESTAMP}-${LABEL}"

export JOB_FILE
export PROJECT_ROOT
export OUTPUT_DIR
export BACKEND_DIR
export CHFS_PREFIX
export IOR_PREFIX
export TIMESTAMP
export PARAM_FILE
export LABEL
export TASKSET
export TASKSET_CORES
export CHUNK_SIZE_MATCH_XFER

echo "=========================================="
echo "CHFS Job Submission (Miyabi)"
echo "=========================================="
echo "PROJECT_ROOT: $PROJECT_ROOT"
echo "CHFS_PREFIX: $CHFS_PREFIX"
echo "IOR_PREFIX: $IOR_PREFIX"
echo "PARAM_FILE: $PARAM_FILE"
echo ""
echo "Checking CHFS-enabled IOR:"
ls -la "${IOR_PREFIX}/src/ior" 2>/dev/null || echo "WARNING: CHFS-enabled IOR not found (needs to be built)"
echo "=========================================="

mkdir -p "${OUTPUT_DIR}"
cd "${OUTPUT_DIR}"
mkdir -p "${BACKEND_DIR}"

# Node count list for benchmarks
nnodes_list=(
  # 2
  # 2 4 8 16
  16
  # 32
)
niter=1

param_set_list=(
  "
  DUMMY=1
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
