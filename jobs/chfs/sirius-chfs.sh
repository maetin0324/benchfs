#!/bin/bash
# CHFS Benchmark Submission Script (Sirius)
#
# Sirius node model (AMD Instinct MI300A):
#   1 physical node = 4 chunks (96 cores total: 4 NUMA nodes x 24 cores, ~501GiB RAM)
#   Each chunk: ncpus=24, mem=124gb, ngpus=1
#   Scratch: /scrN/${PBS_JOBID} (NVMe x4, each 2.9TB, N=0..3 per APU)
#   InfiniBand: 400Gbps (mlx5)
#
# Usage:
#   ./sirius-chfs.sh
#   PARAM_FILE=../params/debug.conf ./sirius-chfs.sh
#   ELAPSTIM_REQ=00:30:00 PARAM_FILE=../params/standard.conf ./sirius-chfs.sh
set -euo pipefail
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE:-$0}" )" &> /dev/null && pwd )"
source "${SCRIPT_DIR}/../benchfs/common.sh"
TIMESTAMP="$(timestamp)"

# Default parameters
: ${ELAPSTIM_REQ:="00:10:00"}
: ${TASKSET:=0}                  # Set to 1 to enable taskset CPU pinning for chfsd
: ${TASKSET_CORES:=0}            # CPU cores to pin chfsd to when TASKSET=1
: ${CHUNK_SIZE_MATCH_XFER:=0}    # Set to 1 to ignore CHFS_CHUNK_SIZE and use transfer_size

JOB_FILE="$(remove_ext "$(this_file)")-job.sh"
PROJECT_ROOT="$(to_fullpath "${SCRIPT_DIR}/../..")"
BACKEND_DIR="$PROJECT_ROOT/backend/chfs"
CHFS_PREFIX="spack"  # CHFS is loaded via spack + $HOME/.local/bin
IOR_PREFIX="${PROJECT_ROOT}/ior_integration/ior"

# Resolve PARAM_FILE to absolute path BEFORE cd
PARAM_FILE_RESOLVED="$(readlink -f "${PARAM_FILE:-${SCRIPT_DIR}/../params/debug.conf}")"

# Set LABEL: use provided value, or derive from param file name (without .conf extension)
if [ -z "${LABEL:-}" ]; then
  LABEL="$(basename "${PARAM_FILE_RESOLVED}" .conf)"
fi

OUTPUT_DIR="$PROJECT_ROOT/results/chfs/${TIMESTAMP}-sirius-${LABEL}"

# Export all variables so they are passed via qsub -V
export JOB_FILE
export PROJECT_ROOT
export OUTPUT_DIR
export BACKEND_DIR
export CHFS_PREFIX
export IOR_PREFIX
export TIMESTAMP
export PARAM_FILE="$PARAM_FILE_RESOLVED"
export LABEL
export TASKSET
export TASKSET_CORES
export CHUNK_SIZE_MATCH_XFER

echo "=========================================="
echo "CHFS Job Submission (Sirius)"
echo "=========================================="
echo "PROJECT_ROOT: $PROJECT_ROOT"
echo "CHFS_PREFIX: $CHFS_PREFIX"
echo "IOR_PREFIX: $IOR_PREFIX"
echo "PARAM_FILE: $PARAM_FILE_RESOLVED"
echo ""
echo "Checking CHFS-enabled IOR:"
ls -la "${IOR_PREFIX}/src/ior" 2>/dev/null || echo "WARNING: CHFS-enabled IOR not found (needs to be built)"
echo "=========================================="

mkdir -p "${OUTPUT_DIR}"
cd "${OUTPUT_DIR}"
mkdir -p "${BACKEND_DIR}"

# Node count list for benchmarks
nnodes_list=(
  # 2 4 8 16 32
  16
  # 2
)
niter=1

for nnodes in "${nnodes_list[@]}"; do
  for ((iter=0; iter<niter; iter++)); do
    cmd_qsub=(
      qsub
      -q mcrp
      -A NBB
      -l select="${nnodes}"
      -l walltime="${ELAPSTIM_REQ}"
      -V
      "${JOB_FILE}"
    )
    echo "${cmd_qsub[@]}"
    "${cmd_qsub[@]}"
  done
done
