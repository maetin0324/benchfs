#!/bin/bash
# BeeOND IOR Benchmark Submission Script (Sirius)
#
# Sirius node model (AMD Instinct MI300A):
#   1 physical node = 4 chunks (96 cores total: 4 NUMA nodes x 24 cores, ~501GiB RAM)
#   Each chunk: ncpus=24, mem=124gb, ngpus=1
#   Scratch: /scrN/${PBS_JOBID} (NVMe x4, each 2.9TB, N=0..3 per APU)
#   InfiniBand: 400Gbps (mlx5)
#
# BeeOND is automatically mounted at /beeond when USE_BEEOND=1 is passed to PBS.
# It aggregates all allocated nodes' local storage into a single parallel filesystem.
#
# Usage:
#   ./sirius-beeond.sh
#   PARAM_FILE=../params/debug.conf ./sirius-beeond.sh
#   ELAPSTIM_REQ=00:30:00 PARAM_FILE=../params/large_scale.conf ./sirius-beeond.sh
set -euo pipefail
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE:-$0}" )" &> /dev/null && pwd )"
source "${SCRIPT_DIR}/../benchfs/common.sh"
TIMESTAMP="$(timestamp)"

# Default parameters
: ${ELAPSTIM_REQ:="00:10:00"}

JOB_FILE="$(remove_ext "$(this_file)")-job.sh"
PROJECT_ROOT="$(to_fullpath "${SCRIPT_DIR}/../..")"
IOR_PREFIX="${PROJECT_ROOT}/ior_integration/ior"

# Resolve PARAM_FILE to absolute path BEFORE cd
PARAM_FILE_RESOLVED="$(readlink -f "${PARAM_FILE:-${SCRIPT_DIR}/../params/debug.conf}")"

# Set LABEL: use provided value, or derive from param file name (without .conf extension)
if [ -z "${LABEL:-}" ]; then
  LABEL="$(basename "${PARAM_FILE_RESOLVED}" .conf)"
fi

OUTPUT_DIR="$PROJECT_ROOT/results/beeond/${TIMESTAMP}-sirius-${LABEL}"

# Variables to pass to the job (via single -v with comma-separated list).
# PBS Pro requires all variables in ONE -v flag; multiple -v flags only keeps the last one.
PARAM_FILE="$PARAM_FILE_RESOLVED"

echo "=========================================="
echo "BeeOND Job Submission (Sirius)"
echo "=========================================="
echo "PROJECT_ROOT: $PROJECT_ROOT"
echo "IOR_PREFIX: $IOR_PREFIX"
echo "PARAM_FILE: $PARAM_FILE_RESOLVED"
echo ""
echo "Checking IOR:"
ls -la "${IOR_PREFIX}/src/ior" 2>/dev/null || echo "ERROR: IOR not found at ${IOR_PREFIX}/src/ior"
echo "=========================================="

mkdir -p "${OUTPUT_DIR}"
cd "${OUTPUT_DIR}"

# Node count list for benchmarks
nnodes_list=(
  # 2 4 8 16 32
  16
  # 2
)
niter=1

for nnodes in "${nnodes_list[@]}"; do
  for ((iter=0; iter<niter; iter++)); do
    # PBS Pro: all -v variables must be in a SINGLE comma-separated -v flag.
    # Multiple -v flags only keeps the last one.
    # Do NOT use -V (export all env vars) - it interferes with BeeOND hook.
    QSUB_VARS="USE_BEEOND=1"
    QSUB_VARS+=",SCRIPT_DIR=${SCRIPT_DIR}"
    QSUB_VARS+=",PROJECT_ROOT=${PROJECT_ROOT}"
    QSUB_VARS+=",OUTPUT_DIR=${OUTPUT_DIR}"
    QSUB_VARS+=",IOR_PREFIX=${IOR_PREFIX}"
    QSUB_VARS+=",PARAM_FILE=${PARAM_FILE}"
    QSUB_VARS+=",LABEL=${LABEL}"

    cmd_qsub=(
      qsub
      -q mcrp
      -A NBB
      -l select="${nnodes}"
      -l place=exclhost
      -l walltime="${ELAPSTIM_REQ}"
      -v "${QSUB_VARS}"
      "${JOB_FILE}"
    )
    echo "${cmd_qsub[@]}"
    "${cmd_qsub[@]}"
  done
done
