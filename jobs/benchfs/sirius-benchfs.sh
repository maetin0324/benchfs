#!/bin/bash
# BenchFS IOR benchmark submission script for Sirius supercomputer
#
# Sirius node model (AMD Instinct MI300A):
#   1 physical node = 4 chunks (96 cores total: 4 NUMA nodes x 24 cores, ~501GiB RAM)
#   Each chunk: ncpus=24, mem=124gb, ngpus=1
#   NOTE: select=1 (single chunk) is required for /scr/${PBS_JOBID} scratch provisioning.
#         Multi-process launches use --oversubscribe to exceed the 24-slot limit.
#   Scratch: /scr/${PBS_JOBID} -> /scrN/${PBS_JOBID} (NVMe x4, each 2.9TB)
#   InfiniBand: 400Gbps (mlx5)
#
# Usage:
#   ./sirius-benchfs.sh
#   PARAM_FILE=../params/debug.conf ./sirius-benchfs.sh
#   ELAPSTIM_REQ=00:30:00 PARAM_FILE=../params/standard.conf ./sirius-benchfs.sh
set -euo pipefail
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE:-$0}" )" &> /dev/null && pwd )"
source "${SCRIPT_DIR}/common.sh"
TIMESTAMP="$(timestamp)"

# default params
: ${ELAPSTIM_REQ:="00:10:00"}
: ${ENABLE_PERFETTO:=0}
: ${ENABLE_CHROME:=0}
: ${RUST_LOG_S:=info}
: ${RUST_LOG_C:=warn}
: ${TASKSET:=0}
: ${TASKSET_CORES:=0,1}
: ${ENABLE_NODE_DIAGNOSTICS:=0}
: ${ENABLE_STATS:=0}
: ${POSIX:=0}            # Set to 1 to use POSIX synchronous I/O instead of io_uring
: ${SEPARATE:=0}         # Set to 1 to use separate nodes for server and client
: ${SERVER_NODES:=}      # Number of server nodes (default: floor(NNODES/2))
: ${CLIENT_NODES:=}      # Number of client nodes (default: NNODES - SERVER_NODES)

JOB_FILE="$(remove_ext "$(this_file)")-job.sh"
PROJECT_ROOT="$(to_fullpath "$(this_directory)/../..")"
BACKEND_DIR="$PROJECT_ROOT/backend/benchfs"
BENCHFS_PREFIX="${PROJECT_ROOT}/target/release"
IOR_PREFIX="${PROJECT_ROOT}/ior_integration/ior"

# Resolve PARAM_FILE to absolute path BEFORE cd (relative paths won't work after cd)
PARAM_FILE_RESOLVED="$(readlink -f "${PARAM_FILE:-${SCRIPT_DIR}/../params/debug.conf}")"

# Set LABEL: use provided value, or derive from param file name (without .conf extension)
if [ -z "${LABEL:-}" ]; then
  LABEL="$(basename "${PARAM_FILE_RESOLVED}" .conf)"
fi

OUTPUT_DIR="$PROJECT_ROOT/results/benchfs/${TIMESTAMP}-sirius-${LABEL}"

# Export all variables so they are passed via qsub -V
export JOB_FILE
export PROJECT_ROOT
export OUTPUT_DIR
export BACKEND_DIR
export BENCHFS_PREFIX
export IOR_PREFIX
export TIMESTAMP
export PARAM_FILE="$PARAM_FILE_RESOLVED"
export LABEL
export ENABLE_PERFETTO
export ENABLE_CHROME
export RUST_LOG_S
export RUST_LOG_C
export TASKSET
export TASKSET_CORES
export ENABLE_NODE_DIAGNOSTICS
export ENABLE_STATS
export POSIX
export SEPARATE
export SERVER_NODES
export CLIENT_NODES

# Debug: Print paths
echo "=========================================="
echo "BenchFS Job Submission (Sirius)"
echo "=========================================="
echo "PROJECT_ROOT: $PROJECT_ROOT"
echo "BENCHFS_PREFIX: $BENCHFS_PREFIX"
echo "IOR_PREFIX: $IOR_PREFIX"
echo "PARAM_FILE: $PARAM_FILE_RESOLVED"
echo "RUST_LOG_S (server): $RUST_LOG_S"
echo "RUST_LOG_C (client): $RUST_LOG_C"
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

# Number of nodes to request
# On Sirius: select=1 gives 1 chunk (24 CPUs) with /scr scratch provisioning.
# Client processes use --oversubscribe to exceed the 24-slot limit.
nnodes_list=(
  # 2 4 8 16 32
  2
)
niter=1

for nnodes in "${nnodes_list[@]}"; do
  for ((iter=0; iter<niter; iter++)); do
    cmd_qsub=(
      qsub
      -q gold
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
