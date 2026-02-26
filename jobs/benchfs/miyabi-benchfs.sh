#!/bin/bash
set -euo pipefail
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE:-$0}" )" &> /dev/null && pwd )"
source "${SCRIPT_DIR}/common.sh"
TIMESTAMP="$(timestamp)"

# default params
: ${ELAPSTIM_REQ:="0:05:00"}
: ${ENABLE_PERFETTO:=0}  # Set to 1 to enable Perfetto tracing (native format with task tracks)
: ${ENABLE_CHROME:=0}    # Set to 1 to enable Chrome tracing (JSON format)
: ${RUST_LOG_S:=info}    # RUST_LOG for server (benchfsd_mpi)
: ${RUST_LOG_C:=warn}    # RUST_LOG for client (IOR)
: ${TASKSET:=0}          # Set to 1 to enable taskset CPU pinning for benchfsd_mpi
: ${TASKSET_CORES:=0,1}  # CPU cores to pin benchfsd_mpi to when TASKSET=1
: ${ENABLE_NODE_DIAGNOSTICS:=0}  # Set to 1 to enable pre-benchmark node diagnostics
: ${ENABLE_STATS:=0}     # Set to 1 to enable detailed timing statistics collection
: ${SEPARATE:=0}         # Set to 1 to use separate nodes for server and client
: ${SERVER_NODES:=}      # Number of server nodes (default: floor(NNODES/2))
: ${CLIENT_NODES:=}      # Number of client nodes (default: NNODES - SERVER_NODES)
: ${LABEL:=default}

JOB_FILE="$(remove_ext "$(this_file)")-job.sh"
PROJECT_ROOT="$(to_fullpath "$(this_directory)/../..")"
OUTPUT_DIR="$PROJECT_ROOT/results/benchfs/${TIMESTAMP}-${LABEL}"
BACKEND_DIR="$PROJECT_ROOT/backend/benchfs"
BENCHFS_PREFIX="${PROJECT_ROOT}/target/release"
IOR_PREFIX="${PROJECT_ROOT}/ior_integration/ior"

# Resolve PARAM_FILE to absolute path BEFORE cd (relative paths won't work after cd)
PARAM_FILE="$(readlink -f "${PARAM_FILE:-${PROJECT_ROOT}/params/debug_large.conf}")"

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
echo "BenchFS Job Submission (Miyabi)"
echo "=========================================="
echo "PROJECT_ROOT: $PROJECT_ROOT"
echo "BENCHFS_PREFIX: $BENCHFS_PREFIX"
echo "IOR_PREFIX: $IOR_PREFIX"
echo "PARAM_FILE: $PARAM_FILE"
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

nnodes_list=(
  # 1 2 4 8
  # 4
  # 2 4 8 16
  2 4 8 16 32
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
