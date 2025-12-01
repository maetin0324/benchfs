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
PARAM_FILE_RESOLVED="$(readlink -f "${PARAM_FILE:-${SCRIPT_DIR}/default_params.conf}")"

# Debug: Print paths
echo "=========================================="
echo "BenchFS Job Submission"
echo "=========================================="
echo "PROJECT_ROOT: $PROJECT_ROOT"
echo "BENCHFS_PREFIX: $BENCHFS_PREFIX"
echo "IOR_PREFIX: $IOR_PREFIX"
echo "PARAM_FILE: $PARAM_FILE_RESOLVED"
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
  # 16
  # 32
  # 64
  # 100
)
niter=1

param_set_list=(
  "
  NQSV_MPI_VER=4.1.8/gcc11.4.0-cuda12.8.1
  "
)

# Calculate total nodes and select appropriate queue
# gen_S: total <= 31 nodes
# gen_M: total <= 63 nodes
# gen_L: total > 63 nodes
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
        -v BENCHFS_PREFIX="$BENCHFS_PREFIX"
        -v IOR_PREFIX="$IOR_PREFIX"
        -v PARAM_FILE="$PARAM_FILE_RESOLVED"
        "${JOB_FILE}"
      )
      echo "${cmd_qsub[@]}"
      "${cmd_qsub[@]}"
    done
  done
done
