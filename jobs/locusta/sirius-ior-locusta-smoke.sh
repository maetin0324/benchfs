#!/bin/bash
# Submit a tiny IOR-over-BenchFS run with BENCHFS_TRANSPORT=locusta to
# validate the FFI client → locusta path end-to-end.
#
# Reuses the existing jobs/benchfs/sirius-benchfs-job.sh job file so we
# get all of the BenchFS daemon + IOR plumbing for free; we just force a
# small select spec and pass BENCHFS_TRANSPORT in the env.

set -euo pipefail
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE:-$0}" )" &> /dev/null && pwd )"
BENCHFS_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$BENCHFS_ROOT/jobs/benchfs/common.sh"
TIMESTAMP="$(timestamp)"

# Fixed: 2-node smoke. Servers + clients share both nodes via ppn>1.
NNODES=${NNODES:-2}
PARAM_FILE_RESOLVED="$(readlink -f "${PARAM_FILE:-$BENCHFS_ROOT/jobs/params/debug_small.conf}")"
LABEL="${LABEL:-locusta-ior-smoke}"

OUTPUT_DIR="$BENCHFS_ROOT/results/benchfs/${TIMESTAMP}-sirius-${LABEL}"
BACKEND_DIR="$BENCHFS_ROOT/backend/benchfs"
BENCHFS_PREFIX="$BENCHFS_ROOT/target/release"
IOR_PREFIX="$BENCHFS_ROOT/ior_integration/ior"

mkdir -p "$OUTPUT_DIR"

# Exports for qsub -V
export JOB_FILE="$BENCHFS_ROOT/jobs/benchfs/sirius-benchfs-job.sh"
export PROJECT_ROOT="$BENCHFS_ROOT"
export OUTPUT_DIR
export BACKEND_DIR
export BENCHFS_PREFIX
export IOR_PREFIX
export TIMESTAMP
export PARAM_FILE="$PARAM_FILE_RESOLVED"
export LABEL
export ENABLE_PERFETTO=${ENABLE_PERFETTO:-0}
export ENABLE_CHROME=${ENABLE_CHROME:-0}
export RUST_LOG_S=${RUST_LOG_S:-info}
export RUST_LOG_C=${RUST_LOG_C:-info}
export TASKSET=${TASKSET:-0}
export TASKSET_CORES=${TASKSET_CORES:-0,1}
export ENABLE_NODE_DIAGNOSTICS=${ENABLE_NODE_DIAGNOSTICS:-0}
export ENABLE_STATS=${ENABLE_STATS:-1}
export POSIX=${POSIX:-0}
export DUMMY=${DUMMY:-0}
export SEPARATE=${SEPARATE:-0}
export SERVER_NODES=${SERVER_NODES:-}
export CLIENT_NODES=${CLIENT_NODES:-}
export CHUNK_SIZE_MATCH_XFER=${CHUNK_SIZE_MATCH_XFER:-0}
export BENCHFS_TRANSPORT=${BENCHFS_TRANSPORT:-locusta}

echo "=========================================="
echo "BenchFS IOR locusta smoke (2 node)"
echo "  TRANSPORT=$BENCHFS_TRANSPORT"
echo "  PARAM=$PARAM_FILE"
echo "  LABEL=$LABEL"
echo "=========================================="

ls -la "$BENCHFS_PREFIX/benchfsd_mpi" || { echo "missing benchfsd_mpi"; exit 1; }
ls -la "$IOR_PREFIX/src/ior"             || { echo "missing ior";          exit 1; }

# Use mcrp queue. Match the chunk spec the queue requires
# (ncpus=96:mem=496gb:ngpus=4 = one full node).
qsub -q mcrp -A NBB \
  -l "select=${NNODES}:ncpus=96:mem=496gb:ngpus=4" \
  -l place=exclhost \
  -l walltime="${ELAPSTIM_REQ:-00:20:00}" \
  -v SCRRAID=no \
  -V \
  "$JOB_FILE"
