#!/bin/bash
#------- qsub option -----------
#PBS -q mcrp
#PBS -A NBB
#PBS -l place=exclhost
#------- Program execution -----------
# Standalone mdtest-hard reproducer: 1 server + 1 client, both singleton.
# No MPI/IOR/mdtest involved on the client side — just a tight C loop of
# benchfs_create + benchfs_write(3901B) + benchfs_close.
#
# Goal: reproduce the heap corruption with the smallest possible test so
# the failure point can be bisected with instrumentation iterations.

set +e

ulimit -n 1048576 2>/dev/null || ulimit -n 524288 2>/dev/null || ulimit -n 65536 2>/dev/null || true

cleanup_exported_bash_functions() {
  unset -f module ml _module_raw 2>/dev/null || true
  while IFS= read -r line; do
    var="${line%%=*}"
    [[ "$var" == BASH_FUNC_* ]] && unset "$var" 2>/dev/null
  done < <(env)
}

module purge
module load openmpi/5.0.9/gcc11.5.0
cleanup_exported_bash_functions

PROJECT_ROOT="${PROJECT_ROOT:-/work/NBB/rmaeda/workspace/rust/benchfs}"
ITERATIONS="${ITERATIONS:-2000}"
: ${OUTPUT_DIR:="$PROJECT_ROOT/results/io500/$(date +%Y.%m.%d-%H.%M.%S)-sirius-mdtest-repro"}

JOB_START=$(date +%Y.%m.%d-%H.%M.%S)
JOBID=$(echo "$PBS_JOBID" | cut -d . -f 1)
JOB_OUTPUT_DIR="${OUTPUT_DIR}/${JOB_START}-${JOBID}"
REGISTRY_DIR="${JOB_OUTPUT_DIR}/registry"
SERVER_LOG_DIR="${JOB_OUTPUT_DIR}/server"
CLIENT_LOG_DIR="${JOB_OUTPUT_DIR}/client"

mkdir -p "${JOB_OUTPUT_DIR}" "${REGISTRY_DIR}" "${SERVER_LOG_DIR}" "${CLIENT_LOG_DIR}"

DATA_DIR=""
for n in 0 1 2 3; do
  d="/scr${n}/${PBS_JOBID}"
  [ -d "$d" ] && DATA_DIR="$d" && break
done
[ -z "$DATA_DIR" ] && DATA_DIR="${JOB_OUTPUT_DIR}/data" && mkdir -p "$DATA_DIR"
echo "Using data dir: $DATA_DIR"
echo "Iterations: $ITERATIONS"

export LD_LIBRARY_PATH="${PROJECT_ROOT}/target/release:/home/NBB/rmaeda/.local/lib:${LD_LIBRARY_PATH:-}"

CONFIG_FILE="${JOB_OUTPUT_DIR}/benchfs.toml"
cat > "${CONFIG_FILE}" <<EOF
[node]
node_id = "node0"
data_dir = "${DATA_DIR}"
log_level = "info"
[storage]
chunk_size = 4194304
use_iouring = true
max_storage_gb = 0
[network]
bind_addr = "0.0.0.0:50051"
timeout_secs = 30
rdma_threshold_bytes = 32768
registry_dir = "${REGISTRY_DIR}"
[cache]
metadata_cache_entries = 10000
chunk_cache_mb = 1024
cache_ttl_secs = 0
EOF

echo "==== Starting benchfsd_mpi server (singleton) ===="
RUST_LOG=warn RUST_BACKTRACE=1 \
  "${PROJECT_ROOT}/target/release/benchfsd_mpi" \
  "${REGISTRY_DIR}" "${CONFIG_FILE}" \
  > "${SERVER_LOG_DIR}/benchfsd_stdout.log" 2> "${SERVER_LOG_DIR}/benchfsd_stderr.log" &
SERVER_PID=$!
echo "  pid=$SERVER_PID"

# Wait for registration
for i in $(seq 1 30); do
  if ls "${REGISTRY_DIR}"/node_*.addr >/dev/null 2>&1; then
    echo "  registered after ${i}s"
    break
  fi
  if ! kill -0 $SERVER_PID 2>/dev/null; then
    echo "ERROR: server died"
    head -30 "${SERVER_LOG_DIR}/benchfsd_stderr.log"
    exit 1
  fi
  sleep 1
done
sleep 2

echo "==== Running mdtest_repro client(s) ===="
REPRO="${PROJECT_ROOT}/ior_integration/scripts/mdtest_repro"
ls -la "$REPRO"
ulimit -c unlimited
cd "${CLIENT_LOG_DIR}"

NCLIENTS="${NCLIENTS:-1}"
echo "Spawning ${NCLIENTS} concurrent client(s), ${ITERATIONS} iterations each"

CLIENT_PIDS=()
for c in $(seq 1 $NCLIENTS); do
  RUST_LOG=warn RUST_BACKTRACE=1 \
    timeout --signal=TERM --kill-after=30 1200 \
    "$REPRO" "${REGISTRY_DIR}" "${ITERATIONS}" \
    > "${CLIENT_LOG_DIR}/repro_stdout_${c}.log" \
    2> "${CLIENT_LOG_DIR}/repro_stderr_${c}.log" &
  CLIENT_PIDS+=($!)
  # tiny stagger so clients start at slightly different times
  sleep 0.05
done

# wait for all clients
for pid in "${CLIENT_PIDS[@]}"; do
  wait $pid
  echo "  client pid=$pid rc=$?"
done
RC=0

echo "==== Stopping server ===="
kill -TERM $SERVER_PID 2>/dev/null
sleep 3
kill -9 $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null

echo ""
echo "=========================================="
echo "Output: ${JOB_OUTPUT_DIR}"
echo "=========================================="
echo ""
echo "=== client stderr summaries ==="
for f in "${CLIENT_LOG_DIR}"/repro_stderr_*.log; do
  echo "--- $(basename $f) (last 5 lines) ---"
  tail -5 "$f" 2>/dev/null
done
echo ""
echo "=== Heap-corruption / signal markers across all clients ==="
grep -E "malloc_consolidate|fastbin chunk|panicked|Segmentation|Aborted|signal " \
  "${CLIENT_LOG_DIR}"/repro_stderr_*.log 2>/dev/null | head -20 || echo "(none)"
