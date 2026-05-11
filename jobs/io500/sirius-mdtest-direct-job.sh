#!/bin/bash
#PBS -q mcrp
#PBS -A NBB
#PBS -l place=exclhost
# Run mdtest binary directly (no io500 wrapper) against benchfsd_mpi.
# Skips ior-easy / ior-hard phases entirely so we can check if mdtest-hard
# crashes immediately on a fresh server with a fresh client.

set +e
ulimit -n 1048576 2>/dev/null || true
module purge
module load openmpi/5.0.9/gcc11.5.0
unset -f module ml _module_raw 2>/dev/null
while IFS= read -r line; do
  v="${line%%=*}"
  [[ "$v" == BASH_FUNC_* ]] && unset "$v" 2>/dev/null
done < <(env)

PROJECT_ROOT="${PROJECT_ROOT:-/work/NBB/rmaeda/workspace/rust/benchfs}"
N_FILES="${N_FILES:-200}"
NRANKS="${NRANKS:-4}"
WRITE_BYTES="${WRITE_BYTES:-3901}"
: ${OUTPUT_DIR:="$PROJECT_ROOT/results/io500/$(date +%Y.%m.%d-%H.%M.%S)-sirius-mdtest-direct"}

JOBID=$(echo "$PBS_JOBID" | cut -d . -f 1)
J="${OUTPUT_DIR}/$(date +%Y.%m.%d-%H.%M.%S)-${JOBID}"
REGISTRY_DIR="${J}/registry"
SERVER_LOG_DIR="${J}/server"
CLIENT_LOG_DIR="${J}/client"
mkdir -p "${J}" "${REGISTRY_DIR}" "${SERVER_LOG_DIR}" "${CLIENT_LOG_DIR}"

DATA_DIR=""
for n in 0 1 2 3; do
  d="/scr${n}/${PBS_JOBID}"
  [ -d "$d" ] && DATA_DIR="$d" && break
done
[ -z "$DATA_DIR" ] && DATA_DIR="${J}/data" && mkdir -p "$DATA_DIR"
echo "DATA_DIR=$DATA_DIR  N_FILES=$N_FILES  NRANKS=$NRANKS"

export LD_LIBRARY_PATH="${PROJECT_ROOT}/target/release:/home/NBB/rmaeda/.local/lib:${LD_LIBRARY_PATH:-}"

CONFIG="${J}/benchfs.toml"
cat > "${CONFIG}" <<EOF
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

echo "==== Server (singleton) ===="
RUST_LOG=warn RUST_BACKTRACE=1 \
  "${PROJECT_ROOT}/target/release/benchfsd_mpi" \
  "${REGISTRY_DIR}" "${CONFIG}" \
  > "${SERVER_LOG_DIR}/stdout.log" 2> "${SERVER_LOG_DIR}/stderr.log" &
SERVER_PID=$!
for i in $(seq 1 30); do
  ls "${REGISTRY_DIR}"/node_*.addr >/dev/null 2>&1 && break
  kill -0 $SERVER_PID 2>/dev/null || { echo "server died"; head -30 "${SERVER_LOG_DIR}/stderr.log"; exit 1; }
  sleep 1
done
sleep 2

echo "==== mdtest -n $N_FILES -F (direct, no io500) ===="
MDTEST_DIR="${DATA_DIR}/mdtest_direct"
mkdir -p "${MDTEST_DIR}"
ulimit -c unlimited
cd "${CLIENT_LOG_DIR}"

mpirun -np "$NRANKS" --bind-to none --oversubscribe \
  --mca pml ucx --mca btl self --mca osc ucx \
  -x PATH -x LD_LIBRARY_PATH \
  -x RUST_LOG=warn -x RUST_BACKTRACE=1 \
  "${PROJECT_ROOT}/ior_integration/ior/src/mdtest" \
    -n "$N_FILES" -t -w "$WRITE_BYTES" -e "$WRITE_BYTES" -F \
    -d "${MDTEST_DIR}" \
    -a BENCHFS \
    --benchfs.registry="${REGISTRY_DIR}" \
    --benchfs.datadir="${DATA_DIR}" \
    --benchfs.chunk-size=4194304 \
  > "${CLIENT_LOG_DIR}/mdtest_stdout.log" \
  2> "${CLIENT_LOG_DIR}/mdtest_stderr.log"
RC=$?
echo "  mdtest rc=$RC"

kill -TERM $SERVER_PID 2>/dev/null
sleep 3
kill -9 $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null

echo ""
echo "===================="
echo "Output: ${J}"
echo "===================="
echo ""
echo "=== mdtest stdout (last 30) ==="
tail -30 "${CLIENT_LOG_DIR}/mdtest_stdout.log" 2>/dev/null
echo ""
echo "=== mdtest stderr (last 40) ==="
tail -40 "${CLIENT_LOG_DIR}/mdtest_stderr.log" 2>/dev/null
echo ""
echo "=== heap markers ==="
grep -E "malloc_consolidate|fastbin|panic|Segmentation|Aborted|signal " \
  "${CLIENT_LOG_DIR}/mdtest_stderr.log" 2>/dev/null | head -10 || echo "(none)"
