#!/bin/bash
#------- qsub option -----------
#PBS -q mcrp
#PBS -A NBB
#PBS -l place=exclhost
#------- Program execution -----------
#
# Singleton-mode ASAN reproducer for mdtest-hard heap corruption.
#
# Why singleton: T6 in asan_benchfs_test2 showed that mpirun + LD_PRELOAD
# libasan + benchfsd_mpi raises SIGILL during MPI_Init's spawn machinery.
# T5 showed that benchfsd_mpi launched directly (no mpirun) under the same
# LD_PRELOAD setup runs cleanly — MPI_Init returns rank 0/1 and the server
# starts. So the workaround is: launch each process directly (no mpirun).
#
# The mdtest-hard heap corruption was NOT concurrency-driven (ppn=1 with
# 40 ranks reproduced it identically to ppn=4 / 160 ranks). So 1 server +
# 1 client should be enough to trigger it under ASAN.

set +e

ulimit -n 1048576 2>/dev/null || ulimit -n 524288 2>/dev/null || ulimit -n 262144 2>/dev/null || ulimit -n 65536 2>/dev/null || true
echo "File descriptor limit: $(ulimit -n)"

cleanup_exported_bash_functions() {
  unset -f module ml _module_raw 2>/dev/null || true
  local vars_to_unset=()
  while IFS= read -r line; do
    local var_name="${line%%=*}"
    if [[ "$var_name" == BASH_FUNC_* ]]; then
      vars_to_unset+=("$var_name")
    fi
  done < <(env)
  for var in "${vars_to_unset[@]}"; do
    unset "$var" 2>/dev/null || true
  done
}

module purge
module load openmpi/5.0.9/gcc11.5.0
cleanup_exported_bash_functions

PROJECT_ROOT="${PROJECT_ROOT:-/work/NBB/rmaeda/workspace/rust/benchfs}"
: ${OUTPUT_DIR:="$PROJECT_ROOT/results/io500/$(date +%Y.%m.%d-%H.%M.%S)-sirius-asan-singleton"}
BENCHFS_PREFIX="${BENCHFS_PREFIX:-${PROJECT_ROOT}/target/x86_64-unknown-linux-gnu/release}"
IO500_DIR="${IO500_DIR:-${PROJECT_ROOT}/ior_integration/io500}"

JOB_START=$(date +%Y.%m.%d-%H.%M.%S)
JOBID=$(echo "$PBS_JOBID" | cut -d . -f 1)
NNODES=$(sort -u "${PBS_NODEFILE}" | wc -l)
JOB_OUTPUT_DIR="${OUTPUT_DIR}/${JOB_START}-${JOBID}-${NNODES}n"
REGISTRY_DIR="${JOB_OUTPUT_DIR}/registry"
SERVER_LOG_DIR="${JOB_OUTPUT_DIR}/server"
CLIENT_LOG_DIR="${JOB_OUTPUT_DIR}/client"

mkdir -p "${JOB_OUTPUT_DIR}" "${REGISTRY_DIR}" "${SERVER_LOG_DIR}" "${CLIENT_LOG_DIR}"

# Find a /scrN/$PBS_JOBID we can use
DATA_DIR=""
for n in 0 1 2 3; do
  d="/scr${n}/${PBS_JOBID}"
  if [ -d "$d" ]; then
    DATA_DIR="$d"
    break
  fi
done
if [ -z "$DATA_DIR" ]; then
  DATA_DIR="${JOB_OUTPUT_DIR}/data"
  mkdir -p "$DATA_DIR"
fi
echo "Using data dir: $DATA_DIR"

export LD_LIBRARY_PATH="${BENCHFS_PREFIX}:/home/NBB/rmaeda/.local/lib:${LD_LIBRARY_PATH:-}"

LIBASAN=/system/apps/rhel/9.6-202602/misc/spack/1.1.1/opt/spack/linux-zen4/gcc-14.2.0-4gtzoowueve3cnvryudhcy3du6tmszuc/lib64/libasan.so.8
ASAN_OPT="halt_on_error=0:abort_on_error=0:detect_leaks=0:print_stacktrace=1:malloc_context_size=30:disable_coredump=1:exitcode=99:detect_stack_use_after_return=0:check_initialization_order=0:handle_segv=0:handle_sigbus=0:handle_sigill=0:handle_sigfpe=0:allow_user_segv_handler=1"

# UCX in TCP-only mode, all hooks off (avoids signal handler conflicts).
export UCX_TLS=tcp,self,sm
export UCX_RCACHE_ENABLE=n
export UCX_MEMTYPE_CACHE=n
export UCX_HANDLE_ERRORS=none
export UCM_ERROR_SIGNALS=
export UCX_LOG_LEVEL=warn

# Server config
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

echo "==== Pre-flight: ASAN-built benchfsd_mpi (10s timeout, no LD_PRELOAD needed) ===="
timeout 10 env \
  ASAN_OPTIONS="$ASAN_OPT" \
  RUST_LOG=warn \
  RUST_BACKTRACE=1 \
  "${BENCHFS_PREFIX}/benchfsd_mpi" "${REGISTRY_DIR}" "${CONFIG_FILE}" \
  > "${SERVER_LOG_DIR}/preflight_asan.log" 2>&1
echo "preflight asan exit=$?"
echo "==== preflight_asan.log (head 80) ===="
head -80 "${SERVER_LOG_DIR}/preflight_asan.log" 2>/dev/null
echo "==== /preflight_asan.log ===="
rm -rf "${REGISTRY_DIR}"/*

echo "==== Starting ASAN-built benchfsd_mpi (singleton) ===="
nohup env \
  ASAN_OPTIONS="$ASAN_OPT" \
  RUST_LOG=warn \
  RUST_BACKTRACE=1 \
  "${BENCHFS_PREFIX}/benchfsd_mpi" \
  "${REGISTRY_DIR}" "${CONFIG_FILE}" \
  > "${SERVER_LOG_DIR}/benchfsd_stdout.log" 2> "${SERVER_LOG_DIR}/benchfsd_stderr.log" &
SERVER_PID=$!

# Wait for the server to register
for i in $(seq 1 30); do
  if ls "${REGISTRY_DIR}"/node_*.addr >/dev/null 2>&1; then
    echo "Server registered after ${i}s"
    break
  fi
  if ! kill -0 $SERVER_PID 2>/dev/null; then
    echo "ERROR: server died before registering"
    head -50 "${SERVER_LOG_DIR}/benchfsd_stderr.log"
    exit 1
  fi
  sleep 1
done
sleep 3

# IO500 config: mdtest-hard only, very small n for speed.
INI="${CLIENT_LOG_DIR}/config.ini"
RESULT_DIR="${CLIENT_LOG_DIR}/io500_result"
mkdir -p "$RESULT_DIR"
cat > "${INI}" <<EOF
[global]
datadir = ${DATA_DIR}/io500_asan
timestamp-datadir = FALSE
resultdir = ${RESULT_DIR}
timestamp-resultdir = FALSE
api = BENCHFS --benchfs.registry=${REGISTRY_DIR} --benchfs.datadir=${DATA_DIR} --benchfs.chunk-size=4194304
drop-caches = FALSE
io-buffers-on-gpu = FALSE
verbosity = 1
scc = FALSE
dataPacketType = timestamp

[debug]
stonewall-time = 5
pause-dir =

[ior-easy]
run = FALSE
[ior-easy-write]
run = FALSE
[mdtest-easy]
n = 1
run = FALSE
[mdtest-easy-write]
run = FALSE
[find-easy]
run = FALSE
[ior-hard]
run = FALSE
[ior-hard-write]
run = FALSE
[mdtest-hard]
n = 200
run = TRUE
[mdtest-hard-write]
run = TRUE
[find]
run = FALSE
[ior-easy-read]
run = FALSE
[mdtest-easy-stat]
run = FALSE
[ior-hard-read]
run = FALSE
[mdtest-hard-stat]
run = FALSE
[mdtest-easy-delete]
run = FALSE
[mdtest-hard-read]
run = FALSE
[mdtest-hard-delete]
run = FALSE
[ior-rnd4K-easy-read]
run = FALSE
EOF

echo "==== Running io500 (singleton) ===="
# io500 is a C binary that dlopens libbenchfs.so via aiori-BENCHFS.c. Set
# LD_LIBRARY_PATH so the ASAN-built libbenchfs.so is picked up first; the
# rust ASAN runtime in libbenchfs.so initializes itself when it loads.
ASAN_LIBBENCHFS_DIR="${BENCHFS_PREFIX}"
timeout --signal=TERM --kill-after=30 300 \
env LD_LIBRARY_PATH="${ASAN_LIBBENCHFS_DIR}:${LD_LIBRARY_PATH:-}" \
ASAN_OPTIONS="$ASAN_OPT" \
RUST_LOG=warn \
RUST_BACKTRACE=1 \
"${IO500_DIR}/io500" "${INI}" \
  > "${CLIENT_LOG_DIR}/io500_stdout.log" 2> "${CLIENT_LOG_DIR}/io500_stderr.log" || true

echo "==== Stopping benchfsd ===="
kill -TERM $SERVER_PID 2>/dev/null || true
sleep 3
kill -9 $SERVER_PID 2>/dev/null || true
wait $SERVER_PID 2>/dev/null || true

echo ""
echo "=========================================="
echo "Results in: ${JOB_OUTPUT_DIR}"
echo "=========================================="

echo ""
echo "=== ASAN findings (client) ==="
grep -E "AddressSanitizer|ERROR: heap-|ERROR: double|ERROR: stack-|ERROR: SEGV|ERROR: alloc|SUMMARY: AddressSanitizer|^==[0-9]+==ERROR" "${CLIENT_LOG_DIR}/io500_stderr.log" 2>/dev/null | head -100 || echo "(no asan findings)"

echo ""
echo "=== ASAN findings (server) ==="
grep -E "AddressSanitizer|ERROR: heap-|ERROR: double|ERROR: stack-|ERROR: SEGV|ERROR: alloc|SUMMARY: AddressSanitizer|^==[0-9]+==ERROR" "${SERVER_LOG_DIR}/benchfsd_stderr.log" 2>/dev/null | head -100 || echo "(no asan findings)"

echo ""
echo "=== Tail of client stderr (last 80 lines, asan-related) ==="
tail -200 "${CLIENT_LOG_DIR}/io500_stderr.log" 2>/dev/null | grep -vE "Registered root|Unregistered root|failed to intercept" | head -80
