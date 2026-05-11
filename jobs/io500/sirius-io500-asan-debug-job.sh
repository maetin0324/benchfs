#!/bin/bash
#------- qsub option -----------
#PBS -q mcrp
#PBS -A NBB
#PBS -l place=exclhost
#------- Program execution -----------
#
# Single-node ASAN reproducer for the mdtest-hard heap corruption.
#
# Strategy:
#   - 1 physical Sirius node (4 vnodes, 4 benchfsd_mpi servers).
#   - Force UCX to TCP transport (no RDMA, no pinned memory).
#   - Disable UCX memory hooks and signal handlers so libasan's mmap-based
#     shadow memory and SIGSEGV/SIGILL handlers can install cleanly.
#   - LD_PRELOAD libasan.so.8 into both server (benchfsd_mpi) and client
#     (io500). libbenchfs.so is NOT compiled with -fsanitize=address; we
#     rely on libasan's malloc interposition to catch heap UB at the
#     malloc/free boundary (overflow via redzones, double-free, free of
#     unallocated). Stack/global UB is not instrumented but heap UB is.
#   - Run mdtest-hard only (cheapest reproducer).

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

SCRIPT_DIR="${SCRIPT_DIR:-/work/NBB/rmaeda/workspace/rust/benchfs/jobs/io500}"
PROJECT_ROOT="${PROJECT_ROOT:-/work/NBB/rmaeda/workspace/rust/benchfs}"
: ${OUTPUT_DIR:="$PROJECT_ROOT/results/io500/$(date +%Y.%m.%d-%H.%M.%S)-sirius-asan-debug"}
BENCHFS_PREFIX="${BENCHFS_PREFIX:-${PROJECT_ROOT}/target/release}"
IO500_DIR="${IO500_DIR:-${PROJECT_ROOT}/ior_integration/io500}"

source "${SCRIPT_DIR}/../benchfs/common.sh"

JOB_START=$(timestamp)
VNODES=$(wc -l < "${PBS_NODEFILE}")
JOBID=$(echo "$PBS_JOBID" | cut -d . -f 1)
NNODES=$(sort -u "${PBS_NODEFILE}" | wc -l)

JOB_OUTPUT_DIR="${OUTPUT_DIR}/${JOB_START}-${JOBID}-${NNODES}n"
BENCHFS_REGISTRY_DIR="${JOB_OUTPUT_DIR}/registry"
BENCHFSD_LOG_BASE_DIR="${JOB_OUTPUT_DIR}/benchfsd_logs"
PHASE_DIR="${JOB_OUTPUT_DIR}/mdtest_hard"

mkdir -p "${JOB_OUTPUT_DIR}" "${PHASE_DIR}" "${BENCHFSD_LOG_BASE_DIR}" "${BENCHFS_REGISTRY_DIR}"

# Scratch dirs
detect_all_scratch_dirs() {
  BENCHFS_ALL_SCRATCH_DIRS=()
  for n in 0 1 2 3; do
    local candidate="/scr${n}/${PBS_JOBID}"
    if [ -d "$candidate" ]; then
      BENCHFS_ALL_SCRATCH_DIRS+=("$candidate")
    fi
  done
  if [ ${#BENCHFS_ALL_SCRATCH_DIRS[@]} -eq 0 ]; then
    echo "ERROR: No scratch directories found for job ${PBS_JOBID}" >&2
    return 1
  fi
}
detect_all_scratch_dirs
BENCHFS_DATA_DIR="${BENCHFS_ALL_SCRATCH_DIRS[0]}"
BENCHFS_SCRATCH_DIRS_CSV=$(IFS=','; echo "${BENCHFS_ALL_SCRATCH_DIRS[*]}")

export LD_LIBRARY_PATH="${PROJECT_ROOT}/target/release:/home/NBB/rmaeda/.local/lib:${LD_LIBRARY_PATH:-}"

ASAN_LIB="${ASAN_LIB:-/system/apps/rhel/9.6-202602/misc/spack/1.1.1/opt/spack/linux-zen4/gcc-14.2.0-4gtzoowueve3cnvryudhcy3du6tmszuc/lib64/libasan.so.8}"
ASAN_OPT_DEFAULT="halt_on_error=1:abort_on_error=1:detect_leaks=0:print_stacktrace=1:malloc_context_size=30:disable_coredump=1:abort_on_error=0:exitcode=99:detect_stack_use_after_return=0:check_initialization_order=0:handle_segv=0:handle_sigbus=0:handle_sigill=0:handle_sigfpe=0:allow_user_segv_handler=1"
ASAN_OPTIONS_VAL="${ASAN_OPTIONS_VAL:-$ASAN_OPT_DEFAULT}"

# UCX: force TCP transport, disable memory hooks and signal handlers so
# libasan's shadow memory mapping and signal handlers don't conflict.
UCX_ENV=(
  -x UCX_TLS=tcp,self,sm
  -x UCX_RCACHE_ENABLE=n
  -x UCX_MEMTYPE_CACHE=n
  -x UCX_HANDLE_ERRORS=none
  -x UCX_LOG_LEVEL=warn
  -x UCM_ERROR_SIGNALS=
)

UNIQUE_HOSTFILE="${JOB_OUTPUT_DIR}/unique_nodes"
sort -u "${PBS_NODEFILE}" > "${UNIQUE_HOSTFILE}"

OMPI_MCA_mpi_yield_when_idle=1 \
OMPI_MCA_btl_base_warn_component_unused=0 \
RUST_BACKTRACE=full

cmd_mpirun_common=(
  mpirun --mca routed direct --mca plm_rsh_no_tree_spawn 1
  --mca pml ucx --mca btl self --mca osc ucx
  -x PATH -x LD_LIBRARY_PATH
)

# ============================================================================
# Start benchfsd (4 servers on the single node)
# ============================================================================
config_file="${JOB_OUTPUT_DIR}/benchfs.toml"
cat > "${config_file}" <<EOF
[node]
node_id = "node0"
data_dir = "${BENCHFS_DATA_DIR}"
log_level = "info"

[storage]
chunk_size = 4194304
use_iouring = true
max_storage_gb = 0

[network]
bind_addr = "0.0.0.0:50051"
timeout_secs = 30
rdma_threshold_bytes = 32768
registry_dir = "${BENCHFS_REGISTRY_DIR}"

[cache]
metadata_cache_entries = 10000
chunk_cache_mb = 1024
cache_ttl_secs = 0
EOF

server_log_dir="${BENCHFSD_LOG_BASE_DIR}/server"
mkdir -p "${server_log_dir}"

datadir_wrapper="${server_log_dir}/benchfsd_datadir_wrapper.sh"
export BENCHFS_INNER_BINARY="${BENCHFS_PREFIX}/benchfsd_mpi"
cat > "${datadir_wrapper}" <<'WRAPPER_EOF'
#!/bin/bash
LOCAL_RANK=${OMPI_COMM_WORLD_LOCAL_RANK:-0}
LOCAL_SCRATCH_DIRS=()
for n in 0 1 2 3; do
  if [ -d "/scr${n}/${PBS_JOBID}" ]; then
    LOCAL_SCRATCH_DIRS+=("/scr${n}/${PBS_JOBID}")
  fi
done
if [ ${#LOCAL_SCRATCH_DIRS[@]} -gt 0 ]; then
  DIR_INDEX=$((LOCAL_RANK % ${#LOCAL_SCRATCH_DIRS[@]}))
  RANK_DATA_DIR="${LOCAL_SCRATCH_DIRS[$DIR_INDEX]}"
else
  IFS=',' read -ra SCRATCH_DIRS <<< "${BENCHFS_SCRATCH_DIRS}"
  DIR_INDEX=$((LOCAL_RANK % ${#SCRATCH_DIRS[@]}))
  RANK_DATA_DIR="${SCRATCH_DIRS[$DIR_INDEX]}"
fi
CONFIG_FILE="$2"
WORLD_RANK=${OMPI_COMM_WORLD_RANK:-${LOCAL_RANK}}
RANK_CONFIG="${CONFIG_FILE%.toml}_rank${WORLD_RANK}.toml"
sed "s|^data_dir = .*|data_dir = \"${RANK_DATA_DIR}\"|" "${CONFIG_FILE}" > "${RANK_CONFIG}"
exec "${BENCHFS_INNER_BINARY}" "$1" "${RANK_CONFIG}" "${@:3}"
WRAPPER_EOF
chmod +x "${datadir_wrapper}"

server_np=$VNODES
echo "==== Starting ${server_np} benchfsd_mpi servers with ASAN preload ===="
"${cmd_mpirun_common[@]}" \
  -np "$server_np" \
  --bind-to none \
  --oversubscribe \
  "${UCX_ENV[@]}" \
  -x LD_PRELOAD="${ASAN_LIB}" \
  -x ASAN_OPTIONS="${ASAN_OPTIONS_VAL}" \
  -x RUST_LOG=warn \
  -x RUST_BACKTRACE=1 \
  -x PBS_JOBID \
  -x BENCHFS_SCRATCH_DIRS="${BENCHFS_SCRATCH_DIRS_CSV}" \
  -x BENCHFS_INNER_BINARY \
  --hostfile "${UNIQUE_HOSTFILE}" \
  "${datadir_wrapper}" \
  "${BENCHFS_REGISTRY_DIR}" \
  "${config_file}" > "${server_log_dir}/benchfsd_stdout.log" 2> "${server_log_dir}/benchfsd_stderr.log" &
BENCHFSD_PID=$!

# Wait for servers to register
echo "Waiting for ${server_np} server registrations..."
for i in $(seq 1 60); do
  ready=$(find "${BENCHFS_REGISTRY_DIR}" -name "node_*.addr" 2>/dev/null | wc -l)
  if [ "$ready" -eq "$server_np" ]; then
    echo "Servers ready: $ready/$server_np"
    break
  fi
  sleep 1
done
sleep 5

# ============================================================================
# Run io500 with mdtest-hard only (small N to keep wall time short)
# ============================================================================
np=$((VNODES * 4))
data_dir="${BENCHFS_DATA_DIR}/io500_asan"
result_dir="${PHASE_DIR}/io500_result"
mkdir -p "${result_dir}"
ini="${PHASE_DIR}/config.ini"

cat > "${ini}" <<EOF
[global]
datadir = ${data_dir}
timestamp-datadir = FALSE
resultdir = ${result_dir}
timestamp-resultdir = FALSE
api = BENCHFS --benchfs.registry=${BENCHFS_REGISTRY_DIR} --benchfs.datadir=${BENCHFS_DATA_DIR} --benchfs.chunk-size=4194304
drop-caches = FALSE
io-buffers-on-gpu = FALSE
verbosity = 1
scc = FALSE
dataPacketType = timestamp

[debug]
stonewall-time = 5
pause-dir =

# Disable everything except mdtest-hard-write
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
n = 1000
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

echo "==== Running io500 (mdtest-hard only) with ASAN preload ===="
timeout --signal=TERM --kill-after=60 600 \
  "${cmd_mpirun_common[@]}" \
  -np "$np" \
  --bind-to none \
  --oversubscribe \
  "${UCX_ENV[@]}" \
  -x LD_PRELOAD="${ASAN_LIB}" \
  -x ASAN_OPTIONS="${ASAN_OPTIONS_VAL}" \
  -x RUST_LOG=warn \
  -x RUST_BACKTRACE=1 \
  -x BENCHFS_EXPECTED_NODES="${VNODES}" \
  "${IO500_DIR}/io500" "${ini}" \
  > "${PHASE_DIR}/io500_stdout.log" 2> "${PHASE_DIR}/io500_stderr.log" || true

echo "==== Stopping benchfsd ===="
pkill -TERM benchfsd_mpi 2>/dev/null || true
sleep 5
pkill -9 benchfsd_mpi 2>/dev/null || true
wait $BENCHFSD_PID 2>/dev/null || true

echo ""
echo "=========================================="
echo "Results in: ${PHASE_DIR}"
echo "Server logs in: ${BENCHFSD_LOG_BASE_DIR}"
echo "=========================================="
echo ""
echo "=== ASAN diagnostics from client stderr (first 200 lines): ==="
grep -E "AddressSanitizer|ERROR:|SUMMARY:|^==|0x[0-9a-f]+ in |^    #[0-9]+ " "${PHASE_DIR}/io500_stderr.log" 2>/dev/null | head -200 || echo "(none)"
echo ""
echo "=== ASAN diagnostics from server stderr (first 200 lines): ==="
grep -E "AddressSanitizer|ERROR:|SUMMARY:|^==|0x[0-9a-f]+ in |^    #[0-9]+ " "${BENCHFSD_LOG_BASE_DIR}/server/benchfsd_stderr.log" 2>/dev/null | head -200 || echo "(none)"
