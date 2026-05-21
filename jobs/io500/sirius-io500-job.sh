#!/bin/bash
#------- qsub option -----------
#PBS -q mcrp
#PBS -A NBB
#PBS -l place=exclhost
#------- Program execution -----------
# Sweep ior-easy parameters with io500/BenchFS, then run ior-hard at the best
# configuration. Uses 10 physical Sirius nodes (40 vnodes).

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
: ${OUTPUT_DIR:="$PROJECT_ROOT/results/io500/$(date +%Y.%m.%d-%H.%M.%S)-sirius"}
BACKEND_DIR="${BACKEND_DIR:-$PROJECT_ROOT/backend/io500}"
BENCHFS_PREFIX="${BENCHFS_PREFIX:-${PROJECT_ROOT}/target/release}"
IO500_DIR="${IO500_DIR:-${PROJECT_ROOT}/ior_integration/io500}"

source "${SCRIPT_DIR}/../benchfs/common.sh"

JOB_START=$(timestamp)
VNODES=$(wc -l < "${PBS_NODEFILE}")
JOBID=$(echo "$PBS_JOBID" | cut -d . -f 1)
NNODES=$(sort -u "${PBS_NODEFILE}" | wc -l)

JOB_OUTPUT_DIR="${OUTPUT_DIR}/${JOB_START}-${JOBID}-${NNODES}n"
JOB_BACKEND_DIR="${BACKEND_DIR}/$(basename -- "${JOB_OUTPUT_DIR}")"
BENCHFS_REGISTRY_DIR="${JOB_BACKEND_DIR}/registry"
# Path to the external find driver built from
# ior_integration/benchfs_backend (libbenchfs-linked). Auto-enables the
# [find] phase via external-script when present.
BENCHFS_PFIND_BIN="${BENCHFS_PFIND_BIN:-$(realpath "${SCRIPT_DIR}/../../ior_integration/benchfs_backend/bin/benchfs_pfind" 2>/dev/null)}"
if [ -x "${BENCHFS_PFIND_BIN:-/nonexistent}" ]; then
  IO500_FIND_RUN="${IO500_FIND_RUN:-TRUE}"
fi
BENCHFSD_LOG_BASE_DIR="${JOB_OUTPUT_DIR}/benchfsd_logs"
SWEEP_DIR="${JOB_OUTPUT_DIR}/sweep"
FINAL_DIR="${JOB_OUTPUT_DIR}/final"
SUMMARY_CSV="${JOB_OUTPUT_DIR}/sweep_summary.csv"

: ${RUST_LOG_S:=warn}
: ${RUST_LOG_C:=warn}
: ${SWEEP_STONEWALL:=30}
: ${FINAL_STONEWALL:=60}

# Run mode selector. Default ("0") skips the parameter sweep and runs io500
# once with the empirically-best ior-easy configuration; the final ior-hard /
# mdtest phases run as usual. Set SKIP_SWEEP=0 to fall back to the legacy
# 24-config sweep + final-best workflow.
: ${SKIP_SWEEP:=1}

# Empirically-best ior-easy parameters (from prior 10-physical-node Sirius
# sweeps with 40 vnodes). Override per-invocation via env if needed.
: ${BEST_TRANSFER:=4m}
: ${BEST_BLOCK:=4g}
: ${BEST_PPN:=4}
: ${BEST_FPP:=TRUE}
: ${BEST_CHUNK_BYTES:=4194304}

# BenchFS scratch dir detection (one /scrN per vnode, up to 4 per physical node).
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

mkdir -p "${JOB_OUTPUT_DIR}" "${SWEEP_DIR}" "${FINAL_DIR}" "${BENCHFSD_LOG_BASE_DIR}"
exec > >(tee "${JOB_OUTPUT_DIR}/job_stdout.log") 2> >(tee "${JOB_OUTPUT_DIR}/job_stderr.log" >&2)

UNIQUE_HOSTFILE="${JOB_OUTPUT_DIR}/unique_nodes"
sort -u "${PBS_NODEFILE}" > "${UNIQUE_HOSTFILE}"
echo "Physical nodes: ${NNODES} (from ${VNODES} vnodes)"

cp "$0" "${JOB_OUTPUT_DIR}"
cp "${PBS_NODEFILE}" "${JOB_OUTPUT_DIR}"
cp "${SCRIPT_DIR}/../benchfs/common.sh" "${JOB_OUTPUT_DIR}"
printenv > "${JOB_OUTPUT_DIR}/env.txt"

echo "=========================================="
echo "IO500 (BenchFS) Job Configuration"
echo "=========================================="
echo "NNODES (physical): ${NNODES}, VNODES: ${VNODES}"
echo "PBS_JOBID: ${PBS_JOBID}"
echo "BENCHFS_PREFIX: ${BENCHFS_PREFIX}"
echo "IO500_DIR: ${IO500_DIR}"
echo "Scratch dirs (head node): ${BENCHFS_ALL_SCRATCH_DIRS[*]}"
echo "RUST_LOG (server): ${RUST_LOG_S}, (client): ${RUST_LOG_C}"
echo "SWEEP_STONEWALL: ${SWEEP_STONEWALL}s"
echo "FINAL_STONEWALL: ${FINAL_STONEWALL}s"
echo ""
echo "Checking binaries:"
ls -la "${BENCHFS_PREFIX}/benchfsd_mpi" || echo "ERROR: benchfsd_mpi missing"
ls -la "${IO500_DIR}/io500" || echo "ERROR: io500 missing"
echo "=========================================="

mkdir -p "${JOB_BACKEND_DIR}" "${BENCHFS_REGISTRY_DIR}"

cleanup_and_exit() {
  local exit_code=${1:-1}
  local signal_name=${2:-"unknown"}
  echo "" ; echo "==== Job interrupted by signal: $signal_name (run=${current_run_label:-N/A}) ===="
  stop_benchfsd 2>/dev/null || true
  rm -rf "${JOB_BACKEND_DIR}" 2>/dev/null || true
  exit "$exit_code"
}
trap 'cleanup_and_exit 1 "SIGHUP"' 1
trap 'cleanup_and_exit 1 "SIGINT"' 2
trap 'cleanup_and_exit 1 "SIGQUIT"' 3
trap 'cleanup_and_exit 1 "SIGTERM"' 15
trap 'rm -rf "${JOB_BACKEND_DIR}" 2>/dev/null || true; exit 0' EXIT

# ==============================================================================
# MPI configuration (UCX via OpenMPI 5.0.9)
# ==============================================================================
export OMPI_MCA_mpi_yield_when_idle=1
export OMPI_MCA_btl_base_warn_component_unused=0
export RUST_BACKTRACE=full

# BenchFS tuning for high-concurrency io500 ior-easy:
# - BENCHFS_RPC_TIMEOUT=120s (was default 30s; stonewall transitions push
#   server response time past 30s briefly, causing spurious timeouts).
# - BENCHFS_IOURING_QUEUE_SIZE=2048 (server-side FixedBuffer pool; 512 was
#   getting exhausted with 16 clients/server × 64 in-flight = 1024).
# - BENCHFS_IOURING_SUBMIT_DEPTH=512 (was 128).
: ${BENCHFS_RPC_TIMEOUT:=120}
: ${BENCHFS_IOURING_QUEUE_SIZE:=2048}
: ${BENCHFS_IOURING_SUBMIT_DEPTH:=512}
# wait_submit_timeout / wait_complete_timeout in the io_uring reactor gate
# whether the runtime polls the reactor. Defaults (1ms) park CQEs for up
# to 1ms before processing — directly inflating chunk-write p99. Drop to
# 10us so completions are drained promptly.
: ${BENCHFS_IOURING_SUBMIT_TIMEOUT_US:=10}
: ${BENCHFS_IOURING_COMPLETE_TIMEOUT_US:=10}
export BENCHFS_RPC_TIMEOUT
export BENCHFS_IOURING_QUEUE_SIZE
export BENCHFS_IOURING_SUBMIT_DEPTH
export BENCHFS_IOURING_SUBMIT_TIMEOUT_US
export BENCHFS_IOURING_COMPLETE_TIMEOUT_US

# Locusta server dispatch tuning. Defaults match the conservative
# values baked into benchfsd_mpi; override here for sweep runs.
: ${BENCHFS_LOCUSTA_DISPATCH_SLEEP_US:=20}
: ${BENCHFS_LOCUSTA_DISPATCH_IDLE_THRESHOLD:=16}
export BENCHFS_LOCUSTA_DISPATCH_SLEEP_US
export BENCHFS_LOCUSTA_DISPATCH_IDLE_THRESHOLD

cmd_mpirun_util=(mpirun --mca routed direct --mca plm_rsh_no_tree_spawn 1 --mca pml ob1 --mca btl tcp,sm,self)
cmd_mpirun_common=(mpirun --mca routed direct --mca plm_rsh_no_tree_spawn 1 --mca pml ucx --mca btl self --mca osc ucx -x PATH -x LD_LIBRARY_PATH -x UCX_RCACHE_ENABLE -x UCX_MEMTYPE_CACHE -x UCX_TLS -x UCX_LOG_LEVEL -x UCX_RNDV_THRESH)

# Optional UCX-only TCP fallback for debugging cross-node RDMA-related bugs.
# Set FORCE_UCX_TCP=1 to force BenchFS's internal UCX context to TCP/SHM
# only (no RDMA, no memory pinning). Switches OpenMPI itself off UCX (uses
# ob1+tcp BTL for MPI coordination) so the UCX_TLS restriction doesn't
# break OpenMPI's own bring-up.
if [ "${FORCE_UCX_TCP:-0}" = "1" ]; then
  echo "FORCE_UCX_TCP=1: BenchFS UCX = TCP-only; OpenMPI = ob1+tcp BTL, hcoll off"
  export UCX_TLS=tcp,self,sm
  export UCX_RCACHE_ENABLE=n
  export UCX_MEMTYPE_CACHE=n
  cmd_mpirun_common=(
    mpirun --mca routed direct --mca plm_rsh_no_tree_spawn 1
    --mca pml ob1 --mca btl tcp,self,sm
    --mca coll '^hcoll'
    --mca osc '^ucx'
    -x PATH -x LD_LIBRARY_PATH
    -x UCX_TLS -x UCX_RCACHE_ENABLE -x UCX_MEMTYPE_CACHE
  )
fi

# ==============================================================================
# benchfsd lifecycle (adapted from sirius-benchfs-job.sh)
# ==============================================================================
parse_size_to_bytes() {
  local size_str="${1,,}"
  local number="${size_str%[kmgt]}"
  local suffix="${size_str: -1}"
  case "$suffix" in
    k) echo $((number * 1024)) ;;
    m) echo $((number * 1024 * 1024)) ;;
    g) echo $((number * 1024 * 1024 * 1024)) ;;
    *) echo "$size_str" ;;
  esac
}

check_server_ready() {
  local expected_count=$1
  local max_attempts=180
  local attempt=0
  while [ $attempt -lt $max_attempts ]; do
    local ready_count=$(find "${BENCHFS_REGISTRY_DIR}" -name "node_*.addr" -type f 2>/dev/null | wc -l)
    if [ "$ready_count" -eq "$expected_count" ]; then
      echo "BenchFS servers registered: $ready_count/$expected_count"
      sleep 10
      return 0
    fi
    sleep 1
    attempt=$((attempt + 1))
  done
  echo "ERROR: BenchFS servers failed to start after $max_attempts seconds"
  return 1
}

stop_benchfsd() {
  if [ -n "${BENCHFSD_PID:-}" ]; then
    echo "Stopping BenchFS servers..."
    pkill -TERM benchfsd_mpi 2>/dev/null || true
    local elapsed=0
    while [ $elapsed -lt 30 ]; do
      if ! kill -0 $BENCHFSD_PID 2>/dev/null; then
        wait $BENCHFSD_PID 2>/dev/null || true
        unset BENCHFSD_PID
        return 0
      fi
      sleep 1; elapsed=$((elapsed + 1))
    done
    kill $BENCHFSD_PID 2>/dev/null || true
    wait $BENCHFSD_PID 2>/dev/null || true
    pkill -9 benchfsd_mpi 2>/dev/null || true
    sleep 3
    unset BENCHFSD_PID
  fi
}

start_benchfsd() {
  local config_id=$1
  local chunk_bytes=$2

  rm -rf "${BENCHFS_REGISTRY_DIR}"/*
  "${cmd_mpirun_util[@]}" --hostfile "${UNIQUE_HOSTFILE}" -np "$NNODES" \
    --bind-to none --oversubscribe -x PBS_JOBID \
    bash -c '
      for n in 0 1 2 3; do
        d="/scr${n}/${PBS_JOBID}"
        [ -d "$d" ] && rm -rf "$d"/* 2>/dev/null
      done
    ' 2>/dev/null || true

  local server_log_dir="${BENCHFSD_LOG_BASE_DIR}/server_${config_id}"
  mkdir -p "${server_log_dir}"
  local config_file="${JOB_OUTPUT_DIR}/benchfs_${config_id}.toml"
  # Backend selection: env or default to locusta (job 20603/20612 confirmed
  # the toml needs [transport]; without it RuntimeConfig::default() set
  # backend="" → UCX, and ior-hard hung on UCX shared-file path).
  local benchfs_backend="${BENCHFS_BACKEND:-locusta}"
  local benchfs_arena_size="${BENCHFS_LOCUSTA_ARENA_SIZE:-268435456}"
  local benchfs_ring_capacity="${BENCHFS_LOCUSTA_RING_CAPACITY:-1024}"
  local benchfs_recv_ring="${BENCHFS_LOCUSTA_RECV_RING_SIZE:-32768}"
  local benchfs_send_buf="${BENCHFS_LOCUSTA_SEND_BUF_SIZE:-32768}"
  local benchfs_max_inflight="${BENCHFS_LOCUSTA_MAX_INFLIGHT:-128}"
  # Translate 0|1 env to toml bool. Empty/unset → true (default).
  local benchfs_metadata_distributed="true"
  if [ "${BENCHFS_METADATA_DISTRIBUTED:-1}" = "0" ]; then
    benchfs_metadata_distributed="false"
  fi
  local benchfs_central_parent_index="true"
  if [ "${BENCHFS_CENTRAL_PARENT_INDEX:-1}" = "0" ]; then
    benchfs_central_parent_index="false"
  fi
  # Reactor-polling: env=1 (default) enables pluvio Reactor as sole driver
  # of locusta inner.tick. +38% mdtest-stat, +22.8% mdtest-easy-write,
  # +8.6% ior-hard-write. NEVER disable for production benchmark.
  local benchfs_reactor_mode="true"
  if [ "${BENCHFS_LOCUSTA_REACTOR:-1}" = "0" ]; then
    benchfs_reactor_mode="false"
  fi
  local benchfs_skip_recv_copy="false"
  if [ "${BENCHFS_SKIP_RECV_COPY:-0}" = "1" ]; then
    benchfs_skip_recv_copy="true"
  fi
  # [locusta] handshake_mode / wait_peer_ack_strict / defer_init_prewarm
  # are surfaced as TOML keys (was env BENCHFS_LOCUSTA_HANDSHAKE etc.).
  local benchfs_handshake_mode="${BENCHFS_LOCUSTA_HANDSHAKE:-udp}"
  local benchfs_exchange_timeout_secs="${BENCHFS_LOCUSTA_EXCHANGE_TIMEOUT_SECS:-300}"
  local benchfs_wait_peer_ack_strict="false"
  if [ "${BENCHFS_LOCUSTA_WAIT_PEER_ACK:-0}" = "1" ]; then
    benchfs_wait_peer_ack_strict="true"
  fi
  local benchfs_defer_init_prewarm="false"
  if [ "${BENCHFS_LOCUSTA_DEFER_HANDSHAKE:-0}" = "1" ]; then
    benchfs_defer_init_prewarm="true"
  fi
  # [rpc] profile / force_put_writes  and [api] open/close_meta_async —
  # these used to be env BENCHFS_RPC_PROFILE / BENCHFS_FORCE_PUT_WRITES
  # / BENCHFS_OPEN_META_ASYNC / BENCHFS_CLOSE_META_ASYNC.
  local benchfs_rpc_profile="false"
  if [ "${BENCHFS_RPC_PROFILE:-0}" = "1" ]; then
    benchfs_rpc_profile="true"
  fi
  local benchfs_force_put_writes="false"
  if [ "${BENCHFS_FORCE_PUT_WRITES:-0}" = "1" ]; then
    benchfs_force_put_writes="true"
  fi
  local benchfs_open_meta_async="false"
  if [ "${BENCHFS_OPEN_META_ASYNC:-0}" = "1" ]; then
    benchfs_open_meta_async="true"
  fi
  local benchfs_close_meta_async="false"
  if [ "${BENCHFS_CLOSE_META_ASYNC:-0}" = "1" ]; then
    benchfs_close_meta_async="true"
  fi
  local benchfs_sequential_chunk_rpcs="false"
  if [ "${BENCHFS_SEQUENTIAL_CHUNK_RPCS:-0}" = "1" ]; then
    benchfs_sequential_chunk_rpcs="true"
  fi
  local benchfs_disable_rdma="false"
  if [ "${BENCHFS_DISABLE_RDMA:-0}" = "1" ]; then
    benchfs_disable_rdma="true"
  fi
  local benchfs_stats_enabled="false"
  if [ "${ENABLE_STATS:-0}" = "1" ]; then
    benchfs_stats_enabled="true"
  fi

  cat > "${config_file}" <<EOF
[node]
node_id = "node0"
data_dir = "${BENCHFS_DATA_DIR}"
log_level = "info"

[storage]
chunk_size = ${chunk_bytes}
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

[transport]
backend = "${benchfs_backend}"

[locusta]
arena_size = ${benchfs_arena_size}
ring_capacity = ${benchfs_ring_capacity}
recv_ring_size = ${benchfs_recv_ring}
send_buf_size = ${benchfs_send_buf}
max_inflight = ${benchfs_max_inflight}
accept_interval_ms = ${BENCHFS_LOCUSTA_ACCEPT_INTERVAL_MS:-100}
reactor_mode = ${benchfs_reactor_mode}
dispatch_idle_sleep_us = ${BENCHFS_LOCUSTA_DISPATCH_SLEEP_US:-20}
dispatch_idle_threshold = ${BENCHFS_LOCUSTA_DISPATCH_IDLE_THRESHOLD:-16}
mlx5_auto_spread = true
skip_recv_copy = ${benchfs_skip_recv_copy}
handshake_mode = "${benchfs_handshake_mode}"
exchange_timeout_secs = ${benchfs_exchange_timeout_secs}
wait_peer_ack_strict = ${benchfs_wait_peer_ack_strict}
defer_init_prewarm = ${benchfs_defer_init_prewarm}

[iouring]
queue_size = ${BENCHFS_IOURING_QUEUE_SIZE:-2048}
submit_depth = ${BENCHFS_IOURING_SUBMIT_DEPTH:-512}
sq_poll_ms = ${BENCHFS_IOURING_SQ_POLL_MS:-200}
chunk_fd_cache_size = ${BENCHFS_CHUNK_FD_CACHE_SIZE:-0}
submit_timeout_us = ${BENCHFS_IOURING_SUBMIT_TIMEOUT_US:-1000}
complete_timeout_us = ${BENCHFS_IOURING_COMPLETE_TIMEOUT_US:-1000}

[metadata]
persist = "${BENCHFS_METADATA_PERSIST:-off}"
flush_interval_ms = ${BENCHFS_METADATA_FLUSH_MS:-50}
dirty_high_watermark = ${BENCHFS_METADATA_DIRTY_HWM:-16384}
distributed = ${benchfs_metadata_distributed}
central_parent_index = ${benchfs_central_parent_index}

[prewarm]
enabled = ${BENCHFS_PREWARM_ENABLED:-true}
concurrency = ${BENCHFS_PREWARM_CONCURRENCY:-80}
stagger_ms_per_rank = ${BENCHFS_PREWARM_STAGGER_MS:-0}

[scheduling]
reactor_poll_interval = ${BENCHFS_REACTOR_POLL_INTERVAL:-2}
status_cache_iters = ${BENCHFS_STATUS_CACHE_ITERS:-100}

[rpc]
max_concurrent_chunk_rpcs = ${BENCHFS_MAX_CONCURRENT_CHUNK_RPCS:-16}
sequential_chunk_rpcs = ${benchfs_sequential_chunk_rpcs}
disable_rdma = ${benchfs_disable_rdma}
force_rdma = false
timeout_secs = ${BENCHFS_RPC_TIMEOUT:-600}
max_retries = 0
retry_delay_ms = 100
retry_backoff = 2.0
profile = ${benchfs_rpc_profile}
force_put_writes = ${benchfs_force_put_writes}
write_eager_threshold = ${BENCHFS_WRITE_EAGER_THRESHOLD:-16384}
add_peer_timeout_secs = ${BENCHFS_ADD_PEER_TIMEOUT_SECS:-300}

[api]
open_meta_async = ${benchfs_open_meta_async}
close_meta_async = ${benchfs_close_meta_async}

[cluster]
expected_nodes = ${BENCHFS_EXPECTED_NODES:-0}

[observability]
chrome_tracing = false
integrity_log = false
integrity_dir = "/tmp"

[stats]
enabled = ${benchfs_stats_enabled}
ucx_am_breakdown = false
EOF

  # Export so children (mpirun -x BENCHFS_CONFIG) inherit it. Without this
  # both benchfsd_mpi and libbenchfs.so fell back to RuntimeConfig::default()
  # (UCX backend), masking BENCHFS_TRANSPORT env entirely.
  export BENCHFS_CONFIG="${config_file}"

  echo "==== Starting benchfsd config_id=${config_id} chunk=${chunk_bytes} bytes ===="
  echo "==== BENCHFS_CONFIG=${config_file} backend=${benchfs_backend} ===="

  # NUMA-aware data_dir wrapper (one /scrN per local rank).
  local datadir_wrapper="${server_log_dir}/benchfsd_datadir_wrapper.sh"
  export BENCHFS_INNER_BINARY="${BENCHFS_PREFIX}/benchfsd_mpi"
  cat > "${datadir_wrapper}" <<'WRAPPER_EOF'
#!/bin/bash
# Raise fd limit per-process — mpirun's remote launchers don't inherit
# the qsub-script's ulimit, so each benchfsd starts with the system soft
# default. fd cache for chunk files needs ≥65k fds per server vnode.
ulimit -n 1048576 2>/dev/null || ulimit -n 524288 2>/dev/null || ulimit -n 262144 2>/dev/null || ulimit -n 65536 2>/dev/null || true
echo "[wrapper] $(hostname) rank=${OMPI_COMM_WORLD_LOCAL_RANK:-0} fd_limit=$(ulimit -n)" >&2
# Disable core dumps — when mdtest-hard crashes, multi-GB cores would land
# on /home (Lustre) and risk filling space / disrupting SSH/Claude session.
ulimit -c 0 2>/dev/null || true
LOCAL_RANK=${OMPI_COMM_WORLD_LOCAL_RANK:-0}
# Use /scrN dirs that the PBS prologue actually created for this job.
# (PBS chunks-per-host map to per-vnode /scrN; missing ones mean that
# /scrN is owned by root with no write access on that host.)
# If a host has only 1 of /scr{0..3} created, all local vnodes end up
# piled on that single NVMe, producing 60× tail latency. The fix is at
# the qsub layer: ensure each chunk maps to a distinct vnode/scrN, not
# in the wrapper.
LOCAL_SCRATCH_DIRS=()
LOCAL_NUMA_NODES=()
for n in 0 1 2 3; do
  if [ -d "/scr${n}/${PBS_JOBID}" ] && [ -w "/scr${n}/${PBS_JOBID}" ]; then
    LOCAL_SCRATCH_DIRS+=("/scr${n}/${PBS_JOBID}")
    LOCAL_NUMA_NODES+=("${n}")
  fi
done
if [ ${#LOCAL_SCRATCH_DIRS[@]} -gt 0 ]; then
  DIR_INDEX=$((LOCAL_RANK % ${#LOCAL_SCRATCH_DIRS[@]}))
  RANK_DATA_DIR="${LOCAL_SCRATCH_DIRS[$DIR_INDEX]}"
  RANK_NUMA="${LOCAL_NUMA_NODES[$DIR_INDEX]}"
  # Warn if this host has fewer than 4 dirs (indicates load-imbalance risk)
  if [ ${#LOCAL_SCRATCH_DIRS[@]} -lt 4 ]; then
    echo "[wrapper] WARNING $(hostname) has only ${#LOCAL_SCRATCH_DIRS[@]} writable /scrN dirs: ${LOCAL_SCRATCH_DIRS[*]}" >&2
  fi
else
  IFS=',' read -ra SCRATCH_DIRS <<< "${BENCHFS_SCRATCH_DIRS}"
  DIR_INDEX=$((LOCAL_RANK % ${#SCRATCH_DIRS[@]}))
  RANK_DATA_DIR="${SCRATCH_DIRS[$DIR_INDEX]}"
  RANK_NUMA=""
fi
CONFIG_FILE="$2"
WORLD_RANK=${OMPI_COMM_WORLD_RANK:-${LOCAL_RANK}}
RANK_CONFIG="${CONFIG_FILE%.toml}_rank${WORLD_RANK}.toml"
sed "s|^data_dir = .*|data_dir = \"${RANK_DATA_DIR}\"|" "${CONFIG_FILE}" > "${RANK_CONFIG}"
if [ -n "${RANK_NUMA}" ] && command -v numactl >/dev/null 2>&1; then
  exec numactl --cpunodebind="${RANK_NUMA}" --membind="${RANK_NUMA}" \
    "${BENCHFS_INNER_BINARY}" "$1" "${RANK_CONFIG}" "${@:3}"
else
  exec "${BENCHFS_INNER_BINARY}" "$1" "${RANK_CONFIG}" "${@:3}"
fi
WRAPPER_EOF
  chmod +x "${datadir_wrapper}"

  # Server-rank density per vnode. Default 1 (one server rank per vnode,
  # NUMA-bound to local NVMe). For ior-hard's single-thread handler ceiling
  # (~85µs per 47KB op limits per-server to ~0.5 GB/s) increasing to 2 can
  # double per-host throughput; multiple ranks share the same /scr dir but
  # different node_ids in the chunk hash so chunk files don't collide.
  local server_ranks_per_vnode="${BENCHFS_SERVER_RANKS_PER_VNODE:-1}"
  local server_np=$((VNODES * server_ranks_per_vnode))
  local cmd=(
    "${cmd_mpirun_common[@]}"
    -np "$server_np"
    --bind-to none
    --oversubscribe
    -x RUST_LOG="${RUST_LOG_S}"
    -x RUST_BACKTRACE
    -x PBS_JOBID
    -x BENCHFS_CONFIG
    -x BENCHFS_SCRATCH_DIRS="${BENCHFS_SCRATCH_DIRS_CSV}"
    -x BENCHFS_INNER_BINARY
    -x PLUVIO_URING_ALWAYS_POLL
    -x LOCUSTA_DAEMON_EVENT_BUDGET
    -x BENCHFS_EXPECTED_NODES="$((VNODES * ${BENCHFS_SERVER_RANKS_PER_VNODE:-1}))"
    "${datadir_wrapper}"
    "${BENCHFS_REGISTRY_DIR}"
    "${config_file}"
  )
  echo "${cmd[@]}"
  "${cmd[@]}" > "${server_log_dir}/benchfsd_stdout.log" 2> "${server_log_dir}/benchfsd_stderr.log" &
  BENCHFSD_PID=$!

  if ! check_server_ready "$server_np"; then
    echo "ERROR: BenchFS servers failed to register"
    echo "STDERR (head):"
    head -80 "${server_log_dir}/benchfsd_stderr.log" 2>/dev/null
    stop_benchfsd
    return 1
  fi
  sleep 5
  return 0
}

# ==============================================================================
# IO500 INI generation and execution
# ==============================================================================

# Only ior-easy phases are enabled during sweep. Final run also enables ior-hard.
# We always disable mdtest/find/rnd phases since the user only asked for IOR.
write_ini() {
  local ini_path="$1"
  local data_dir="$2"
  local result_dir="$3"
  local stonewall="$4"
  local transfer_size="$5"
  local block_size="$6"
  local file_per_proc="$7"  # TRUE/FALSE
  local enable_hard="$8"    # 1 or 0
  local chunk_bytes_arg="$9"
  local enable_mdtest="${10:-0}"  # 1 or 0; 0 keeps the legacy IOR-only behavior

  local mdtest_run="FALSE"
  if [ "${enable_mdtest}" = "1" ]; then
    mdtest_run="TRUE"
  fi
  local hard_run="FALSE"
  if [ "${enable_hard}" = "1" ]; then
    hard_run="TRUE"
  fi
  # mdtest-hard heap corruption was fixed 2026-05-14 (pluvio_timer::Delay
  # in benchfs_close). Default now enabled; set SKIP_MDTEST_HARD=1 to
  # disable for legacy debugging only.
  local mdtest_hard_run="FALSE"
  if [ "${enable_mdtest}" = "1" ] && [ "${SKIP_MDTEST_HARD:-0}" != "1" ]; then
    mdtest_hard_run="TRUE"
  fi

  # SKIP_IOR=1 disables all ior-easy / ior-hard phases. Used to isolate
  # the mdtest-hard heap-corruption debug loop without paying the
  # ior-easy + ior-hard warm-up time.
  local ior_easy_run="TRUE"
  local ior_hard_run="${hard_run}"
  if [ "${SKIP_IOR:-0}" = "1" ]; then
    ior_easy_run="FALSE"
    ior_hard_run="FALSE"
  fi
  # SKIP_IOR_HARD_READ=1 keeps ior-hard-write but disables ior-hard-read.
  # Workaround for the ior-hard-read CQE error / verification instability
  # at ppn=8 (see task #73). Run it independently when investigating.
  local ior_hard_read_run="${ior_hard_run}"
  if [ "${SKIP_IOR_HARD_READ:-0}" = "1" ]; then
    ior_hard_read_run="FALSE"
  fi

  # The api line also carries BENCHFS-specific aiori options
  # (registry/datadir/chunk-size) — io500 splits api on whitespace and feeds
  # the rest into IOR's option parser.
  local api_line="BENCHFS --benchfs.registry=${BENCHFS_REGISTRY_DIR} --benchfs.datadir=${BENCHFS_DATA_DIR} --benchfs.chunk-size=${chunk_bytes_arg}"

  cat > "${ini_path}" <<EOF
[global]
datadir = ${data_dir}
timestamp-datadir = FALSE
resultdir = ${result_dir}
timestamp-resultdir = FALSE
api = ${api_line}
drop-caches = FALSE
io-buffers-on-gpu = FALSE
verbosity = 1
scc = FALSE
dataPacketType = timestamp

[debug]
stonewall-time = ${stonewall}
pause-dir =

[ior-easy]
API =
transferSize = ${transfer_size}
blockSize = ${block_size}
filePerProc = ${file_per_proc}
uniqueDir = FALSE
run = ${ior_easy_run}

[ior-easy-write]
API =
run = ${ior_easy_run}

[mdtest-easy]
API =
n = ${IO500_MDTEST_EASY_N:-1}
run = ${mdtest_run}

[mdtest-easy-write]
run = ${mdtest_run}

[find-easy]
run = ${mdtest_run}

[ior-hard]
API =
segmentCount = ${IO500_IOR_HARD_SEGMENTS:-100000}
collective =
run = ${ior_hard_run}

[ior-hard-write]
run = ${ior_hard_run}

[mdtest-hard]
n = ${IO500_MDTEST_HARD_N:-1}
run = ${mdtest_hard_run}
[mdtest-hard-write]
run = ${mdtest_hard_run}
[find]
run = ${IO500_FIND_RUN:-FALSE}
external-script = ${BENCHFS_PFIND_BIN:-}
# benchfs_pfind is MPI-aware; when BENCHFS_PFIND_NPROC>1, prefix with
# mpirun. Default empty → singleton (MPI_Init returns size=1) — avoids
# the nested-mpirun hang in job 20536 where 32 pfind ranks × 40 server
# peers = 1280 lazy add_peer handshakes against servers already serving
# the io500 clients.
external-mpi-args = ${BENCHFS_PFIND_MPI_ARGS:-}
external-extra-args = ${BENCHFS_PFIND_EXTRA:-}
[ior-easy-read]
run = ${ior_easy_run}
[mdtest-easy-stat]
run = ${mdtest_run}
[ior-hard-read]
run = ${ior_hard_read_run}
[mdtest-hard-stat]
run = ${mdtest_hard_run}
[mdtest-easy-delete]
run = ${mdtest_run}
[mdtest-hard-read]
run = ${mdtest_hard_run}
[mdtest-hard-delete]
run = ${mdtest_hard_run}
[ior-rnd4K-easy-read]
run = ${IO500_RND4K_RUN:-FALSE}
EOF
}

# Run io500 for one INI; output dir holds result.txt and stdout/stderr.
# Returns 0 on success regardless of IO500 INVALID flag — caller parses score.
run_io500() {
  local ini="$1"
  local out_dir="$2"
  local np="$3"
  mkdir -p "${out_dir}"
  # Defensive: disable core dumps. mdtest-hard heap corruption can produce
  # multi-GB cores that fill /home (Lustre) and break SSH / the supervising
  # Claude session.
  ulimit -c 0 2>/dev/null || true
  # Optional heap UB diagnostics. Two modes:
  #   IO500_ASAN_LIB=<libasan.so> — full AddressSanitizer (often fails to
  #     init on UCX/MPI hosts because ASAN's shadow remap collides with
  #     pinned-memory regions; if you see DEADLYSIGNAL during init, fall
  #     back to the malloc-check mode below).
  #   IO500_MALLOC_CHECK=1 — glibc MALLOC_CHECK_=3 + MALLOC_PERTURB_=85
  #     (cheap, no recompile, aborts with a backtrace at the first heap
  #     UB; freed memory is filled with 0x55 to surface UAF reads early).
  local asan_args=()
  if [ -n "${IO500_ASAN_LIB:-}" ] && [ -f "${IO500_ASAN_LIB}" ]; then
    asan_args=(
      -x LD_PRELOAD="${IO500_ASAN_LIB}"
      -x ASAN_OPTIONS="${IO500_ASAN_OPTIONS:-halt_on_error=1:abort_on_error=1:detect_leaks=0:print_stacktrace=1:malloc_context_size=30:disable_coredump=0:unmap_shadow_on_exit=1}"
    )
  elif [ "${IO500_MALLOC_CHECK:-0}" = "1" ]; then
    asan_args=(
      -x MALLOC_CHECK_=3
      -x MALLOC_PERTURB_=85
      -x LIBC_FATAL_STDERR_=1
    )
  fi
  local cmd=(
    "${cmd_mpirun_common[@]}"
    -np "$np"
    --bind-to none
    --oversubscribe
    -x RUST_LOG="${RUST_LOG_C}"
    -x RUST_BACKTRACE
    -x BENCHFS_CONFIG
    -x BENCHFS_EXPECTED_NODES="$((VNODES * ${BENCHFS_SERVER_RANKS_PER_VNODE:-1}))"
    -x PLUVIO_URING_ALWAYS_POLL
    -x LOCUSTA_DAEMON_EVENT_BUDGET
    -x BENCHFS_UCX_AM_BREAKDOWN
    -x BENCHFS_DHAT_DIR="${BENCHFS_DHAT_DIR:-${out_dir}/dhat}"
    # benchfs_pfind (io500 external-script for the find phase) inherits
    # rank 0's env via popen and needs the registry path to bootstrap a
    # fresh locusta client. Skipped silently when the binary isn't built.
    -x BENCHFS_REGISTRY_DIR="${BENCHFS_REGISTRY_DIR}"
    "${asan_args[@]}"
    "${IO500_DIR}/io500"
    "${ini}"
  )
  echo "Running: ${cmd[*]}"
  # Pick the stonewall that's actually in play for this invocation: final runs
  # use FINAL_STONEWALL, sweep runs use SWEEP_STONEWALL. Caller can override
  # via IO500_STONEWALL_FOR_TIMEOUT. Default for the final run is
  # FINAL_STONEWALL so all enabled phases get a budget proportional to their
  # actual stonewall (job 17043 was killed at 14 min mid-find-easy because
  # this fell back to SWEEP_STONEWALL=30 → timeout=840s).
  local default_stonewall="${SWEEP_STONEWALL}"
  if [ "${out_dir}" = "${FINAL_DIR}" ]; then
    default_stonewall="${FINAL_STONEWALL}"
  fi
  local stonewall_for_timeout="${IO500_STONEWALL_FOR_TIMEOUT:-${default_stonewall}}"
  local timeout_s=$(( stonewall_for_timeout > 0 ? stonewall_for_timeout * 12 + 600 : 1800 ))
  timeout --signal=TERM --kill-after=30 "${timeout_s}" \
    "${cmd[@]}" > "${out_dir}/io500_stdout.log" 2> "${out_dir}/io500_stderr.log" || true
}

# Parse [RESULT] lines from io500 stdout. Echoes "phase score" per line.
parse_io500_scores() {
  local stdout_log="$1"
  awk '/^\[RESULT\]/ { print $2, $3 }' "${stdout_log}" 2>/dev/null
}

# ==============================================================================
# Sweep
# ==============================================================================

# Parameter axes for the ior-easy sweep.
# benchfsd is restarted between every run so /scrN state stays clean — stonewall
# at SWEEP_STONEWALL seconds × ~hundreds of ranks × hundreds of MB/s/rank fills
# tens of TB per run, which would exhaust the 4×2.9TB×nnodes scratch otherwise.
transfer_size_list=(1m 4m 16m)
block_size_list=(1g 4g)
ppn_list=(4 16)
fpp_list=(TRUE FALSE)

# Total client procs: ppn * VNODES (we treat each vnode as 1 server slot).
NP_BASE=$VNODES

server_config_id=0
runid=0

echo "phase,run_label,transfer_size,block_size,ppn,np,filePerProc,benchfs_chunk_bytes,score_GiB_s,stonewall,exit_status" > "${SUMMARY_CSV}"

best_score=0
best_label=""
best_transfer=""
best_block=""
best_ppn=""
best_fpp=""
best_chunk_bytes=""

if [ "${SKIP_SWEEP}" = "1" ]; then
  echo ""
  echo "=========================================="
  echo "SKIP_SWEEP=1: using empirical best config:"
  echo "  transfer=${BEST_TRANSFER} block=${BEST_BLOCK} ppn=${BEST_PPN}"
  echo "  fpp=${BEST_FPP} chunk_bytes=${BEST_CHUNK_BYTES}"
  echo "=========================================="
  best_transfer="${BEST_TRANSFER}"
  best_block="${BEST_BLOCK}"
  best_ppn="${BEST_PPN}"
  best_fpp="${BEST_FPP}"
  best_chunk_bytes="${BEST_CHUNK_BYTES}"
  best_label="empirical_best"
  best_score="-"
else
for transfer_size in "${transfer_size_list[@]}"; do
  chunk_bytes=$(parse_size_to_bytes "${transfer_size}")

  for block_size in "${block_size_list[@]}"; do
    for ppn in "${ppn_list[@]}"; do
      np=$((NP_BASE * ppn))
      for fpp in "${fpp_list[@]}"; do
        run_label="t${transfer_size}_b${block_size}_p${ppn}_fpp${fpp}_chunk${transfer_size}"
        current_run_label="$run_label"
        echo ""
        echo "==== Sweep run #${runid}: ${run_label} ===="

        # Restart benchfsd so each run gets a clean scratch + clean metadata.
        stop_benchfsd
        if ! start_benchfsd "${server_config_id}" "${chunk_bytes}"; then
          echo "Skipping ${run_label} (server start failed)"
          server_config_id=$((server_config_id + 1))
          runid=$((runid + 1))
          continue
        fi

        run_dir="${SWEEP_DIR}/run_${runid}_${run_label}"
        mkdir -p "${run_dir}"
        data_dir="${BENCHFS_DATA_DIR}/io500_run${runid}"
        result_dir="${run_dir}/io500_result"
        mkdir -p "${result_dir}"

        ini="${run_dir}/config.ini"
        write_ini "${ini}" "${data_dir}" "${result_dir}" \
          "${SWEEP_STONEWALL}" "${transfer_size}" "${block_size}" "${fpp}" "0" "${chunk_bytes}"

        run_io500 "${ini}" "${run_dir}" "${np}"

        # Parse scores
        write_score=$(parse_io500_scores "${run_dir}/io500_stdout.log" | awk '$1=="ior-easy-write"{print $2}')
        read_score=$(parse_io500_scores "${run_dir}/io500_stdout.log" | awk '$1=="ior-easy-read"{print $2}')
        write_score=${write_score:-0}
        read_score=${read_score:-0}
        avg=$(awk -v w="$write_score" -v r="$read_score" 'BEGIN{ printf "%.4f", (w+r)/2 }')
        status="ok"
        if [ "$write_score" = "0" ] && [ "$read_score" = "0" ]; then
          status="failed"
        fi
        echo "  ior-easy-write=${write_score} GiB/s, ior-easy-read=${read_score} GiB/s, avg=${avg} (${status})"

        echo "ior-easy-write,${run_label},${transfer_size},${block_size},${ppn},${np},${fpp},${chunk_bytes},${write_score},${SWEEP_STONEWALL},${status}" >> "${SUMMARY_CSV}"
        echo "ior-easy-read,${run_label},${transfer_size},${block_size},${ppn},${np},${fpp},${chunk_bytes},${read_score},${SWEEP_STONEWALL},${status}" >> "${SUMMARY_CSV}"

        # Track best by avg(write,read).
        better=$(awk -v a="$avg" -v b="$best_score" 'BEGIN{print (a>b)?1:0}')
        if [ "$better" = "1" ]; then
          best_score="$avg"
          best_label="$run_label"
          best_transfer="$transfer_size"
          best_block="$block_size"
          best_ppn="$ppn"
          best_fpp="$fpp"
          best_chunk_bytes="$chunk_bytes"
        fi

        runid=$((runid + 1))
        server_config_id=$((server_config_id + 1))
      done
    done
  done
done

stop_benchfsd

echo ""
echo "=========================================="
echo "Sweep complete: ${runid} runs"
echo "Best ior-easy avg: ${best_score} GiB/s"
echo "  transfer_size=${best_transfer} block_size=${best_block} ppn=${best_ppn} fpp=${best_fpp} chunk_bytes=${best_chunk_bytes}"
echo "=========================================="
fi  # end SKIP_SWEEP=0 branch

cat > "${JOB_OUTPUT_DIR}/best_config.json" <<EOF
{
  "transfer_size": "${best_transfer}",
  "block_size": "${best_block}",
  "ppn": ${best_ppn:-0},
  "filePerProc": "${best_fpp}",
  "benchfs_chunk_bytes": ${best_chunk_bytes:-0},
  "ior_easy_avg_GiB_s": ${best_score}
}
EOF

# ==============================================================================
# Final run: best config + ior-hard with longer stonewall
# ==============================================================================
if [ -n "${best_transfer}" ]; then
  echo ""
  echo "==== Final run with best config + ior-hard ===="
  if ! start_benchfsd "final" "${best_chunk_bytes}"; then
    echo "ERROR: failed to start benchfsd for final run"
    exit 1
  fi

  np=$((NP_BASE * best_ppn))
  ini="${FINAL_DIR}/config.ini"
  data_dir="${BENCHFS_DATA_DIR}/io500_final"
  result_dir="${FINAL_DIR}/io500_result"
  mkdir -p "${result_dir}"

  # IO500_FINAL_HARD / IO500_FINAL_MDTEST let callers disable ior-hard /
  # mdtest for ior-easy-focused tuning runs (1=enable, 0=disable).
  # Use plain assignment (not `local`) — this block is not inside a function.
  final_hard="${IO500_FINAL_HARD:-1}"
  final_mdtest="${IO500_FINAL_MDTEST:-1}"
  write_ini "${ini}" "${data_dir}" "${result_dir}" \
    "${FINAL_STONEWALL}" "${best_transfer}" "${best_block}" "${best_fpp}" "${final_hard}" "${best_chunk_bytes}" "${final_mdtest}"

  current_run_label="final"
  IO500_STONEWALL_FOR_TIMEOUT="${FINAL_STONEWALL}" \
    run_io500 "${ini}" "${FINAL_DIR}" "${np}"

  echo ""
  echo "Final scores:"
  parse_io500_scores "${FINAL_DIR}/io500_stdout.log" | tee -a "${JOB_OUTPUT_DIR}/final_scores.txt"

  while read -r phase score; do
    [ -z "$phase" ] && continue
    echo "${phase},final,${best_transfer},${best_block},${best_ppn},${np},${best_fpp},${best_chunk_bytes},${score},${FINAL_STONEWALL},final" >> "${SUMMARY_CSV}"
  done < <(parse_io500_scores "${FINAL_DIR}/io500_stdout.log")

  stop_benchfsd
else
  echo "WARNING: no successful sweep config — skipping final ior-hard run"
fi

echo ""
echo "=========================================="
echo "All io500 work complete. Summary CSV: ${SUMMARY_CSV}"
echo "=========================================="
