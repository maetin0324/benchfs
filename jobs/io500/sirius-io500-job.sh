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
export BENCHFS_RPC_TIMEOUT
export BENCHFS_IOURING_QUEUE_SIZE
export BENCHFS_IOURING_SUBMIT_DEPTH

cmd_mpirun_util=(mpirun --mca routed direct --mca plm_rsh_no_tree_spawn 1 --mca pml ob1 --mca btl tcp,sm,self)
cmd_mpirun_common=(mpirun --mca routed direct --mca plm_rsh_no_tree_spawn 1 --mca pml ucx --mca btl self --mca osc ucx -x PATH -x LD_LIBRARY_PATH)

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
EOF

  echo "==== Starting benchfsd config_id=${config_id} chunk=${chunk_bytes} bytes ===="

  # NUMA-aware data_dir wrapper (one /scrN per local rank).
  local datadir_wrapper="${server_log_dir}/benchfsd_datadir_wrapper.sh"
  export BENCHFS_INNER_BINARY="${BENCHFS_PREFIX}/benchfsd_mpi"
  cat > "${datadir_wrapper}" <<'WRAPPER_EOF'
#!/bin/bash
LOCAL_RANK=${OMPI_COMM_WORLD_LOCAL_RANK:-0}
LOCAL_SCRATCH_DIRS=()
LOCAL_NUMA_NODES=()
for n in 0 1 2 3; do
  if [ -d "/scr${n}/${PBS_JOBID}" ]; then
    LOCAL_SCRATCH_DIRS+=("/scr${n}/${PBS_JOBID}")
    LOCAL_NUMA_NODES+=("${n}")
  fi
done
if [ ${#LOCAL_SCRATCH_DIRS[@]} -gt 0 ]; then
  DIR_INDEX=$((LOCAL_RANK % ${#LOCAL_SCRATCH_DIRS[@]}))
  RANK_DATA_DIR="${LOCAL_SCRATCH_DIRS[$DIR_INDEX]}"
  RANK_NUMA="${LOCAL_NUMA_NODES[$DIR_INDEX]}"
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

  # 1 server per vnode (benchfsd uses local NVMe + bound to NUMA).
  local server_np=$VNODES
  local cmd=(
    "${cmd_mpirun_common[@]}"
    -np "$server_np"
    --bind-to none
    --oversubscribe
    -x RUST_LOG="${RUST_LOG_S}"
    -x RUST_BACKTRACE
    -x PBS_JOBID
    -x BENCHFS_SCRATCH_DIRS="${BENCHFS_SCRATCH_DIRS_CSV}"
    -x BENCHFS_INNER_BINARY
    -x BENCHFS_RPC_TIMEOUT
    -x BENCHFS_IOURING_QUEUE_SIZE
    -x BENCHFS_IOURING_SUBMIT_DEPTH
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
  # mdtest-hard is currently unstable (client-side heap corruption with
  # 100k+ tiny file creates per phase). Keep mdtest-easy on and gate the
  # hard variant separately so the easy phases still produce results.
  local mdtest_hard_run="FALSE"
  if [ "${enable_mdtest}" = "1" ] && [ "${SKIP_MDTEST_HARD:-1}" != "1" ]; then
    mdtest_hard_run="TRUE"
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
run = TRUE

[ior-easy-write]
API =
run = TRUE

[mdtest-easy]
API =
n = 1
run = ${mdtest_run}

[mdtest-easy-write]
run = ${mdtest_run}

[find-easy]
run = ${mdtest_run}

[ior-hard]
API =
segmentCount = 100000
collective =
run = ${hard_run}

[ior-hard-write]
run = ${hard_run}

[mdtest-hard]
run = ${mdtest_hard_run}
[mdtest-hard-write]
run = ${mdtest_hard_run}
[find]
run = ${mdtest_hard_run}
[ior-easy-read]
run = TRUE
[mdtest-easy-stat]
run = ${mdtest_run}
[ior-hard-read]
run = ${hard_run}
[mdtest-hard-stat]
run = ${mdtest_hard_run}
[mdtest-easy-delete]
run = ${mdtest_run}
[mdtest-hard-read]
run = ${mdtest_hard_run}
[mdtest-hard-delete]
run = ${mdtest_hard_run}
[ior-rnd4K-easy-read]
run = FALSE
EOF
}

# Run io500 for one INI; output dir holds result.txt and stdout/stderr.
# Returns 0 on success regardless of IO500 INVALID flag — caller parses score.
run_io500() {
  local ini="$1"
  local out_dir="$2"
  local np="$3"
  mkdir -p "${out_dir}"
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
    -x BENCHFS_EXPECTED_NODES="${VNODES}"
    -x BENCHFS_RPC_TIMEOUT
    "${asan_args[@]}"
    "${IO500_DIR}/io500"
    "${ini}"
  )
  echo "Running: ${cmd[*]}"
  # Pick the stonewall that's actually in play for this invocation: final runs
  # use FINAL_STONEWALL, sweep runs use SWEEP_STONEWALL. The previous formula
  # (SWEEP_STONEWALL*6+600) capped the final run at 780s and killed io500
  # mid-ior-easy-write when FINAL_STONEWALL=120 and multiple phases were enabled.
  local stonewall_for_timeout="${IO500_STONEWALL_FOR_TIMEOUT:-${SWEEP_STONEWALL}}"
  local timeout_s=$(( stonewall_for_timeout > 0 ? stonewall_for_timeout * 8 + 600 : 1800 ))
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
  local final_hard="${IO500_FINAL_HARD:-1}"
  local final_mdtest="${IO500_FINAL_MDTEST:-1}"
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
