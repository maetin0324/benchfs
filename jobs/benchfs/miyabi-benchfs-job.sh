#!/bin/bash
#------- qsub option -----------
#PBS -W group_list="xg24i002"
#PBS -q debug-g
#PBS -V
#------- Program execution -----------
set -euo pipefail

cleanup_exported_bash_functions() {
  # PBS -V exports bash functions (module/ml) as environment variables.
  # Open MPI spawns /bin/sh on remote nodes, which fails to import them and
  # causes launches like the one in miyabi-benchfs-job.sh.e1074667 to abort.
  local entry var_name func_name
  while IFS= read -r -d '' entry; do
    var_name=${entry%%=*}
    [[ "${var_name}" == BASH_FUNC_*%% ]] || continue
    func_name=${var_name#BASH_FUNC_}
    func_name=${func_name%%%}
    unset -f "${func_name}" 2>/dev/null || true
    unset "${var_name}" 2>/dev/null || true
  done < <(env -0)
}

# Increase file descriptor limit for large-scale MPI jobs
# This prevents FD exhaustion when running with high ppn values
# ulimit -n 65536

# set gcc and OpenMPI modules
module purge
module load gcc-toolset/14
module load ompi-cuda/4.1.6-12.6
cleanup_exported_bash_functions

unset OMPI_MCA_mca_base_env_list

SCRIPT_DIR="/work/xg24i002/x10043/workspace/rust/benchfs/jobs/benchfs"
JOB_FILE="/work/xg24i002/x10043/workspace/rust/benchfs/jobs/benchfs/miyabi-benchfs-job.sh"
PROJECT_ROOT="/work/xg24i002/x10043/workspace/rust/benchfs"
OUTPUT_DIR="$PROJECT_ROOT/results/benchfs/${TIMESTAMP}-${PBS_JOBID}"
BACKEND_DIR="$PROJECT_ROOT/backend/benchfs"
BENCHFS_PREFIX="${PROJECT_ROOT}/target/release"
IOR_PREFIX="${PROJECT_ROOT}/ior_integration/ior"

# Requires
# - SCRIPT_DIR
# - OUTPUT_DIR
# - BACKEND_DIR
# - BENCHFS_PREFIX
# - IOR_PREFIX

source "$SCRIPT_DIR/common.sh"
# NOTE: Disabled process substitution to avoid FD leak
# exec 1> >(addtimestamp)
# exec 2> >(addtimestamp >&2)

JOB_START=$(timestamp)
NNODES=$(wc --lines "${PBS_NODEFILE}" | awk '{print $1}')
JOBID=$(echo "$PBS_JOBID" | cut -d : -f 2)
JOB_OUTPUT_DIR="${OUTPUT_DIR}/${JOB_START}-${PBS_JOBID}-${MPI_PROC}"
JOB_BACKEND_DIR="${BACKEND_DIR}/$(basename -- "${JOB_OUTPUT_DIR}")"
BENCHFS_REGISTRY_DIR="${JOB_BACKEND_DIR}/registry"
BENCHFS_DATA_DIR="/local"
BENCHFSD_LOG_BASE_DIR="${JOB_OUTPUT_DIR}/benchfsd_logs"
IOR_OUTPUT_DIR="${JOB_OUTPUT_DIR}/ior_results"

# ==============================================================================
# 1. トランスポート層設定（最優先）
# ==============================================================================
# UD/DC を許可すると DCI QP で Input/output error が発生し、返信が
# 捨てられて RPC がハングする。GPU (cuda_copy) も無効化し CPU 専用にする。

detect_active_ib() {
  if command -v ibstat >/dev/null 2>&1; then
    if ibstat 2>/dev/null | grep -q "State:.*Active"; then
      return 0
    fi
  fi

  if command -v ibv_devinfo >/dev/null 2>&1; then
    if ibv_devinfo 2>/dev/null | grep -q "state:.*PORT_ACTIVE"; then
      return 0
    fi
  fi

  if compgen -G "/sys/class/infiniband/*/ports/*/state" >/dev/null; then
    while IFS= read -r state_file; do
      if grep -q "ACTIVE" "$state_file" 2>/dev/null; then
        return 0
      fi
    done < <(find /sys/class/infiniband -maxdepth 3 -name state -print)
  fi

  return 1
}

detect_ib_device() {
  if command -v ibdev2netdev >/dev/null 2>&1; then
    local selection
    selection=$(
      ibdev2netdev 2>/dev/null | awk '
        /==>/ {
          last_dev=$1;
          last_port=$3;
          if ($0 ~ /\(Up\)/) {
            printf "%s:%s\n", $1, $3;
            exit;
          }
        }
        END {
          if (NR > 0 && last_dev != "" && last_port != "") {
            printf "%s:%s\n", last_dev, last_port;
          }
        }'
    )
    if [[ -n "${selection}" ]]; then
      echo "${selection}"
      return
    fi
  fi

  local first_device
  first_device=$(ls /sys/class/infiniband 2>/dev/null | head -n 1)
  if [[ -n "${first_device}" ]]; then
    if [[ -d "/sys/class/infiniband/${first_device}/ports/1" ]]; then
      echo "${first_device}:1"
    else
      echo "${first_device}"
    fi
  fi
}

detect_primary_netdev() {
  command -v ip >/dev/null 2>&1 || return
  ip route get 1.1.1.1 2>/dev/null \
    | awk '{for (i = 1; i <= NF; i++) if ($i == "dev") {print $(i + 1); exit}}'
}

should_override_ucx_net_devices() {
  local current="${UCX_NET_DEVICES:-}"
  if [[ -z "${current}" ]]; then
    return 0
  fi

  local lower=${current,,}
  if [[ "${lower}" == "all" || "${lower}" == "auto" ]]; then
    return 0
  fi

  return 1
}

if [[ -z "${UCX_TLS:-}" ]]; then
  if detect_active_ib; then
    export UCX_TLS="rc_mlx5,rc_verbs,self"
  else
    export UCX_TLS="tcp,self"
  fi
fi

# UCX が GPU メモリタイプを誤検出しないように memtype cache を無効化
export UCX_MEMTYPE_CACHE="n"

# UCX が勝手に net device を切り替えないよう、RC 使用時はデバイスも固定
if should_override_ucx_net_devices; then
  if [[ "${UCX_TLS}" == *rc* ]]; then
    ib_device=$(detect_ib_device)
    if [[ -n "${ib_device}" ]]; then
      export UCX_NET_DEVICES="${ib_device}"
    else
      primary_netdev=$(detect_primary_netdev)
      if [[ -n "${primary_netdev}" ]]; then
        export UCX_NET_DEVICES="${primary_netdev}"
      else
        export UCX_NET_DEVICES="all"
      fi
    fi
  else
    primary_netdev=$(detect_primary_netdev)
    if [[ -n "${primary_netdev}" ]]; then
      export UCX_NET_DEVICES="${primary_netdev}"
    else
      export UCX_NET_DEVICES="all"
    fi
  fi

  echo "Auto-selected UCX_NET_DEVICES=${UCX_NET_DEVICES}"
else
  echo "UCX_NET_DEVICES preset to ${UCX_NET_DEVICES}, leaving unchanged"
fi

# 明示的に UD/DC を使わせない
export UCX_PROTOS="^ud,dc"

# ==============================================================================
# 2. タイムアウトとリトライ設定
# ==============================================================================
# UCXのデフォルト値では不十分な場合があるため、増加

export UCX_RC_TIMEOUT=2.0s               # タイムアウト時間（デフォルト: 1.0s）
export UCX_RC_RETRY_COUNT=16             # リトライ回数（デフォルト: 7）
export UCX_RC_TIMEOUT_MULTIPLIER=4.0     # タイムアウト乗数（デフォルト: 2.0）

# ==============================================================================
# 3. Active Message設定
# ==============================================================================
# Active Messageのバッファサイズとプロトコル閾値を最適化

export UCX_AM_MAX_SHORT=128              # Short AMの最大サイズ (デフォルト: 128B)
export UCX_AM_MAX_EAGER=8192             # Eager AMの最大サイズ (8KB)
# export UCX_RNDV_THRESH=16384             # Rendezvous閾値 (16KB)
export UCX_RNDV_THRESH=inf              # Rendezvousプロトコル無効化（全てEagerに）

# AMストリームのキューサイズ
export UCX_AM_SEND_QUEUE_SIZE=1024       # 送信キューサイズ
export UCX_AM_RECV_QUEUE_SIZE=1024       # 受信キューサイズ

# ==============================================================================
# 4. RDMA設定
# ==============================================================================
# ゼロコピーとRendezvousプロトコルの最適化

export UCX_ZCOPY_THRESH=0                # ゼロコピー常時有効（0 = 常時）
export UCX_RNDV_SCHEME=get_zcopy         # Rendezvous方式: GET with zero-copy

# InfiniBand固有設定
export UCX_IB_NUM_PATHS=2                # IBパス数
export UCX_RC_MLX5_TM_ENABLE=y           # タグマッチングハードウェア加速
export UCX_RC_MLX5_RX_QUEUE_LEN=4096     # 受信キューの長さ（デフォルト: 1024）

# ==============================================================================
# 5. メモリ登録キャッシュ
# ==============================================================================
# memtype cache は GPU 誤検出を避けるためセクション1で n に設定済み
export UCX_RCACHE_ENABLE=y               # 登録キャッシュ有効

# ==============================================================================
# 6. フロー制御
# ==============================================================================
export UCX_RC_FC_ENABLE=y                # フロー制御有効化
export UCX_RC_MAX_NUM_EPS=-1             # エンドポイント数無制限

# ==============================================================================
# 7. ネットワーク層設定
# ==============================================================================
export UCX_IB_SEG_SIZE=8192              # IBセグメントサイズ (8KB)
export UCX_RC_PATH_MTU=4096              # Path MTU (4KB推奨)

# RoCE使用時（必要に応じて有効化）
# export UCX_IB_GID_INDEX=0              # GIDインデックス

# ==============================================================================
# 8. プログレス設定
# ==============================================================================
export UCX_ADAPTIVE_PROGRESS=y           # アダプティブプログレス
export UCX_ASYNC_MAX_EVENTS=256          # 非同期イベント最大数

# シングルスレッドの場合（MPIプロセス内でスレッド不使用）
export UCX_USE_MT_MUTEX=n                # マルチスレッドmutex無効

# UCX Configuration for avoiding Rendezvous protocol issues
# - UCX_TLS: Use only TCP, shared memory, and self transports (avoid InfiniBand)
# - UCX_RNDV_THRESH: Set to inf to disable Rendezvous protocol completely
#   This forces all messages to use Eager protocol, which is compatible
#   with current implementation
UCX_LOG_LEVEL="DEBUG"

export UCX_LOG_LEVEL
export UCX_RNDV_THRESH

# Calculate project root from SCRIPT_DIR and set LD_LIBRARY_PATH dynamically
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
export LD_LIBRARY_PATH="${PROJECT_ROOT}/target/release:${LD_LIBRARY_PATH:-}"

# IFS=" " read -r -a nqsii_mpiopts_array <<<"$NQSII_MPIOPTS"

echo "prepare the output directory: ${JOB_OUTPUT_DIR}"
mkdir -p "${JOB_OUTPUT_DIR}"
cp "$0" "${JOB_OUTPUT_DIR}"
cp "${PBS_NODEFILE}" "${JOB_OUTPUT_DIR}"
cp "${SCRIPT_DIR}/common.sh" "${JOB_OUTPUT_DIR}"
printenv >"${JOB_OUTPUT_DIR}/env.txt"

# Debug: Print key variables
echo "=========================================="
echo "Job Configuration"
echo "=========================================="
echo "BENCHFS_PREFIX: ${BENCHFS_PREFIX}"
echo "IOR_PREFIX: ${IOR_PREFIX}"
echo "BACKEND_DIR: ${BACKEND_DIR}"
echo "Registry: ${BENCHFS_REGISTRY_DIR}"
echo "Data: ${BENCHFS_DATA_DIR}"
echo ""
echo "Checking binary:"
ls -la "${BENCHFS_PREFIX}/benchfsd_mpi" || echo "ERROR: Binary not found at ${BENCHFS_PREFIX}/benchfsd_mpi"
echo ""
echo "Checking IOR:"
ls -la "${IOR_PREFIX}/src/ior" || echo "ERROR: IOR not found at ${IOR_PREFIX}/src/ior"
echo "=========================================="
echo ""
echo "=========================================="
echo "MPI Configuration Diagnostics"
echo "=========================================="
# echo "NQSII_MPIOPTS: ${NQSII_MPIOPTS:-<not set>}"
# echo "NQSII_MPIOPTS_ARRAY (${#nqsii_mpiopts_array[@]} elements):"
# for i in "${!nqsii_mpiopts_array[@]}"; do
#   echo "  [$i] = ${nqsii_mpiopts_array[$i]}"
# done
echo "=========================================="
echo ""

echo "prepare backend dir: ${JOB_BACKEND_DIR}"
mkdir -p "${JOB_BACKEND_DIR}"
trap 'rm -rf "${JOB_BACKEND_DIR}" ; exit 1' 1 2 3 15
trap 'rm -rf "${JOB_BACKEND_DIR}" ; exit 0' EXIT

echo "prepare benchfs registry dir: ${BENCHFS_REGISTRY_DIR}"
mkdir -p "${BENCHFS_REGISTRY_DIR}"

echo "prepare benchfs data dir: ${BENCHFS_DATA_DIR}"
mkdir -p "${BENCHFS_DATA_DIR}"

echo "prepare benchfsd log dir: ${BENCHFSD_LOG_BASE_DIR}"
mkdir -p "${BENCHFSD_LOG_BASE_DIR}"

echo "prepare ior output dir: ${IOR_OUTPUT_DIR}"
mkdir -p "${IOR_OUTPUT_DIR}"

save_job_metadata() {
  cat <<EOS >"${JOB_OUTPUT_DIR}"/job_metadata_${runid}.json
{
  "jobid": "$JOBID",
  "runid": ${runid},
  "benchfs_chunk_size": ${benchfs_chunk_size},
  "job_start_time": "${JOB_START}",
  "nnodes": ${NNODES}
}
EOS
}

# Network Configuration
# ==================================================
# FIX for IOR JSON output hang issue:
#
# ISSUE: Open MPI 4.1.8 has a conflict between openib BTL and UCX when both
# are enabled simultaneously. This causes OpenFabrics initialization errors
# and leads to MPI communication deadlocks during IOR result gathering.
#
# ERROR: "WARNING: There was an error initializing an OpenFabrics device."
# SYMPTOM: Hangs during JSON output (MPI_Gather/MPI_Allreduce)
#
# SOLUTION: Use optimized MPI settings with immediate mitigation variables.

# Immediate mitigation environment variables
export OMPI_MCA_mpi_yield_when_idle=1
export OMPI_MCA_btl_base_warn_component_unused=0
export OMPI_MCA_mpi_show_handle_leaks=0

export RUST_LOG=Trace
export RUST_BACKTRACE=full

# MPI Configuration Fix for UCX Transport Layer Issues
# ==================================================
# Automatically detect whether UCX PML is available; fall back to TCP/ob1 if not.

supports_ucx_pml() {
  command -v ompi_info >/dev/null 2>&1 || return 1
  ompi_info --param pml all --level 9 2>/dev/null | grep -q "mca:pml:.*ucx"
}

if supports_ucx_pml; then
  USE_UCX_PML=1
  echo "UCX PML detected – using --mca pml ucx configuration"
else
  USE_UCX_PML=0
  echo "WARNING: UCX PML not available – falling back to ob1/tcp configuration"
fi

if [[ "${USE_UCX_PML}" -eq 1 ]]; then
  cmd_mpirun_common=(
    mpirun
    # "${nqsii_mpiopts_array[@]}"
    --mca mca_base_env_list ""
    --mca pml ucx
    --mca btl self
    --mca osc ucx
    -x UCX_TLS
    -x UCX_NET_DEVICES
    -x UCX_MEMTYPE_CACHE
    -x UCX_PROTOS
    # -x UCX_LOG_LEVEL
    -x UCX_RNDV_THRESH
    -x UCX_RNDV_SCHEME
    -x UCX_RC_TIMEOUT
    -x UCX_RC_RETRY_COUNT
    -x UCX_RC_TIMEOUT_MULTIPLIER
    -x UCX_AM_MAX_SHORT
    -x UCX_AM_MAX_EAGER
    -x PATH
    -x LD_LIBRARY_PATH
  )
else
  cmd_mpirun_common=(
    mpirun
    "${nqsii_mpiopts_array[@]}"
    --mca pml ob1
    --mca btl tcp,vader,self
    --mca btl_openib_allow_ib 0
    -x PATH
    -x LD_LIBRARY_PATH
  )
fi

# Kill any previous benchfsd instances
cmd_mpirun_kill=(
  "${cmd_mpirun_common[@]}"
  -np "$NNODES"
  -map-by ppr:1:node
  pkill -9 benchfsd_mpi
)

echo "Kill any previous benchfsd instances"
"${cmd_mpirun_kill[@]}" || true

# Load benchmark parameters from configuration file
PARAM_FILE="${PARAM_FILE:-${SCRIPT_DIR}/default_params.conf}"
if [ -f "$PARAM_FILE" ]; then
    echo "Loading parameters from: $PARAM_FILE"
    source "$PARAM_FILE"
else
    echo "WARNING: Parameter file not found: $PARAM_FILE"
    echo "Using built-in default parameters"
    # Fallback default values
    # NOTE: block_size must be a multiple of transfer_size
    # WARNING: transfer_size > 4m can cause UCX Active Message deadlock
    transfer_size_list=(4m)
    block_size_list=(64m 256m 512m 1g)
    ppn_list=(1 2 4)
    ior_flags_list=("-w -r -F")
    benchfs_chunk_size_list=(4194304 16777216)
fi

# Save parameter configuration for reproducibility
cat > "${JOB_OUTPUT_DIR}/parameters.json" <<EOF
{
  "parameter_file": "$PARAM_FILE",
  "transfer_sizes": [$(printf '"%s",' "${transfer_size_list[@]}" | sed 's/,$//; s/,$//')],
  "block_sizes": [$(printf '"%s",' "${block_size_list[@]}" | sed 's/,$//; s/,$//')],
  "ppn_values": [$(printf '%s,' "${ppn_list[@]}" | sed 's/,$//; s/,$//')],
  "ior_flags": [$(printf '"%s",' "${ior_flags_list[@]}" | sed 's/,$//; s/,$//')],
  "chunk_sizes": [$(printf '%s,' "${benchfs_chunk_size_list[@]}" | sed 's/,$//; s/,$//')]
}
EOF

check_server_ready() {
  local max_attempts=60
  local attempt=0

  while [ $attempt -lt $max_attempts ]; do
    local ready_count=$(find "${BENCHFS_REGISTRY_DIR}" -name "node_*.addr" -type f 2>/dev/null | wc -l)

    if [ "$ready_count" -eq "$NNODES" ]; then
      echo "BenchFS servers registered: $ready_count/$NNODES nodes"

      # Additional wait for RPC handler initialization
      # All nodes have registered, but their RPC handlers may still be initializing
      # This prevents "connection refused" errors when 256 clients connect simultaneously
      local rpc_wait_time=10
      echo "Waiting ${rpc_wait_time}s for RPC handler initialization..."
      sleep $rpc_wait_time

      echo "BenchFS servers are fully ready"
      return 0
    fi

    echo "Waiting for BenchFS servers: $ready_count/$NNODES nodes (attempt $((attempt+1))/$max_attempts)"
    sleep 1
    attempt=$((attempt + 1))
  done

  echo "ERROR: BenchFS servers failed to start after $max_attempts seconds"
  return 1
}

runid=0
for benchfs_chunk_size in "${benchfs_chunk_size_list[@]}"; do
  for ppn in "${ppn_list[@]}"; do
    np=$((NNODES * ppn))

    for transfer_size in "${transfer_size_list[@]}"; do
      for block_size in "${block_size_list[@]}"; do
        for ior_flags in "${ior_flags_list[@]}"; do
          echo "=========================================="
          echo "Run ID: $runid"
          echo "Nodes: $NNODES, PPN: $ppn, NP: $np"
          echo "Transfer size: $transfer_size, Block size: $block_size"
          echo "IOR flags: $ior_flags"
          echo "BenchFS chunk size: $benchfs_chunk_size bytes"
          echo "=========================================="

          # Clean up previous run
          rm -rf "${BENCHFS_REGISTRY_DIR}"/*
          # rm -rf "${BENCHFS_DATA_DIR}"/*

          run_log_dir="${BENCHFSD_LOG_BASE_DIR}/run_${runid}"
          mkdir -p "${run_log_dir}"

          # Create BenchFS config file for this run
          config_file="${JOB_OUTPUT_DIR}/benchfs_${runid}.toml"
          cat > "${config_file}" <<EOF
[node]
node_id = "node0"
data_dir = "${BENCHFS_DATA_DIR}"
log_level = "info"

[storage]
chunk_size = ${benchfs_chunk_size}
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

          # Launch BenchFS servers
          echo "Launching BenchFS servers..."
          echo "Registry dir: ${BENCHFS_REGISTRY_DIR}"
          echo "Config file: ${config_file}"
          echo "Binary: ${BENCHFS_PREFIX}/benchfsd_mpi"

          # UCX/Network Debug Information (only on first run for efficiency)
          if [ "$runid" -eq 0 ]; then
            echo ""
            echo "=========================================="
            echo "Network Configuration Debug Information"
            echo "=========================================="
            echo "UCX Configuration:"
            echo "  UCX_TLS=${UCX_TLS:-not set}"
            echo "  UCX_NET_DEVICES=${UCX_NET_DEVICES:-not set}"
            echo ""
            echo "Available network interfaces:"
            ip -o -4 addr show | awk '{print "  " $2 " : " $4}' || echo "  Unable to list interfaces"
            echo ""
            echo "InfiniBand status:"
            which ibstat >/dev/null 2>&1 && ibstat 2>/dev/null | grep -E "State:|Physical state:" | head -4 || echo "  InfiniBand not available or ibstat not found"
            echo ""
            echo "UCX info (if available):"
            which ucx_info >/dev/null 2>&1 && ucx_info -v 2>/dev/null | head -3 || echo "  ucx_info not available"
            echo "=========================================="
            echo ""
          fi

          # Verify files exist
          ls -la "${BENCHFS_REGISTRY_DIR}" || echo "WARNING: Registry dir not accessible"
          ls -la "${config_file}" || echo "WARNING: Config file not found"
          ls -la "${BENCHFS_PREFIX}/benchfsd_mpi" || echo "WARNING: Binary not found"

          cmd_benchfsd=(
            "${cmd_mpirun_common[@]}"
            -np "$NNODES"
            --bind-to none
            -map-by ppr:1:node
            -x RUST_LOG
            -x RUST_BACKTRACE
            # Note: PATH and LD_LIBRARY_PATH are already set in cmd_mpirun_common
            "${BENCHFS_PREFIX}/benchfsd_mpi"
            "${BENCHFS_REGISTRY_DIR}"
            "${config_file}"
          )

          echo "${cmd_benchfsd[@]}"
          "${cmd_benchfsd[@]}" > "${run_log_dir}/benchfsd_stdout.log" 2> "${run_log_dir}/benchfsd_stderr.log" &
          BENCHFSD_PID=$!

          # Wait for servers to be ready
          if ! check_server_ready; then
            echo "ERROR: BenchFS servers failed to start"
            echo "=========================================="
            echo "BenchFS Server STDOUT:"
            echo "=========================================="
            cat "${run_log_dir}/benchfsd_stdout.log" || echo "No stdout log"
            echo ""
            echo "=========================================="
            echo "BenchFS Server STDERR:"
            echo "=========================================="
            cat "${run_log_dir}/benchfsd_stderr.log" || echo "No stderr log"
            echo ""
            echo "=========================================="
            echo "Registry Directory Contents:"
            echo "=========================================="
            ls -la "${BENCHFS_REGISTRY_DIR}/" || echo "Cannot access registry"
            echo ""
            echo "=========================================="
            echo "Data Directory Contents:"
            echo "=========================================="
            ls -la "${BENCHFS_DATA_DIR}/" || echo "Cannot access data dir"
            echo ""
            kill $BENCHFSD_PID 2>/dev/null || true
            wait $BENCHFSD_PID 2>/dev/null || true
            exit 1
          fi

          # Give servers a bit more time to fully initialize
          sleep 5

          # MPI Debug: Testing MPI communication before IOR
          echo "MPI Debug: Testing MPI communication before IOR"
          "${cmd_mpirun_common[@]}" -np "$np" --map-by "ppr:${ppn}:node" hostname > "${IOR_OUTPUT_DIR}/mpi_test_${runid}.txt" 2>&1
          echo "MPI Debug: Communication test completed"

          # Run IOR benchmark
          echo "Running IOR benchmark..."
          ior_json_file="${IOR_OUTPUT_DIR}/ior_result_${runid}.json"
          ior_stdout_file="${IOR_OUTPUT_DIR}/ior_stdout_${runid}.log"

          cmd_ior=(
            time_json -o "${JOB_OUTPUT_DIR}/time_${runid}.json"
            "${cmd_mpirun_common[@]}"
            -np "$np"
            --bind-to none
            --map-by "ppr:${ppn}:node"
            -x RUST_LOG
            -x RUST_BACKTRACE
            # Note: PATH and LD_LIBRARY_PATH are already set in cmd_mpirun_common
            "${IOR_PREFIX}/src/ior"
            -vvv
            -a BENCHFS
            -t "$transfer_size"
            -b "$block_size"
            $ior_flags
            --benchfs.registry="${BENCHFS_REGISTRY_DIR}"
            --benchfs.datadir="${BENCHFS_DATA_DIR}"
            -o "${BENCHFS_DATA_DIR}/testfile"
            -O summaryFormat=JSON
            -O summaryFile="${ior_json_file}"
          )

          save_job_metadata

          echo "${cmd_ior[@]}"
          # NOTE: Use simple redirection instead of process substitution to avoid FD leak
          "${cmd_ior[@]}" \
            > "${ior_stdout_file}" \
            2> "${IOR_OUTPUT_DIR}/ior_stderr_${runid}.log" || true

          # Stop BenchFS servers gracefully
          # Using SIGTERM first allows graceful shutdown, then SIGKILL as fallback
          # This prevents MPI from reporting killed processes as errors
          echo "Stopping BenchFS servers..."

          # First, try graceful shutdown with SIGTERM via mpirun
          # This ensures all nodes receive the signal properly
          "${cmd_mpirun_common[@]}" -np "$NNODES" -map-by ppr:1:node \
            pkill -TERM benchfsd_mpi 2>/dev/null || true

          # Wait for graceful shutdown (benchfsd handles SIGTERM)
          sleep 3

          # Kill the mpirun process that launched benchfsd
          kill $BENCHFSD_PID 2>/dev/null || true
          wait $BENCHFSD_PID 2>/dev/null || true

          # Force cleanup of any orphaned processes with SIGKILL
          # Only use this as a last resort after graceful shutdown attempt
          echo "Force cleanup of orphaned processes..."
          "${cmd_mpirun_common[@]}" -np "$NNODES" -map-by ppr:1:node \
            pkill -9 benchfsd_mpi 2>/dev/null || true

          # Also clean up local processes (for any edge cases)
          pkill -9 benchfsd_mpi 2>/dev/null || true

          # Wait for cleanup and FD release
          sleep 3

          runid=$((runid + 1))
        done
      done
    done
  done
done

echo "All benchmarks completed"
