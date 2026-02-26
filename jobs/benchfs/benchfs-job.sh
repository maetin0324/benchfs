#!/bin/bash
#------- qsub option -----------
#PBS -A NBB
#PBS -l elapstim_req=12:00:00
#PBS -T openmpi
#PBS -v NQSV_MPI_VER=4.1.8/gcc11.4.0-cuda12.8.1
#------- Program execution -----------
set -euo pipefail

# Increase file descriptor limit for large-scale MPI jobs
# This prevents FD exhaustion when running with high ppn values
# For large block sizes (16g+) with many clients (256+), we need more FDs:
#   16GiB / 32MiB chunk = 512 chunks per file
#   256 clients * 512 chunks = 131,072 potential open files
# Setting to 1M to be safe for future scaling
ulimit -n 1048576 2>/dev/null || ulimit -n 524288 2>/dev/null || ulimit -n 262144 2>/dev/null || ulimit -n 65536
echo "File descriptor limit: $(ulimit -n)"

module purge
module load "openmpi/$NQSV_MPI_VER"

# Requires
# - SCRIPT_DIR
# - OUTPUT_DIR
# - BACKEND_DIR
# - BENCHFS_PREFIX
# - IOR_PREFIX
# Optional:
# - ENABLE_PERFETTO (default: 0) - Set to 1 to enable Perfetto tracing (task-level tracks)
# - ENABLE_CHROME (default: 0) - Set to 1 to enable Chrome trace format
# - RUST_LOG_S (default: info) - RUST_LOG level for server (benchfsd_mpi)
# - RUST_LOG_C (default: warn) - RUST_LOG level for client (IOR)

source "$SCRIPT_DIR/common.sh"
# NOTE: Disabled process substitution to avoid FD leak
# exec 1> >(addtimestamp)
# exec 2> >(addtimestamp >&2)

JOB_START=$(timestamp)
NNODES=$(wc --lines "${PBS_NODEFILE}" | awk '{print $1}')
JOBID=$(echo "$PBS_JOBID" | cut -d : -f 2)
JOB_OUTPUT_DIR="${OUTPUT_DIR}/${JOB_START}-${JOBID}-${NNODES}"
JOB_BACKEND_DIR="${BACKEND_DIR}/$(basename -- "${JOB_OUTPUT_DIR}")"
BENCHFS_REGISTRY_DIR="${JOB_BACKEND_DIR}/registry"
BENCHFS_DATA_DIR="/scr"
BENCHFSD_LOG_BASE_DIR="${JOB_OUTPUT_DIR}/benchfsd_logs"
IOR_OUTPUT_DIR="${JOB_OUTPUT_DIR}/ior_results"
PERFETTO_OUTPUT_DIR="${JOB_OUTPUT_DIR}/perfetto"
STATS_OUTPUT_DIR="${JOB_OUTPUT_DIR}/stats"

# Default trace format flags to 0 if not set
: ${ENABLE_PERFETTO:=0}
: ${ENABLE_CHROME:=0}
# Default RUST_LOG levels for server and client
: ${RUST_LOG_S:=info}
: ${RUST_LOG_C:=warn}
# Taskset configuration for CPU pinning
: ${TASKSET:=0}
: ${TASKSET_CORES:=0,1}
# Node diagnostics configuration
: ${ENABLE_NODE_DIAGNOSTICS:=0}
: ${FIO_RUNTIME:=30}
# Separate server/client node mode
: ${SEPARATE:=0}
: ${SERVER_NODES:=}
: ${CLIENT_NODES:=}

# ==============================================================================
# 1. トランスポート層設定（最優先）
# ==============================================================================
# NOTE: UCX_* 環境変数を全てコメントアウト
# remote_io_benchプロジェクトでも同様の現象が発生し、UCX_*をコメントアウトしたところ動いた
# UD/DC を許可すると DCI QP で Input/output error が発生し、返信が
# 捨てられて RPC がハングする。GPU (cuda_copy) も無効化し CPU 専用にする。

# detect_active_ib() {
#   if command -v ibstat >/dev/null 2>&1; then
#     if ibstat 2>/dev/null | grep -q "State:.*Active"; then
#       return 0
#     fi
#   fi
#
#   if command -v ibv_devinfo >/dev/null 2>&1; then
#     if ibv_devinfo 2>/dev/null | grep -q "state:.*PORT_ACTIVE"; then
#       return 0
#     fi
#   fi
#
#   if compgen -G "/sys/class/infiniband/*/ports/*/state" >/dev/null; then
#     while IFS= read -r state_file; do
#       if grep -q "ACTIVE" "$state_file" 2>/dev/null; then
#         return 0
#       fi
#     done < <(find /sys/class/infiniband -maxdepth 3 -name state -print)
#   fi
#
#   return 1
# }
#
# detect_ib_device() {
#   if command -v ibdev2netdev >/dev/null 2>&1; then
#     local selection
#     selection=$(
#       ibdev2netdev 2>/dev/null | awk '
#         /==>/ {
#           last_dev=$1;
#           last_port=$3;
#           if ($0 ~ /\(Up\)/) {
#             printf "%s:%s\n", $1, $3;
#             exit;
#           }
#         }
#         END {
#           if (NR > 0 && last_dev != "" && last_port != "") {
#             printf "%s:%s\n", last_dev, last_port;
#           }
#         }'
#     )
#     if [[ -n "${selection}" ]]; then
#       echo "${selection}"
#       return
#     fi
#   fi
#
#   local first_device
#   first_device=$(ls /sys/class/infiniband 2>/dev/null | head -n 1)
#   if [[ -n "${first_device}" ]]; then
#     if [[ -d "/sys/class/infiniband/${first_device}/ports/1" ]]; then
#       echo "${first_device}:1"
#     else
#       echo "${first_device}"
#     fi
#   fi
# }
#
# detect_primary_netdev() {
#   if command -v ip >/dev/null 2>&1; then
#     local dev
#     dev=$(
#       ip route get 1.1.1.1 2>/dev/null \
#         | awk '{for (i = 1; i <= NF; i++) if ($i == "dev") {print $(i + 1); exit}}'
#     )
#     if [[ -n "${dev}" ]]; then
#       echo "${dev}"
#       return
#     fi
#   fi
#
#   if [[ -r /proc/net/route ]]; then
#     awk '
#       $2 == "00000000" && $3 != "00000000" {
#         print $1
#         exit
#       }
#     ' /proc/net/route
#   fi
# }
#
# should_override_ucx_net_devices() {
#   local current="${UCX_NET_DEVICES:-}"
#   if [[ -z "${current}" ]]; then
#     return 0
#   fi
#
#   local lower=${current,,}
#   if [[ "${lower}" == "all" || "${lower}" == "auto" ]]; then
#     return 0
#   fi
#
#   return 1
# }
#
# if [[ -z "${UCX_TLS:-}" ]]; then
#   if detect_active_ib; then
#     export UCX_TLS="all"
#   else
#     export UCX_TLS="tcp,self"
#   fi
# fi
#
# # UCX が GPU メモリタイプを誤検出しないように memtype cache を無効化
# export UCX_MEMTYPE_CACHE="n"
#
# # UCX が勝手に net device を切り替えないよう、RC 使用時はデバイスも固定
# # if should_override_ucx_net_devices; then
# #   if [[ "${UCX_TLS}" == *rc* ]]; then
# #     ib_device=$(detect_ib_device)
# #     primary_netdev=$(detect_primary_netdev)
#
# #     if [[ -n "${ib_device}" && -n "${primary_netdev}" ]]; then
# #       export UCX_NET_DEVICES="${ib_device},${primary_netdev}"
# #       export BENCHFS_STREAM_INTERFACE="${primary_netdev}"
# #     elif [[ -n "${ib_device}" ]]; then
# #       export UCX_NET_DEVICES="${ib_device}"
# #       unset BENCHFS_STREAM_INTERFACE
# #     elif [[ -n "${primary_netdev}" ]]; then
# #       export UCX_NET_DEVICES="${primary_netdev}"
# #       export BENCHFS_STREAM_INTERFACE="${primary_netdev}"
# #     else
# #       export UCX_NET_DEVICES="all"
# #       unset BENCHFS_STREAM_INTERFACE
# #     fi
# #   else
# #     primary_netdev=$(detect_primary_netdev)
# #     if [[ -n "${primary_netdev}" ]]; then
# #       export UCX_NET_DEVICES="${primary_netdev}"
# #       export BENCHFS_STREAM_INTERFACE="${primary_netdev}"
# #     else
# #       export UCX_NET_DEVICES="all"
# #       unset BENCHFS_STREAM_INTERFACE
# #     fi
# #   fi
#
# #   echo "Auto-selected UCX_NET_DEVICES=${UCX_NET_DEVICES}"
# # else
# #   echo "UCX_NET_DEVICES preset to ${UCX_NET_DEVICES}, leaving unchanged"
# # fi
#
# export UCX_NET_DEVICES="all"
#
# # 明示的に UD/DC を使わせない
# export UCX_PROTOS="^ud,dc"

# ==============================================================================
# 2. タイムアウトとリトライ設定
# ==============================================================================
# UCXのデフォルト値では不十分な場合があるため、増加

# export UCX_RC_TIMEOUT=2.0s               # タイムアウト時間（デフォルト: 1.0s）
# export UCX_RC_RETRY_COUNT=16             # リトライ回数（デフォルト: 7）
# export UCX_RC_TIMEOUT_MULTIPLIER=4.0     # タイムアウト乗数（デフォルト: 2.0）

# ==============================================================================
# 3. Active Message設定
# ==============================================================================
# Active Messageのバッファサイズとプロトコル閾値を最適化

# export UCX_AM_MAX_SHORT=128              # Short AMの最大サイズ (デフォルト: 128B)
# export UCX_AM_MAX_EAGER=8192             # Eager AMの最大サイズ (8KB)
# export UCX_RNDV_THRESH=16384             # Rendezvous閾値 (16KB)
# export UCX_RNDV_THRESH=inf              # Rendezvousプロトコル無効化（全てEagerに）

# AMストリームのキューサイズ
# export UCX_AM_SEND_QUEUE_SIZE=4096       # 送信キューサイズ
# export UCX_AM_RECV_QUEUE_SIZE=4096       # 受信キューサイズ

# ==============================================================================
# 4. RDMA設定
# ==============================================================================
# ゼロコピーとRendezvousプロトコルの最適化

# export UCX_ZCOPY_THRESH=0                # ゼロコピー常時有効（0 = 常時）
# export UCX_RNDV_SCHEME=put_zcopy             

# InfiniBand固有設定
# export UCX_IB_NUM_PATHS=2                # IBパス数
# export UCX_RC_MLX5_TM_ENABLE=y           # タグマッチングハードウェア加速
# export UCX_RC_MLX5_RX_QUEUE_LEN=4096     # 受信キューの長さ（デフォルト: 1024）

# ==============================================================================
# 5. メモリ登録キャッシュ
# ==============================================================================
# memtype cache は GPU 誤検出を避けるためセクション1で n に設定済み
# export UCX_RCACHE_ENABLE=n               # 登録キャッシュ有効
# export UCX_IB_REG_METHODS=rcache,direct
# export UCX_MLX5_DEVX_OBJECTS=''
# export UCX_MLX5_DEVX=n

# ==============================================================================
# 6. フロー制御
# ==============================================================================
# export UCX_RC_FC_ENABLE=y                # フロー制御有効化
# export UCX_RC_MAX_NUM_EPS=-1             # エンドポイント数無制限

# ==============================================================================
# 7. ネットワーク層設定
# ==============================================================================
# export UCX_IB_SEG_SIZE=8192              # IBセグメントサイズ (8KB)
# export UCX_RC_PATH_MTU=4096              # Path MTU (4KB推奨)

# RoCE使用時（必要に応じて有効化）
# export UCX_IB_GID_INDEX=0              # GIDインデックス

# ==============================================================================
# 8. プログレス設定
# ==============================================================================
# export UCX_ADAPTIVE_PROGRESS=y           # アダプティブプログレス
# export UCX_ASYNC_MAX_EVENTS=256          # 非同期イベント最大数

# シングルスレッドの場合（MPIプロセス内でスレッド不使用）
# export UCX_USE_MT_MUTEX=n                # マルチスレッドmutex無効

# UCX Configuration for avoiding Rendezvous protocol issues
# - UCX_TLS: Use only TCP, shared memory, and self transports (avoid InfiniBand)
# - UCX_RNDV_THRESH: Set to inf to disable Rendezvous protocol completely
#   This forces all messages to use Eager protocol, which is compatible
#   with current implementation
# UCX_LOG_LEVEL="TRACE"

# export UCX_LOG_LEVEL
# export UCX_RNDV_THRESH

# Calculate project root from SCRIPT_DIR and set LD_LIBRARY_PATH dynamically
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
export LD_LIBRARY_PATH="${PROJECT_ROOT}/target/release:${LD_LIBRARY_PATH:-}"

IFS=" " read -r -a nqsii_mpiopts_array <<<"$NQSII_MPIOPTS"

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
echo "Tracing: ENABLE_PERFETTO=${ENABLE_PERFETTO}, ENABLE_CHROME=${ENABLE_CHROME}"
if [ "${ENABLE_PERFETTO}" -eq 1 ] || [ "${ENABLE_CHROME}" -eq 1 ]; then
  echo "Trace Output: ${PERFETTO_OUTPUT_DIR}"
fi
echo "RUST_LOG (server): ${RUST_LOG_S}"
echo "RUST_LOG (client): ${RUST_LOG_C}"
echo "TASKSET: ${TASKSET} (cores: ${TASKSET_CORES})"
echo "SEPARATE: ${SEPARATE} (server_nodes: ${SERVER_NODES:-auto}, client_nodes: ${CLIENT_NODES:-auto})"
echo "Node Diagnostics: ENABLE_NODE_DIAGNOSTICS=${ENABLE_NODE_DIAGNOSTICS:-0} (FIO_RUNTIME=${FIO_RUNTIME:-30}s)"
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
echo "NQSII_MPIOPTS: ${NQSII_MPIOPTS:-<not set>}"
echo "NQSII_MPIOPTS_ARRAY (${#nqsii_mpiopts_array[@]} elements):"
for i in "${!nqsii_mpiopts_array[@]}"; do
  echo "  [$i] = ${nqsii_mpiopts_array[$i]}"
done
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

echo "prepare stats output dir: ${STATS_OUTPUT_DIR}"
mkdir -p "${STATS_OUTPUT_DIR}"

# ==============================================================================
# Node Diagnostics Functions
# ==============================================================================
# These functions perform pre-benchmark diagnostics to identify nodes with
# potential I/O performance issues before the actual benchmark runs.

DIAGNOSTICS_DIR="${JOB_OUTPUT_DIR}/diagnostics"

# Run diagnostics on a single node
# Usage: run_node_diagnostics <node>
run_node_diagnostics() {
    local node=$1
    local diag_dir="${DIAGNOSTICS_DIR}/${node}"
    mkdir -p "$diag_dir"

    echo "Running diagnostics on ${node}..."

    # 1. fio READ performance test (single file)
    ssh "$node" "cd /scr && fio --name=read_test --rw=read --bs=4M --size=1G \
        --direct=1 --runtime=${FIO_RUNTIME} --time_based --output-format=json \
        --filename=/scr/fio_test_read_\$(hostname).dat" 2>/dev/null > "$diag_dir/fio_read.json" &
    local read_pid=$!

    # 2. fio WRITE performance test (runs after read completes)
    wait $read_pid
    ssh "$node" "cd /scr && fio --name=write_test --rw=write --bs=4M --size=1G \
        --direct=1 --runtime=${FIO_RUNTIME} --time_based --output-format=json \
        --filename=/scr/fio_test_write_\$(hostname).dat" 2>/dev/null > "$diag_dir/fio_write.json"

    # 3. Multi-file fio READ test (simulates BenchFS workload)
    # Creates 100 files of 4MB each to test directory/metadata performance
    ssh "$node" "test_dir=/scr/fio_multifile_test_\$(hostname) && \
        mkdir -p \$test_dir && \
        cd \$test_dir && \
        fio --name=multifile_write --rw=write --bs=4M --filesize=4M \
            --nrfiles=100 --directory=\$test_dir \
            --direct=1 --runtime=${FIO_RUNTIME} --time_based \
            --output-format=json 2>/dev/null && \
        fio --name=multifile_read --rw=read --bs=4M --filesize=4M \
            --nrfiles=100 --directory=\$test_dir \
            --direct=1 --runtime=${FIO_RUNTIME} --time_based \
            --output-format=json" 2>/dev/null > "$diag_dir/fio_multifile_read.json" &
    local multifile_pid=$!

    # 4. iostat baseline (r_await analysis)
    # Collect 5 samples at 1-second intervals to get average r_await
    ssh "$node" "iostat -x 1 5 2>/dev/null | grep -E '^(Device|nvme|md|sd)'" > "$diag_dir/iostat_baseline.txt" &

    # 5. RAID status check
    ssh "$node" "cat /proc/mdstat 2>/dev/null || echo 'No RAID'" > "$diag_dir/mdstat.txt" &

    # 6. NVMe SMART information
    ssh "$node" "sudo nvme smart-log /dev/nvme0n1 2>/dev/null || echo 'NVMe SMART not available'" > "$diag_dir/nvme_smart.txt" &

    # 7. Other processes' I/O status
    ssh "$node" "ps aux --sort=-%mem | head -20" > "$diag_dir/processes.txt" &

    # 8. Disk usage
    ssh "$node" "df -h /scr" > "$diag_dir/disk_usage.txt" &

    # Wait for multifile test to complete
    wait $multifile_pid

    # 9. Clean up test files (single file tests)
    ssh "$node" "rm -f /scr/fio_test_read_*.dat /scr/fio_test_write_*.dat" &

    # 10. Clean up multi-file test directory
    ssh "$node" "rm -rf /scr/fio_multifile_test_\$(hostname)" &

    wait
    echo "Diagnostics completed for ${node}"
}

# Run diagnostics on all nodes in parallel
# Usage: run_all_node_diagnostics
run_all_node_diagnostics() {
    if [ "${ENABLE_NODE_DIAGNOSTICS}" -ne 1 ]; then
        echo "Node diagnostics disabled (set ENABLE_NODE_DIAGNOSTICS=1 to enable)"
        return 0
    fi

    echo "=========================================="
    echo "Running Pre-Benchmark Node Diagnostics"
    echo "=========================================="

    mkdir -p "${DIAGNOSTICS_DIR}"

    # Get unique nodes from PBS_NODEFILE
    local nodes=($(sort -u "${PBS_NODEFILE}"))
    local pids=()

    echo "Running diagnostics on ${#nodes[@]} nodes..."

    # Run diagnostics on all nodes in parallel
    for node in "${nodes[@]}"; do
        run_node_diagnostics "$node" &
        pids+=($!)
    done

    # Wait for all diagnostics to complete
    for pid in "${pids[@]}"; do
        wait $pid
    done

    echo "All node diagnostics completed"
    echo "Results saved to: ${DIAGNOSTICS_DIR}"
    echo ""

    # Quick summary of results
    echo "Quick diagnostic summary:"
    printf "  %-12s %12s %12s %12s %12s %12s\n" "Node" "READ" "WRITE" "MF_READ" "md0_r_await" "RAID"
    printf "  %-12s %12s %12s %12s %12s %12s\n" "----" "----" "-----" "-------" "-----------" "----"
    for node in "${nodes[@]}"; do
        local read_bw="N/A"
        local write_bw="N/A"
        local mf_read_bw="N/A"
        local md0_rawait="N/A"
        local raid_status="N/A"

        if [ -f "${DIAGNOSTICS_DIR}/${node}/fio_read.json" ]; then
            read_bw=$(jq -r '.jobs[0].read.bw_bytes // 0' "${DIAGNOSTICS_DIR}/${node}/fio_read.json" 2>/dev/null | awk '{printf "%.0f", $1/1024/1024}')
        fi
        if [ -f "${DIAGNOSTICS_DIR}/${node}/fio_write.json" ]; then
            write_bw=$(jq -r '.jobs[0].write.bw_bytes // 0' "${DIAGNOSTICS_DIR}/${node}/fio_write.json" 2>/dev/null | awk '{printf "%.0f", $1/1024/1024}')
        fi
        if [ -f "${DIAGNOSTICS_DIR}/${node}/fio_multifile_read.json" ]; then
            mf_read_bw=$(jq -r '.jobs[0].read.bw_bytes // 0' "${DIAGNOSTICS_DIR}/${node}/fio_multifile_read.json" 2>/dev/null | awk '{printf "%.0f", $1/1024/1024}')
        fi
        if [ -f "${DIAGNOSTICS_DIR}/${node}/iostat_baseline.txt" ]; then
            # Get r_await (column 6 in newer iostat, or extract from the line)
            # iostat -x output: Device r/s rkB/s rrqm/s %rrqm r_await ...
            md0_rawait=$(grep "^md0" "${DIAGNOSTICS_DIR}/${node}/iostat_baseline.txt" 2>/dev/null | tail -1 | awk '{print $6}')
            if [ -z "$md0_rawait" ]; then
                md0_rawait="N/A"
            fi
        fi
        if [ -f "${DIAGNOSTICS_DIR}/${node}/mdstat.txt" ]; then
            if grep -q "No RAID" "${DIAGNOSTICS_DIR}/${node}/mdstat.txt" 2>/dev/null; then
                raid_status="NoRAID"
            elif grep -q "rebuilding\|recovery" "${DIAGNOSTICS_DIR}/${node}/mdstat.txt" 2>/dev/null; then
                raid_status="REBUILD"
            elif grep -q "active" "${DIAGNOSTICS_DIR}/${node}/mdstat.txt" 2>/dev/null; then
                raid_status="OK"
            else
                raid_status="UNKNOWN"
            fi
        fi

        printf "  %-12s %12s %12s %12s %12s %12s\n" "$node" "${read_bw}" "${write_bw}" "${mf_read_bw}" "${md0_rawait}" "${raid_status}"
    done
    echo "=========================================="
    echo ""
}

# Start background iostat monitoring on a node
# Usage: start_iostat_monitoring <node> <output_file>
# Note: -t option adds timestamps for time-series analysis
start_iostat_monitoring() {
    local node=$1
    local output_file=$2
    ssh "$node" "iostat -xt 5 > ${output_file} 2>&1 &" &
}

# Start iostat monitoring on all nodes
# Usage: start_all_iostat_monitoring <run_id>
start_all_iostat_monitoring() {
    if [ "${ENABLE_NODE_DIAGNOSTICS}" -ne 1 ]; then
        return 0
    fi

    local run_id=$1
    local nodes=($(sort -u "${PBS_NODEFILE}"))

    echo "Starting iostat monitoring on all nodes for run ${run_id}..."
    for node in "${nodes[@]}"; do
        local diag_dir="${DIAGNOSTICS_DIR}/${node}"
        mkdir -p "$diag_dir"
        start_iostat_monitoring "$node" "${diag_dir}/iostat_during_run${run_id}.txt"
    done
}

# Stop iostat monitoring on all nodes
# Usage: stop_all_iostat_monitoring
stop_all_iostat_monitoring() {
    if [ "${ENABLE_NODE_DIAGNOSTICS}" -ne 1 ]; then
        return 0
    fi

    local nodes=($(sort -u "${PBS_NODEFILE}"))
    echo "Stopping iostat monitoring on all nodes..."
    for node in "${nodes[@]}"; do
        ssh "$node" "pkill -f 'iostat -xt 5'" 2>/dev/null || true
    done
}

if [ "${ENABLE_PERFETTO}" -eq 1 ] || [ "${ENABLE_CHROME}" -eq 1 ]; then
  echo "prepare trace output dir: ${PERFETTO_OUTPUT_DIR}"
  mkdir -p "${PERFETTO_OUTPUT_DIR}"
  if [ "${ENABLE_PERFETTO}" -eq 1 ]; then
    echo "Perfetto tracing enabled (task-level tracks) - traces will be saved to ${PERFETTO_OUTPUT_DIR}"
  elif [ "${ENABLE_CHROME}" -eq 1 ]; then
    echo "Chrome tracing enabled - traces will be saved to ${PERFETTO_OUTPUT_DIR}"
  fi
fi

save_job_metadata() {
  local file_per_proc=0
  [[ "$ior_flags" == *"-F"* ]] && file_per_proc=1
  cat <<EOS >"${JOB_OUTPUT_DIR}"/job_metadata_${runid}.json
{
  "jobid": "$JOBID",
  "runid": ${runid},
  "nnodes": ${NNODES},
  "separate": ${SEPARATE},
  "server_nnodes": ${SERVER_NNODES},
  "client_nnodes": ${CLIENT_NNODES},
  "server_ppn": ${server_ppn},
  "server_np": ${server_np},
  "client_ppn": ${ppn},
  "client_np": ${np},
  "transfer_size": "${transfer_size}",
  "block_size": "${block_size}",
  "benchfs_chunk_size": ${benchfs_chunk_size},
  "file_per_proc": ${file_per_proc},
  "ior_flags": "${ior_flags}",
  "job_start_time": "${JOB_START}"
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

# RUST_LOG is now set separately for server (RUST_LOG_S) and client (RUST_LOG_C)
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

# NOTE: UCX_* 環境変数をコメントアウトしたため、-x UCX_* オプションも削除
# remote_io_benchプロジェクトでも同様の現象が発生し、UCX_*をコメントアウトしたところ動いた
if [[ "${USE_UCX_PML}" -eq 1 ]]; then
  cmd_mpirun_common=(
    mpirun
    "${nqsii_mpiopts_array[@]}"
    --mca pml ucx
    --mca btl self
    --mca osc ucx
    # -x UCX_TLS
    # -x UCX_NET_DEVICES
    # -x UCX_MEMTYPE_CACHE
    # -x UCX_PROTOS
    # -x UCX_LOG_LEVEL
    # -x UCX_RNDV_THRESH
    # -x UCX_RNDV_SCHEME
    # -x UCX_RC_TIMEOUT
    # -x UCX_RC_RETRY_COUNT
    # -x UCX_RC_TIMEOUT_MULTIPLIER
    # -x UCX_AM_MAX_SHORT
    # -x UCX_AM_MAX_EAGER
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

# ==============================================================================
# Separate server/client node mode
# ==============================================================================
# When SEPARATE=1, server and client processes run on different physical nodes.
# This eliminates resource contention (CPU, memory, NVMe I/O) between them.

_replace_result=()
replace_nqsii_hostfile() {
  local new_hostfile=$1
  _replace_result=()
  local i=0
  local found_hostfile=0
  while [ $i -lt ${#nqsii_mpiopts_array[@]} ]; do
    if [ "${nqsii_mpiopts_array[$i]}" = "-hostfile" ] || [ "${nqsii_mpiopts_array[$i]}" = "--hostfile" ]; then
      _replace_result+=("${nqsii_mpiopts_array[$i]}" "$new_hostfile")
      i=$((i + 2))
      found_hostfile=1
    else
      _replace_result+=("${nqsii_mpiopts_array[$i]}")
      i=$((i + 1))
    fi
  done
  if [ $found_hostfile -eq 0 ]; then
    _replace_result+=("--hostfile" "$new_hostfile")
  fi
}

if [ "${SEPARATE}" -eq 1 ]; then
  # Default: floor(NNODES/2) for server, remainder for client (client gets extra if odd)
  : ${SERVER_NODES:=$((NNODES / 2))}
  : ${CLIENT_NODES:=$((NNODES - SERVER_NODES))}

  if [ $((SERVER_NODES + CLIENT_NODES)) -ne "$NNODES" ]; then
    echo "ERROR: SERVER_NODES($SERVER_NODES) + CLIENT_NODES($CLIENT_NODES) != NNODES($NNODES)"
    exit 1
  fi
  if [ "$SERVER_NODES" -lt 1 ] || [ "$CLIENT_NODES" -lt 1 ]; then
    echo "ERROR: Both SERVER_NODES and CLIENT_NODES must be >= 1"
    exit 1
  fi

  # Create separate hostfiles from PBS_NODEFILE
  all_unique_nodes=($(sort -u "${PBS_NODEFILE}"))
  SERVER_NODEFILE="${JOB_OUTPUT_DIR}/server_nodes.txt"
  CLIENT_NODEFILE="${JOB_OUTPUT_DIR}/client_nodes.txt"

  printf '%s\n' "${all_unique_nodes[@]:0:${SERVER_NODES}}" > "${SERVER_NODEFILE}"
  printf '%s\n' "${all_unique_nodes[@]:${SERVER_NODES}:${CLIENT_NODES}}" > "${CLIENT_NODEFILE}"

  # Build separate mpirun base commands with different hostfiles
  replace_nqsii_hostfile "${SERVER_NODEFILE}"
  nqsii_server_opts=("${_replace_result[@]}")
  replace_nqsii_hostfile "${CLIENT_NODEFILE}"
  nqsii_client_opts=("${_replace_result[@]}")

  if [[ "${USE_UCX_PML}" -eq 1 ]]; then
    cmd_mpirun_server=(mpirun "${nqsii_server_opts[@]}" --mca pml ucx --mca btl self --mca osc ucx -x PATH -x LD_LIBRARY_PATH)
    cmd_mpirun_client=(mpirun "${nqsii_client_opts[@]}" --mca pml ucx --mca btl self --mca osc ucx -x PATH -x LD_LIBRARY_PATH)
  else
    cmd_mpirun_server=(mpirun "${nqsii_server_opts[@]}" --mca pml ob1 --mca btl tcp,vader,self --mca btl_openib_allow_ib 0 -x PATH -x LD_LIBRARY_PATH)
    cmd_mpirun_client=(mpirun "${nqsii_client_opts[@]}" --mca pml ob1 --mca btl tcp,vader,self --mca btl_openib_allow_ib 0 -x PATH -x LD_LIBRARY_PATH)
  fi

  SERVER_NNODES=${SERVER_NODES}
  CLIENT_NNODES=${CLIENT_NODES}

  echo "=========================================="
  echo "SEPARATE Mode Configuration"
  echo "=========================================="
  echo "  Total nodes: $NNODES"
  echo "  Server nodes ($SERVER_NNODES): $(tr '\n' ' ' < "${SERVER_NODEFILE}")"
  echo "  Client nodes ($CLIENT_NNODES): $(tr '\n' ' ' < "${CLIENT_NODEFILE}")"
  echo "=========================================="
else
  cmd_mpirun_server=("${cmd_mpirun_common[@]}")
  cmd_mpirun_client=("${cmd_mpirun_common[@]}")
  SERVER_NNODES=${NNODES}
  CLIENT_NNODES=${NNODES}
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

# Run pre-benchmark node diagnostics (if enabled)
run_all_node_diagnostics

# Load benchmark parameters from configuration file
PARAM_FILE="${PARAM_FILE:-${SCRIPT_DIR}/../params/standard.conf}"
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
    server_ppn_list=(1)
    ior_flags_list=("-w -r -F")
    benchfs_chunk_size_list=(4194304 16777216)
fi

# Default server_ppn_list if not defined in param file
if [ -z "${server_ppn_list+x}" ]; then
    server_ppn_list=(1)
fi

# Convert size string (e.g., 4m, 16m, 1g) to bytes
parse_size_to_bytes() {
    local size_str="${1,,}"  # Convert to lowercase
    local number="${size_str%[kmgt]}"
    local suffix="${size_str: -1}"

    case "$suffix" in
        k) echo $((number * 1024)) ;;
        m) echo $((number * 1024 * 1024)) ;;
        g) echo $((number * 1024 * 1024 * 1024)) ;;
        t) echo $((number * 1024 * 1024 * 1024 * 1024)) ;;
        [0-9]) echo "$size_str" ;;  # Already a number
        *) echo "$size_str" ;;  # Return as-is if unknown
    esac
}

# Save parameter configuration for reproducibility
cat > "${JOB_OUTPUT_DIR}/parameters.json" <<EOF
{
  "parameter_file": "$PARAM_FILE",
  "nnodes": ${NNODES},
  "separate": ${SEPARATE},
  "server_nnodes": ${SERVER_NNODES},
  "client_nnodes": ${CLIENT_NNODES},
  "transfer_sizes": [$(printf '"%s",' "${transfer_size_list[@]}" | sed 's/,$//; s/,$//')],
  "block_sizes": [$(printf '"%s",' "${block_size_list[@]}" | sed 's/,$//; s/,$//')],
  "client_ppn_values": [$(printf '%s,' "${ppn_list[@]}" | sed 's/,$//; s/,$//')],
  "server_ppn_values": [$(printf '%s,' "${server_ppn_list[@]}" | sed 's/,$//; s/,$//')],
  "ior_flags": [$(printf '"%s",' "${ior_flags_list[@]}" | sed 's/,$//; s/,$//')],
  "chunk_sizes": [$(printf '%s,' "${benchfs_chunk_size_list[@]}" | sed 's/,$//; s/,$//')]
}
EOF

check_server_ready() {
  local expected_count=$1
  local max_attempts=60
  local attempt=0

  while [ $attempt -lt $max_attempts ]; do
    local ready_count=$(find "${BENCHFS_REGISTRY_DIR}" -name "node_*.addr" -type f 2>/dev/null | wc -l)

    if [ "$ready_count" -eq "$expected_count" ]; then
      echo "BenchFS servers registered: $ready_count/$expected_count processes"

      # Additional wait for RPC handler initialization
      # All nodes have registered, but their RPC handlers may still be initializing
      # This prevents "connection refused" errors when 256 clients connect simultaneously
      local rpc_wait_time=10
      echo "Waiting ${rpc_wait_time}s for RPC handler initialization..."
      sleep $rpc_wait_time

      echo "BenchFS servers are fully ready"
      return 0
    fi

    echo "Waiting for BenchFS servers: $ready_count/$expected_count processes (attempt $((attempt+1))/$max_attempts)"
    sleep 1
    attempt=$((attempt + 1))
  done

  echo "ERROR: BenchFS servers failed to start after $max_attempts seconds"
  return 1
}

runid=0
for benchfs_chunk_size_str in "${benchfs_chunk_size_list[@]}"; do
  # Convert chunk size string to bytes
  benchfs_chunk_size=$(parse_size_to_bytes "$benchfs_chunk_size_str")

  for server_ppn in "${server_ppn_list[@]}"; do
    server_np=$((SERVER_NNODES * server_ppn))

    for ppn in "${ppn_list[@]}"; do
      np=$((CLIENT_NNODES * ppn))

      for transfer_size in "${transfer_size_list[@]}"; do
        for block_size in "${block_size_list[@]}"; do
          for ior_flags in "${ior_flags_list[@]}"; do
            echo "=========================================="
            echo "Run ID: $runid"
            echo "Nodes: $NNODES (server: $SERVER_NNODES, client: $CLIENT_NNODES, separate: $SEPARATE)"
            echo "Server: PPN=$server_ppn, NP=$server_np"
            echo "Client: PPN=$ppn, NP=$np"
            echo "Transfer size: $transfer_size, Block size: $block_size"
          echo "IOR flags: $ior_flags"
          echo "BenchFS chunk size: $benchfs_chunk_size_str ($benchfs_chunk_size bytes)"
          echo "=========================================="

          # Clean up previous run
          rm -rf "${BENCHFS_REGISTRY_DIR}"/*
          rm -rf "${BENCHFS_DATA_DIR}"/*

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

            # Determine the binary to use (with optional taskset wrapper)
            BENCHFSD_BINARY="${BENCHFS_PREFIX}/benchfsd_mpi"
            if [ "${TASKSET}" = "1" ]; then
              echo "TASKSET mode enabled: limiting benchfsd_mpi to cores ${TASKSET_CORES}"
              # Create a wrapper script that uses taskset
              TASKSET_WRAPPER="${run_log_dir}/benchfsd_taskset_wrapper.sh"
              cat > "${TASKSET_WRAPPER}" <<EOF
#!/bin/bash
exec taskset -c ${TASKSET_CORES} ${BENCHFS_PREFIX}/benchfsd_mpi "\$@"
EOF
              chmod +x "${TASKSET_WRAPPER}"
              BENCHFSD_BINARY="${TASKSET_WRAPPER}"
              echo "Created taskset wrapper: ${TASKSET_WRAPPER}"
            fi

            # Build benchfsd command with optional Perfetto tracing and stats output
            stats_file="${STATS_OUTPUT_DIR}/stats_run${runid}.csv"
            cmd_benchfsd=(
              "${cmd_mpirun_server[@]}"
              -np "$server_np"
              --bind-to none
              -map-by "ppr:${server_ppn}:node"
              -x RUST_LOG="${RUST_LOG_S}"
              -x RUST_BACKTRACE
              # Note: PATH and LD_LIBRARY_PATH are already set in cmd_mpirun_server
              "${BENCHFSD_BINARY}"
              "${BENCHFS_REGISTRY_DIR}"
              "${config_file}"
              --stats-output "${stats_file}"
            )
            echo "Stats output enabled for this run: ${stats_file}"

            # Add tracing option if enabled
            # Note: -x options must be inserted before the binary (4 elements from end: binary, registry, config, stats-output, stats_file)
            if [ "${ENABLE_PERFETTO}" -eq 1 ]; then
              trace_file="${PERFETTO_OUTPUT_DIR}/trace_run${runid}.pftrace"
              binary_idx=$((${#cmd_benchfsd[@]} - 5))
              cmd_benchfsd=("${cmd_benchfsd[@]:0:binary_idx}" -x ENABLE_PERFETTO=1 "${cmd_benchfsd[@]:binary_idx}")
              cmd_benchfsd+=(--trace-output "${trace_file}")
              echo "Perfetto tracing enabled for this run: ${trace_file}"
            elif [ "${ENABLE_CHROME}" -eq 1 ]; then
              trace_file="${PERFETTO_OUTPUT_DIR}/trace_run${runid}.json"
              binary_idx=$((${#cmd_benchfsd[@]} - 5))
              cmd_benchfsd=("${cmd_benchfsd[@]:0:binary_idx}" -x ENABLE_CHROME=1 "${cmd_benchfsd[@]:binary_idx}")
              cmd_benchfsd+=(--trace-output "${trace_file}")
              echo "Chrome tracing enabled for this run: ${trace_file}"
            fi

            # Add --enable-stats option if detailed timing statistics are requested
            if [ "${ENABLE_STATS:-0}" -eq 1 ]; then
              cmd_benchfsd+=(--enable-stats)
              echo "Detailed timing statistics collection enabled"
            fi

          echo "${cmd_benchfsd[@]}"
          "${cmd_benchfsd[@]}" > "${run_log_dir}/benchfsd_stdout.log" 2> "${run_log_dir}/benchfsd_stderr.log" &
          BENCHFSD_PID=$!

            # Wait for servers to be ready
            if ! check_server_ready "$server_np"; then
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
          "${cmd_mpirun_client[@]}" -np "$np" --map-by "ppr:${ppn}:node" hostname > "${IOR_OUTPUT_DIR}/mpi_test_${runid}.txt" 2>&1
          echo "MPI Debug: Communication test completed"

          # Run IOR benchmark
          echo "Running IOR benchmark..."
          ior_json_file="${IOR_OUTPUT_DIR}/ior_result_${runid}.json"
          ior_stdout_file="${IOR_OUTPUT_DIR}/ior_stdout_${runid}.log"

          # Create directory for retry stats from IOR clients
          retry_stats_dir="${STATS_OUTPUT_DIR}/retry_stats_run${runid}"
          mkdir -p "${retry_stats_dir}"

          cmd_ior=(
            time_json -o "${JOB_OUTPUT_DIR}/time_${runid}.json"
            "${cmd_mpirun_client[@]}"
            -np "$np"
            --bind-to none
            --map-by "ppr:${ppn}:node"
            -x RUST_LOG="${RUST_LOG_C}"
            -x RUST_BACKTRACE
            -x BENCHFS_RETRY_STATS_OUTPUT="${retry_stats_dir}/"
            -x BENCHFS_EXPECTED_NODES="${server_np}"
            # Note: PATH and LD_LIBRARY_PATH are already set in cmd_mpirun_common
            "${IOR_PREFIX}/src/ior"
            -vvv
            -a BENCHFS
            -t "$transfer_size"
            -b "$block_size"
            -D 120
            -e
            $ior_flags
            --benchfs.registry="${BENCHFS_REGISTRY_DIR}"
            --benchfs.datadir="${BENCHFS_DATA_DIR}"
            --benchfs.chunk-size="${benchfs_chunk_size}"
            -o "${BENCHFS_DATA_DIR}/testfile"
            -O summaryFormat=JSON
            -O summaryFile="${ior_json_file}"
          )
          echo "Retry stats output enabled for IOR clients: ${retry_stats_dir}/"

          save_job_metadata

          # Start iostat monitoring during benchmark (if enabled)
          start_all_iostat_monitoring "$runid"

          echo "${cmd_ior[@]}"
          # NOTE: Use simple redirection instead of process substitution to avoid FD leak
          "${cmd_ior[@]}" \
            > "${ior_stdout_file}" \
            2> "${IOR_OUTPUT_DIR}/ior_stderr_${runid}.log" || true

          # Stop iostat monitoring
          stop_all_iostat_monitoring

          # Stop BenchFS servers gracefully (for perf compatibility)
          # Using SIGTERM allows graceful shutdown so perf can flush its data
          echo "Stopping BenchFS servers (graceful shutdown for perf compatibility)..."

          # Graceful shutdown timeout in seconds
          graceful_timeout=${BENCHFS_SHUTDOWN_TIMEOUT:-30}

          # First, try graceful shutdown with SIGTERM via mpirun
          echo "Sending SIGTERM to benchfsd_mpi processes..."
          "${cmd_mpirun_common[@]}" -np "$NNODES" -map-by ppr:1:node \
            pkill -TERM benchfsd_mpi 2>/dev/null || true

          # Wait for graceful shutdown - poll until process exits or timeout
          echo "Waiting up to ${graceful_timeout}s for graceful shutdown..."
          elapsed=0
          graceful_success=0
          while [ $elapsed -lt $graceful_timeout ]; do
            # Check if mpirun process has exited
            if ! kill -0 $BENCHFSD_PID 2>/dev/null; then
              echo "BenchFS servers stopped gracefully after ${elapsed}s"
              wait $BENCHFSD_PID 2>/dev/null || true
              graceful_success=1
              break
            fi
            sleep 1
            elapsed=$((elapsed + 1))

            # Show progress every 5 seconds
            if [ $((elapsed % 5)) -eq 0 ]; then
              echo "  Still waiting for graceful shutdown... (${elapsed}s/${graceful_timeout}s)"
            fi
          done

          # If graceful shutdown failed, use SIGKILL as last resort
          if [ $graceful_success -eq 0 ]; then
            echo "WARNING: Graceful shutdown timed out after ${graceful_timeout}s"
            echo "WARNING: perf results may be incomplete due to forced termination"

            # Kill the mpirun process that launched benchfsd
            kill $BENCHFSD_PID 2>/dev/null || true
            wait $BENCHFSD_PID 2>/dev/null || true

            # Force cleanup of any orphaned processes with SIGKILL
            "${cmd_mpirun_common[@]}" -np "$NNODES" -map-by ppr:1:node \
              pkill -9 benchfsd_mpi 2>/dev/null || true
            pkill -9 benchfsd_mpi 2>/dev/null || true

            # Wait for cleanup and FD release
            sleep 3
          fi

          # Merge client retry stats into a single CSV file
          merged_retry_stats="${STATS_OUTPUT_DIR}/retry_stats_run${runid}.csv"
          if [ -d "${retry_stats_dir}" ]; then
            echo "Merging client retry stats into ${merged_retry_stats}..."
            # Write header once
            echo "node_id,total_requests,total_retries,retry_successes,retry_failures,retry_rate" > "${merged_retry_stats}"
            # Append data rows from all client CSVs (skip headers)
            for csv_file in "${retry_stats_dir}"/*.csv; do
              if [ -f "$csv_file" ]; then
                tail -n +2 "$csv_file" >> "${merged_retry_stats}"
              fi
            done
            # Remove individual CSV files directory
            rm -rf "${retry_stats_dir}"
            echo "Merged $(wc -l < "${merged_retry_stats}" | tr -d ' ') lines (including header) into ${merged_retry_stats}"
          fi

            runid=$((runid + 1))
          done
        done
      done
    done
  done
done

echo "All benchmarks completed"
