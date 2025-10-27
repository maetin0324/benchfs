#!/bin/bash
#------- qsub option -----------
#PBS -A NBB
#PBS -l elapstim_req=12:00:00
#PBS -T openmpi
#PBS -v NQSV_MPI_VER=4.1.8/gcc11.4.0-cuda12.8.1
#------- Program execution -----------
set -euo pipefail

module purge
module load "openmpi/$NQSV_MPI_VER"

# Requires
# - SCRIPT_DIR
# - OUTPUT_DIR
# - BACKEND_DIR
# - BENCHFS_PREFIX
# - IOR_PREFIX

source "$SCRIPT_DIR/common.sh"
exec 1> >(addtimestamp)
exec 2> >(addtimestamp >&2)

JOB_START=$(timestamp)
NNODES=$(wc --lines "${PBS_NODEFILE}" | awk '{print $1}')
JOBID=$(echo "$PBS_JOBID" | cut -d : -f 2)
JOB_OUTPUT_DIR="${OUTPUT_DIR}/${JOB_START}-${JOBID}-${NNODES}"
JOB_BACKEND_DIR="${BACKEND_DIR}/$(basename -- "${JOB_OUTPUT_DIR}")"
BENCHFS_REGISTRY_DIR="${JOB_BACKEND_DIR}/registry"
BENCHFS_DATA_DIR="/scr"
BENCHFSD_LOG_BASE_DIR="${JOB_OUTPUT_DIR}/benchfsd_logs"
IOR_OUTPUT_DIR="${JOB_OUTPUT_DIR}/ior_results"
export LD_LIBRARY_PATH="/work/0/NBB/rmaeda/workspace/rust/benchfs/target/release:$LD_LIBRARY_PATH"

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
echo ""
echo "Checking binary:"
ls -la "${BENCHFS_PREFIX}/benchfsd_mpi" || echo "ERROR: Binary not found at ${BENCHFS_PREFIX}/benchfsd_mpi"
echo ""
echo "Checking IOR:"
ls -la "${IOR_PREFIX}/src/ior" || echo "ERROR: IOR not found at ${IOR_PREFIX}/src/ior"
echo "=========================================="

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

save_job_params() {
  cat <<EOS >"${JOB_OUTPUT_DIR}"/job_params_${runid}.json
{
  "nnodes": ${NNODES},
  "ppn": ${ppn},
  "np": ${np},
  "jobid": "$JOBID",
  "runid": ${runid},
  "transfer_size": "${transfer_size}",
  "block_size": "${block_size}",
  "ior_flags": "${ior_flags}",
  "benchfs_chunk_size": ${benchfs_chunk_size}
}
EOS
}

cmd_mpirun_common=(
  mpirun
  "${nqsii_mpiopts_array[@]}"
  --mca btl "self,tcp"
  --mca btl_tcp_if_include eno1
  -x "UCX_TLS=self,tcp"
  -x PATH
  -x LD_LIBRARY_PATH     # ← これを追加
)

# Kill any previous benchfsd instances
cmd_mpirun_kill=(
  "${cmd_mpirun_common[@]}"
  -np "$NNODES"
  -map-by ppr:1:node
  pkill -9 benchfsd_mpi
)

echo "Kill any previous benchfsd instances"
"${cmd_mpirun_kill[@]}" || true

# Parameter lists for benchmarking
transfer_size_list=(
  16m   # 16 MiB
  16g   # 16 GiB
)

block_size_list=(
  4m    # 4 MiB
  16m   # 16 MiB
  64m   # 64 MiB
)

ppn_list=(
  1
  2
  4
)

# IOR flags: -w (write), -r (read), -F (file per process)
ior_flags_list=(
  "-w -r -F"
  "-w -r"
)

benchfs_chunk_size_list=(
  4194304  # 4 MiB (default)
)

check_server_ready() {
  local max_attempts=60
  local attempt=0

  while [ $attempt -lt $max_attempts ]; do
    local ready_count=$(find "${BENCHFS_REGISTRY_DIR}" -name "node_*.addr" -type f 2>/dev/null | wc -l)

    if [ "$ready_count" -eq "$NNODES" ]; then
      echo "BenchFS servers are ready: $ready_count/$NNODES nodes registered"
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

          # Verify files exist
          ls -la "${BENCHFS_REGISTRY_DIR}" || echo "WARNING: Registry dir not accessible"
          ls -la "${config_file}" || echo "WARNING: Config file not found"
          ls -la "${BENCHFS_PREFIX}/benchfsd_mpi" || echo "WARNING: Binary not found"

          cmd_benchfsd=(
            "${cmd_mpirun_common[@]}"
            -np "$NNODES"
            --bind-to none
            -map-by ppr:1:node
            -x PATH
            -x "RUST_LOG=debug"
            -x "RUST_BACKTRACE=1"
            -x "UCX_TLS=tcp,sm,self"
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

          # Run IOR benchmark
          echo "Running IOR benchmark..."
          ior_output_file="${IOR_OUTPUT_DIR}/ior_result_${runid}.txt"

          cmd_ior=(
            time_json -o "${JOB_OUTPUT_DIR}/time_${runid}.json"
            "${cmd_mpirun_common[@]}"
            -np "$np"
            --bind-to none
            --map-by "ppr:${ppn}:node"
            -x "UCX_TLS=tcp,sm,self"
            -x PATH
            "${IOR_PREFIX}/src/ior"
            -a BENCHFS
            -t "$transfer_size"
            -b "$block_size"
            $ior_flags
            --benchfs.registry="${BENCHFS_REGISTRY_DIR}"
            --benchfs.datadir="${BENCHFS_DATA_DIR}"
            -o "${BENCHFS_DATA_DIR}/testfile"
          )

          save_job_params

          echo "${cmd_ior[@]}"
          "${cmd_ior[@]}" \
            > >(tee "${ior_output_file}") \
            2> >(tee "${IOR_OUTPUT_DIR}/ior_stderr_${runid}.txt" >&2) || true

          # Stop BenchFS servers
          echo "Stopping BenchFS servers..."
          kill $BENCHFSD_PID 2>/dev/null || true
          wait $BENCHFSD_PID 2>/dev/null || true

          # Wait for cleanup
          sleep 2

          runid=$((runid + 1))
        done
      done
    done
  done
done

echo "All benchmarks completed"
