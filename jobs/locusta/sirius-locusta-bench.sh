#!/bin/bash
#PBS -q mcrp
#PBS -A NBB
# Submit with -v PBS_SELECT_SPEC, see sirius-locusta-submit.sh

set -e
echo "=== locusta rrrpc-bench on Sirius ==="
hostname
echo "PBS_NODEFILE: $PBS_NODEFILE"
[ -f "$PBS_NODEFILE" ] && echo "Nodes:" && cat "$PBS_NODEFILE"

# Load modules
source /etc/profile.d/modules.sh 2>/dev/null
module purge
module load openmpi/5.0.9/gcc11.5.0
echo "openmpi loaded: $(which mpirun)"

BENCH_BIN=/work/NBB/rmaeda/workspace/rust/benchfs/lib/locusta/target/release/rrrpc-bench
ls -la "$BENCH_BIN" || { echo "ERROR: bench binary not found"; exit 1; }

OUTDIR=/work/NBB/rmaeda/workspace/rust/benchfs/results/locusta/$(date +%Y%m%d-%H%M%S)-${PBS_JOBID%.pbs}
mkdir -p "$OUTDIR"
cd "$OUTDIR"
echo "OUTDIR: $OUTDIR"

# Common mpirun args (UCX for MPI itself; locusta uses its own mlx5 transport)
COMMON_MPI=(
    mpirun
    --mca prte_silence_shared_fs 1
    --mca routed direct
    --mca plm_rsh_no_tree_spawn 1
    --mca pml ob1
    --mca btl tcp,sm,self
    --bind-to none
    --oversubscribe
    -x PATH
    -x LD_LIBRARY_PATH
)

# 2-process MPI launch (1 process per node). Each process runs:
#   1 client thread + 1 relay thread + 1 server thread (= 3 threads).
COMM_TYPES=(roundtrip-eager roundtrip-put roundtrip-get)
SIZES_E=(64 256)            # eager protocol message sizes
SIZES_DMA=(65536 1048576 4194304)  # DMA pattern message sizes

for COMM_TYPE in "${COMM_TYPES[@]}"; do
  case "$COMM_TYPE" in
    roundtrip-eager)
      SIZES=("${SIZES_E[@]}")
      ;;
    *)
      SIZES=("${SIZES_DMA[@]}")
      ;;
  esac
  for SIZE in "${SIZES[@]}"; do
    LABEL="${COMM_TYPE}-${SIZE}"
    OUTFILE="${OUTDIR}/${LABEL}.parquet"
    LOGFILE="${OUTDIR}/${LABEL}.log"
    echo ""
    echo "=== Run: ${LABEL} ==="
    timeout 90 "${COMMON_MPI[@]}" -np 2 \
      "$BENCH_BIN" \
        --comm-type "$COMM_TYPE" \
        --message-size "$SIZE" \
        --num-clients 1 \
        --num-servers 1 \
        --num-relays 1 \
        --stonewall-time 15 \
        --max-inflights 32 \
        --ring-size 256 \
        --label "$LABEL" \
        --output "$OUTFILE" 2>&1 | tee "$LOGFILE"
    echo "exit: $?"
  done
done

echo ""
echo "=== All runs complete ==="
ls -la "$OUTDIR"
