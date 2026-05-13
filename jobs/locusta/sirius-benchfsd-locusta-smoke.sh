#!/bin/bash
#PBS -q debug
#PBS -A NBB
#
# Smoke test: start `benchfsd_mpi` with `BENCHFS_TRANSPORT=locusta` on 2 ranks
# and verify each rank successfully runs `LocustaTransport::init`.
#
# Pass criterion: every rank log shows
#   "LocustaTransport connected to 1 peers"
# within 30 seconds of startup.

set -e
hostname
echo "PBS_NODEFILE: $PBS_NODEFILE"
mapfile -t HOSTS < <(sort -u "$PBS_NODEFILE")
echo "unique hosts (${#HOSTS[@]}):"
printf '  %s\n' "${HOSTS[@]}"

source /etc/profile.d/modules.sh 2>/dev/null
module purge 2>/dev/null
module load openmpi/5.0.9/gcc11.5.0
echo "openmpi: $(which mpirun)"

BENCHFSD=/work/NBB/rmaeda/workspace/rust/benchfs/target/release/benchfsd_mpi
ls -la "$BENCHFSD" || { echo "ERROR: benchfsd_mpi missing"; exit 1; }

OUTDIR=/work/NBB/rmaeda/workspace/rust/benchfs/results/locusta/benchfsd-smoke-$(date +%Y%m%d-%H%M%S)-${PBS_JOBID%.pbs}
mkdir -p "$OUTDIR"
REGISTRY="$OUTDIR/registry"
mkdir -p "$REGISTRY"
echo "OUTDIR: $OUTDIR"

cat > "$OUTDIR/config.toml" <<EOF
[node]
node_id = "node_0"

[storage]
backend = "iouring"
scratch_dirs = ["/tmp/benchfs_smoke"]
chunk_size = 4194304
EOF

# Per-rank wrapper that `exec`s benchfsd_mpi so MPI_Init sees the
# correct mpirun-provided environment (backgrounding broke this in
# the first attempt — MPI_COMM_WORLD_SIZE came back as 1).
# The daemon runs until the mpirun job hits its walltime or we kill it
# externally; for a smoke test that's good enough — we only check that
# `LocustaTransport connected to N peers` appeared in the log.
WRAPPER="$OUTDIR/launch.sh"
cat > "$WRAPPER" <<EOF
#!/bin/bash
exec >> "$OUTDIR/rank\${OMPI_COMM_WORLD_RANK}.log" 2>&1
mkdir -p /tmp/benchfs_smoke
echo "[rank=\$OMPI_COMM_WORLD_RANK host=\$(hostname -s)] starting benchfsd_mpi"
export RUST_LOG=info,benchfs=debug
export BENCHFS_TRANSPORT=locusta
exec "$BENCHFSD" "$REGISTRY" "$OUTDIR/config.toml"
EOF
chmod +x "$WRAPPER"

# Always use mpirun so MPI_Init sees a real 2-rank world. When PBS
# gave us only 1 host (loopback), map both ranks onto it; otherwise
# scatter one per node.
if [ "${#HOSTS[@]}" -ge 2 ]; then
    HOSTLIST=$(printf "%s," "${HOSTS[@]:0:2}")
    HOSTLIST="${HOSTLIST%,}"
    PLACE="--map-by ppr:1:node"
else
    HOSTLIST="${HOSTS[0]}:2"
    PLACE="--map-by core"
fi
echo "mpirun -np 2 --host $HOSTLIST $PLACE ..."
mpirun -np 2 --host "$HOSTLIST" $PLACE --bind-to none \
    -x BENCHFS_TRANSPORT -x RUST_LOG \
    "$WRAPPER" || echo "mpirun exit=$?"

echo ""
echo "===rank0 log==="
tail -40 "$OUTDIR/rank0.log" 2>/dev/null
echo ""
echo "===rank1 log==="
tail -40 "$OUTDIR/rank1.log" 2>/dev/null
