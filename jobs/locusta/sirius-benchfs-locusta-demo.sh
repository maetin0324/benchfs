#!/bin/bash
#PBS -q mcrp
#PBS -A NBB
#
# Cross-node validation of LocustaTransport via mpirun.
#
# Sirius's sshd refuses connections originating from PBS-allocated TCP source
# ports between different a-nodes — so we use `mpirun --map-by ppr:1:node` to
# launch one process per host. The launched processes auto-pick their role
# from `OMPI_COMM_WORLD_RANK` (rank 0 = server, rank 1 = client).
#
# Knobs (env vars):
#   PINGS=N            number of round-trip pings (default 1000)
#   SERVE_SECS=N       how long the server stays up (default 120)
#   MODE=...           raw | metadata | amrpc | amrpc-put | put | get | dispatch
#                      (default metadata)
#   LOOKUP_PATH=...    path string for metadata-style modes
#                      (default /test/file.bin)
#   PAYLOAD_BYTES=N    DMA size for put/get modes (default 4 MiB)

set -e
hostname
echo "PBS_NODEFILE: $PBS_NODEFILE"
mapfile -t HOSTS < <(sort -u "$PBS_NODEFILE")
echo "unique hosts (${#HOSTS[@]}):"
printf '  %s\n' "${HOSTS[@]}"

if [ "${#HOSTS[@]}" -lt 1 ]; then
    echo "ERROR: PBS_NODEFILE empty"
    exit 1
fi

source /etc/profile.d/modules.sh 2>/dev/null
module purge 2>/dev/null
module load openmpi/5.0.9/gcc11.5.0
echo "openmpi: $(which mpirun)"

DEMO_BIN=/work/NBB/rmaeda/workspace/rust/benchfs/target/release/benchfs_locusta_demo
ls -la "$DEMO_BIN" || {
    echo "ERROR: demo binary missing — build with: cargo build --release --bin benchfs_locusta_demo --features transport-locusta"
    exit 1
}

REGISTRY=/work/NBB/rmaeda/workspace/rust/benchfs/results/locusta/demo-$(date +%Y%m%d-%H%M%S)-${PBS_JOBID%.pbs}/registry
mkdir -p "$REGISTRY"
echo "REGISTRY: $REGISTRY"

PINGS=${PINGS:-1000}
SERVE_SECS=${SERVE_SECS:-120}
MODE=${MODE:-metadata}
LOOKUP_PATH=${LOOKUP_PATH:-/test/file.bin}
PAYLOAD_BYTES=${PAYLOAD_BYTES:-4194304}

# Build per-rank wrapper. mpirun dispatches it to each rank; the wrapper
# looks at OMPI_COMM_WORLD_RANK and execs `benchfs_locusta_demo` with the
# right role + arg set. We avoid heredoc / quoting hell by writing to a
# tmp file in $REGISTRY.
WRAPPER="$REGISTRY/launch_demo.sh"
cat > "$WRAPPER" <<EOF
#!/bin/bash
set -e
exec >> "$REGISTRY/rank\${OMPI_COMM_WORLD_RANK}.log" 2>&1
echo "[rank=\$OMPI_COMM_WORLD_RANK host=\$(hostname -s)] starting"
case "\$OMPI_COMM_WORLD_RANK" in
    0)
        "$DEMO_BIN" --role server --local server_node --peer client_node \\
            --registry-dir "$REGISTRY" --serve-secs $SERVE_SECS --mode $MODE
        ;;
    1)
        # Give the server a moment to publish its QP info.
        sleep 2
        "$DEMO_BIN" --role client --local client_node --peer server_node \\
            --registry-dir "$REGISTRY" --pings $PINGS --mode $MODE \\
            --path $LOOKUP_PATH --payload-bytes $PAYLOAD_BYTES
        ;;
    *)
        echo "[rank=\$OMPI_COMM_WORLD_RANK] unexpected rank; exiting"
        exit 1
        ;;
esac
EOF
chmod +x "$WRAPPER"

# `--map-by ppr:1:node` places exactly 1 process per host. With select=2
# we get rank 0 on HOSTS[0] and rank 1 on HOSTS[1]. With select=1 (the
# loopback case) we'd only get 1 process — skip and fall back below.
if [ "${#HOSTS[@]}" -ge 2 ]; then
    HOSTLIST=$(printf "%s," "${HOSTS[@]:0:2}")
    HOSTLIST="${HOSTLIST%,}"
    echo "mpirun -np 2 --host $HOSTLIST ..."
    mpirun -np 2 --host "$HOSTLIST" --map-by ppr:1:node --bind-to none \
        -x BENCHFS_TRANSPORT \
        -x RUST_BACKTRACE \
        "$WRAPPER" || echo "mpirun exit=$?"
else
    # 1-host loopback — just run both directly under bash.
    echo "single-host loopback (no mpirun)"
    OMPI_COMM_WORLD_RANK=0 bash "$WRAPPER" &
    SERVER_PID=$!
    sleep 2
    OMPI_COMM_WORLD_RANK=1 bash "$WRAPPER" || echo "client exit=$?"
    wait $SERVER_PID || true
fi

echo ""
echo "===rank0 (server) log==="
cat "$REGISTRY/rank0.log" 2>/dev/null | head -60
echo ""
echo "===rank1 (client) log==="
cat "$REGISTRY/rank1.log" 2>/dev/null
