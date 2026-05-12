#!/bin/bash
#PBS -q mcrp
#PBS -A NBB
# Submit with: qsub -l select=4:host=a01+4:host=a14 -l place=exclhost -l walltime=00:10:00 -V sirius-benchfs-locusta-demo.sh
#
# Phase 1b validation: drive LocustaTransport (from src/rpc/transport_locusta.rs)
# via the benchfs_locusta_demo binary on 2 nodes.

set -e
hostname
echo "PBS_NODEFILE: $PBS_NODEFILE"
mapfile -t HOSTS < <(sort -u "$PBS_NODEFILE")
echo "unique hosts (${#HOSTS[@]}):"
printf '  %s\n' "${HOSTS[@]}"

SERVER_HOST="${HOSTS[0]}"
if [ "${#HOSTS[@]}" -ge 2 ]; then
    CLIENT_HOST="${HOSTS[1]}"
else
    CLIENT_HOST="${HOSTS[0]}"
    echo "NOTE: only 1 host available — running server + client on same host (loopback)"
fi
echo "SERVER: $SERVER_HOST"
echo "CLIENT: $CLIENT_HOST"

DEMO_BIN=/work/NBB/rmaeda/workspace/rust/benchfs/target/release/benchfs_locusta_demo
ls -la "$DEMO_BIN" || { echo "ERROR: demo binary missing — build with: cargo build --release --bin benchfs_locusta_demo --features transport-locusta"; exit 1; }

REGISTRY=/work/NBB/rmaeda/workspace/rust/benchfs/results/locusta/demo-$(date +%Y%m%d-%H%M%S)-${PBS_JOBID%.pbs}/registry
mkdir -p "$REGISTRY"
echo "REGISTRY: $REGISTRY"

PINGS=${PINGS:-1000}
SERVE_SECS=${SERVE_SECS:-120}
MODE=${MODE:-metadata}            # raw | metadata
LOOKUP_PATH=${LOOKUP_PATH:-/test/file.bin}

# Helper: run a command either via ssh (different host) or directly (same host as the
# script). PBS allocates this script onto the first chunk's host, so an ssh into that
# same host can be replaced by a direct invocation, which avoids sshd loopback
# rejection (Sirius sshd refuses connections from PBS-allocated TCP source ports).
THIS_HOST="$(hostname -s)"
run_on() {
    local target="$1"; shift
    if [ "$target" = "$THIS_HOST" ]; then
        bash -lc "$@"
    else
        ssh -o StrictHostKeyChecking=no "$target" bash -lc "$@"
    fi
}

# Launch server in background on SERVER_HOST
run_on "$SERVER_HOST" "
    cd /work/NBB/rmaeda/workspace/rust/benchfs &&
    $DEMO_BIN --role server --local server_node --peer client_node \
      --registry-dir $REGISTRY --serve-secs $SERVE_SECS --mode $MODE \
      2>&1 | sed 's/^/SRV: /'
" > $REGISTRY/server.log 2>&1 &
SERVER_PID=$!
echo "server pid: $SERVER_PID"

sleep 2  # let server prepare QP info

# Run client in foreground on CLIENT_HOST
run_on "$CLIENT_HOST" "
    cd /work/NBB/rmaeda/workspace/rust/benchfs &&
    $DEMO_BIN --role client --local client_node --peer server_node \
      --registry-dir $REGISTRY --pings $PINGS --mode $MODE --path $LOOKUP_PATH \
      2>&1 | sed 's/^/CLI: /'
" > $REGISTRY/client.log 2>&1
CLIENT_EXIT=$?
echo "client exit: $CLIENT_EXIT"

sleep 1
wait $SERVER_PID || true

echo ""
echo "===Server log==="
cat $REGISTRY/server.log
echo ""
echo "===Client log==="
cat $REGISTRY/client.log
