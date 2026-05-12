#!/bin/bash
#PBS -q mcrp
#PBS -A NBB
# Submit with: qsub -l select=4:host=a01+4:host=a14 -l place=exclhost -l walltime=00:10:00 -V sirius-cross-node-ping.sh
#
# Locusta cross-node ping using file-based QP exchange.
# Runs 1 server process on the first host, 1 client process on the second host,
# both processes communicate via /work/.../registry/.

set -e
hostname
echo "PBS_NODEFILE: $PBS_NODEFILE"
mapfile -t HOSTS < <(sort -u "$PBS_NODEFILE")
echo "unique hosts (${#HOSTS[@]}):"
printf '  %s\n' "${HOSTS[@]}"

if [ "${#HOSTS[@]}" -lt 2 ]; then
    echo "ERROR: need at least 2 hosts"
    exit 1
fi
SERVER_HOST="${HOSTS[0]}"
CLIENT_HOST="${HOSTS[1]}"
echo "SERVER: $SERVER_HOST"
echo "CLIENT: $CLIENT_HOST"

PING_BIN=/work/NBB/rmaeda/workspace/rust/benchfs/lib/locusta/target/release/cross-node-ping
ls -la "$PING_BIN" || { echo "ERROR: ping binary missing"; exit 1; }

REGISTRY=/work/NBB/rmaeda/workspace/rust/benchfs/results/locusta/ping-$(date +%Y%m%d-%H%M%S)-${PBS_JOBID%.pbs}/registry
mkdir -p "$REGISTRY"
echo "REGISTRY: $REGISTRY"

# Launch server in background on SERVER_HOST
ssh -o StrictHostKeyChecking=no "$SERVER_HOST" bash -lc "
    cd /work/NBB/rmaeda/workspace/rust/benchfs/lib/locusta &&
    $PING_BIN --role server --registry-dir $REGISTRY --serve-secs 120 \
      2>&1 | sed 's/^/SRV: /'
" > $REGISTRY/server.log 2>&1 &
SERVER_PID=$!
echo "server pid: $SERVER_PID"

sleep 2  # let server prepare

# Run client in foreground on CLIENT_HOST
ssh -o StrictHostKeyChecking=no "$CLIENT_HOST" bash -lc "
    cd /work/NBB/rmaeda/workspace/rust/benchfs/lib/locusta &&
    $PING_BIN --role client --registry-dir $REGISTRY --pings 1000 --batch-size 1 \
      2>&1 | sed 's/^/CLI: /'
" > $REGISTRY/client.log 2>&1
CLIENT_EXIT=$?
echo "client exit: $CLIENT_EXIT"

# Wait briefly then stop server (it will exit on serve-secs timeout)
sleep 1
wait $SERVER_PID || true

echo ""
echo "===Server log==="
cat $REGISTRY/server.log
echo ""
echo "===Client log==="
cat $REGISTRY/client.log
