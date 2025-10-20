#!/bin/bash
# Run MPI test for BenchFS
set -euo pipefail

NNODES=${1:-2}
REGISTRY_DIR="/shared/registry"
CONFIG_FILE="/configs/benchfs_test.toml"

echo "=========================================="
echo "BenchFS MPI Test"
echo "Number of nodes: $NNODES"
echo "Registry directory: $REGISTRY_DIR"
echo "=========================================="

# Clean up previous run
echo "Cleaning up previous run..."
rm -rf ${REGISTRY_DIR}/*
mkdir -p ${REGISTRY_DIR}

# Create hostfile
HOSTFILE="/tmp/hostfile"
if [ "$NNODES" -eq 2 ]; then
    cat > ${HOSTFILE} <<EOF
server1 slots=1
server2 slots=1
EOF
elif [ "$NNODES" -eq 4 ]; then
    cat > ${HOSTFILE} <<EOF
server1 slots=1
server2 slots=1
server3 slots=1
server4 slots=1
EOF
else
    echo "ERROR: Unsupported number of nodes: $NNODES"
    echo "Supported: 2, 4"
    exit 1
fi

echo "Hostfile:"
cat ${HOSTFILE}
echo

# Wait for SSH to be ready on all hosts
echo "Waiting for SSH services to be ready..."
sleep 3

for host in $(awk '{print $1}' ${HOSTFILE}); do
    echo "Checking connectivity to $host..."
    for i in {1..30}; do
        if ssh -o ConnectTimeout=2 ${host} "echo 'Connected to ${host}'" 2>/dev/null; then
            echo "  $host is ready"
            break
        fi
        if [ $i -eq 30 ]; then
            echo "  ERROR: Failed to connect to $host"
            exit 1
        fi
        sleep 1
    done
done

echo
echo "All nodes are ready. Starting BenchFS servers..."
echo

# Launch BenchFS servers with MPI
mpirun \
    --hostfile ${HOSTFILE} \
    -np ${NNODES} \
    --mca btl tcp,self \
    --mca btl_tcp_if_include eth0 \
    -x UCX_TLS=tcp,sm,self \
    -x RUST_LOG=info \
    benchfsd_mpi ${REGISTRY_DIR} ${CONFIG_FILE} &

SERVER_PID=$!

# Wait for servers to register
echo "Waiting for BenchFS servers to register..."
MAX_WAIT=60
WAIT_COUNT=0

while [ $WAIT_COUNT -lt $MAX_WAIT ]; do
    REGISTERED=$(find ${REGISTRY_DIR} -name "node_*.addr" 2>/dev/null | wc -l)

    if [ "$REGISTERED" -eq "$NNODES" ]; then
        echo "All $NNODES servers registered successfully!"
        break
    fi

    echo "  Registered: $REGISTERED/$NNODES"
    sleep 1
    WAIT_COUNT=$((WAIT_COUNT + 1))
done

if [ "$REGISTERED" -ne "$NNODES" ]; then
    echo "ERROR: Only $REGISTERED/$NNODES servers registered after ${MAX_WAIT}s"
    kill $SERVER_PID 2>/dev/null || true
    exit 1
fi

echo
echo "BenchFS cluster is running!"
echo "Registry contents:"
ls -lh ${REGISTRY_DIR}/

# Keep servers running
echo
echo "Press Ctrl+C to stop the servers..."
wait $SERVER_PID
