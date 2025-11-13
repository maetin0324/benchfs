#!/bin/bash
# IOR Test Script for BenchFS Mini with Separate Server/Client Processes
#
# Architecture:
#   - Servers: Run as standalone Rust processes (2 processes with mpirun)
#   - Clients: Run IOR benchmark (2 processes with mpirun)
#   - Servers and clients communicate via shared registry

set -e

# Configuration
REGISTRY_DIR="/shared/registry_mini"
NUM_SERVERS=2
NUM_CLIENTS=2

# IOR parameters (small values for debugging)
TRANSFER_SIZE="1m"    # 1 MB per transfer
BLOCK_SIZE="4m"       # 4 MB per process
SEGMENTS=2            # 2 segments

echo "========================================="
echo "BenchFS Mini - IOR Test"
echo "========================================="
echo "Cluster Configuration:"
echo "  Servers:         $NUM_SERVERS (separate processes)"
echo "  Clients:         $NUM_CLIENTS (IOR processes)"
echo "  Registry dir:    $REGISTRY_DIR"
echo ""
echo "IOR Configuration:"
echo "  Transfer size:   $TRANSFER_SIZE"
echo "  Block size:      $BLOCK_SIZE"
echo "  Segments:        $SEGMENTS"
echo "========================================="

# Clean registry directory
echo ""
echo "[Setup] Cleaning registry directory..."
rm -rf $REGISTRY_DIR/*
mkdir -p $REGISTRY_DIR
chmod 777 $REGISTRY_DIR

# Generate MPI hostfile for servers
SERVER_HOSTFILE="/tmp/hostfile_servers"
echo "[Setup] Generating server hostfile..."
cat > $SERVER_HOSTFILE << EOF
server1 slots=1
server2 slots=1
EOF

# Generate MPI hostfile for clients
CLIENT_HOSTFILE="/tmp/hostfile_clients"
echo "[Setup] Generating client hostfile..."
cat > $CLIENT_HOSTFILE << EOF
client1 slots=1
client2 slots=1
EOF

echo "[Setup] Server hostfile contents:"
cat $SERVER_HOSTFILE
echo ""
echo "[Setup] Client hostfile contents:"
cat $CLIENT_HOSTFILE

# Set up SSH known_hosts
echo ""
echo "[Setup] Setting up SSH known_hosts..."
mkdir -p ~/.ssh
chmod 700 ~/.ssh

for host in server1 server2 client1 client2; do
    ssh-keyscan -H $host >> ~/.ssh/known_hosts 2>/dev/null || true
done

# Test SSH connectivity
echo ""
echo "[Setup] Testing SSH connectivity..."
for host in server1 server2 client1 client2; do
    if ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 $host "echo OK" >/dev/null 2>&1; then
        echo "  ✓ $host: SSH OK"
    else
        echo "  ✗ $host: SSH FAILED"
        exit 1
    fi
done

# Wait for all nodes to be ready
echo ""
echo "[Setup] Waiting for all nodes to be ready..."
sleep 2

# Set environment variables
export RUST_LOG=debug
export RUST_BACKTRACE=full
export UCX_TLS=tcp,self
export UCX_LOG_LEVEL=trace

echo ""
echo "========================================="
echo "Step 1: Starting BenchFS Mini Servers"
echo "========================================="

# Start servers in background using mpirun
# Each server process will register itself in the shared registry
SERVER_LOG="/tmp/benchfs_servers.log"
mpirun \
  --allow-run-as-root \
  --hostfile $SERVER_HOSTFILE \
  -np $NUM_SERVERS \
  --map-by node \
  --bind-to none \
  -x RUST_LOG \
  -x RUST_BACKTRACE \
  -x UCX_TLS \
  -x UCX_LOG_LEVEL \
  benchfsd_mini --registry=$REGISTRY_DIR --server > $SERVER_LOG 2>&1 &

SERVER_PID=$!
echo "[Servers] Started with PID: $SERVER_PID"
echo "[Servers] Log file: $SERVER_LOG"

# Wait for servers to register
echo ""
echo "[Servers] Waiting for servers to register..."
MAX_WAIT=30
WAIT_COUNT=0
while [ $WAIT_COUNT -lt $MAX_WAIT ]; do
    # Count registered servers
    REGISTERED=$(ls $REGISTRY_DIR/server_*.txt 2>/dev/null | wc -l)

    if [ $REGISTERED -ge $NUM_SERVERS ]; then
        echo "[Servers] All $NUM_SERVERS servers registered successfully"
        break
    fi

    echo "[Servers] Waiting... ($REGISTERED/$NUM_SERVERS registered)"
    sleep 1
    WAIT_COUNT=$((WAIT_COUNT + 1))
done

if [ $WAIT_COUNT -ge $MAX_WAIT ]; then
    echo "[ERROR] Timeout waiting for servers to register"
    echo "[ERROR] Expected $NUM_SERVERS servers, found $REGISTERED"
    echo ""
    echo "[Debug] Registry directory contents:"
    ls -la $REGISTRY_DIR/
    echo ""
    echo "[Debug] Server logs:"
    cat $SERVER_LOG
    kill $SERVER_PID 2>/dev/null || true
    exit 1
fi

# Show registered servers
echo ""
echo "[Servers] Registered servers:"
for f in $REGISTRY_DIR/server_*.txt; do
    if [ -f "$f" ]; then
        echo "  - $(basename $f): $(cat $f)"
    fi
done

# Additional wait to ensure servers are fully ready
echo ""
echo "[Servers] Waiting for servers to be fully ready..."
sleep 3

echo ""
echo "========================================="
echo "Step 2: Running IOR Benchmark (Clients)"
echo "========================================="

# IOR command
IOR_CMD="ior \
  -a BENCHFSMINI \
  -t $TRANSFER_SIZE \
  -b $BLOCK_SIZE \
  -s $SEGMENTS \
  -F \
  -e \
  -w \
  -r \
  -C \
  -v \
  --benchfs.registry=$REGISTRY_DIR"

echo ""
echo "[IOR] Command: $IOR_CMD"
echo ""

# Run IOR with mpirun
mpirun \
  --allow-run-as-root \
  --hostfile $CLIENT_HOSTFILE \
  -np $NUM_CLIENTS \
  --map-by node \
  --bind-to none \
  -x RUST_LOG \
  -x RUST_BACKTRACE \
  -x UCX_TLS \
  -x UCX_LOG_LEVEL \
  $IOR_CMD

IOR_EXIT_CODE=$?

echo ""
echo "========================================="
echo "Step 3: Cleanup"
echo "========================================="

# Stop servers
echo ""
echo "[Cleanup] Stopping servers (PID: $SERVER_PID)..."
kill $SERVER_PID 2>/dev/null || true
wait $SERVER_PID 2>/dev/null || true
echo "[Cleanup] Servers stopped"

# Show server logs if IOR failed
if [ $IOR_EXIT_CODE -ne 0 ]; then
    echo ""
    echo "[Debug] Server logs:"
    cat $SERVER_LOG
fi

echo ""
echo "========================================="
if [ $IOR_EXIT_CODE -eq 0 ]; then
    echo "IOR Test: SUCCESS"
else
    echo "IOR Test: FAILED (exit code: $IOR_EXIT_CODE)"
fi
echo "========================================="

exit $IOR_EXIT_CODE
