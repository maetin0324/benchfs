#!/bin/bash
# Complete test script for BenchFS cluster
set -euo pipefail

TEST_NAME=${1:-"basic"}
NNODES=${2:-2}

echo "=========================================="
echo "BenchFS Cluster Test: $TEST_NAME"
echo "Nodes: $NNODES"
echo "=========================================="

REGISTRY_DIR="/shared/registry"
DATA_DIR="/shared/data"
CONFIG_FILE="/configs/benchfs_test.toml"
RESULTS_DIR="/shared/results"

# Clean up
echo "Cleaning up previous test..."
rm -rf ${REGISTRY_DIR}/* ${DATA_DIR}/* ${RESULTS_DIR}/*
mkdir -p ${REGISTRY_DIR} ${DATA_DIR} ${RESULTS_DIR}

# Create hostfile
HOSTFILE="/tmp/hostfile_${NNODES}"
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
    echo "ERROR: Unsupported NNODES: $NNODES (supported: 2, 4)"
    exit 1
fi

# Wait for SSH
echo "Waiting for SSH services..."
for host in $(awk '{print $1}' ${HOSTFILE}); do
    for i in {1..30}; do
        if ssh -o ConnectTimeout=2 ${host} "echo OK" &>/dev/null; then
            echo "  $host: ready"
            break
        fi
        [ $i -eq 30 ] && { echo "ERROR: $host unreachable"; exit 1; }
        sleep 1
    done
done

echo
echo "Launching BenchFS servers..."

# Start servers in background
mpirun \
    --hostfile ${HOSTFILE} \
    -np ${NNODES} \
    --mca btl tcp,self \
    --mca btl_tcp_if_include eth0 \
    -x UCX_TLS=tcp,sm,self \
    -x RUST_LOG=info \
    benchfsd_mpi ${REGISTRY_DIR} ${CONFIG_FILE} \
    > ${RESULTS_DIR}/server_stdout.log 2> ${RESULTS_DIR}/server_stderr.log &

SERVER_PID=$!
echo "Server PID: $SERVER_PID"

# Wait for registration
echo "Waiting for server registration..."
for i in {1..60}; do
    REGISTERED=$(find ${REGISTRY_DIR} -name "node_*.addr" 2>/dev/null | wc -l)

    if [ "$REGISTERED" -eq "$NNODES" ]; then
        echo "All servers registered: $REGISTERED/$NNODES"
        break
    fi

    [ $i -eq 60 ] && {
        echo "ERROR: Registration timeout ($REGISTERED/$NNODES)"
        kill $SERVER_PID 2>/dev/null || true
        cat ${RESULTS_DIR}/server_stderr.log
        exit 1
    }

    [ $((i % 10)) -eq 0 ] && echo "  Waiting: $REGISTERED/$NNODES"
    sleep 1
done

# Give servers time to stabilize
sleep 2

echo
echo "=========================================="
echo "Running test: $TEST_NAME"
echo "=========================================="

case "$TEST_NAME" in
    "basic")
        echo "Test: Basic server connectivity"
        echo "Registry contents:"
        ls -lh ${REGISTRY_DIR}/

        echo
        echo "Checking server logs:"
        tail -20 ${RESULTS_DIR}/server_stdout.log

        TEST_RESULT="PASS"
        ;;

    "stress")
        echo "Test: Stress test with multiple operations"
        # TODO: Implement stress test
        TEST_RESULT="NOT_IMPLEMENTED"
        ;;

    *)
        echo "ERROR: Unknown test: $TEST_NAME"
        TEST_RESULT="FAIL"
        ;;
esac

# Cleanup
echo
echo "Stopping servers..."
kill $SERVER_PID 2>/dev/null || true
wait $SERVER_PID 2>/dev/null || true

echo
echo "=========================================="
echo "Test Result: $TEST_RESULT"
echo "=========================================="

if [ "$TEST_RESULT" = "PASS" ]; then
    echo "Server logs saved to: ${RESULTS_DIR}/"
    exit 0
else
    echo "Test failed. Check logs in: ${RESULTS_DIR}/"
    exit 1
fi
