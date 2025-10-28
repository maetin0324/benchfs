#!/bin/bash
# 4GiB SSF test (16GB total across 4 ranks) - intermediate size test
set -euo pipefail

echo "=========================================="
echo "BenchFS IOR Test: 4GiB SSF"
echo "Nodes: 4, Size: 4GiB per rank (16GB total)"
echo "=========================================="

REGISTRY_DIR="/shared/registry"
DATA_DIR="/shared/data"
CONFIG_FILE="/configs/benchfs_test.toml"
RESULTS_DIR="/shared/results"
IOR_BIN="/usr/local/bin/ior"

# Clean up
echo "Cleaning up previous test..."
rm -rf ${REGISTRY_DIR}/* ${DATA_DIR}/* ${RESULTS_DIR}/*
mkdir -p ${REGISTRY_DIR} ${DATA_DIR} ${RESULTS_DIR}

# Create hostfile
HOSTFILE="/tmp/hostfile_4"
cat > ${HOSTFILE} <<EOF
server1 slots=1
server2 slots=1
server3 slots=1
server4 slots=1
EOF

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
    -np 4 \
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

    if [ "$REGISTERED" -eq 4 ]; then
        echo "All servers registered: $REGISTERED/4"
        break
    fi

    [ $i -eq 60 ] && {
        echo "ERROR: Registration timeout ($REGISTERED/4)"
        kill $SERVER_PID 2>/dev/null || true
        cat ${RESULTS_DIR}/server_stderr.log
        exit 1
    }

    [ $((i % 10)) -eq 0 ] && echo "  Waiting: $REGISTERED/4"
    sleep 1
done

# Give servers time to stabilize
sleep 2

echo
echo "=========================================="
echo "Running 4GiB SSF Test"
echo "Size: 4GiB per rank (64MB x 64 segments)"
echo "Total: 16GB across 4 ranks"
echo "=========================================="

mpirun \
    --hostfile ${HOSTFILE} \
    -np 4 \
    --mca btl tcp,self \
    --mca btl_tcp_if_include eth0 \
    ${IOR_BIN} \
        -a BENCHFS \
        --benchfs.registry ${REGISTRY_DIR} \
        --benchfs.datadir ${DATA_DIR} \
        -w -r \
        -t 2m -b 64m -s 64 \
        -o /test_4gib_ssf \
        -O summaryFormat=JSON \
        > ${RESULTS_DIR}/ior_4gib_output.txt 2>&1

IOR_EXIT_CODE=$?

echo
echo "IOR Output (last 100 lines):"
tail -100 ${RESULTS_DIR}/ior_4gib_output.txt

# Cleanup
echo
echo "Stopping servers..."
kill $SERVER_PID 2>/dev/null || true
wait $SERVER_PID 2>/dev/null || true

if [ $IOR_EXIT_CODE -eq 0 ]; then
    echo "=========================================="
    echo "Test Result: PASS"
    echo "=========================================="
    exit 0
else
    echo "=========================================="
    echo "Test Result: FAIL"
    echo "=========================================="
    echo
    echo "=== Server stderr (last 100 lines) ==="
    tail -100 ${RESULTS_DIR}/server_stderr.log
    exit 1
fi
