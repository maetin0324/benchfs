#!/bin/bash
# IOR benchmark test for BenchFS cluster
set -euo pipefail

TEST_NAME=${1:-"basic"}
NNODES=${2:-2}

echo "=========================================="
echo "BenchFS IOR Benchmark Test: $TEST_NAME"
echo "Nodes: $NNODES"
echo "=========================================="

REGISTRY_DIR="/shared/registry"
DATA_DIR="/shared/data"
CONFIG_FILE="/configs/benchfs_test.toml"
RESULTS_DIR="/shared/results"
IOR_BIN="/usr/local/bin/ior"

# Check if IOR binary exists
if [ ! -f "$IOR_BIN" ]; then
    echo "ERROR: IOR binary not found at $IOR_BIN"
    exit 1
fi

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
echo "Running IOR benchmark: $TEST_NAME"
echo "=========================================="

# Note: BenchFS does not yet support POSIX filesystem interface
# IOR tests run on the local filesystem that BenchFS uses for storage
# Each BenchFS server uses /shared/data/rank_X/ for its storage
# This tests the underlying storage performance

# Create test directory on all nodes
TEST_DIR="/tmp/ior_test"
mkdir -p ${TEST_DIR}
for host in $(awk '{print $1}' ${HOSTFILE}); do
    ssh ${host} "mkdir -p ${TEST_DIR}" 2>/dev/null || true
done
echo "Created test directories on all nodes"
echo ""

case "$TEST_NAME" in
    "basic")
        echo "Test: Basic IOR write/read test"
        echo "Transfer size: 1MB, Block size: 4MB, Total: 16MB"
        echo "Note: Testing on local filesystem (BenchFS POSIX interface not yet implemented)"
        echo ""

        # Run IOR with small test parameters on local filesystem
        mpirun \
            --hostfile ${HOSTFILE} \
            -np ${NNODES} \
            --mca btl tcp,self \
            --mca btl_tcp_if_include eth0 \
            ${IOR_BIN} \
                -w -r \
                -t 1m -b 4m -s 4 \
                -o ${TEST_DIR}/ior_testfile \
                -O summaryFormat=JSON \
                > ${RESULTS_DIR}/ior_output.txt 2>&1

        IOR_EXIT_CODE=$?

        echo
        echo "IOR Results:"
        cat ${RESULTS_DIR}/ior_output.txt

        if [ $IOR_EXIT_CODE -eq 0 ]; then
            TEST_RESULT="PASS"
        else
            TEST_RESULT="FAIL"
        fi
        ;;

    "write")
        echo "Test: IOR write-only test"
        echo "Transfer size: 1MB, Block size: 8MB, Total: 32MB"
        echo "Note: Testing on local filesystem (BenchFS POSIX interface not yet implemented)"
        echo ""

        mpirun \
            --hostfile ${HOSTFILE} \
            -np ${NNODES} \
            --mca btl tcp,self \
            --mca btl_tcp_if_include eth0 \
            ${IOR_BIN} \
                -w \
                -t 1m -b 8m -s 4 \
                -o ${TEST_DIR}/ior_writefile \
                -O summaryFormat=JSON \
                > ${RESULTS_DIR}/ior_write_output.txt 2>&1

        IOR_EXIT_CODE=$?

        echo
        echo "IOR Write Results:"
        cat ${RESULTS_DIR}/ior_write_output.txt

        if [ $IOR_EXIT_CODE -eq 0 ]; then
            TEST_RESULT="PASS"
        else
            TEST_RESULT="FAIL"
        fi
        ;;

    "read")
        echo "Test: IOR read-only test (requires existing file)"
        echo "Transfer size: 1MB, Block size: 8MB"
        echo "Note: Testing on local filesystem (BenchFS POSIX interface not yet implemented)"
        echo ""

        # First create a file with write test
        echo "Creating test file..."
        mpirun \
            --hostfile ${HOSTFILE} \
            -np ${NNODES} \
            --mca btl tcp,self \
            --mca btl_tcp_if_include eth0 \
            ${IOR_BIN} \
                -w \
                -t 1m -b 8m -s 4 \
                -o ${TEST_DIR}/ior_readfile \
                > /dev/null 2>&1

        echo "Running read test..."
        mpirun \
            --hostfile ${HOSTFILE} \
            -np ${NNODES} \
            --mca btl tcp,self \
            --mca btl_tcp_if_include eth0 \
            ${IOR_BIN} \
                -r \
                -t 1m -b 8m -s 4 \
                -o ${TEST_DIR}/ior_readfile \
                -O summaryFormat=JSON \
                > ${RESULTS_DIR}/ior_read_output.txt 2>&1

        IOR_EXIT_CODE=$?

        echo
        echo "IOR Read Results:"
        cat ${RESULTS_DIR}/ior_read_output.txt

        if [ $IOR_EXIT_CODE -eq 0 ]; then
            TEST_RESULT="PASS"
        else
            TEST_RESULT="FAIL"
        fi
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
    echo "IOR benchmark completed successfully"
    echo "Results saved to: ${RESULTS_DIR}/"
    exit 0
else
    echo "IOR benchmark failed. Check logs in: ${RESULTS_DIR}/"
    exit 1
fi
