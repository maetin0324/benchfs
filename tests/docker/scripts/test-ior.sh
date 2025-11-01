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
    -x RUST_LOG=debug \
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

# IOR tests will use BenchFS backend (-a BENCHFS)
# This tests the actual distributed filesystem performance

echo "Using BenchFS backend with registry: ${REGISTRY_DIR}"
echo ""

case "$TEST_NAME" in
    "basic")
        echo "Test: Basic IOR write/read test with BenchFS"
        echo "Transfer size: 1MB, Block size: 4MB, Total: 16MB"
        echo ""

        # Run IOR with BenchFS backend
        mpirun \
            --hostfile ${HOSTFILE} \
            -np ${NNODES} \
            --mca btl tcp,self \
            --mca btl_tcp_if_include eth0 \
            -x RUST_LOG \
            -x BENCHFS_OPERATION_TIMEOUT \
            ${IOR_BIN} \
                -a BENCHFS \
                --benchfs.registry ${REGISTRY_DIR} \
                --benchfs.datadir ${DATA_DIR} \
                -w -r \
                -t 1m -b 4m -s 4 \
                -o /testfile \
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
        echo "Test: IOR write-only test with BenchFS"
        echo "Transfer size: 1MB, Block size: 8MB, Total: 32MB"
        echo ""

        mpirun \
            --hostfile ${HOSTFILE} \
            -np ${NNODES} \
            --mca btl tcp,self \
            --mca btl_tcp_if_include eth0 \
            ${IOR_BIN} \
                -a BENCHFS \
                --benchfs.registry ${REGISTRY_DIR} \
                --benchfs.datadir ${DATA_DIR} \
                -w \
                -t 1m -b 8m -s 4 \
                -o /writefile \
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
        echo "Test: IOR read-only test with BenchFS (requires existing file)"
        echo "Transfer size: 1MB, Block size: 8MB"
        echo ""

        # First create a file with write test
        echo "Creating test file..."
        mpirun \
            --hostfile ${HOSTFILE} \
            -np ${NNODES} \
            --mca btl tcp,self \
            --mca btl_tcp_if_include eth0 \
            ${IOR_BIN} \
                -a BENCHFS \
                --benchfs.registry ${REGISTRY_DIR} \
                --benchfs.datadir ${DATA_DIR} \
                -w \
                -t 1m -b 8m -s 4 \
                -o /readfile \
                > /dev/null 2>&1

        echo "Running read test..."
        mpirun \
            --hostfile ${HOSTFILE} \
            -np ${NNODES} \
            --mca btl tcp,self \
            --mca btl_tcp_if_include eth0 \
            ${IOR_BIN} \
                -a BENCHFS \
                --benchfs.registry ${REGISTRY_DIR} \
                --benchfs.datadir ${DATA_DIR} \
                -r \
                -t 1m -b 8m -s 4 \
                -o /readfile \
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

    "large-perf")
        echo "Test: Large IOR test with perf profiling"
        echo "Transfer size: 1MB, Block size: 32MB, Segments: 32, Total: 4GB"
        echo ""

        # Start perf on server1
        echo "Starting perf profiling on server1..."
        ssh server1 "perf record -a -g -F 99 -o /shared/results/perf-server1.data -- sleep 45" > /dev/null 2>&1 &
        PERF_PID=$!

        sleep 2

        # Run IOR with dataset that runs for ~30 seconds
        mpirun \
            --hostfile ${HOSTFILE} \
            -np ${NNODES} \
            --mca btl tcp,self \
            --mca btl_tcp_if_include eth0 \
            ${IOR_BIN} \
                -a BENCHFS \
                --benchfs.registry ${REGISTRY_DIR} \
                --benchfs.datadir ${DATA_DIR} \
                -w -r \
                -t 1m -b 32m -s 32 \
                -o /largefile \
                -O summaryFormat=JSON \
                > ${RESULTS_DIR}/ior_large_output.txt 2>&1

        IOR_EXIT_CODE=$?

        # Stop perf
        kill $PERF_PID 2>/dev/null || true
        wait $PERF_PID 2>/dev/null || true

        echo
        echo "IOR Large Results:"
        cat ${RESULTS_DIR}/ior_large_output.txt

        # Generate perf report
        echo
        echo "Generating perf report..."
        ssh server1 "cd /shared/results && perf report -i perf-server1.data --stdio > perf-report.txt 2>&1" || true

        if [ $IOR_EXIT_CODE -eq 0 ]; then
            TEST_RESULT="PASS"
        else
            TEST_RESULT="FAIL"
        fi
        ;;

    "8gib-ssf")
        echo "Test: 8GiB Shared Single File (SSF) I/O test"
        echo "Transfer size: 2MB, Block size: 64MB, Segments: 128, Total: 8GiB per rank (32GiB total)"
        echo "Mode: SSF (Shared Single File) - all ranks write to same file"
        echo "Nodes: 4 (4 MPI ranks)"
        echo ""

        # Verify we have 4 nodes
        if [ "$NNODES" -ne 4 ]; then
            echo "ERROR: 8gib-ssf test requires exactly 4 nodes (got $NNODES)"
            TEST_RESULT="FAIL"
            exit 1
        fi

        # SSF (Shared Single File) mode: no -F flag
        # 8GiB per rank calculation: 64MB (block size) × 128 (segments) = 8192MB = 8GiB per rank
        # Total across 4 ranks in SSF mode: 8GiB × 4 = 32GiB aggregate
        # Transfer size 2MB provides good I/O granularity for network and storage

        echo "Starting 8GiB SSF benchmark (this may take several minutes)..."
        echo "Each rank will write/read 8GiB (64MB × 128 segments)"

        mpirun \
            --hostfile ${HOSTFILE} \
            -np ${NNODES} \
            --mca btl tcp,self \
            --mca btl_tcp_if_include eth0 \
            ${IOR_BIN} \
                -a BENCHFS \
                --benchfs.registry ${REGISTRY_DIR} \
                --benchfs.datadir ${DATA_DIR} \
                -w -r \
                -t 2m -b 64m -s 128 \
                -o /8gib_ssf_testfile \
                -O summaryFormat=JSON \
                > ${RESULTS_DIR}/ior_8gib_ssf_output.txt 2>&1

        IOR_EXIT_CODE=$?

        echo
        echo "IOR 8GiB SSF Results:"
        cat ${RESULTS_DIR}/ior_8gib_ssf_output.txt

        # Display key metrics
        echo
        echo "Key Performance Metrics:"
        grep -E "write|read|Max Write|Max Read|bandwidth" ${RESULTS_DIR}/ior_8gib_ssf_output.txt || true

        if [ $IOR_EXIT_CODE -eq 0 ]; then
            TEST_RESULT="PASS"
            echo
            echo "SUCCESS: 8GiB SSF benchmark completed"
        else
            TEST_RESULT="FAIL"
            echo
            echo "FAILURE: 8GiB SSF benchmark failed with exit code $IOR_EXIT_CODE"
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
