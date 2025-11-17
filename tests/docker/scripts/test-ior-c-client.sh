#!/bin/bash
# IOR benchmark test for BenchFS with C Client
# This script runs IOR using the pure C client library (libbenchfs_c_api.so)
# instead of the Rust FFI version.

set -euo pipefail

TEST_NAME=${1:-"basic"}
NNODES=${2:-4}

echo "=========================================="
echo "BenchFS IOR Test with C Client: $TEST_NAME"
echo "Nodes: $NNODES"
echo "=========================================="

REGISTRY_DIR="/shared/registry"
DATA_DIR="/shared/data"
RESULTS_DIR="/shared/results"
IOR_BIN="/usr/local/bin/ior"

# Verify C client library is being used
echo "Verifying C client library..."
if ldd ${IOR_BIN} | grep -q "libbenchfs.so"; then
    echo "✓ IOR is linked with libbenchfs.so (C client)"
    ldd ${IOR_BIN} | grep libbenchfs
else
    echo "✗ ERROR: IOR not properly linked with BenchFS library"
    ldd ${IOR_BIN}
    exit 1
fi

# Check library details
echo ""
echo "Library information:"
ls -lh /usr/local/lib/libbenchfs.so
file /usr/local/lib/libbenchfs.so

# Clean results directory
echo ""
echo "Preparing results directory..."
rm -rf ${RESULTS_DIR}/*
mkdir -p ${RESULTS_DIR}

# Wait for registry to be populated by servers
echo ""
echo "Waiting for BenchFS servers to register..."
for i in {1..60}; do
    # Check for AM RPC registration files (node_*.am_hostname)
    REGISTERED=$(find ${REGISTRY_DIR} -name "node_*.am_hostname" 2>/dev/null | wc -l)

    if [ "$REGISTERED" -eq "$NNODES" ]; then
        echo "✓ All servers registered: $REGISTERED/$NNODES"
        break
    fi

    [ $i -eq 60 ] && {
        echo "✗ ERROR: Server registration timeout ($REGISTERED/$NNODES)"
        echo "Registry contents:"
        ls -la ${REGISTRY_DIR}/ || true
        exit 1
    }

    [ $((i % 10)) -eq 0 ] && echo "  Waiting: $REGISTERED/$NNODES nodes registered"
    sleep 1
done

# List registered servers
echo ""
echo "Registered servers:"
ls -1 ${REGISTRY_DIR}/node_*.am_hostname 2>/dev/null || true

# Give servers time to stabilize
sleep 2

echo ""
echo "=========================================="
echo "Running IOR benchmark: $TEST_NAME"
echo "=========================================="

# Note: IOR will use the BENCHFS AIORI adapter which calls benchfs_c_api.h
# The C client implementation (libbenchfs_c_api.so) is linked as libbenchfs.so

case "$TEST_NAME" in
    "basic")
        echo "Test: Basic IOR write/read test with C client"
        echo "Configuration:"
        echo "  - Transfer size: 1MB"
        echo "  - Block size: 4MB"
        echo "  - Segments per rank: 4"
        echo "  - Total per rank: 16MB"
        echo "  - Total aggregate: $((16 * NNODES))MB"
        echo ""

        # Run IOR with BenchFS backend
        # DEBUG: Not redirecting stderr to see debug logs
        mpirun \
            -np ${NNODES} \
            --mca btl tcp,self \
            --mca btl_tcp_if_include eth0 \
            --allow-run-as-root \
            -x LD_LIBRARY_PATH=/usr/local/lib \
            -x UCX_TLS=tcp,self \
            -x UCX_LOG_LEVEL=warn \
            ${IOR_BIN} \
                -a BENCHFS \
                --benchfs.registry ${REGISTRY_DIR} \
                --benchfs.datadir ${DATA_DIR} \
                -w -r \
                -t 1m -b 4m -s 4 \
                -o /testfile \
                -O summaryFormat=JSON \
                -v \
                > ${RESULTS_DIR}/ior_output.txt

        IOR_EXIT_CODE=$?
        ;;

    "write")
        echo "Test: IOR write-only test with C client"
        echo "Configuration:"
        echo "  - Transfer size: 1MB"
        echo "  - Block size: 8MB"
        echo "  - Segments per rank: 4"
        echo "  - Total per rank: 32MB"
        echo ""

        mpirun \
            -np ${NNODES} \
            --mca btl tcp,self \
            --mca btl_tcp_if_include eth0 \
            --allow-run-as-root \
            -x LD_LIBRARY_PATH=/usr/local/lib \
            -x UCX_TLS=tcp,self \
            ${IOR_BIN} \
                -a BENCHFS \
                --benchfs.registry ${REGISTRY_DIR} \
                --benchfs.datadir ${DATA_DIR} \
                -w \
                -t 1m -b 8m -s 4 \
                -o /writefile \
                -O summaryFormat=JSON \
                -v \
                > ${RESULTS_DIR}/ior_output.txt 2>&1

        IOR_EXIT_CODE=$?
        ;;

    "read")
        echo "Test: IOR read-only test with C client"
        echo "Configuration:"
        echo "  - Transfer size: 1MB"
        echo "  - Block size: 8MB"
        echo "  - Segments per rank: 4"
        echo ""

        # First create a file
        echo "Creating test file..."
        mpirun \
            -np ${NNODES} \
            --mca btl tcp,self \
            --mca btl_tcp_if_include eth0 \
            --allow-run-as-root \
            -x LD_LIBRARY_PATH=/usr/local/lib \
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
            -np ${NNODES} \
            --mca btl tcp,self \
            --mca btl_tcp_if_include eth0 \
            --allow-run-as-root \
            -x LD_LIBRARY_PATH=/usr/local/lib \
            ${IOR_BIN} \
                -a BENCHFS \
                --benchfs.registry ${REGISTRY_DIR} \
                --benchfs.datadir ${DATA_DIR} \
                -r \
                -t 1m -b 8m -s 4 \
                -o /readfile \
                -O summaryFormat=JSON \
                -v \
                > ${RESULTS_DIR}/ior_output.txt 2>&1

        IOR_EXIT_CODE=$?
        ;;

    "4gib-ssf")
        echo "Test: 4GiB Shared Single File (SSF) I/O test with C client"
        echo "Configuration:"
        echo "  - Transfer size: 2MB"
        echo "  - Block size: 64MB"
        echo "  - Segments per rank: 64"
        echo "  - Total per rank: 4GiB"
        echo "  - Total aggregate: $((4 * NNODES))GiB"
        echo "  - Mode: SSF (Shared Single File)"
        echo ""

        if [ "$NNODES" -ne 4 ]; then
            echo "WARNING: This test is optimized for 4 nodes (got $NNODES)"
        fi

        echo "Starting 4GiB SSF benchmark (this may take several minutes)..."

        mpirun \
            -np ${NNODES} \
            --mca btl tcp,self \
            --mca btl_tcp_if_include eth0 \
            --allow-run-as-root \
            -x LD_LIBRARY_PATH=/usr/local/lib \
            -x UCX_TLS=tcp,self \
            ${IOR_BIN} \
                -a BENCHFS \
                --benchfs.registry ${REGISTRY_DIR} \
                --benchfs.datadir ${DATA_DIR} \
                -w -r \
                -t 2m -b 64m -s 64 \
                -o /4gib_ssf_testfile \
                -O summaryFormat=JSON \
                -v \
                > ${RESULTS_DIR}/ior_output.txt 2>&1

        IOR_EXIT_CODE=$?
        ;;

    "8gib-ssf")
        echo "Test: 8GiB Shared Single File (SSF) I/O test with C client"
        echo "Configuration:"
        echo "  - Transfer size: 2MB"
        echo "  - Block size: 64MB"
        echo "  - Segments per rank: 128"
        echo "  - Total per rank: 8GiB"
        echo "  - Total aggregate: $((8 * NNODES))GiB"
        echo "  - Mode: SSF (Shared Single File)"
        echo ""

        if [ "$NNODES" -ne 4 ]; then
            echo "WARNING: This test is optimized for 4 nodes (got $NNODES)"
        fi

        echo "Starting 8GiB SSF benchmark (this may take several minutes)..."

        mpirun \
            -np ${NNODES} \
            --mca btl tcp,self \
            --mca btl_tcp_if_include eth0 \
            --allow-run-as-root \
            -x LD_LIBRARY_PATH=/usr/local/lib \
            -x UCX_TLS=tcp,self \
            ${IOR_BIN} \
                -a BENCHFS \
                --benchfs.registry ${REGISTRY_DIR} \
                --benchfs.datadir ${DATA_DIR} \
                -w -r \
                -t 2m -b 64m -s 128 \
                -o /8gib_ssf_testfile \
                -O summaryFormat=JSON \
                -v \
                > ${RESULTS_DIR}/ior_output.txt 2>&1

        IOR_EXIT_CODE=$?
        ;;

    "quick")
        echo "Test: Quick sanity check with C client"
        echo "Configuration:"
        echo "  - Transfer size: 512KB"
        echo "  - Block size: 2MB"
        echo "  - Segments per rank: 2"
        echo "  - Total per rank: 4MB"
        echo ""

        mpirun \
            -np ${NNODES} \
            --mca btl tcp,self \
            --mca btl_tcp_if_include eth0 \
            --allow-run-as-root \
            -x LD_LIBRARY_PATH=/usr/local/lib \
            ${IOR_BIN} \
                -a BENCHFS \
                --benchfs.registry ${REGISTRY_DIR} \
                --benchfs.datadir ${DATA_DIR} \
                -w -r \
                -t 512k -b 2m -s 2 \
                -o /quicktest \
                -O summaryFormat=JSON \
                > ${RESULTS_DIR}/ior_output.txt 2>&1

        IOR_EXIT_CODE=$?
        ;;

    *)
        echo "ERROR: Unknown test: $TEST_NAME"
        echo "Available tests: basic, write, read, quick, 4gib-ssf, 8gib-ssf"
        exit 1
        ;;
esac

# Display results
echo ""
echo "=========================================="
echo "IOR Results:"
echo "=========================================="
cat ${RESULTS_DIR}/ior_output.txt

# Extract key metrics if available
echo ""
echo "Key Performance Metrics:"
grep -E "write|read|Max Write|Max Read|bandwidth" ${RESULTS_DIR}/ior_output.txt || true

# Check exit code
echo ""
echo "=========================================="
if [ $IOR_EXIT_CODE -eq 0 ]; then
    echo "✓ Test Result: PASS"
    echo "=========================================="
    echo ""
    echo "IOR benchmark with C client completed successfully"
    echo "Results saved to: ${RESULTS_DIR}/"
    exit 0
else
    echo "✗ Test Result: FAIL (exit code: $IOR_EXIT_CODE)"
    echo "=========================================="
    echo ""
    echo "IOR benchmark failed. Check logs in: ${RESULTS_DIR}/"
    exit 1
fi
