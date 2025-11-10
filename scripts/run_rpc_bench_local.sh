#!/bin/bash
#
# Local test script for benchfs_rpc_bench
#
# This script runs the RPC benchmark in a single-node environment
# for testing RPC communication without IOR or io_uring.
#

set -e

# Configuration
REGISTRY_DIR="${REGISTRY_DIR:-/tmp/benchfs_rpc_bench_registry}"
PING_ITERATIONS="${PING_ITERATIONS:-10000}"

# Number of MPI processes (1 client + N servers)
NPROCS="${NPROCS:-4}"

# Binary path
BENCHFS_BIN="${BENCHFS_BIN:-./target/debug/benchfs_rpc_bench}"

# JSON output path (optional)
JSON_OUTPUT="${JSON_OUTPUT:-}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}BenchFS RPC Benchmark - Local Test${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "Configuration:"
echo "  MPI Processes:    $NPROCS (1 client + $((NPROCS - 1)) servers)"
echo "  Registry Dir:     $REGISTRY_DIR"
echo "  Ping Iterations:  $PING_ITERATIONS"
echo "  Binary:           $BENCHFS_BIN"
if [ -n "$JSON_OUTPUT" ]; then
    echo "  JSON Output:      $JSON_OUTPUT"
fi
echo ""

# Check if binary exists
if [ ! -f "$BENCHFS_BIN" ]; then
    echo -e "${RED}ERROR: Binary not found: $BENCHFS_BIN${NC}"
    echo "Please build the binary first:"
    echo "  cargo build --bin benchfs_rpc_bench --features mpi-support"
    exit 1
fi

# Cleanup old registry
echo -e "${YELLOW}Cleaning up old registry...${NC}"
rm -rf "$REGISTRY_DIR"
mkdir -p "$REGISTRY_DIR"

# Run RPC benchmark
echo -e "${GREEN}Running RPC benchmark...${NC}"
echo ""

# Build command with optional JSON output
RPC_CMD="mpirun -np $NPROCS \"$BENCHFS_BIN\" \"$REGISTRY_DIR\" --ping-iterations $PING_ITERATIONS"
if [ -n "$JSON_OUTPUT" ]; then
    RPC_CMD="$RPC_CMD --output \"$JSON_OUTPUT\""
fi

eval $RPC_CMD

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}RPC Benchmark Complete!${NC}"
echo -e "${GREEN}========================================${NC}"

# Display JSON output path if used
if [ -n "$JSON_OUTPUT" ]; then
    if [ -f "$JSON_OUTPUT" ]; then
        echo -e "${GREEN}JSON results written to: $JSON_OUTPUT${NC}"
    else
        echo -e "${RED}WARNING: JSON output file not found: $JSON_OUTPUT${NC}"
    fi
fi
