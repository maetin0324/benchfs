#!/bin/bash
#
# BenchFS Performance Benchmark Script
#
# This script runs the benchfs_perf tool in server or client mode.
# For 2-node benchmarks, run this script on each node with the appropriate mode.
#
# Usage:
#   Server (Node 1): ./run_perf_benchmark.sh server
#   Client (Node 2): ./run_perf_benchmark.sh client [server_node_id]
#
# Environment variables:
#   REGISTRY_DIR - Directory for WorkerAddress exchange (must be shared, e.g., NFS)
#   DATA_DIR     - Directory for chunk data storage (local)
#   OUTPUT_DIR   - Directory for trace output (local)
#   LOG_LEVEL    - Log level (default: info)
#   ITERATIONS   - Number of benchmark iterations (default: 100)
#   BLOCK_SIZE   - Block size in bytes (default: 4096)
#   TIMEOUT      - Connection timeout in seconds (default: 30)
#

set -e

# Default configuration
REGISTRY_DIR="${REGISTRY_DIR:-/tmp/benchfs_perf/registry}"
DATA_DIR="${DATA_DIR:-/local/benchfs_perf/data}"
OUTPUT_DIR="${OUTPUT_DIR:-./traces}"
LOG_LEVEL="${LOG_LEVEL:-info}"
ITERATIONS="${ITERATIONS:-128}"
BLOCK_SIZE="${BLOCK_SIZE:-33554432}"
TIMEOUT="${TIMEOUT:-30}"

# Binary path
BENCHFS_PERF="${BENCHFS_PERF:-./target/release/benchfs_perf}"

# Get hostname for default node ID
HOSTNAME=$(hostname -s)

print_usage() {
    cat << EOF
Usage: $0 <mode> [options]

Modes:
    server              Run as server
    client [server_id]  Run as client (server_id defaults to "server")
    clean               Clean up registry and data directories
    help                Show this help message

Environment variables:
    REGISTRY_DIR   Directory for WorkerAddress exchange (default: /tmp/benchfs_perf/registry)
    DATA_DIR       Directory for chunk data storage (default: /tmp/benchfs_perf/data)
    OUTPUT_DIR     Directory for trace output (default: ./traces)
    LOG_LEVEL      Log level (default: info)
    ITERATIONS     Number of benchmark iterations (default: 128 for 4GB total)
    BLOCK_SIZE     Block size in bytes (default: 33554432 = 32MB)
    TIMEOUT        Connection timeout in seconds (default: 30)

Examples:
    # On Node 1 (server):
    REGISTRY_DIR=/shared/nfs/benchfs ./run_perf_benchmark.sh server

    # On Node 2 (client):
    REGISTRY_DIR=/shared/nfs/benchfs ./run_perf_benchmark.sh client server

    # Run locally (same node for testing):
    ./run_perf_benchmark.sh server &
    sleep 2
    ./run_perf_benchmark.sh client server
EOF
}

setup_directories() {
    echo "Setting up directories..."
    mkdir -p "$REGISTRY_DIR"
    mkdir -p "$DATA_DIR"
    mkdir -p "$OUTPUT_DIR"
    echo "  Registry: $REGISTRY_DIR"
    echo "  Data:     $DATA_DIR"
    echo "  Output:   $OUTPUT_DIR"
}

clean_directories() {
    echo "Cleaning up..."
    rm -rf "$REGISTRY_DIR"
    rm -rf "$DATA_DIR"
    echo "  Removed: $REGISTRY_DIR"
    echo "  Removed: $DATA_DIR"
}

run_server() {
    local node_id="${1:-server}"
    local output_file="$OUTPUT_DIR/benchfs_server_${HOSTNAME}_$(date +%Y%m%d_%H%M%S).json"

    setup_directories

    echo "=========================================="
    echo "BenchFS Performance Server"
    echo "=========================================="
    echo "Node ID:      $node_id"
    echo "Registry:     $REGISTRY_DIR"
    echo "Data:         $DATA_DIR"
    echo "Output:       $output_file"
    echo "Log level:    $LOG_LEVEL"
    echo "=========================================="
    echo ""

    exec "$BENCHFS_PERF" server \
        --registry-dir "$REGISTRY_DIR" \
        --data-dir "$DATA_DIR" \
        --output "$output_file" \
        --log-level "$LOG_LEVEL" \
        --node-id "$node_id"
}

run_client() {
    local server_node="${1:-server}"
    local node_id="${2:-client}"
    local output_file="$OUTPUT_DIR/benchfs_client_${HOSTNAME}_$(date +%Y%m%d_%H%M%S).json"

    setup_directories

    echo "=========================================="
    echo "BenchFS Performance Client"
    echo "=========================================="
    echo "Node ID:      $node_id"
    echo "Server:       $server_node"
    echo "Registry:     $REGISTRY_DIR"
    echo "Output:       $output_file"
    echo "Log level:    $LOG_LEVEL"
    echo "Iterations:   $ITERATIONS"
    echo "Block size:   $BLOCK_SIZE bytes"
    echo "Timeout:      $TIMEOUT seconds"
    echo "=========================================="
    echo ""

    exec "$BENCHFS_PERF" client \
        --registry-dir "$REGISTRY_DIR" \
        --server-node "$server_node" \
        --output "$output_file" \
        --log-level "$LOG_LEVEL" \
        --node-id "$node_id" \
        --iterations "$ITERATIONS" \
        --block-size "$BLOCK_SIZE" \
        --timeout "$TIMEOUT"
}

# Main
case "${1:-help}" in
    server)
        shift
        run_server "$@"
        ;;
    client)
        shift
        run_client "$@"
        ;;
    clean)
        clean_directories
        ;;
    help|--help|-h)
        print_usage
        ;;
    *)
        echo "Unknown mode: $1"
        print_usage
        exit 1
        ;;
esac
