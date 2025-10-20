#!/bin/bash
# Debug script to check the latest job results

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RESULTS_DIR="$PROJECT_ROOT/results/benchfs"

# Find the latest job directory
LATEST_JOB=$(find "$RESULTS_DIR" -maxdepth 2 -type d -name "*-*-*" | sort -r | head -1)

if [ -z "$LATEST_JOB" ]; then
    echo "No job results found in $RESULTS_DIR"
    exit 1
fi

echo "=========================================="
echo "Latest Job Directory: $LATEST_JOB"
echo "=========================================="
echo

# Check if benchfsd_logs exists
if [ -d "$LATEST_JOB/benchfsd_logs/run_0" ]; then
    echo "BenchFS Server STDERR:"
    echo "=========================================="
    cat "$LATEST_JOB/benchfsd_logs/run_0/benchfsd_stderr.log" 2>/dev/null || echo "No stderr log found"
    echo ""

    echo "BenchFS Server STDOUT:"
    echo "=========================================="
    cat "$LATEST_JOB/benchfsd_logs/run_0/benchfsd_stdout.log" 2>/dev/null || echo "No stdout log found"
    echo ""
else
    echo "No benchfsd_logs directory found"
fi

# Check job parameters
if [ -f "$LATEST_JOB/job_params_0.json" ]; then
    echo "Job Parameters:"
    echo "=========================================="
    cat "$LATEST_JOB/job_params_0.json"
    echo ""
fi

# Check config file
CONFIG_FILE=$(find "$LATEST_JOB" -name "benchfs_0.toml" -type f | head -1)
if [ -n "$CONFIG_FILE" ]; then
    echo "BenchFS Config:"
    echo "=========================================="
    cat "$CONFIG_FILE"
    echo ""
fi

# Try to find backend directory from config file
if [ -n "$CONFIG_FILE" ]; then
    BACKEND_REGISTRY=$(grep "registry_dir" "$CONFIG_FILE" | cut -d'"' -f2)
    if [ -n "$BACKEND_REGISTRY" ] && [ -d "$BACKEND_REGISTRY" ]; then
        echo "Registry Directory: $BACKEND_REGISTRY"
        echo "Registry Contents:"
        echo "=========================================="
        ls -la "$BACKEND_REGISTRY" 2>/dev/null || echo "Registry not accessible"
        echo ""
    fi

    BACKEND_DATA=$(grep "data_dir" "$CONFIG_FILE" | cut -d'"' -f2)
    if [ -n "$BACKEND_DATA" ] && [ -d "$BACKEND_DATA" ]; then
        echo "Data Directory: $BACKEND_DATA"
        echo "Data Directory Contents:"
        echo "=========================================="
        ls -la "$BACKEND_DATA" 2>/dev/null || echo "Data directory not accessible"
        echo ""
    fi
fi

# Check environment variables
if [ -f "$LATEST_JOB/env.txt" ]; then
    echo "Key Environment Variables:"
    echo "=========================================="
    grep -E "(BENCHFS_PREFIX|IOR_PREFIX|BACKEND_DIR)" "$LATEST_JOB/env.txt" || echo "Variables not found"
    echo ""
fi

echo "=========================================="
echo "Debug Complete"
echo "=========================================="
