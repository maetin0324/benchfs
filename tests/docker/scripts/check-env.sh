#!/bin/bash
# Quick environment check script
set -euo pipefail

echo "=== BenchFS Environment Check ==="
echo

# Check binary
echo "1. Checking benchfsd_mpi binary:"
if [ -x /usr/local/bin/benchfsd_mpi ]; then
    echo "   ✓ Binary exists and is executable"
    ls -lh /usr/local/bin/benchfsd_mpi
else
    echo "   ✗ Binary not found or not executable"
    exit 1
fi

echo

# Check registry directory
echo "2. Checking registry directory:"
if [ -d /shared/registry ]; then
    echo "   ✓ Registry directory exists"
    ls -la /shared/registry/
else
    echo "   ✗ Registry directory not found"
    exit 1
fi

echo

# Check data directory
echo "3. Checking data directory:"
if [ -d /shared/data ]; then
    echo "   ✓ Data directory exists"
    ls -la /shared/data/ | head -5
else
    echo "   ✗ Data directory not found"
    exit 1
fi

echo

# Check config file
echo "4. Checking config file:"
if [ -f /configs/benchfs_test.toml ]; then
    echo "   ✓ Config file exists"
    head -10 /configs/benchfs_test.toml
else
    echo "   ✗ Config file not found"
    exit 1
fi

echo
echo "=== All checks passed ==="
