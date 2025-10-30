#!/bin/bash
# Test script to verify MPI configuration is working correctly
# Updated for TCP-only configuration

set -euo pipefail

echo "==================================="
echo "MPI Configuration Test Script"
echo "Updated: TCP-only mode for stability"
echo "==================================="
echo ""

# Test 1: Check MPI version
echo "1. Checking MPI version..."
mpirun --version || { echo "ERROR: mpirun not found"; exit 1; }
echo ""

# Test 2: Network interface information
echo "2. Checking network interfaces..."
echo "Available interfaces:"
ip -o -4 addr show | awk '{print "  " $2 " : " $4}' || echo "  Unable to list interfaces"
echo ""

# Test 3: InfiniBand status (if available)
echo "3. Checking InfiniBand status..."
which ibstat >/dev/null 2>&1 && ibstat 2>/dev/null | head -5 || echo "  InfiniBand not available or ibstat not found"
echo ""

# Test 4: Test TCP-only configuration (CURRENT PRODUCTION CONFIG)
echo "4. Testing TCP-only configuration (PRODUCTION)..."
mpirun \
  --mca pml ob1 \
  --mca btl tcp,vader,self \
  --mca btl_openib_allow_ib 0 \
  -np 2 \
  hostname 2>&1 | grep -v "^Warning" || true
echo ""

# Test 5: Test UCX with auto-detection (EXPERIMENTAL)
echo "5. Testing UCX auto-detection (EXPERIMENTAL)..."
echo "   Note: This may fail if UCX/IB is not available"
mpirun \
  --mca pml ucx \
  --mca btl self \
  --mca osc ucx \
  -x "UCX_TLS=all" \
  -x "UCX_NET_DEVICES=all" \
  -x "UCX_LOG_LEVEL=error" \
  -np 2 \
  hostname 2>&1 | grep -v "^Warning" || echo "   UCX test failed (expected if IB not available)"
echo ""

echo "==================================="
echo "MPI Configuration Test Complete"
echo "==================================="
echo ""
echo "IMPORTANT: Test #4 (TCP-only) should work reliably."
echo "Test #5 (UCX) may fail if InfiniBand is not available."
echo ""
echo "If Test #4 passes, you can submit your job to the supercomputer."