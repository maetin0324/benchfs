#!/bin/bash
# Test script to verify MPI configuration is working correctly
# Run this locally before submitting the job to the supercomputer

set -euo pipefail

echo "==================================="
echo "MPI Configuration Test Script"
echo "==================================="
echo ""

# Test 1: Check MPI version
echo "1. Checking MPI version..."
mpirun --version || { echo "ERROR: mpirun not found"; exit 1; }
echo ""

# Test 2: Test the UCX configuration with simple hostname command
echo "2. Testing UCX configuration with hostname..."
mpirun \
  --mca pml ucx \
  --mca btl self,vader \
  --mca osc ucx \
  -x "UCX_TLS=rc_mlx5,sm,self" \
  -x "UCX_NET_DEVICES=mlx5_0:1" \
  -x "UCX_RC_TIMEOUT=10s" \
  -x "UCX_RC_RETRY_COUNT=7" \
  -x "UCX_LOG_LEVEL=error" \
  -x "UCX_WARN_UNUSED_ENV_VARS=n" \
  -np 2 \
  hostname 2>&1 | grep -v "^Warning" || true
echo ""

# Test 3: Test TCP fallback configuration
echo "3. Testing TCP fallback configuration..."
mpirun \
  --mca pml ob1 \
  --mca btl tcp,vader,self \
  --mca btl_tcp_if_include lo \
  -np 2 \
  hostname 2>&1 | grep -v "^Warning" || true
echo ""

echo "==================================="
echo "MPI Configuration Test Complete"
echo "==================================="
echo ""
echo "If both tests completed without errors, the MPI configuration is correct."
echo "You can now submit your job to the supercomputer."