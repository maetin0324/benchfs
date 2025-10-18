#!/bin/bash
#
# Simple IOR test with BENCHFS backend
#
# This script demonstrates how to run IOR with BENCHFS backend
# in a single-node environment for testing purposes.
#

set -e

# Configuration
REGISTRY_DIR="${REGISTRY_DIR:-/tmp/benchfs_registry}"
DATA_DIR="${DATA_DIR:-/tmp/benchfs_data}"
TEST_FILE="${TEST_FILE:-/testfile}"

# Number of MPI processes
NPROCS="${NPROCS:-4}"

# IOR parameters
TRANSFER_SIZE="${TRANSFER_SIZE:-1m}"
BLOCK_SIZE="${BLOCK_SIZE:-16m}"
ITERATIONS="${ITERATIONS:-3}"

# Colors
RED='\033[0:31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}BenchFS IOR Simple Test${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "Configuration:"
echo "  MPI Processes:  $NPROCS"
echo "  Registry Dir:   $REGISTRY_DIR"
echo "  Data Dir:       $DATA_DIR"
echo "  Test File:      $TEST_FILE"
echo "  Transfer Size:  $TRANSFER_SIZE"
echo "  Block Size:     $BLOCK_SIZE"
echo "  Iterations:     $ITERATIONS"
echo ""

# Cleanup old registry
echo -e "${YELLOW}Cleaning up old registry...${NC}"
rm -rf "$REGISTRY_DIR"
mkdir -p "$REGISTRY_DIR"
mkdir -p "$DATA_DIR"

# Check if IOR is built with BENCHFS backend
if ! ./ior/src/ior -h | grep -q "BENCHFS"; then
    echo -e "${RED}ERROR: IOR not built with BENCHFS backend${NC}"
    echo "Please build IOR with BENCHFS support first."
    echo ""
    echo "Steps:"
    echo "  1. Copy aiori-BENCHFS.c to ior/src/"
    echo "  2. Edit ior/src/aiori.c to add extern declaration"
    echo "  3. Edit ior/configure.ac to add BENCHFS"
    echo "  4. Rebuild IOR"
    exit 1
fi

# Run IOR with BENCHFS backend
echo -e "${GREEN}Running IOR with BENCHFS backend...${NC}"
echo ""

mpirun -np $NPROCS \
    ./ior/src/ior \
    -a BENCHFS \
    -t $TRANSFER_SIZE \
    -b $BLOCK_SIZE \
    -i $ITERATIONS \
    -v \
    -w -r \
    -o $TEST_FILE \
    -O benchfs.registry="$REGISTRY_DIR" \
    -O benchfs.datadir="$DATA_DIR"

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}IOR Test Complete!${NC}"
echo -e "${GREEN}========================================${NC}"
