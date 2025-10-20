#!/bin/bash
# Automatic IOR setup script for BenchFS
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IOR_INTEGRATION_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
IOR_DIR="$IOR_INTEGRATION_DIR/ior"

echo "=========================================="
echo "IOR Setup for BenchFS"
echo "=========================================="
echo

# Check if IOR directory already exists
if [ -d "$IOR_DIR" ]; then
    echo "IOR directory already exists: $IOR_DIR"
    read -p "Do you want to remove it and re-clone? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "Removing existing IOR directory..."
        rm -rf "$IOR_DIR"
    else
        echo "Keeping existing IOR directory."
        exit 0
    fi
fi

# Clone IOR
echo "Cloning IOR from GitHub..."
cd "$IOR_INTEGRATION_DIR"
git clone https://github.com/hpc/ior.git
cd ior

echo
echo "IOR cloned successfully."
echo

# Check for autotools
echo "Checking for autotools..."
if ! command -v autoconf &> /dev/null; then
    echo "WARNING: autoconf not found. Please install autotools."
    echo "  Ubuntu/Debian: sudo apt-get install autoconf automake libtool"
    echo "  CentOS/RHEL:   sudo yum install autoconf automake libtool"
    echo "  HPC Module:    module load autotools"
    exit 1
fi

# Check for MPI
echo "Checking for MPI..."
if ! command -v mpicc &> /dev/null; then
    echo "WARNING: mpicc not found. Please install or load MPI."
    echo "  Ubuntu/Debian: sudo apt-get install libopenmpi-dev"
    echo "  HPC Module:    module load openmpi"
    exit 1
fi

echo "  mpicc: $(which mpicc)"
echo "  MPI version: $(mpicc --version | head -1)"
echo

# Bootstrap
echo "Running bootstrap..."
./bootstrap

# Configure
echo "Running configure..."
./configure

# Build
echo "Building IOR..."
make -j$(nproc)

echo
echo "=========================================="
echo "IOR Setup Complete!"
echo "=========================================="
echo
echo "IOR binary location: $IOR_DIR/src/ior"
echo
echo "Test IOR:"
echo "  $IOR_DIR/src/ior --help"
echo
echo "Next steps:"
echo "  1. Build BenchFS MPI binary:"
echo "     cd $(cd $IOR_INTEGRATION_DIR/.. && pwd)"
echo "     cargo build --release --features mpi-support --bin benchfsd_mpi"
echo
echo "  2. Run job script:"
echo "     cd jobs/benchfs"
echo "     ./benchfs.sh"
echo
