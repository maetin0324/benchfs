#!/bin/bash
# Apply BenchFS modifications to IOR on supercomputer
# This script should be run after transferring the benchfs_export directory

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Detect if we're in the export directory or the scripts directory
if [ -f "$SCRIPT_DIR/benchfs_ior.patch" ]; then
    # Running from export directory
    EXPORT_DIR="$SCRIPT_DIR"
    IOR_INTEGRATION_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
else
    # Running from scripts directory
    EXPORT_DIR="$(cd "$SCRIPT_DIR/../benchfs_export" && pwd)"
    IOR_INTEGRATION_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
fi

IOR_DIR="$IOR_INTEGRATION_DIR/ior"

echo "=========================================="
echo "Apply BenchFS Modifications to IOR"
echo "=========================================="
echo
echo "Export directory: $EXPORT_DIR"
echo "IOR integration directory: $IOR_INTEGRATION_DIR"
echo "IOR directory: $IOR_DIR"
echo

# Check if export files exist
if [ ! -f "$EXPORT_DIR/benchfs_ior.patch" ] || [ ! -f "$EXPORT_DIR/benchfs_backend.tar.gz" ]; then
    echo "Error: Required files not found in $EXPORT_DIR"
    echo "Expected files:"
    echo "  - benchfs_ior.patch"
    echo "  - benchfs_backend.tar.gz"
    echo
    echo "Please ensure you have transferred the complete benchfs_export directory."
    exit 1
fi

# Check if IOR directory exists
if [ ! -d "$IOR_DIR" ]; then
    echo "Error: IOR directory not found: $IOR_DIR"
    echo
    echo "Please clone IOR first:"
    echo "  cd $IOR_INTEGRATION_DIR"
    echo "  git clone https://github.com/hpc/ior.git"
    exit 1
fi

# 1. Extract benchfs_backend
echo "[1/3] Extracting BenchFS backend files..."
cd "$IOR_INTEGRATION_DIR"

if [ -d "benchfs_backend" ]; then
    echo "  Warning: benchfs_backend directory already exists"
    read -p "  Do you want to overwrite it? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "  Skipping backend extraction"
    else
        rm -rf benchfs_backend
        tar xzf "$EXPORT_DIR/benchfs_backend.tar.gz"
        echo "  ✓ BenchFS backend extracted"
    fi
else
    tar xzf "$EXPORT_DIR/benchfs_backend.tar.gz"
    echo "  ✓ BenchFS backend extracted"
fi
echo

# 2. Copy aiori-BENCHFS.c to IOR source
echo "[2/3] Copying aiori-BENCHFS.c to IOR source tree..."
if [ -f "benchfs_backend/src/aiori-BENCHFS.c" ]; then
    cp benchfs_backend/src/aiori-BENCHFS.c "$IOR_DIR/src/"
    echo "  ✓ aiori-BENCHFS.c copied to $IOR_DIR/src/"
else
    echo "  ✗ Error: benchfs_backend/src/aiori-BENCHFS.c not found"
    exit 1
fi
echo

# 3. Apply git patch
echo "[3/3] Applying IOR modifications patch..."
cd "$IOR_DIR"

# Check if patch is already applied
if git diff --quiet configure.ac src/Makefile.am src/aiori.c src/aiori.h; then
    echo "  Applying patch..."

    if git apply --check "$EXPORT_DIR/benchfs_ior.patch" 2>/dev/null; then
        git apply "$EXPORT_DIR/benchfs_ior.patch"
        echo "  ✓ Patch applied successfully"
    else
        echo "  ⚠ Warning: Patch cannot be applied cleanly"
        echo
        echo "  This may be due to:"
        echo "    - Different IOR version"
        echo "    - Already applied modifications"
        echo "    - Conflicting local changes"
        echo
        echo "  Attempting to apply with 3-way merge..."

        if git apply --3way "$EXPORT_DIR/benchfs_ior.patch" 2>/dev/null; then
            echo "  ✓ Patch applied with 3-way merge"
            echo "  ⚠ Please review any conflicts"
        else
            echo "  ✗ Automatic patch application failed"
            echo
            echo "  You will need to apply changes manually:"
            echo "    1. Review the patch: cat $EXPORT_DIR/benchfs_ior.patch"
            echo "    2. Edit the following files:"
            echo "       - configure.ac"
            echo "       - src/Makefile.am"
            echo "       - src/aiori.c"
            echo "       - src/aiori.h"
            echo
            echo "  See ior_integration/README.md for manual modification instructions"
            exit 1
        fi
    fi
else
    echo "  ⚠ IOR files already modified (skipping patch)"
    echo "  Modified files:"
    git diff --name-only configure.ac src/Makefile.am src/aiori.c src/aiori.h 2>/dev/null | sed 's/^/    - /' || echo "    (none)"
fi
echo

echo "=========================================="
echo "Modifications Applied Successfully!"
echo "=========================================="
echo
echo "Next steps:"
echo
echo "1. Build IOR with BenchFS support:"
echo "   cd $IOR_DIR"
echo "   ./bootstrap"
echo "   ./configure"
echo "   make -j\$(nproc)"
echo
echo "2. Verify BenchFS backend:"
echo "   $IOR_DIR/src/ior -h | grep BENCHFS"
echo
echo "3. Build BenchFS MPI binary:"
echo "   cd $(cd $IOR_INTEGRATION_DIR/.. && pwd)"
echo "   cargo build --release --features mpi-support --bin benchfsd_mpi"
echo
echo "4. Run benchmark:"
echo "   cd jobs/benchfs"
echo "   ./benchfs.sh"
echo
