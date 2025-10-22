#!/bin/bash
# Export BenchFS modifications for IOR
# This script packages all BenchFS-specific files for transfer to supercomputer

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IOR_INTEGRATION_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
IOR_DIR="$IOR_INTEGRATION_DIR/ior"
EXPORT_DIR="$IOR_INTEGRATION_DIR/benchfs_export"

echo "=========================================="
echo "BenchFS IOR Modifications Export"
echo "=========================================="
echo

# Create export directory
mkdir -p "$EXPORT_DIR"
cd "$IOR_DIR"

# 1. Create git patch for IOR modifications
echo "[1/3] Creating git patch for IOR modifications..."
git diff configure.ac src/Makefile.am src/aiori.c src/aiori.h > "$EXPORT_DIR/benchfs_ior.patch"

if [ -s "$EXPORT_DIR/benchfs_ior.patch" ]; then
    echo "  ✓ Patch created: benchfs_ior.patch"
    echo "    Modified files:"
    git diff --name-only configure.ac src/Makefile.am src/aiori.c src/aiori.h | sed 's/^/      - /'
else
    echo "  ⚠ Warning: No modifications found in IOR repository"
    echo "    This might indicate that the patch has already been applied"
fi
echo

# 2. Package BenchFS backend files
echo "[2/3] Packaging BenchFS backend files..."
cd "$IOR_INTEGRATION_DIR"

if [ -d "benchfs_backend" ]; then
    tar czf "$EXPORT_DIR/benchfs_backend.tar.gz" benchfs_backend/
    echo "  ✓ Archive created: benchfs_backend.tar.gz"
    echo "    Contents:"
    tar tzf "$EXPORT_DIR/benchfs_backend.tar.gz" | sed 's/^/      - /'
else
    echo "  ✗ Error: benchfs_backend directory not found"
    exit 1
fi
echo

# 3. Create README
echo "[3/3] Creating transfer instructions..."
cat > "$EXPORT_DIR/README.txt" <<'EOF'
BenchFS IOR Modifications
=========================

This archive contains all BenchFS-specific modifications for IOR.

Contents:
---------
1. benchfs_ior.patch       - Git patch for IOR source modifications
2. benchfs_backend.tar.gz  - BenchFS backend implementation files
3. apply_modifications.sh  - Automated application script

Installation on Supercomputer:
-------------------------------

1. Transfer this directory to the supercomputer:

   scp -r benchfs_export/ <supercomputer>:/path/to/benchfs/ior_integration/

2. On the supercomputer, navigate to the IOR integration directory:

   cd /path/to/benchfs/ior_integration

3. Clone IOR repository (if not already done):

   git clone https://github.com/hpc/ior.git
   cd ior

4. Apply BenchFS modifications:

   cd ../benchfs_export
   ./apply_modifications.sh

5. Build IOR with BenchFS support:

   cd ../ior
   ./bootstrap
   ./configure
   make -j$(nproc)

6. Verify BenchFS backend is available:

   ./src/ior -h | grep BENCHFS

Manual Installation:
--------------------

If the automated script fails, you can apply modifications manually:

1. Extract benchfs_backend:

   cd /path/to/benchfs/ior_integration
   tar xzf benchfs_export/benchfs_backend.tar.gz

2. Copy BenchFS backend to IOR:

   cp benchfs_backend/src/aiori-BENCHFS.c ior/src/

3. Apply git patch:

   cd ior
   git apply ../benchfs_export/benchfs_ior.patch

4. Build IOR:

   ./bootstrap
   ./configure
   make -j$(nproc)

Troubleshooting:
----------------

If patch application fails:
  - The IOR repository might be at a different version
  - Apply changes manually by editing the files listed in the patch

If configure fails:
  - Ensure MPI is loaded: module load openmpi
  - Check that autotools are available: module load autotools

If build fails:
  - Check that benchfs_c_api.h is in the include path
  - Ensure BenchFS Rust library is built

For more information, see:
  - ior_integration/README.md
  - SETUP_IOR.md

EOF

echo "  ✓ README created: README.txt"
echo

# 4. Copy the apply script
cp "$SCRIPT_DIR/apply_benchfs_modifications.sh" "$EXPORT_DIR/" 2>/dev/null || true

echo "=========================================="
echo "Export Complete!"
echo "=========================================="
echo
echo "Export location: $EXPORT_DIR"
echo
echo "Files created:"
ls -lh "$EXPORT_DIR" | tail -n +2 | awk '{printf "  - %-30s (%s)\n", $9, $5}'
echo
echo "Next steps:"
echo "  1. Transfer to supercomputer:"
echo "     scp -r $EXPORT_DIR <host>:/work/NBB/rmaeda/workspace/rust/benchfs/ior_integration/"
echo
echo "  2. On supercomputer, run:"
echo "     cd /work/NBB/rmaeda/workspace/rust/benchfs/ior_integration/benchfs_export"
echo "     ./apply_modifications.sh"
echo
