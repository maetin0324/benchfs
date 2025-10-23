#!/bin/bash
# Sync BenchFS backend files to IOR source directory

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCHFS_SRC="$SCRIPT_DIR/benchfs_backend/src/aiori-BENCHFS.c"
IOR_SRC="$SCRIPT_DIR/ior/src/aiori-BENCHFS.c"

echo "Syncing BenchFS backend files to IOR..."

# Copy BENCHFS AIORI implementation
if [ -f "$BENCHFS_SRC" ]; then
    cp -v "$BENCHFS_SRC" "$IOR_SRC"
    echo "✓ Copied aiori-BENCHFS.c"
else
    echo "✗ Error: $BENCHFS_SRC not found"
    exit 1
fi

echo ""
echo "BenchFS backend files synced successfully!"
echo "You can now build IOR with BenchFS support."
