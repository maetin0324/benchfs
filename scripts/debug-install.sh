#!/bin/bash
# BenchFS Debug Installation Script
#
# This script builds and installs BenchFS with debug symbols for perf profiling.
# Based on install.sh but with CARGO_PROFILE_RELEASE_DEBUG=2 to enable debug symbols.
#
# Usage:
#   ./scripts/debug-install.sh           # Build with debug symbols (release optimized)
#   DEBUG_BUILD=1 ./scripts/debug-install.sh  # Pure debug build (slower but more info)

set -e

# Default installation prefix
PREFIX="${PREFIX:-$HOME/.local}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "=========================================="
echo "BenchFS Debug Installation Script"
echo "=========================================="
echo ""

# Check if we're in the right directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE:-$0}" )" &> /dev/null && pwd )"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${PROJECT_ROOT}"

if [ ! -f "Cargo.toml" ]; then
    echo -e "${RED}Error: Cargo.toml not found. Please run this script from the BenchFS root directory.${NC}"
    exit 1
fi

# Check if pure debug build is requested
if [ "${DEBUG_BUILD:-0}" = "1" ]; then
    echo "Build mode: DEBUG (unoptimized, full debug info)"
    BUILD_DIR="${PROJECT_ROOT}/target/debug"
    BUILD_FLAGS=""
else
    echo "Build mode: RELEASE with debug symbols"
    BUILD_DIR="${PROJECT_ROOT}/target/release"
    BUILD_FLAGS="--release"
    # Enable debug symbols in release builds
    export CARGO_PROFILE_RELEASE_DEBUG=2
fi

# Display installation paths
echo ""
echo "Installation prefix: $PREFIX"
echo "  Libraries: $PREFIX/lib"
echo "  Headers: $PREFIX/include"
echo "  pkg-config: $PREFIX/lib/pkgconfig"
echo "  Build directory: $BUILD_DIR"
echo ""

# Build BenchFS library
echo -e "${YELLOW}Building BenchFS library with debug symbols...${NC}"
cargo build ${BUILD_FLAGS} --features mpi-support
cargo build ${BUILD_FLAGS} --lib

if [ $? -ne 0 ]; then
    echo -e "${RED}Error: Failed to build BenchFS library${NC}"
    exit 1
fi

# Create installation directories
echo -e "${YELLOW}Creating installation directories...${NC}"
mkdir -p "$PREFIX/lib"
mkdir -p "$PREFIX/include"
mkdir -p "$PREFIX/lib/pkgconfig"

# Install library
echo -e "${YELLOW}Installing library files...${NC}"
cp -v "${BUILD_DIR}/libbenchfs.so" "$PREFIX/lib/" || {
    echo -e "${RED}Error: Failed to copy library file${NC}"
    exit 1
}

# Install headers
echo -e "${YELLOW}Installing header files...${NC}"
if [ -d "ior_integration/benchfs_backend/include" ]; then
    cp -rv ior_integration/benchfs_backend/include/* "$PREFIX/include/" || {
        echo -e "${RED}Error: Failed to copy header files${NC}"
        exit 1
    }
else
    echo -e "${YELLOW}Warning: Header directory not found. Skipping header installation.${NC}"
fi

# Generate and install pkg-config file
echo -e "${YELLOW}Generating pkg-config file...${NC}"
PKG_CONFIG_FILE="$PREFIX/lib/pkgconfig/benchfs.pc"

cat > "$PKG_CONFIG_FILE" << PKGEOF
# BenchFS pkg-config file (debug build with symbols)

prefix=$PREFIX
exec_prefix=\${prefix}
libdir=\${prefix}/lib
includedir=\${prefix}/include

Name: BenchFS
Description: BenchFS distributed filesystem library (debug symbols enabled)
Version: 0.1.0
Libs: -L\${libdir} -lbenchfs -lpthread -ldl -lm
Cflags: -I\${includedir}
PKGEOF

echo -e "${GREEN}pkg-config file created: $PKG_CONFIG_FILE${NC}"

# Check debug symbols
echo ""
echo -e "${YELLOW}Checking debug symbols...${NC}"
if file "${BUILD_DIR}/libbenchfs.so" | grep -q "not stripped"; then
    echo -e "${GREEN}  OK: Library contains debug symbols${NC}"
else
    echo -e "${YELLOW}  WARNING: Library may be stripped${NC}"
fi

SYMBOL_COUNT=$(nm "${BUILD_DIR}/libbenchfs.so" 2>/dev/null | wc -l || echo "0")
echo "  Symbol count: ${SYMBOL_COUNT}"

# Also check benchfsd_mpi
if [ -f "${BUILD_DIR}/benchfsd_mpi" ]; then
    if file "${BUILD_DIR}/benchfsd_mpi" | grep -q "not stripped"; then
        echo -e "${GREEN}  OK: benchfsd_mpi contains debug symbols${NC}"
    else
        echo -e "${YELLOW}  WARNING: benchfsd_mpi may be stripped${NC}"
    fi
fi

# Display environment setup instructions
echo ""
echo -e "${GREEN}=========================================="
echo "Installation Complete!"
echo "==========================================${NC}"
echo ""
echo "Built binaries with debug symbols:"
ls -la "${BUILD_DIR}/benchfsd_mpi" 2>/dev/null || echo "  benchfsd_mpi: not found"
ls -la "${BUILD_DIR}/benchfsd" 2>/dev/null || echo "  benchfsd: not found"
ls -la "${BUILD_DIR}/libbenchfs.so" 2>/dev/null || echo "  libbenchfs.so: not found"
echo ""
echo "To use BenchFS with perf profiling, add these lines to your job script:"
echo ""
echo -e "${YELLOW}export PKG_CONFIG_PATH=\"$PREFIX/lib/pkgconfig:\$PKG_CONFIG_PATH\"${NC}"
echo -e "${YELLOW}export LD_LIBRARY_PATH=\"$PREFIX/lib:\$LD_LIBRARY_PATH\"${NC}"
echo -e "${YELLOW}export BENCHFS_PREFIX=\"${BUILD_DIR}\"${NC}"
echo ""
echo "For perf profiling jobs:"
echo -e "${YELLOW}./jobs/benchfs/benchfs-perf.sh${NC}"
echo ""
