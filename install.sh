#!/bin/bash
# BenchFS Installation Script for Supercomputer Environment
#
# This script installs BenchFS library and headers for use with IOR

set -e

# Default installation prefix
PREFIX="${PREFIX:-$HOME/.local}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "=========================================="
echo "BenchFS Installation Script"
echo "=========================================="
echo ""

# Check if we're in the right directory
if [ ! -f "Cargo.toml" ]; then
    echo -e "${RED}Error: Cargo.toml not found. Please run this script from the BenchFS root directory.${NC}"
    exit 1
fi

# Display installation paths
echo "Installation prefix: $PREFIX"
echo "  Libraries: $PREFIX/lib"
echo "  Headers: $PREFIX/include"
echo "  pkg-config: $PREFIX/lib/pkgconfig"
echo ""

# Ask for confirmation
# read -p "Continue with installation? [Y/n] " -n 1 -r
# echo
# if [[ ! $REPLY =~ ^[Yy]$ ]] && [[ ! -z $REPLY ]]; then
#     echo "Installation cancelled."
#     exit 0
# fi

# Build BenchFS library
echo -e "${YELLOW}Building BenchFS library...${NC}"
cargo build --release --features mpi-support
cargo build --release --lib --features daemon-mode

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
cp -v target/release/libbenchfs.so "$PREFIX/lib/" || {
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
# BenchFS pkg-config file

prefix=$PREFIX
exec_prefix=\${prefix}
libdir=\${prefix}/lib
includedir=\${prefix}/include

Name: BenchFS
Description: BenchFS distributed filesystem library
Version: 0.1.0
Libs: -L\${libdir} -lbenchfs -lpthread -ldl -lm
Cflags: -I\${includedir}
PKGEOF

echo -e "${GREEN}pkg-config file created: $PKG_CONFIG_FILE${NC}"

# Display environment setup instructions
echo ""
echo -e "${GREEN}=========================================="
echo "Installation Complete!"
echo "==========================================${NC}"
echo ""
echo "To use BenchFS with IOR, add these lines to your ~/.bashrc or job script:"
echo ""
echo -e "${YELLOW}export PKG_CONFIG_PATH=\"$PREFIX/lib/pkgconfig:\$PKG_CONFIG_PATH\"${NC}"
echo -e "${YELLOW}export LD_LIBRARY_PATH=\"$PREFIX/lib:\$LD_LIBRARY_PATH\"${NC}"
echo ""
echo "Then source your bashrc or re-login:"
echo -e "${YELLOW}source ~/.bashrc${NC}"
echo ""
echo "To verify installation:"
echo -e "${YELLOW}pkg-config --modversion benchfs${NC}"
echo -e "${YELLOW}pkg-config --libs benchfs${NC}"
echo ""
echo "To build IOR with BenchFS support:"
echo -e "${YELLOW}cd ior_integration/ior${NC}"
echo -e "${YELLOW}./bootstrap${NC}"
echo -e "${YELLOW}./configure --prefix=\$PREFIX --with-benchfs CFLAGS=\"\$(pkg-config --cflags benchfs)\" LDFLAGS=\"\$(pkg-config --libs benchfs)\"${NC}"
echo -e "${YELLOW}make && make install${NC}"
echo ""
