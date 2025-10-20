#!/bin/bash
# Main test runner for BenchFS Docker tests
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

usage() {
    cat <<EOF
Usage: $0 [OPTIONS]

Options:
  -t, --test TYPE     Test type: small, full (default: small)
  -c, --clean         Clean up before running
  -b, --build         Force rebuild of images
  -h, --help          Show this help message

Examples:
  $0                  # Run small test (2 nodes)
  $0 --test full      # Run full test (4 nodes)
  $0 --clean --build  # Clean, rebuild, and test

EOF
    exit 0
}

# Parse arguments
TEST_TYPE="small"
CLEAN=false
BUILD=false

while [[ $# -gt 0 ]]; do
    case $1 in
        -t|--test)
            TEST_TYPE="$2"
            shift 2
            ;;
        -c|--clean)
            CLEAN=true
            shift
            ;;
        -b|--build)
            BUILD=true
            shift
            ;;
        -h|--help)
            usage
            ;;
        *)
            log_error "Unknown option: $1"
            usage
            ;;
    esac
done

# Validate test type
if [[ "$TEST_TYPE" != "small" && "$TEST_TYPE" != "full" ]]; then
    log_error "Invalid test type: $TEST_TYPE (must be 'small' or 'full')"
    exit 1
fi

log_info "BenchFS Docker Test Runner"
log_info "Test type: $TEST_TYPE"

# Check prerequisites
log_info "Checking prerequisites..."

if ! command -v docker &> /dev/null; then
    log_error "Docker is not installed"
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    log_error "docker-compose is not installed"
    exit 1
fi

if ! command -v cargo &> /dev/null; then
    log_error "Cargo (Rust) is not installed"
    exit 1
fi

# Clean up if requested
if [ "$CLEAN" = true ]; then
    log_info "Cleaning up existing containers and volumes..."
    make clean || true
fi

# Build BenchFS binary
log_info "Building BenchFS MPI binary..."
cd ../..
if ! cargo build --release --features mpi-support --bin benchfsd_mpi; then
    log_error "Failed to build BenchFS MPI binary"
    exit 1
fi
cd "$SCRIPT_DIR"

# Check if binary exists
if [ ! -f "../../target/release/benchfsd_mpi" ]; then
    log_error "BenchFS MPI binary not found at ../../target/release/benchfsd_mpi"
    exit 1
fi
log_info "BenchFS MPI binary ready"

# Build Docker images if requested
if [ "$BUILD" = true ]; then
    log_info "Building Docker images..."
    if ! make build-simple; then
        log_error "Failed to build Docker images"
        exit 1
    fi
fi

# Run tests based on type
log_info "Starting test: $TEST_TYPE"

case "$TEST_TYPE" in
    small)
        log_info "Running small test (2 nodes)..."
        if make test-small; then
            log_info "Small test PASSED ✓"
            exit 0
        else
            log_error "Small test FAILED ✗"
            log_warn "Check logs with: make logs"
            exit 1
        fi
        ;;
    full)
        log_info "Running full test (4 nodes)..."
        if make test; then
            log_info "Full test PASSED ✓"
            exit 0
        else
            log_error "Full test FAILED ✗"
            log_warn "Check logs with: make logs"
            exit 1
        fi
        ;;
esac
