#!/bin/bash
# Complete test runner for BenchFS with C client
# This script:
#   1. Starts the BenchFS cluster (servers)
#   2. Runs IOR tests with C client
#   3. Collects results and cleans up

set -euo pipefail

TEST_NAME=${1:-"basic"}
NNODES=${2:-4}

echo "=========================================="
echo "BenchFS C Client Test Runner"
echo "=========================================="
echo "Test: $TEST_NAME"
echo "Nodes: $NNODES"
echo ""

# Configuration
COMPOSE_FILE="docker-compose.c-client.yml"
PROJECT_NAME="benchfs_c_test"

# Change to docker directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="${SCRIPT_DIR}/.."
cd "${DOCKER_DIR}"

echo "Working directory: $(pwd)"
echo ""

# Function to cleanup on exit
cleanup() {
    local exit_code=$?
    echo ""
    echo "Cleaning up..."
    docker-compose -f ${COMPOSE_FILE} -p ${PROJECT_NAME} down -v 2>/dev/null || true
    exit $exit_code
}

trap cleanup EXIT INT TERM

# Build images if needed
echo "Checking Docker images..."
if ! docker image inspect benchfs:latest >/dev/null 2>&1; then
    echo "Building benchfs:latest image..."
    make -C ../.. docker-build-optimized
fi

if ! docker image inspect benchfs-c-client:latest >/dev/null 2>&1; then
    echo "Building benchfs-c-client:latest image..."
    cd ../..
    docker build -f tests/docker/Dockerfile.c-client -t benchfs-c-client:latest .
    cd tests/docker
fi

echo "✓ Docker images ready"
echo ""

# Start containers
echo "Starting BenchFS cluster..."
docker-compose -f ${COMPOSE_FILE} -p ${PROJECT_NAME} up -d

# Wait for containers to be ready
echo "Waiting for containers to start..."
sleep 5

# Check container status
echo ""
echo "Container status:"
docker-compose -f ${COMPOSE_FILE} -p ${PROJECT_NAME} ps

# Verify all containers are running
RUNNING=$(docker-compose -f ${COMPOSE_FILE} -p ${PROJECT_NAME} ps | grep -c "Up" || true)
if [ "$RUNNING" -lt 6 ]; then
    echo "ERROR: Not all containers are running (expected 6, got $RUNNING)"
    docker-compose -f ${COMPOSE_FILE} -p ${PROJECT_NAME} logs
    exit 1
fi

echo "✓ All containers running"
echo ""

# Clean registry and prepare for test
echo "Preparing test environment..."
docker exec benchfs_controller bash -c "rm -rf /shared/registry/* && mkdir -p /shared/registry"

# Create hostfile for MPI
HOSTFILE_CONTENT=""
if [ "$NNODES" -eq 2 ]; then
    HOSTFILE_CONTENT="server1 slots=1
server2 slots=1"
elif [ "$NNODES" -eq 4 ]; then
    HOSTFILE_CONTENT="server1 slots=1
server2 slots=1
server3 slots=1
server4 slots=1"
else
    echo "ERROR: Unsupported NNODES: $NNODES (supported: 2, 4)"
    exit 1
fi

docker exec benchfs_controller bash -c "cat > /tmp/hostfile <<EOF
${HOSTFILE_CONTENT}
EOF"

# Start BenchFS servers
echo "Starting BenchFS servers (AM RPC only)..."
docker exec -d benchfs_controller bash -c "
    mpirun \
        --hostfile /tmp/hostfile \
        -np ${NNODES} \
        --mca btl tcp,self \
        --mca btl_tcp_if_include eth0 \
        -x UCX_TLS=tcp,self \
        -x RUST_LOG=info \
        -x RUST_BACKTRACE=1 \
        benchfsd_mpi /shared/registry /configs/benchfs_test.toml \
        > /shared/results/server_stdout.log 2> /shared/results/server_stderr.log
"

# Wait for servers to register (AM RPC registration)
echo "Waiting for server registration..."
for i in {1..60}; do
    REGISTERED=$(docker exec benchfs_controller bash -c "find /shared/registry -name 'node_*.am_hostname' 2>/dev/null | wc -l" || echo "0")

    if [ "$REGISTERED" -eq "$NNODES" ]; then
        echo "✓ All servers registered: $REGISTERED/$NNODES"
        break
    fi

    if [ $i -eq 60 ]; then
        echo "✗ ERROR: Server registration timeout ($REGISTERED/$NNODES)"
        echo "Server logs:"
        docker exec benchfs_controller cat /shared/results/server_stderr.log || true
        exit 1
    fi

    if [ $((i % 10)) -eq 0 ]; then
        echo "  Waiting: $REGISTERED/$NNODES nodes registered"
    fi
    sleep 1
done

# Give servers time to stabilize
sleep 3

# Run IOR test from client container
echo ""
echo "Running IOR test with C client..."
docker exec benchfs_ior_client /scripts/test-ior-c-client.sh ${TEST_NAME} ${NNODES}

TEST_EXIT_CODE=$?

# Collect results
echo ""
echo "Collecting results..."
RESULTS_DIR="./results/c-client-$(date +%Y%m%d-%H%M%S)"
mkdir -p ${RESULTS_DIR}

docker cp benchfs_ior_client:/shared/results/ior_output.txt ${RESULTS_DIR}/ 2>/dev/null || true
docker cp benchfs_controller:/shared/results/server_stdout.log ${RESULTS_DIR}/ 2>/dev/null || true
docker cp benchfs_controller:/shared/results/server_stderr.log ${RESULTS_DIR}/ 2>/dev/null || true

echo "Results saved to: ${RESULTS_DIR}"

# Show server logs if test failed
if [ $TEST_EXIT_CODE -ne 0 ]; then
    echo ""
    echo "=========================================="
    echo "Server stderr log (last 50 lines):"
    echo "=========================================="
    docker exec benchfs_controller tail -50 /shared/results/server_stderr.log || true
fi

# Summary
echo ""
echo "=========================================="
if [ $TEST_EXIT_CODE -eq 0 ]; then
    echo "✓ C Client Test PASSED"
else
    echo "✗ C Client Test FAILED"
fi
echo "=========================================="

exit $TEST_EXIT_CODE
