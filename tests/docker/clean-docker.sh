#!/bin/bash
# Complete cleanup script for BenchFS Docker environment
set -euo pipefail

echo "=========================================="
echo "BenchFS Docker Complete Cleanup"
echo "=========================================="

# Stop all compose services
echo
echo "1. Stopping Docker Compose services..."
docker-compose -f docker-compose.yml down -v 2>/dev/null || true
docker-compose -f docker-compose.small.yml down -v 2>/dev/null || true

# Remove containers
echo
echo "2. Removing BenchFS containers..."
docker rm -f benchfs_controller benchfs_server1 benchfs_server2 benchfs_server3 benchfs_server4 2>/dev/null || true

# Remove images
echo
echo "3. Removing BenchFS images..."
docker rmi -f docker_controller docker_server1 docker_server2 docker_server3 docker_server4 2>/dev/null || true

# Remove volumes
echo
echo "4. Removing BenchFS volumes..."
docker volume rm docker_shared_registry 2>/dev/null || true
docker volume rm docker_server1_data 2>/dev/null || true
docker volume rm docker_server2_data 2>/dev/null || true
docker volume rm docker_server3_data 2>/dev/null || true
docker volume rm docker_server4_data 2>/dev/null || true
docker volume rm docker_controller_results 2>/dev/null || true

# List remaining volumes
echo
echo "5. Checking remaining volumes..."
docker volume ls | grep -E "(docker|benchfs)" || echo "No BenchFS volumes remaining"

# List remaining images
echo
echo "6. Checking remaining images..."
docker images | grep -E "(docker|benchfs)" || echo "No BenchFS images remaining"

echo
echo "=========================================="
echo "Cleanup Complete!"
echo "=========================================="
echo
echo "You can now rebuild with:"
echo "  make build-simple"
echo "  make up-small"
