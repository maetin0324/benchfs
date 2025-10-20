#!/bin/bash
# Force cleanup all BenchFS Docker resources
set -x  # Enable debug output

echo "========================================"
echo "Force Cleanup - BenchFS Docker"
echo "========================================"

# Stop and remove all containers with benchfs in the name
echo "1. Stopping all BenchFS containers..."
docker ps -a --format '{{.Names}}' | grep -i benchfs | xargs -r docker rm -f
docker ps -a --format '{{.Names}}' | grep -i controller | xargs -r docker rm -f
docker ps -a --format '{{.Names}}' | grep -i server | xargs -r docker rm -f

# Remove with docker-compose
echo "2. Docker Compose cleanup..."
cd "$(dirname "$0")"
docker-compose -f docker-compose.yml down -v --remove-orphans 2>/dev/null || true
docker-compose -f docker-compose.small.yml down -v --remove-orphans 2>/dev/null || true

# Remove all volumes
echo "3. Removing volumes..."
docker volume ls --format '{{.Name}}' | grep -E '(docker|benchfs)' | xargs -r docker volume rm 2>/dev/null || true

# Remove images
echo "4. Removing images..."
docker images --format '{{.Repository}}:{{.Tag}}' | grep -E '(docker|benchfs)' | xargs -r docker rmi -f 2>/dev/null || true

# Show remaining resources
echo "5. Checking remaining resources..."
echo "Containers:"
docker ps -a | grep -E '(benchfs|controller|server)' || echo "  None"
echo ""
echo "Volumes:"
docker volume ls | grep -E '(docker|benchfs)' || echo "  None"
echo ""
echo "Images:"
docker images | grep -E '(docker|benchfs)' || echo "  None"

echo ""
echo "========================================"
echo "Cleanup Complete!"
echo "========================================"
