# BenchFS Docker Test Makefile

.PHONY: help build build-no-cache build-simple build-optimized build-base up up-small down test test-small test-ior test-ior-small test-ior-8gib-ssf clean clean-all logs shell exec-controller debug check-env

# Default configuration
COMPOSE_FILE ?= docker-compose.yml
COMPOSE_PROJECT_NAME ?= benchfs
NNODES ?= 4

# Export for docker-compose
export COMPOSE_PROJECT_NAME

help:
	@echo "BenchFS Docker Test Commands:"
	@echo ""
	@echo "Build Commands:"
	@echo "  make build          - Build Docker images (full build, 15-20 min)"
	@echo "  make build-base     - Build UCX/MPI base image (once, 10-15 min)"
	@echo "  make build-optimized - Build using cached base (fast, 2-3 min) ⚡"
	@echo "  make build-simple   - Build simple Docker images (using host binaries)"
	@echo ""
	@echo "Cluster Commands:"
	@echo "  make up             - Start 4-node cluster"
	@echo "  make up-small       - Start 2-node cluster"
	@echo "  make down           - Stop and remove containers"
	@echo ""
	@echo "Test Commands:"
	@echo "  make test           - Run tests on 4-node cluster"
	@echo "  make test-small     - Run tests on 2-node cluster"
	@echo "  make test-ior       - Run IOR benchmark on 4-node cluster"
	@echo "  make test-ior-small - Run IOR benchmark on 2-node cluster"
	@echo "  make test-ior-8gib-ssf - Run 8GiB SSF benchmark on 4-node cluster"
	@echo ""
	@echo "Utility Commands:"
	@echo "  make logs           - View container logs"
	@echo "  make shell          - Open shell in controller"
	@echo "  make exec-controller - Execute test script in controller"
	@echo "  make debug          - Run debug script to diagnose issues"
	@echo "  make check-env      - Check environment on all nodes"
	@echo "  make clean          - Remove all containers, volumes, and images"
	@echo "  make clean-all      - Deep clean including Docker cache"
	@echo ""
	@echo "Optimized Build Workflow (Recommended):"
	@echo "  1. make build-base         # First time only (10-15 min)"
	@echo "  2. make build-optimized    # After code changes (2-3 min)"
	@echo "  3. make up                 # Start cluster"
	@echo ""
	@echo "See UCX_MPI_BASE_IMAGE.md for details on optimized builds"
	@echo ""

build:
	docker-compose -p $(COMPOSE_PROJECT_NAME) -f docker-compose.yml build

build-no-cache:
	docker-compose -p $(COMPOSE_PROJECT_NAME) -f docker-compose.yml build --no-cache

build-simple:
	docker-compose -p $(COMPOSE_PROJECT_NAME) -f docker-compose.yml build

# Optimized build using cached UCX/MPI base image
build-base:
	@echo "Building UCX/MPI base image (this takes 10-15 minutes but only needed once)..."
	@echo "See UCX_MPI_BASE_IMAGE.md for more information"
	docker build -f Dockerfile.ucx-mpi-base -t benchfs/ucx-mpi-base:1.0 ../../..
	@echo ""
	@echo "✓ Base image built successfully!"
	@echo "Now you can build BenchFS quickly with: make build-optimized"

build-optimized:
	@echo "Checking if base image exists..."
	@if ! docker image inspect benchfs/ucx-mpi-base:1.0 > /dev/null 2>&1; then \
		echo "ERROR: Base image not found: benchfs/ucx-mpi-base:1.0"; \
		echo "Please build it first: make build-base"; \
		exit 1; \
	fi
	@echo "Building BenchFS using cached UCX/MPI base (fast build)..."
	docker build -f Dockerfile.optimized -t benchfs:latest ../../..
	@echo ""
	@echo "✓ BenchFS image built successfully in 2-3 minutes!"

up:
	@echo "Building BenchFS MPI binary first..."
	cd ../.. && cargo build --release --features mpi-support --bin benchfsd_mpi
	@echo "Starting 4-node cluster..."
	docker-compose -p $(COMPOSE_PROJECT_NAME) -f docker-compose.yml up -d --remove-orphans
	@echo "Waiting for services to be ready..."
	sleep 5
	docker-compose -p $(COMPOSE_PROJECT_NAME) -f docker-compose.yml ps

up-small:
	@echo "Building BenchFS MPI binary first..."
	cd ../.. && cargo build --release --features mpi-support --bin benchfsd_mpi
	@echo "Starting 2-node cluster..."
	docker-compose -p $(COMPOSE_PROJECT_NAME) -f docker-compose.small.yml up -d --remove-orphans
	@echo "Waiting for services to be ready..."
	sleep 5
	docker-compose -p $(COMPOSE_PROJECT_NAME) -f docker-compose.small.yml ps

down:
	docker-compose -p $(COMPOSE_PROJECT_NAME) -f docker-compose.yml down --remove-orphans
	docker-compose -p $(COMPOSE_PROJECT_NAME) -f docker-compose.small.yml down --remove-orphans

test: up
	@echo "Running test on 4-node cluster..."
	@sleep 2
	@CONTAINER_NAME=$$(docker ps --format '{{.Names}}' | grep controller | head -1); \
	if [ -z "$$CONTAINER_NAME" ]; then \
		echo "ERROR: Controller container not found"; \
		docker ps -a; \
		exit 1; \
	fi; \
	echo "Using container: $$CONTAINER_NAME"; \
	docker exec $$CONTAINER_NAME /scripts/test-cluster.sh basic 4

test-small: up-small
	@echo "Running test on 2-node cluster..."
	@sleep 2
	@CONTAINER_NAME=$$(docker ps --format '{{.Names}}' | grep controller | head -1); \
	if [ -z "$$CONTAINER_NAME" ]; then \
		echo "ERROR: Controller container not found"; \
		docker ps -a; \
		exit 1; \
	fi; \
	echo "Using container: $$CONTAINER_NAME"; \
	docker exec $$CONTAINER_NAME /scripts/test-cluster.sh basic 2

test-ior: up
	@echo "Running IOR benchmark on 4-node cluster..."
	@sleep 2
	@CONTAINER_NAME=$$(docker ps --format '{{.Names}}' | grep controller | head -1); \
	if [ -z "$$CONTAINER_NAME" ]; then \
		echo "ERROR: Controller container not found"; \
		docker ps -a; \
		exit 1; \
	fi; \
	echo "Using container: $$CONTAINER_NAME"; \
	docker exec $$CONTAINER_NAME /scripts/test-ior.sh basic 4

test-ior-small: up-small
	@echo "Running IOR benchmark on 2-node cluster..."
	@sleep 2
	@CONTAINER_NAME=$$(docker ps --format '{{.Names}}' | grep controller | head -1); \
	if [ -z "$$CONTAINER_NAME" ]; then \
		echo "ERROR: Controller container not found"; \
		docker ps -a; \
		exit 1; \
	fi; \
	echo "Using container: $$CONTAINER_NAME"; \
	docker exec $$CONTAINER_NAME /scripts/test-ior.sh basic 2

test-ior-8gib-ssf: up
	@echo "Running 8GiB SSF IOR benchmark on 4-node cluster..."
	@echo "This test will write/read 32GiB total (8GiB per node) and may take several minutes..."
	@sleep 2
	@CONTAINER_NAME=$$(docker ps --format '{{.Names}}' | grep controller | head -1); \
	if [ -z "$$CONTAINER_NAME" ]; then \
		echo "ERROR: Controller container not found"; \
		docker ps -a; \
		exit 1; \
	fi; \
	echo "Using container: $$CONTAINER_NAME"; \
	docker exec $$CONTAINER_NAME /scripts/test-ior.sh 8gib-ssf 4

logs:
	docker-compose -p $(COMPOSE_PROJECT_NAME) -f docker-compose.yml logs -f

logs-controller:
	@CONTAINER_NAME=$$(docker ps --format '{{.Names}}' | grep controller | head -1); \
	docker logs -f $$CONTAINER_NAME

shell:
	@CONTAINER_NAME=$$(docker ps --format '{{.Names}}' | grep controller | head -1); \
	docker exec -it $$CONTAINER_NAME /bin/bash

exec-controller:
	@CONTAINER_NAME=$$(docker ps --format '{{.Names}}' | grep controller | head -1); \
	docker exec $$CONTAINER_NAME /scripts/run-mpi-test.sh $(NNODES)

debug:
	@echo "Running debug script..."
	@CONTAINER_NAME=$$(docker ps --format '{{.Names}}' | grep controller | head -1); \
	docker exec $$CONTAINER_NAME /scripts/debug-cluster.sh

check-env:
	@echo "Checking environment on all nodes..."
	@echo ""
	@echo "=== Controller ==="
	@CONTAINER_NAME=$$(docker ps --format '{{.Names}}' | grep controller | head -1); \
	docker exec $$CONTAINER_NAME /scripts/check-env.sh || true
	@echo ""
	@echo "=== Server1 ==="
	@CONTAINER_NAME=$$(docker ps --format '{{.Names}}' | grep server1 | head -1); \
	docker exec $$CONTAINER_NAME /scripts/check-env.sh || true
	@echo ""
	@echo "=== Server2 ==="
	@CONTAINER_NAME=$$(docker ps --format '{{.Names}}' | grep server2 | head -1); \
	docker exec $$CONTAINER_NAME /scripts/check-env.sh || true

clean: down
	@echo "Removing containers..."
	docker rm -f benchfs_controller benchfs_server1 benchfs_server2 benchfs_server3 benchfs_server4 2>/dev/null || true
	@echo "Removing volumes..."
	docker volume rm docker_shared_registry 2>/dev/null || true
	docker volume rm docker_server1_data 2>/dev/null || true
	docker volume rm docker_server2_data 2>/dev/null || true
	docker volume rm docker_server3_data 2>/dev/null || true
	docker volume rm docker_server4_data 2>/dev/null || true
	docker volume rm docker_controller_results 2>/dev/null || true
	@echo "Removing images..."
	docker rmi -f docker_controller docker_server1 docker_server2 docker_server3 docker_server4 2>/dev/null || true
	@echo "Cleaned up Docker resources"

clean-all: clean
	@echo "Performing deep clean..."
	docker system prune -af --volumes
	@echo "Deep clean completed"

# Quick test workflow
quick-test: build-simple up-small test-small down
	@echo "Quick test completed"

# Full test workflow
full-test: build-simple up test down
	@echo "Full test completed"
