# BenchFS Mini Docker Test Makefile
# Minimal test environment for debugging Stream RPC issues

SHELL := /bin/bash
PROJECT_NAME := benchfs-mini
COMPOSE := docker-compose -p $(PROJECT_NAME)

# Build context paths (relative to this Makefile's directory)
BUILD_CONTEXT := ../..
PLUVIO_PATH := $(BUILD_CONTEXT)/lib/pluvio
BENCHFS_PATH := $(BUILD_CONTEXT)

.PHONY: help build-mini up down logs test-ior clean clean-all status

help:
	@echo "BenchFS Mini Docker Test Targets:"
	@echo "  build-mini    - Build benchfs_mini binary and Docker image"
	@echo "  up            - Start mini cluster (2 servers + 2 clients + controller)"
	@echo "  down          - Stop mini cluster"
	@echo "  logs          - Show container logs"
	@echo "  test-ior      - Run IOR benchmark with BENCHFSMINI backend"
	@echo "  clean         - Stop containers and remove volumes"
	@echo "  clean-all     - Clean everything including Docker images"
	@echo "  status        - Show cluster status"

# Build benchfsd_mini binary
build-binary:
	@echo "=== Building benchfsd_mini binary ==="
	cd $(BUILD_CONTEXT) && cargo build --bin benchfsd_mini --features mpi-support

# Build Docker image
build-mini: build-binary
	@echo "=== Building BenchFS Mini Docker image ==="
	@echo "Build context: $(BUILD_CONTEXT)"
	@# Check if base image exists
	@if ! docker images | grep -q "benchfs/ucx-mpi-base.*1.0"; then \
		echo "ERROR: Base image benchfs/ucx-mpi-base:1.0 not found!"; \
		echo "Please build it first with:"; \
		echo "  cd ../docker && make build-base"; \
		exit 1; \
	fi
	docker build \
		--build-arg BUILDKIT_INLINE_CACHE=1 \
		-f Dockerfile.mini \
		-t benchfs-mini:latest \
		$(BUILD_CONTEXT)
	@echo "=== Build complete ==="

# Start cluster
up:
	@echo "=== Starting BenchFS Mini cluster ==="
	$(COMPOSE) up -d
	@echo "=== Waiting for services to be ready ==="
	@sleep 5
	@$(MAKE) status

# Stop cluster
down:
	@echo "=== Stopping BenchFS Mini cluster ==="
	$(COMPOSE) down

# Show logs
logs:
	$(COMPOSE) logs -f

# Show cluster status
status:
	@echo "=== BenchFS Mini Cluster Status ==="
	@docker ps --filter "name=$(PROJECT_NAME)" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# Run IOR test
test-ior:
	@echo "=== Running IOR test with BENCHFSMINI backend ==="
	@# Ensure cluster is running
	@if [ -z "$$(docker ps -q -f name=benchfs_mini_controller)" ]; then \
		echo "ERROR: Cluster is not running. Run 'make up' first."; \
		exit 1; \
	fi
	@# Run test script
	docker exec benchfs_mini_controller /scripts/run_ior_mini.sh

# Clean containers and volumes
clean:
	@echo "=== Cleaning BenchFS Mini environment ==="
	$(COMPOSE) down -v
	@echo "=== Removed all containers and volumes ==="

# Clean everything including images
clean-all: clean
	@echo "=== Removing Docker images ==="
	@docker rmi benchfs-mini:latest || true
	@echo "=== Clean complete ==="

# Debug target: Shell into controller
shell:
	docker exec -it benchfs_mini_controller /bin/bash

# Debug target: Show registry files
show-registry:
	@echo "=== Server Registry Files ==="
	@docker exec benchfs_mini_controller ls -la /shared/registry_mini/ || echo "Registry directory not found"
	@docker exec benchfs_mini_controller cat /shared/registry_mini/server_*.txt || echo "No server files found"
