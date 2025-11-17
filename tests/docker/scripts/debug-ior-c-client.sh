#!/bin/bash
# Debug script to run IOR under gdb to get backtrace

set -euo pipefail

NNODES=${1:-4}
REGISTRY_DIR="/shared/registry"
DATA_DIR="/shared/data"
IOR_BIN="/usr/local/bin/ior"

echo "=========================================="
echo "Debugging IOR with gdb"
echo "Nodes: $NNODES"
echo "=========================================="

# Wait for registry to be populated
echo "Waiting for BenchFS servers to register..."
for i in {1..60}; do
    REGISTERED=$(find ${REGISTRY_DIR} -name "node_*.am_hostname" 2>/dev/null | wc -l)
    if [ "$REGISTERED" -eq "$NNODES" ]; then
        echo "✓ All servers registered: $REGISTERED/$NNODES"
        break
    fi
    [ $i -eq 60 ] && {
        echo "✗ ERROR: Server registration timeout ($REGISTERED/$NNODES)"
        exit 1
    }
    [ $((i % 10)) -eq 0 ] && echo "  Waiting: $REGISTERED/$NNODES nodes registered"
    sleep 1
done

sleep 2

echo ""
echo "Running IOR under gdb (rank 0 only)..."
echo "=========================================="

# Create gdb command file
cat > /tmp/gdb_commands.txt <<'EOF'
set pagination off
set logging file /shared/results/gdb_output.txt
set logging on
run -a BENCHFS --benchfs.registry /shared/registry --benchfs.datadir /shared/data -w -r -t 1m -b 4m -s 4 -o /testfile -v
bt full
info registers
quit
EOF

# Run IOR under gdb (single process, no MPI)
cd /shared/results
gdb -batch -x /tmp/gdb_commands.txt ${IOR_BIN} 2>&1 | tee /shared/results/gdb_console.txt

echo ""
echo "=========================================="
echo "GDB output saved to /shared/results/"
echo "=========================================="
cat /shared/results/gdb_output.txt
