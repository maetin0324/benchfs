#!/bin/bash
# Debug script for BenchFS cluster issues
set -euo pipefail

echo "=========================================="
echo "BenchFS Cluster Debug Information"
echo "=========================================="

echo
echo "1. Container Status"
echo "-------------------"
hostname
uname -a

echo
echo "2. Mount Points"
echo "---------------"
df -h | grep -E "(Filesystem|/shared)"
ls -la /shared/ || echo "ERROR: /shared not accessible"
ls -la /shared/registry/ || echo "ERROR: /shared/registry not accessible"
ls -la /shared/data/ || echo "ERROR: /shared/data not accessible"

echo
echo "3. Network Configuration"
echo "------------------------"
ip addr show eth0 || echo "ERROR: eth0 not found"

echo
echo "4. SSH Connectivity"
echo "-------------------"
for host in server1 server2; do
    echo -n "  Testing $host: "
    if ssh -o ConnectTimeout=2 $host "echo OK" 2>/dev/null; then
        echo "SUCCESS"
    else
        echo "FAILED"
    fi
done

echo
echo "5. MPI Configuration"
echo "--------------------"
which mpirun || echo "ERROR: mpirun not found"
mpirun --version || echo "ERROR: mpirun failed"
echo "OMPI_ALLOW_RUN_AS_ROOT=$OMPI_ALLOW_RUN_AS_ROOT"

echo
echo "6. BenchFS Binary"
echo "-----------------"
ls -lh /usr/local/bin/benchfsd_mpi || echo "ERROR: benchfsd_mpi not found"
file /usr/local/bin/benchfsd_mpi || echo "ERROR: Cannot check file type"

echo
echo "7. UCX Configuration"
echo "--------------------"
printenv | grep UCX || echo "No UCX environment variables"
ucx_info -v 2>/dev/null || echo "ucx_info not available"

echo
echo "8. Config File"
echo "--------------"
ls -lh /configs/benchfs_test.toml || echo "ERROR: Config file not found"
cat /configs/benchfs_test.toml || echo "ERROR: Cannot read config"

echo
echo "9. Test Simple MPI Run"
echo "----------------------"
cat > /tmp/hostfile_test <<EOF
server1 slots=1
server2 slots=1
EOF

echo "Hostfile:"
cat /tmp/hostfile_test

echo
echo "Testing MPI with hostname:"
mpirun \
    --hostfile /tmp/hostfile_test \
    -np 2 \
    --mca btl tcp,self \
    --mca btl_tcp_if_include eth0 \
    hostname || echo "ERROR: MPI test failed"

echo
echo "10. Running Processes"
echo "---------------------"
ps aux | head -20

echo
echo "=========================================="
echo "Debug information collection complete"
echo "=========================================="
