#!/bin/bash
#PBS -q gold
#PBS -A NBB
#PBS -l select=1
#PBS -l walltime=0:05:00

echo "=========================================="
echo "Sirius Node Diagnostics"
echo "=========================================="
echo ""
echo "--- hostname ---"
hostname
echo ""
echo "--- PBS_JOBID ---"
echo "$PBS_JOBID"
echo ""
echo "--- PBS_NODEFILE ---"
cat "$PBS_NODEFILE"
echo ""
echo "--- lscpu ---"
lscpu
echo ""
echo "--- free -h ---"
free -h
echo ""
echo "--- df -h /scr* ---"
df -h /scr* 2>/dev/null || echo "/scr not found"
echo ""
echo "--- ls -la /scr/ ---"
ls -la /scr/ 2>/dev/null || echo "/scr not found"
echo ""
echo "--- ls -la /scr/${PBS_JOBID}/ ---"
ls -la "/scr/${PBS_JOBID}/" 2>/dev/null || echo "/scr/${PBS_JOBID} not found"
echo ""
echo "--- numactl --hardware ---"
numactl --hardware 2>/dev/null || echo "numactl not available"
echo ""
echo "--- module list ---"
module list 2>&1
echo ""
echo "--- ip -o -4 addr show ---"
ip -o -4 addr show 2>/dev/null
echo ""
echo "--- ibstat (summary) ---"
ibstat 2>/dev/null | head -20 || echo "ibstat not available"
echo ""
echo "Done."
