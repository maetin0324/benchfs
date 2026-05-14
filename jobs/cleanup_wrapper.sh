#!/bin/bash
#PBS -q mcrp -A NBB
cd $PBS_O_WORKDIR
echo "=== cleanup launch from $(hostname) at $(date) ==="
NODES=$(sort -u $PBS_NODEFILE)
echo "NODES:"; echo "$NODES"
for h in $NODES; do
  echo "--- $h ---"
  ssh -o StrictHostKeyChecking=no "$h" "bash /work/NBB/rmaeda/workspace/rust/benchfs/jobs/cleanup_scr.sh" 2>&1
done
echo "=== cleanup done at $(date) ==="
