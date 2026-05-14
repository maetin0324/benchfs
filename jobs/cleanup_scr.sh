#!/bin/bash
# Clean /scr0-3 of stale benchfs data on this compute node.
# Run via mpirun --map-by node so it executes once per physical host.
hn=$(hostname)
for d in /scr0 /scr1 /scr2 /scr3; do
  if [ -d "$d" ]; then
    # Remove jobid subdirs only (don't touch /scr root files / non-our content)
    find "$d" -mindepth 1 -maxdepth 1 -type d -name '[0-9]*.pbs' -exec rm -rf {} + 2>/dev/null
  fi
  echo "$hn $d: $(df -h "$d" 2>/dev/null | tail -1 | awk '{print $4 " free"}')"
done
