#!/bin/bash
# Cancel all PBS jobs for the current user's group
# Usage: ./cancel-jobs.sh [-y] [pattern]
#   -y: Skip confirmation prompt
#   pattern: Optional job name pattern to filter (e.g., "benchfs")

set -euo pipefail

SKIP_CONFIRM=0
PATTERN=""
GROUP="xg24i002"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        -y|--yes)
            SKIP_CONFIRM=1
            shift
            ;;
        *)
            PATTERN="$1"
            shift
            ;;
    esac
done

echo "=========================================="
echo "PBS Job Cancellation Script"
echo "=========================================="
echo "Group: $GROUP"

# Get list of jobs for the group (skip header lines, filter by group)
if [[ -n "$PATTERN" ]]; then
    echo "Filtering jobs matching pattern: $PATTERN"
    jobs=$(qstat 2>/dev/null | grep "$GROUP" | grep "$PATTERN" | awk '{print $1}') || true
else
    echo "Getting all jobs for group: $GROUP"
    jobs=$(qstat 2>/dev/null | grep "$GROUP" | awk '{print $1}') || true
fi

if [[ -z "$jobs" ]]; then
    echo "No jobs found."
    exit 0
fi

# Convert to array
readarray -t job_array <<< "$jobs"
job_count=${#job_array[@]}

echo "Found $job_count job(s) to cancel:"
echo ""

# Show header and first 10 jobs
qstat 2>/dev/null | head -4
shown=0
for job_id in "${job_array[@]}"; do
    if [[ $shown -lt 10 ]]; then
        qstat 2>/dev/null | grep "^$job_id" || echo "$job_id"
        ((shown++))
    else
        echo "... and $((job_count - 10)) more jobs"
        break
    fi
done

echo ""

if [[ $SKIP_CONFIRM -eq 0 ]]; then
    read -p "Cancel all $job_count job(s)? [y/N] " -n 1 -r </dev/tty
    echo ""
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Cancelled by user."
        exit 0
    fi
fi

echo "Cancelling $job_count jobs..."
for job_id in "${job_array[@]}"; do
    qdel "$job_id" 2>/dev/null && echo "  Cancelled: $job_id" || echo "  Failed: $job_id"
done

echo ""
echo "Done."
