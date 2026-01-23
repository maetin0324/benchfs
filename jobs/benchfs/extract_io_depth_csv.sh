#!/bin/bash
# ==============================================================================
# extract_io_depth_csv.sh - Extract io_depth time series data from logs
# ==============================================================================
# This script extracts io_uring io_depth log entries from benchmark logs and
# outputs them as CSV for time-series analysis.
#
# Log format expected:
#   [bnode044] 2026-01-16T09:51:36.192122Z DEBUG pluvio_uring::reactor:...: io_uring: submitting N SQEs (io_depth=N)
#
# Usage:
#   ./extract_io_depth_csv.sh <job_results_dir> [--aggregate <interval_sec>]
#
# Output:
#   - io_depth_raw.csv: Raw per-submit data
#   - io_depth_aggregated.csv: Aggregated by time interval and node (if --aggregate)
#   - io_depth_summary.txt: Human-readable summary
# ==============================================================================

set -euo pipefail

# Parse arguments
if [ $# -lt 1 ]; then
    echo "Usage: $0 <job_results_dir> [--aggregate <interval_sec>]"
    echo ""
    echo "Options:"
    echo "  --aggregate <sec>  Aggregate data per time interval (default: no aggregation)"
    echo ""
    echo "Example:"
    echo "  $0 /path/to/results/benchfs/2026.01.23-13.09.00-debug_large/... --aggregate 1"
    exit 1
fi

JOB_DIR="$1"
shift

AGGREGATE_INTERVAL=0

while [ $# -gt 0 ]; do
    case "$1" in
        --aggregate)
            AGGREGATE_INTERVAL="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

if [ ! -d "$JOB_DIR" ]; then
    echo "ERROR: Job results directory not found: $JOB_DIR"
    exit 1
fi

# Define subdirectories
BENCHFSD_DIR="${JOB_DIR}/benchfsd_logs"

# Output files (in job directory)
RAW_CSV="${JOB_DIR}/io_depth_raw.csv"
AGGREGATED_CSV="${JOB_DIR}/io_depth_aggregated.csv"
SUMMARY_TXT="${JOB_DIR}/io_depth_summary.txt"

echo "==========================================="
echo "Extracting io_depth data from logs"
echo "==========================================="
echo "Job directory: $JOB_DIR"
echo "BenchFS logs:  $BENCHFSD_DIR"
echo ""

# Python script for parsing (embedded)
PARSE_SCRIPT=$(cat << 'PYTHON_EOF'
#!/usr/bin/env python3
"""Parse io_depth log entries from stdin and output CSV."""
import sys
import re

# Pattern to strip ANSI escape codes
ansi_escape = re.compile(r'\x1b\[[0-9;]*m')

# Pattern for io_uring submit
# [bnode044] 2026-01-16T09:51:36.192122Z DEBUG pluvio_uring::reactor:...: io_uring: submitting 1 SQEs (io_depth=1)
pattern = re.compile(
    r"\[([^\]]+)\]\s+"  # node
    r"(\S+)\s+"  # timestamp
    r"DEBUG\s+pluvio_uring::reactor:\S+:\s+"  # log prefix
    r"io_uring:\s+submitting\s+(\d+)\s+SQEs\s+"  # SQE count
    r"\(io_depth=(\d+)\)"  # io_depth
)

for line in sys.stdin:
    # Strip ANSI escape codes first
    line = ansi_escape.sub('', line)
    match = pattern.search(line)
    if match:
        node = match.group(1)
        timestamp = match.group(2)
        sqe_count = match.group(3)
        io_depth = match.group(4)
        print(f"{timestamp},{node},{sqe_count},{io_depth}")
PYTHON_EOF
)

# Initialize raw CSV with header
echo "timestamp,node,sqe_count,io_depth" > "$RAW_CSV"

# Process benchfsd logs
echo "Processing logs..."
log_count=0

if [ -d "$BENCHFSD_DIR" ]; then
    for run_dir in "${BENCHFSD_DIR}"/run_*/; do
        if [ -d "$run_dir" ]; then
            for log_file in "${run_dir}"*.log; do
                if [ -f "$log_file" ]; then
                    echo "  Processing: $log_file"
                    # Use grep to filter lines first, then parse with Python
                    { grep "io_depth=" "$log_file" 2>/dev/null || true; } | \
                        python3 -c "$PARSE_SCRIPT" >> "$RAW_CSV"
                    ((log_count++)) || true
                fi
            done
        fi
    done
fi

echo ""
echo "Processed $log_count log files"

# Count entries
entry_count=$(tail -n +2 "$RAW_CSV" | wc -l)
echo "Extracted $entry_count io_depth entries"

# Generate aggregated CSV if requested
if [ "$AGGREGATE_INTERVAL" -gt 0 ]; then
    echo ""
    echo "Aggregating by ${AGGREGATE_INTERVAL}s intervals..."

    # Use Python for aggregation (similar to extract_node_transfer_csv.sh)
    python3 << AGGREGATE_EOF
import csv
from collections import defaultdict
from datetime import datetime
import statistics

# Read raw CSV
entries = []
with open("$RAW_CSV", "r") as f:
    reader = csv.DictReader(f)
    for row in reader:
        entries.append(row)

if not entries:
    print("No entries to aggregate")
    exit(0)

# Parse timestamps and aggregate
interval = $AGGREGATE_INTERVAL
# Store all io_depth values for each bucket to calculate statistics
aggregated = defaultdict(lambda: {"total_sqes": 0, "submit_count": 0, "io_depths": []})

for entry in entries:
    ts_str = entry["timestamp"]
    node = entry["node"]
    sqe_count = int(entry["sqe_count"])
    io_depth = int(entry["io_depth"])

    # Parse timestamp
    try:
        # Remove Z suffix and parse
        ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        # Calculate time bucket (seconds from midnight)
        total_sec = ts.hour * 3600 + ts.minute * 60 + ts.second
        bucket = (total_sec // interval) * interval
    except:
        bucket = 0

    key = (bucket, node)
    aggregated[key]["total_sqes"] += sqe_count
    aggregated[key]["submit_count"] += 1
    aggregated[key]["io_depths"].append(io_depth)

# Write aggregated CSV
with open("$AGGREGATED_CSV", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["time_bucket", "node", "total_sqes", "submit_count", "avg_io_depth", "min_io_depth", "max_io_depth"])

    for (bucket, node), data in sorted(aggregated.items()):
        depths = data["io_depths"]
        avg_depth = sum(depths) / len(depths) if depths else 0
        min_depth = min(depths) if depths else 0
        max_depth = max(depths) if depths else 0
        writer.writerow([bucket, node, data["total_sqes"], data["submit_count"], f"{avg_depth:.2f}", min_depth, max_depth])

print(f"Generated {len(aggregated)} aggregated entries")
AGGREGATE_EOF
fi

# Generate summary using Python
python3 << SUMMARY_EOF
import csv
from collections import defaultdict

# Read raw CSV
entries = []
with open("$RAW_CSV", "r") as f:
    reader = csv.DictReader(f)
    for row in reader:
        entries.append(row)

with open("$SUMMARY_TXT", "w") as f:
    f.write("===========================================\n")
    f.write("io_depth Analysis Summary\n")
    f.write("===========================================\n\n")
    f.write(f"Source: $JOB_DIR\n")
    f.write(f"Total submit events: {len(entries)}\n\n")

    if not entries:
        f.write("No entries found.\n")
        exit(0)

    # Calculate statistics
    io_depths = [int(e["io_depth"]) for e in entries]
    total_sqes = sum(int(e["sqe_count"]) for e in entries)

    f.write(f"Total SQEs submitted: {total_sqes:,}\n")
    f.write(f"Average io_depth: {sum(io_depths)/len(io_depths):.2f}\n")
    f.write(f"Min io_depth: {min(io_depths)}\n")
    f.write(f"Max io_depth: {max(io_depths)}\n\n")

    # io_depth distribution
    f.write("=== io_depth Distribution ===\n")
    depth_counts = defaultdict(int)
    for d in io_depths:
        depth_counts[d] += 1

    total = len(io_depths)
    for depth in sorted(depth_counts.keys()):
        count = depth_counts[depth]
        pct = count / total * 100
        bar = "#" * int(pct / 2)
        f.write(f"  io_depth={depth:3d}: {count:8d} ({pct:5.1f}%) {bar}\n")

    # Per-node statistics
    f.write("\n=== Per-Node Statistics ===\n")
    node_depths = defaultdict(list)
    for e in entries:
        node_depths[e["node"]].append(int(e["io_depth"]))

    f.write(f"{'Node':<15} {'Count':>10} {'Avg':>8} {'Max':>6}\n")
    f.write("-" * 45 + "\n")
    for node in sorted(node_depths.keys()):
        depths = node_depths[node]
        avg = sum(depths) / len(depths)
        f.write(f"{node:<15} {len(depths):>10} {avg:>8.2f} {max(depths):>6}\n")

    # Performance warning
    f.write("\n=== Performance Analysis ===\n")
    low_depth_count = sum(1 for d in io_depths if d == 1)
    low_depth_pct = low_depth_count / total * 100

    if low_depth_pct > 50:
        f.write(f"WARNING: {low_depth_pct:.1f}% of submits have io_depth=1\n")
        f.write("io_uring batching is inefficient.\n")
    elif low_depth_pct > 30:
        f.write(f"Note: {low_depth_pct:.1f}% of submits have io_depth=1\n")
    else:
        f.write(f"Good: Only {low_depth_pct:.1f}% of submits have io_depth=1\n")

print("Summary generated")
SUMMARY_EOF

echo ""
echo "==========================================="
echo "Analysis complete"
echo "==========================================="
echo "Output files:"
echo "  Raw CSV:        $RAW_CSV"
if [ "$AGGREGATE_INTERVAL" -gt 0 ]; then
    echo "  Aggregated CSV: $AGGREGATED_CSV"
fi
echo "  Summary:        $SUMMARY_TXT"
echo ""
cat "$SUMMARY_TXT"
