#!/bin/bash
# ==============================================================================
# extract_rpc_concurrency_csv.sh - Extract RPC concurrency time series data from logs
# ==============================================================================
# This script extracts RPC concurrency statistics log entries from benchmark logs
# and outputs them as CSV for time-series analysis.
#
# Log format expected:
#   [bnode044] 2026-01-16T09:51:36.192122Z DEBUG benchfs::rpc::server:...: ongoing=N received=N completed=N peak=N RPC_CONCURRENCY_STATS
#
# Usage:
#   ./extract_rpc_concurrency_csv.sh <job_results_dir> [--aggregate <interval_sec>]
#
# Output:
#   - rpc_concurrency_raw.csv: Raw per-log data
#   - rpc_concurrency_aggregated.csv: Aggregated by time interval and node (if --aggregate)
#   - rpc_concurrency_summary.txt: Human-readable summary
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
RAW_CSV="${JOB_DIR}/rpc_concurrency_raw.csv"
AGGREGATED_CSV="${JOB_DIR}/rpc_concurrency_aggregated.csv"
SUMMARY_TXT="${JOB_DIR}/rpc_concurrency_summary.txt"

echo "==========================================="
echo "Extracting RPC concurrency data from logs"
echo "==========================================="
echo "Job directory: $JOB_DIR"
echo "BenchFS logs:  $BENCHFSD_DIR"
echo ""

# Python script for parsing (embedded)
PARSE_SCRIPT=$(cat << 'PYTHON_EOF'
#!/usr/bin/env python3
"""Parse RPC concurrency log entries from stdin and output CSV."""
import sys
import re

# Pattern to strip ANSI escape codes
ansi_escape = re.compile(r'\x1b\[[0-9;]*m')

# Pattern for RPC concurrency stats
# [bnode044] 2026-01-16T09:51:36.192122Z DEBUG benchfs::rpc::server:...: RPC_CONCURRENCY_STATS ongoing=N received=N completed=N peak=N
# Note: tracing outputs message first, then fields
pattern = re.compile(
    r"\[([^\]]+)\]\s+"  # node
    r"(\S+)\s+"  # timestamp
    r"DEBUG\s+benchfs::rpc::server:\S*:\s+"  # log prefix
    r"RPC_CONCURRENCY_STATS\s+"  # marker (comes first in tracing output)
    r"ongoing=(\d+)\s+"  # ongoing requests
    r"received=(\d+)\s+"  # total received
    r"completed=(\d+)\s+"  # total completed
    r"peak=(\d+)"  # peak concurrent
)

for line in sys.stdin:
    # Strip ANSI escape codes first
    line = ansi_escape.sub('', line)
    match = pattern.search(line)
    if match:
        node = match.group(1)
        timestamp = match.group(2)
        ongoing = match.group(3)
        received = match.group(4)
        completed = match.group(5)
        peak = match.group(6)
        print(f"{timestamp},{node},{ongoing},{received},{completed},{peak}")
PYTHON_EOF
)

# Initialize raw CSV with header
echo "timestamp,node,ongoing,received,completed,peak" > "$RAW_CSV"

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
                    { grep "RPC_CONCURRENCY_STATS" "$log_file" 2>/dev/null || true; } | \
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
echo "Extracted $entry_count RPC concurrency entries"

# Generate aggregated CSV if requested
if [ "$AGGREGATE_INTERVAL" -gt 0 ]; then
    echo ""
    echo "Aggregating by ${AGGREGATE_INTERVAL}s intervals..."

    # Use Python for aggregation
    python3 << AGGREGATE_EOF
import csv
from collections import defaultdict
from datetime import datetime

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
# Store values for each bucket to calculate statistics
aggregated = defaultdict(lambda: {
    "ongoing_values": [],
    "received_values": [],
    "completed_values": [],
    "peak_values": [],
    "count": 0
})

for entry in entries:
    ts_str = entry["timestamp"]
    node = entry["node"]
    ongoing = int(entry["ongoing"])
    received = int(entry["received"])
    completed = int(entry["completed"])
    peak = int(entry["peak"])

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
    aggregated[key]["ongoing_values"].append(ongoing)
    aggregated[key]["received_values"].append(received)
    aggregated[key]["completed_values"].append(completed)
    aggregated[key]["peak_values"].append(peak)
    aggregated[key]["count"] += 1

# Write aggregated CSV
with open("$AGGREGATED_CSV", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow([
        "time_bucket", "node", "sample_count",
        "avg_ongoing", "max_ongoing",
        "avg_peak", "max_peak",
        "received_delta", "completed_delta"
    ])

    for (bucket, node), data in sorted(aggregated.items()):
        ongoing_vals = data["ongoing_values"]
        peak_vals = data["peak_values"]
        received_vals = data["received_values"]
        completed_vals = data["completed_values"]

        avg_ongoing = sum(ongoing_vals) / len(ongoing_vals) if ongoing_vals else 0
        max_ongoing = max(ongoing_vals) if ongoing_vals else 0
        avg_peak = sum(peak_vals) / len(peak_vals) if peak_vals else 0
        max_peak = max(peak_vals) if peak_vals else 0

        # Calculate delta (difference between first and last sample in interval)
        received_delta = received_vals[-1] - received_vals[0] if len(received_vals) > 1 else 0
        completed_delta = completed_vals[-1] - completed_vals[0] if len(completed_vals) > 1 else 0

        writer.writerow([
            bucket, node, data["count"],
            f"{avg_ongoing:.2f}", max_ongoing,
            f"{avg_peak:.2f}", max_peak,
            received_delta, completed_delta
        ])

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
    f.write("RPC Concurrency Analysis Summary\n")
    f.write("===========================================\n\n")
    f.write(f"Source: $JOB_DIR\n")
    f.write(f"Total log entries: {len(entries)}\n\n")

    if not entries:
        f.write("No entries found.\n")
        exit(0)

    # Calculate statistics
    ongoing_values = [int(e["ongoing"]) for e in entries]
    peak_values = [int(e["peak"]) for e in entries]

    f.write("=== Ongoing RPC Requests ===\n")
    f.write(f"Average: {sum(ongoing_values)/len(ongoing_values):.2f}\n")
    f.write(f"Min: {min(ongoing_values)}\n")
    f.write(f"Max: {max(ongoing_values)}\n\n")

    f.write("=== Peak Concurrent Requests ===\n")
    f.write(f"Average: {sum(peak_values)/len(peak_values):.2f}\n")
    f.write(f"Min: {min(peak_values)}\n")
    f.write(f"Max: {max(peak_values)}\n\n")

    # Ongoing distribution
    f.write("=== Ongoing Distribution ===\n")
    ongoing_counts = defaultdict(int)
    for v in ongoing_values:
        ongoing_counts[v] += 1

    total = len(ongoing_values)
    # Show top 10 most common values
    sorted_counts = sorted(ongoing_counts.items(), key=lambda x: -x[1])[:10]
    for value, count in sorted_counts:
        pct = count / total * 100
        bar = "#" * int(pct / 2)
        f.write(f"  ongoing={value:3d}: {count:8d} ({pct:5.1f}%) {bar}\n")

    # Per-node statistics
    f.write("\n=== Per-Node Statistics ===\n")
    node_ongoing = defaultdict(list)
    node_peak = defaultdict(list)
    for e in entries:
        node_ongoing[e["node"]].append(int(e["ongoing"]))
        node_peak[e["node"]].append(int(e["peak"]))

    f.write(f"{'Node':<15} {'Samples':>10} {'Avg Ongoing':>12} {'Max Ongoing':>12} {'Peak':>8}\n")
    f.write("-" * 65 + "\n")
    for node in sorted(node_ongoing.keys()):
        ongoing = node_ongoing[node]
        peak = node_peak[node]
        avg_ongoing = sum(ongoing) / len(ongoing)
        max_ongoing = max(ongoing)
        max_peak = max(peak)
        f.write(f"{node:<15} {len(ongoing):>10} {avg_ongoing:>12.2f} {max_ongoing:>12} {max_peak:>8}\n")

    # Performance analysis
    f.write("\n=== Performance Analysis ===\n")
    low_ongoing_count = sum(1 for v in ongoing_values if v <= 2)
    low_ongoing_pct = low_ongoing_count / total * 100

    if low_ongoing_pct > 70:
        f.write(f"WARNING: {low_ongoing_pct:.1f}% of samples have ongoing<=2\n")
        f.write("RPC concurrency is very low. This indicates:\n")
        f.write("  - Client requests are not arriving fast enough, OR\n")
        f.write("  - Handler processing is completing too quickly\n")
    elif low_ongoing_pct > 50:
        f.write(f"Note: {low_ongoing_pct:.1f}% of samples have ongoing<=2\n")
        f.write("RPC concurrency is moderate.\n")
    else:
        f.write(f"Good: Only {low_ongoing_pct:.1f}% of samples have ongoing<=2\n")
        f.write("RPC concurrency is healthy.\n")

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
