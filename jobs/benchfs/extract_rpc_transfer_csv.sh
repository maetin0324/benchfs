#!/bin/bash
# ==============================================================================
# extract_rpc_transfer_csv.sh - Extract RPC transfer timing data from logs
# ==============================================================================
# This script extracts RPC_TRANSFER log entries from benchmark logs and
# outputs them as CSV for time-series analysis of RPC performance.
#
# Log format expected:
#   [bnode044] 2026-01-23T09:51:36.192122Z DEBUG rpc_transfer: RPC_TRANSFER op="READ" target_node=bnode115 bytes=4194304 elapsed_us=1234 bandwidth_mib_s="123.45"
#
# Usage:
#   ./extract_rpc_transfer_csv.sh <job_results_dir> [--aggregate <interval_sec>]
#
# Output:
#   - rpc_transfer_raw.csv: Raw per-transfer data
#   - rpc_transfer_aggregated.csv: Aggregated by time interval, node, and op (if --aggregate)
#   - rpc_transfer_summary.txt: Human-readable summary
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
IOR_DIR="${JOB_DIR}/ior_results"

# Output files (in job directory)
RAW_CSV="${JOB_DIR}/rpc_transfer_raw.csv"
AGGREGATED_CSV="${JOB_DIR}/rpc_transfer_aggregated.csv"
SUMMARY_TXT="${JOB_DIR}/rpc_transfer_summary.txt"

echo "==========================================="
echo "Extracting RPC_TRANSFER data from logs"
echo "==========================================="
echo "Job directory: $JOB_DIR"
echo "BenchFS logs:  $BENCHFSD_DIR"
echo "IOR logs:      $IOR_DIR"
echo ""

# Python script for parsing (embedded)
PARSE_SCRIPT=$(cat << 'PYTHON_EOF'
#!/usr/bin/env python3
"""Parse RPC_TRANSFER log entries from stdin and output CSV."""
import sys
import re

# Pattern to strip ANSI escape codes (comprehensive pattern)
# Matches codes ending with any letter, not just 'm'
ansi_escape = re.compile(r'\x1b\[[0-9;]*[a-zA-Z]')

# Pattern for RPC_TRANSFER (after ANSI stripping)
# [bnode044] 2026-01-23T09:51:36.192122Z DEBUG rpc_transfer:src/api/file_ops.rs:904: RPC_TRANSFER op="READ" target_node=node_13 bytes=4194304 elapsed_us=1234 bandwidth_mib_s="123.45"
pattern = re.compile(
    r"\[([^\]]+)\]\s+"  # client node
    r"(\S+)\s+"  # timestamp
    r"DEBUG\s+rpc_transfer:\S+:\s+"  # log prefix (module:file:line:)
    r"RPC_TRANSFER\s+"  # marker
    r'op="?([A-Z]+)"?\s+'  # operation (READ or WRITE)
    r"target_node=(\S+)\s+"  # target node
    r"bytes=(\d+)\s+"  # bytes transferred
    r"elapsed_us=(\d+)\s+"  # elapsed time in microseconds
    r'bandwidth_mib_s="?([0-9.]+)"?'  # bandwidth
)

# Pattern to extract clean node name (handles corrupted names like '0[bnode031')
node_pattern = re.compile(r'(bnode\d+|node_\d+)')

for line in sys.stdin:
    # Strip ANSI escape codes first
    line = ansi_escape.sub('', line)
    match = pattern.search(line)
    if match:
        client_node_raw = match.group(1)
        timestamp = match.group(2)
        op = match.group(3)
        target_node_raw = match.group(4)
        bytes_val = match.group(5)
        elapsed_us = match.group(6)
        bandwidth = match.group(7)

        # Clean up node names
        client_match = node_pattern.search(client_node_raw)
        client_node = client_match.group(1) if client_match else client_node_raw
        target_match = node_pattern.search(target_node_raw)
        target_node = target_match.group(1) if target_match else target_node_raw

        print(f"{timestamp},{client_node},{op},{target_node},{bytes_val},{elapsed_us},{bandwidth}")
PYTHON_EOF
)

# Initialize raw CSV with header
echo "timestamp,client_node,op,target_node,bytes,elapsed_us,bandwidth_mib_s" > "$RAW_CSV"

# Process logs
echo "Processing logs..."
log_count=0

# 1. Process IOR results logs (client-side RPC logs)
if [ -d "$IOR_DIR" ]; then
    echo ""
    echo "=== IOR Results (client logs) ==="
    for log_file in "${IOR_DIR}"/*.log; do
        if [ -f "$log_file" ]; then
            echo "  Processing: $log_file"
            # Use grep to filter lines first, then parse with Python
            { grep "RPC_TRANSFER" "$log_file" 2>/dev/null || true; } | \
                python3 -c "$PARSE_SCRIPT" >> "$RAW_CSV"
            ((log_count++)) || true
        fi
    done
fi

# 2. Process benchfsd logs (server-side logs, usually no RPC_TRANSFER)
if [ -d "$BENCHFSD_DIR" ]; then
    echo ""
    echo "=== BenchFS daemon logs ==="
    for run_dir in "${BENCHFSD_DIR}"/run_*/; do
        if [ -d "$run_dir" ]; then
            for log_file in "${run_dir}"*.log; do
                if [ -f "$log_file" ]; then
                    echo "  Processing: $log_file"
                    # Use grep to filter lines first, then parse with Python
                    { grep "RPC_TRANSFER" "$log_file" 2>/dev/null || true; } | \
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
echo "Extracted $entry_count RPC_TRANSFER entries"

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
# Store all values for each bucket to calculate statistics
aggregated = defaultdict(lambda: {
    "total_bytes": 0,
    "transfer_count": 0,
    "elapsed_sum": 0,
    "bandwidth_sum": 0,
    "elapsed_list": [],
    "bandwidth_list": []
})

for entry in entries:
    ts_str = entry["timestamp"]
    client_node = entry["client_node"]
    op = entry["op"]
    target_node = entry["target_node"]
    bytes_val = int(entry["bytes"])
    elapsed_us = int(entry["elapsed_us"])
    bandwidth = float(entry["bandwidth_mib_s"])

    # Parse timestamp
    try:
        # Remove Z suffix and parse
        ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        # Calculate time bucket (seconds from midnight)
        total_sec = ts.hour * 3600 + ts.minute * 60 + ts.second
        bucket = (total_sec // interval) * interval
    except:
        bucket = 0

    key = (bucket, client_node, op)
    aggregated[key]["total_bytes"] += bytes_val
    aggregated[key]["transfer_count"] += 1
    aggregated[key]["elapsed_sum"] += elapsed_us
    aggregated[key]["bandwidth_sum"] += bandwidth
    aggregated[key]["elapsed_list"].append(elapsed_us)
    aggregated[key]["bandwidth_list"].append(bandwidth)

# Write aggregated CSV
with open("$AGGREGATED_CSV", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow([
        "time_bucket", "client_node", "op", "total_bytes", "transfer_count",
        "avg_elapsed_us", "min_elapsed_us", "max_elapsed_us",
        "avg_bandwidth_mib_s", "min_bandwidth_mib_s", "max_bandwidth_mib_s"
    ])

    for (bucket, client_node, op), data in sorted(aggregated.items()):
        elapsed_list = data["elapsed_list"]
        bandwidth_list = data["bandwidth_list"]
        avg_elapsed = data["elapsed_sum"] / data["transfer_count"] if data["transfer_count"] > 0 else 0
        min_elapsed = min(elapsed_list) if elapsed_list else 0
        max_elapsed = max(elapsed_list) if elapsed_list else 0
        avg_bandwidth = data["bandwidth_sum"] / data["transfer_count"] if data["transfer_count"] > 0 else 0
        min_bandwidth = min(bandwidth_list) if bandwidth_list else 0
        max_bandwidth = max(bandwidth_list) if bandwidth_list else 0
        writer.writerow([
            bucket, client_node, op, data["total_bytes"], data["transfer_count"],
            f"{avg_elapsed:.2f}", min_elapsed, max_elapsed,
            f"{avg_bandwidth:.2f}", f"{min_bandwidth:.2f}", f"{max_bandwidth:.2f}"
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
    f.write("RPC_TRANSFER Analysis Summary\n")
    f.write("===========================================\n\n")
    f.write(f"Source: $JOB_DIR\n")
    f.write(f"Total transfer events: {len(entries)}\n\n")

    if not entries:
        f.write("No entries found.\n")
        exit(0)

    # Separate by operation type
    read_entries = [e for e in entries if e["op"] == "READ"]
    write_entries = [e for e in entries if e["op"] == "WRITE"]

    def analyze_entries(entries, op_name):
        if not entries:
            return f"No {op_name} entries found.\n"

        elapsed_list = [int(e["elapsed_us"]) for e in entries]
        bandwidth_list = [float(e["bandwidth_mib_s"]) for e in entries]
        bytes_list = [int(e["bytes"]) for e in entries]

        total_bytes = sum(bytes_list)
        total_bytes_gib = total_bytes / (1024**3)

        result = []
        result.append(f"=== {op_name} Operations ===\n")
        result.append(f"  Count: {len(entries):,}\n")
        result.append(f"  Total bytes: {total_bytes_gib:.2f} GiB\n")
        result.append(f"\n  Latency (us):\n")
        result.append(f"    Mean:   {sum(elapsed_list)/len(elapsed_list):,.2f}\n")
        result.append(f"    Min:    {min(elapsed_list):,}\n")
        result.append(f"    Max:    {max(elapsed_list):,}\n")

        # Percentiles
        sorted_elapsed = sorted(elapsed_list)
        n = len(sorted_elapsed)
        p50 = sorted_elapsed[int(n * 0.50)]
        p90 = sorted_elapsed[int(n * 0.90)]
        p95 = sorted_elapsed[int(n * 0.95)]
        p99 = sorted_elapsed[min(int(n * 0.99), n-1)]
        result.append(f"    P50:    {p50:,}\n")
        result.append(f"    P90:    {p90:,}\n")
        result.append(f"    P95:    {p95:,}\n")
        result.append(f"    P99:    {p99:,}\n")

        result.append(f"\n  Bandwidth (MiB/s):\n")
        result.append(f"    Mean:   {sum(bandwidth_list)/len(bandwidth_list):.2f}\n")
        result.append(f"    Min:    {min(bandwidth_list):.2f}\n")
        result.append(f"    Max:    {max(bandwidth_list):.2f}\n")

        sorted_bw = sorted(bandwidth_list)
        bw_p50 = sorted_bw[int(n * 0.50)]
        bw_p90 = sorted_bw[int(n * 0.90)]
        result.append(f"    P50:    {bw_p50:.2f}\n")
        result.append(f"    P90:    {bw_p90:.2f}\n")

        return "".join(result)

    f.write(analyze_entries(write_entries, "WRITE"))
    f.write("\n")
    f.write(analyze_entries(read_entries, "READ"))

    # Performance comparison
    if read_entries and write_entries:
        f.write("\n=== Performance Comparison ===\n")
        read_avg_elapsed = sum(int(e["elapsed_us"]) for e in read_entries) / len(read_entries)
        write_avg_elapsed = sum(int(e["elapsed_us"]) for e in write_entries) / len(write_entries)
        read_avg_bw = sum(float(e["bandwidth_mib_s"]) for e in read_entries) / len(read_entries)
        write_avg_bw = sum(float(e["bandwidth_mib_s"]) for e in write_entries) / len(write_entries)

        f.write(f"  WRITE avg latency: {write_avg_elapsed:,.2f} us\n")
        f.write(f"  READ avg latency:  {read_avg_elapsed:,.2f} us\n")
        f.write(f"  Ratio (READ/WRITE): {read_avg_elapsed/write_avg_elapsed:.2f}x\n")
        f.write(f"\n")
        f.write(f"  WRITE avg bandwidth: {write_avg_bw:.2f} MiB/s\n")
        f.write(f"  READ avg bandwidth:  {read_avg_bw:.2f} MiB/s\n")
        f.write(f"  Ratio (WRITE/READ): {write_avg_bw/read_avg_bw:.2f}x\n")

        if read_avg_elapsed > write_avg_elapsed * 2:
            f.write(f"\n*** WARNING: READ latency is {read_avg_elapsed/write_avg_elapsed:.1f}x higher than WRITE ***\n")
            f.write("This suggests RPC response handling is a bottleneck.\n")

    # Per-client statistics
    f.write("\n=== Per-Client Statistics ===\n")
    client_stats = defaultdict(lambda: {"read_count": 0, "write_count": 0, "read_bytes": 0, "write_bytes": 0})
    for e in entries:
        client = e["client_node"]
        op = e["op"]
        bytes_val = int(e["bytes"])
        if op == "READ":
            client_stats[client]["read_count"] += 1
            client_stats[client]["read_bytes"] += bytes_val
        else:
            client_stats[client]["write_count"] += 1
            client_stats[client]["write_bytes"] += bytes_val

    f.write(f"{'Client':<15} {'WRITE Count':>12} {'READ Count':>12} {'WRITE GiB':>10} {'READ GiB':>10}\n")
    f.write("-" * 65 + "\n")
    for client in sorted(client_stats.keys()):
        stats = client_stats[client]
        f.write(f"{client:<15} {stats['write_count']:>12} {stats['read_count']:>12} "
                f"{stats['write_bytes']/(1024**3):>10.2f} {stats['read_bytes']/(1024**3):>10.2f}\n")

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
