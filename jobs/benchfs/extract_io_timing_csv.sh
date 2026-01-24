#!/bin/bash
# ==============================================================================
# extract_io_timing_csv.sh - Extract I/O timing data from benchfsd logs
# ==============================================================================
# This script extracts read_fixed_direct and write_fixed_direct timing logs
# and outputs them as CSV for time-series analysis of actual disk I/O performance.
#
# Log format expected:
#   [bnode100] 2026-01-23T05:45:05.234069Z DEBUG benchfs::storage::iouring:...: write_fixed_direct: 4194304 bytes in 10.025617ms to fd=100 at offset=0 (398.98 MiB/s)
#   [bnode100] 2026-01-23T05:47:19.896646Z DEBUG benchfs::storage::iouring:...: read_fixed_direct: 4194304 bytes in 54.043494ms from fd=173339 at offset=0 (74.01 MiB/s)
#
# Usage:
#   ./extract_io_timing_csv.sh <job_results_dir> [--aggregate <interval_sec>]
#
# Output:
#   - io_timing_raw.csv: Raw per-operation data
#   - io_timing_aggregated.csv: Aggregated by time interval, node, and op (if --aggregate)
#   - io_timing_summary.txt: Human-readable summary
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
    echo "  $0 /path/to/results/benchfs/2026.01.23-14.36.23-debug_large/... --aggregate 1"
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
RAW_CSV="${JOB_DIR}/io_timing_raw.csv"
AGGREGATED_CSV="${JOB_DIR}/io_timing_aggregated.csv"
SUMMARY_TXT="${JOB_DIR}/io_timing_summary.txt"

echo "==========================================="
echo "Extracting I/O timing data from logs"
echo "==========================================="
echo "Job directory: $JOB_DIR"
echo "BenchFS logs:  $BENCHFSD_DIR"
echo ""

# Python script for parsing (embedded)
PARSE_SCRIPT=$(cat << 'PYTHON_EOF'
#!/usr/bin/env python3
"""Parse read_fixed_direct and write_fixed_direct log entries from stdin and output CSV."""
import sys
import re

# Pattern to strip ANSI escape codes (comprehensive pattern)
# Matches codes ending with any letter, not just 'm'
ansi_escape = re.compile(r'\x1b\[[0-9;]*[a-zA-Z]')

# Pattern for read_fixed_direct and write_fixed_direct
# [bnode100] 2026-01-23T05:45:05.234069Z DEBUG benchfs::storage::iouring:...: write_fixed_direct: 4194304 bytes in 10.025617ms to fd=100 at offset=0 (398.98 MiB/s)
# [bnode100] 2026-01-23T05:47:19.896646Z DEBUG benchfs::storage::iouring:...: read_fixed_direct: 4194304 bytes in 54.043494ms from fd=173339 at offset=0 (74.01 MiB/s)
pattern = re.compile(
    r"\[([^\]]+)\]\s+"  # node
    r"(\S+)\s+"  # timestamp
    r"DEBUG\s+benchfs::storage::iouring:\S+:\s+"  # log prefix
    r"(read|write)_fixed_direct:\s+"  # operation
    r"(\d+)\s+bytes\s+in\s+"  # bytes
    r"(\S+)\s+"  # elapsed time (e.g., "10.025617ms")
    r"(?:from|to)\s+fd=(\d+)\s+"  # fd
    r"at\s+offset=(\d+)\s+"  # offset
    r"\(([0-9.]+)\s+MiB/s\)"  # bandwidth
)

# Pattern to extract clean node name (handles corrupted names like '0[bnode031')
node_pattern = re.compile(r'(bnode\d+)')

def parse_time(time_str):
    """Parse time string like '10.025617ms' or '54.043494ms' to microseconds."""
    time_str = time_str.strip()
    if time_str.endswith("ms"):
        return float(time_str[:-2]) * 1000  # ms to us
    elif time_str.endswith("µs") or time_str.endswith("us"):
        return float(time_str[:-2])
    elif time_str.endswith("s") and not time_str.endswith("ms") and not time_str.endswith("ns"):
        return float(time_str[:-1]) * 1_000_000  # s to us
    elif time_str.endswith("ns"):
        return float(time_str[:-2]) / 1000  # ns to us
    else:
        return float(time_str) * 1000  # assume ms

for line in sys.stdin:
    # Strip ANSI escape codes first
    line = ansi_escape.sub('', line)
    match = pattern.search(line)
    if match:
        node_raw = match.group(1)
        timestamp = match.group(2)
        op = match.group(3).upper()  # READ or WRITE
        bytes_val = match.group(4)
        elapsed_str = match.group(5)
        fd = match.group(6)
        offset = match.group(7)
        bandwidth = match.group(8)

        # Clean up node name (extract 'bnodeXXX' from potentially corrupted string)
        node_match = node_pattern.search(node_raw)
        node = node_match.group(1) if node_match else node_raw

        try:
            elapsed_us = parse_time(elapsed_str)
            print(f"{timestamp},{node},{op},{bytes_val},{elapsed_us:.2f},{fd},{offset},{bandwidth}")
        except:
            pass
PYTHON_EOF
)

# Initialize raw CSV with header
echo "timestamp,node,op,bytes,elapsed_us,fd,offset,bandwidth_mib_s" > "$RAW_CSV"

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
                    { grep -E "(read|write)_fixed_direct:" "$log_file" 2>/dev/null || true; } | \
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
echo "Extracted $entry_count I/O timing entries"

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
    "io_count": 0,
    "elapsed_sum": 0,
    "bandwidth_sum": 0,
    "elapsed_list": [],
    "bandwidth_list": []
})

for entry in entries:
    ts_str = entry["timestamp"]
    node = entry["node"]
    op = entry["op"]
    bytes_val = int(entry["bytes"])
    elapsed_us = float(entry["elapsed_us"])
    bandwidth = float(entry["bandwidth_mib_s"])

    # Parse timestamp
    try:
        ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        total_sec = ts.hour * 3600 + ts.minute * 60 + ts.second
        bucket = (total_sec // interval) * interval
    except:
        bucket = 0

    key = (bucket, node, op)
    aggregated[key]["total_bytes"] += bytes_val
    aggregated[key]["io_count"] += 1
    aggregated[key]["elapsed_sum"] += elapsed_us
    aggregated[key]["bandwidth_sum"] += bandwidth
    aggregated[key]["elapsed_list"].append(elapsed_us)
    aggregated[key]["bandwidth_list"].append(bandwidth)

# Write aggregated CSV
with open("$AGGREGATED_CSV", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow([
        "time_bucket", "node", "op", "total_bytes", "io_count",
        "avg_elapsed_us", "min_elapsed_us", "max_elapsed_us",
        "avg_bandwidth_mib_s", "min_bandwidth_mib_s", "max_bandwidth_mib_s",
        "throughput_gib_s"
    ])

    for (bucket, node, op), data in sorted(aggregated.items()):
        elapsed_list = data["elapsed_list"]
        bandwidth_list = data["bandwidth_list"]
        avg_elapsed = data["elapsed_sum"] / data["io_count"] if data["io_count"] > 0 else 0
        min_elapsed = min(elapsed_list) if elapsed_list else 0
        max_elapsed = max(elapsed_list) if elapsed_list else 0
        avg_bandwidth = data["bandwidth_sum"] / data["io_count"] if data["io_count"] > 0 else 0
        min_bandwidth = min(bandwidth_list) if bandwidth_list else 0
        max_bandwidth = max(bandwidth_list) if bandwidth_list else 0
        throughput_gib_s = data["total_bytes"] / (1024**3) / interval
        writer.writerow([
            bucket, node, op, data["total_bytes"], data["io_count"],
            f"{avg_elapsed:.2f}", f"{min_elapsed:.2f}", f"{max_elapsed:.2f}",
            f"{avg_bandwidth:.2f}", f"{min_bandwidth:.2f}", f"{max_bandwidth:.2f}",
            f"{throughput_gib_s:.4f}"
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
    f.write("I/O Timing Analysis Summary\n")
    f.write("===========================================\n\n")
    f.write(f"Source: $JOB_DIR\n")
    f.write(f"Total I/O operations: {len(entries)}\n\n")

    if not entries:
        f.write("No entries found.\n")
        exit(0)

    # Separate by operation type
    read_entries = [e for e in entries if e["op"] == "READ"]
    write_entries = [e for e in entries if e["op"] == "WRITE"]

    def analyze_entries(entries, op_name):
        if not entries:
            return f"No {op_name} entries found.\n"

        elapsed_list = [float(e["elapsed_us"]) for e in entries]
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
        result.append(f"    Min:    {min(elapsed_list):,.2f}\n")
        result.append(f"    Max:    {max(elapsed_list):,.2f}\n")

        # Percentiles
        sorted_elapsed = sorted(elapsed_list)
        n = len(sorted_elapsed)
        p50 = sorted_elapsed[int(n * 0.50)]
        p90 = sorted_elapsed[int(n * 0.90)]
        p95 = sorted_elapsed[int(n * 0.95)]
        p99 = sorted_elapsed[min(int(n * 0.99), n-1)]
        result.append(f"    P50:    {p50:,.2f}\n")
        result.append(f"    P90:    {p90:,.2f}\n")
        result.append(f"    P95:    {p95:,.2f}\n")
        result.append(f"    P99:    {p99:,.2f}\n")

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

    # Per-node statistics
    f.write("\n=== Per-Node Statistics ===\n")
    node_stats = defaultdict(lambda: {
        "read_count": 0, "write_count": 0,
        "read_bytes": 0, "write_bytes": 0,
        "read_bw_sum": 0, "write_bw_sum": 0
    })
    for e in entries:
        node = e["node"]
        op = e["op"]
        bytes_val = int(e["bytes"])
        bw = float(e["bandwidth_mib_s"])
        if op == "READ":
            node_stats[node]["read_count"] += 1
            node_stats[node]["read_bytes"] += bytes_val
            node_stats[node]["read_bw_sum"] += bw
        else:
            node_stats[node]["write_count"] += 1
            node_stats[node]["write_bytes"] += bytes_val
            node_stats[node]["write_bw_sum"] += bw

    f.write(f"{'Node':<15} {'WRITE Cnt':>10} {'READ Cnt':>10} {'WRITE GiB':>10} {'READ GiB':>10} {'W Avg MiB/s':>12} {'R Avg MiB/s':>12}\n")
    f.write("-" * 85 + "\n")
    for node in sorted(node_stats.keys()):
        stats = node_stats[node]
        w_avg_bw = stats['write_bw_sum'] / stats['write_count'] if stats['write_count'] > 0 else 0
        r_avg_bw = stats['read_bw_sum'] / stats['read_count'] if stats['read_count'] > 0 else 0
        f.write(f"{node:<15} {stats['write_count']:>10} {stats['read_count']:>10} "
                f"{stats['write_bytes']/(1024**3):>10.2f} {stats['read_bytes']/(1024**3):>10.2f} "
                f"{w_avg_bw:>12.2f} {r_avg_bw:>12.2f}\n")

    # Performance comparison
    if read_entries and write_entries:
        f.write("\n=== Performance Comparison ===\n")
        read_avg_bw = sum(float(e["bandwidth_mib_s"]) for e in read_entries) / len(read_entries)
        write_avg_bw = sum(float(e["bandwidth_mib_s"]) for e in write_entries) / len(write_entries)
        read_avg_elapsed = sum(float(e["elapsed_us"]) for e in read_entries) / len(read_entries)
        write_avg_elapsed = sum(float(e["elapsed_us"]) for e in write_entries) / len(write_entries)

        f.write(f"  WRITE avg bandwidth: {write_avg_bw:.2f} MiB/s\n")
        f.write(f"  READ avg bandwidth:  {read_avg_bw:.2f} MiB/s\n")
        f.write(f"  Ratio (WRITE/READ): {write_avg_bw/read_avg_bw:.2f}x\n")
        f.write(f"\n")
        f.write(f"  WRITE avg latency: {write_avg_elapsed:,.2f} us\n")
        f.write(f"  READ avg latency:  {read_avg_elapsed:,.2f} us\n")
        f.write(f"  Ratio (READ/WRITE): {read_avg_elapsed/write_avg_elapsed:.2f}x\n")

        if read_avg_bw < write_avg_bw * 0.5:
            f.write(f"\n*** WARNING: READ bandwidth is {write_avg_bw/read_avg_bw:.1f}x lower than WRITE ***\n")
            f.write("This suggests disk I/O is a bottleneck for READ operations.\n")

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
