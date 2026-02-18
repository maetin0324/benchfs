#!/bin/bash
# ==============================================================================
# extract_iostat_csv.sh - Extract iostat data from diagnostics logs
# ==============================================================================
# This script extracts iostat monitoring data and outputs as CSV for
# time-series analysis of disk I/O performance.
#
# Supports both:
# - iostat -xt output (with timestamps)
# - iostat -x output (without timestamps - uses sample index * interval)
#
# Usage:
#   ./extract_iostat_csv.sh <job_results_dir> [--aggregate <interval_sec>]
#
# Output:
#   - iostat_raw.csv: Raw per-sample data
#   - iostat_aggregated.csv: Aggregated by time interval and node (if --aggregate)
#   - iostat_summary.txt: Human-readable summary
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
    echo "  $0 /path/to/results/benchfs/2026.01.23-14.36.23-debug_large/... --aggregate 5"
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
DIAGNOSTICS_DIR="${JOB_DIR}/diagnostics"

# Output files (in job directory)
RAW_CSV="${JOB_DIR}/iostat_raw.csv"
AGGREGATED_CSV="${JOB_DIR}/iostat_aggregated.csv"
SUMMARY_TXT="${JOB_DIR}/iostat_summary.txt"

echo "==========================================="
echo "Extracting iostat data from diagnostics"
echo "==========================================="
echo "Job directory: $JOB_DIR"
echo "Diagnostics:   $DIAGNOSTICS_DIR"
echo ""

# Python script for parsing iostat output
PARSE_SCRIPT=$(cat << 'PYTHON_EOF'
#!/usr/bin/env python3
"""Parse iostat output (with or without timestamps) and generate CSV."""
import sys
import re
from datetime import datetime, timedelta

# Arguments: node_name, input_file
if len(sys.argv) < 3:
    print("Usage: script.py <node_name> <input_file>", file=sys.stderr)
    sys.exit(1)

node_name = sys.argv[1]
input_file = sys.argv[2]

# iostat interval (seconds) - typically 5 seconds
IOSTAT_INTERVAL = 5

# Pattern for explicit timestamp line (iostat -t)
# Format: 01/25/2026 04:55:05 PM or 2026年01月25日 01時53分17秒
timestamp_pattern1 = re.compile(r'(\d{2}/\d{2}/\d{4})\s+(\d{2}:\d{2}:\d{2})\s*(AM|PM)?')
timestamp_pattern2 = re.compile(r'(\d{4})年(\d{2})月(\d{2})日\s+(\d{2})時(\d{2})分(\d{2})秒')

# Pattern for header line to get date (iostat without -t)
# Linux 5.15.0-135-generic (bnode052) 	2026年01月25日 	_x86_64_	(48 CPU)
header_date_pattern = re.compile(r'(\d{4})年(\d{2})月(\d{2})日')

# Pattern for device line (nvme, md, sd devices - skip loop devices)
device_pattern = re.compile(r'^(nvme\d+n\d+|md\d+|sd[a-z]+)\s+')

# Pattern for "Device" header line (marks new sample block)
device_header_pattern = re.compile(r'^Device\s+')

current_timestamp = None
base_date = None
sample_index = 0
has_explicit_timestamps = False

def parse_explicit_timestamp(line):
    """Parse explicit timestamp from iostat -t output."""
    # Try format 1: MM/DD/YYYY HH:MM:SS AM/PM
    m = timestamp_pattern1.search(line)
    if m:
        date_str = m.group(1)
        time_str = m.group(2)
        ampm = m.group(3) or ''
        try:
            if ampm:
                dt = datetime.strptime(f"{date_str} {time_str} {ampm}", "%m/%d/%Y %I:%M:%S %p")
            else:
                dt = datetime.strptime(f"{date_str} {time_str}", "%m/%d/%Y %H:%M:%S")
            return dt
        except:
            pass

    # Try format 2: YYYY年MM月DD日 HH時MM分SS秒
    m = timestamp_pattern2.search(line)
    if m:
        try:
            year, month, day, hour, minute, sec = m.groups()
            dt = datetime(int(year), int(month), int(day), int(hour), int(minute), int(sec))
            return dt
        except:
            pass

    return None

def extract_base_date(line):
    """Extract base date from iostat header."""
    m = header_date_pattern.search(line)
    if m:
        try:
            year, month, day = m.groups()
            return datetime(int(year), int(month), int(day))
        except:
            pass
    return None

with open(input_file, 'r', errors='replace') as f:
    for line in f:
        line = line.strip()
        if not line:
            continue

        # Check for header line to extract base date
        if line.startswith('Linux') and base_date is None:
            base_date = extract_base_date(line)
            continue

        # Check for explicit timestamp (iostat -t output)
        ts = parse_explicit_timestamp(line)
        if ts:
            current_timestamp = ts
            has_explicit_timestamps = True
            continue

        # Check for "Device" header line (new sample block)
        if device_header_pattern.match(line):
            if not has_explicit_timestamps:
                # Without explicit timestamps, increment sample index
                sample_index += 1
                if base_date:
                    # Estimate timestamp based on sample index and interval
                    current_timestamp = base_date + timedelta(seconds=sample_index * IOSTAT_INTERVAL)
                else:
                    # Fallback: use sample index as epoch offset
                    current_timestamp = datetime(2026, 1, 1) + timedelta(seconds=sample_index * IOSTAT_INTERVAL)
            continue

        # Check for device line
        m = device_pattern.match(line)
        if m and current_timestamp:
            parts = line.split()
            if len(parts) >= 22:  # Full extended stats line
                device = parts[0]
                try:
                    # Parse iostat -x columns
                    # Device r/s rkB/s rrqm/s %rrqm r_await rareq-sz w/s wkB/s wrqm/s %wrqm w_await wareq-sz d/s dkB/s drqm/s %drqm d_await dareq-sz f/s f_await aqu-sz %util
                    r_s = float(parts[1])
                    rkb_s = float(parts[2])
                    r_await = float(parts[5])
                    w_s = float(parts[7])
                    wkb_s = float(parts[8])
                    w_await = float(parts[11])
                    aqu_sz = float(parts[21])
                    util = float(parts[22])

                    # Format timestamp
                    ts_str = current_timestamp.strftime("%Y-%m-%dT%H:%M:%S")

                    # Output CSV line
                    print(f"{ts_str},{node_name},{device},{sample_index},{r_s},{rkb_s},{r_await},{w_s},{wkb_s},{w_await},{aqu_sz},{util}")
                except (ValueError, IndexError) as e:
                    pass
PYTHON_EOF
)

# Initialize raw CSV with header
echo "timestamp,node,device,sample,r_s,rkb_s,r_await,w_s,wkb_s,w_await,aqu_sz,util" > "$RAW_CSV"

# Process iostat files from diagnostics
echo "Processing iostat files..."
file_count=0

if [ -d "$DIAGNOSTICS_DIR" ]; then
    for node_dir in "${DIAGNOSTICS_DIR}"/*/; do
        if [ -d "$node_dir" ]; then
            node_name=$(basename "$node_dir")
            for iostat_file in "${node_dir}"iostat_during_run*.txt; do
                if [ -f "$iostat_file" ]; then
                    echo "  Processing: $iostat_file"
                    python3 -c "$PARSE_SCRIPT" "$node_name" "$iostat_file" >> "$RAW_CSV"
                    ((file_count++)) || true
                fi
            done
        fi
    done
fi

echo ""
echo "Processed $file_count iostat files"

# Count entries
entry_count=$(tail -n +2 "$RAW_CSV" | wc -l)
echo "Extracted $entry_count iostat entries"

# Generate aggregated CSV if requested
if [ "$AGGREGATE_INTERVAL" -gt 0 ]; then
    echo ""
    echo "Aggregating by ${AGGREGATE_INTERVAL}s intervals..."

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

interval = $AGGREGATE_INTERVAL

# Aggregate by time bucket, node, and device
aggregated = defaultdict(lambda: {
    "r_s_sum": 0, "rkb_s_sum": 0, "r_await_sum": 0,
    "w_s_sum": 0, "wkb_s_sum": 0, "w_await_sum": 0,
    "aqu_sz_sum": 0, "util_sum": 0, "count": 0,
    "r_await_list": [], "w_await_list": [], "aqu_sz_list": []
})

for entry in entries:
    ts_str = entry["timestamp"]
    node = entry["node"]
    device = entry["device"]

    try:
        ts = datetime.fromisoformat(ts_str)
        total_sec = ts.hour * 3600 + ts.minute * 60 + ts.second
        bucket = (total_sec // interval) * interval
    except:
        bucket = 0

    key = (bucket, node, device)
    aggregated[key]["r_s_sum"] += float(entry["r_s"])
    aggregated[key]["rkb_s_sum"] += float(entry["rkb_s"])
    aggregated[key]["r_await_sum"] += float(entry["r_await"])
    aggregated[key]["w_s_sum"] += float(entry["w_s"])
    aggregated[key]["wkb_s_sum"] += float(entry["wkb_s"])
    aggregated[key]["w_await_sum"] += float(entry["w_await"])
    aggregated[key]["aqu_sz_sum"] += float(entry["aqu_sz"])
    aggregated[key]["util_sum"] += float(entry["util"])
    aggregated[key]["count"] += 1
    aggregated[key]["r_await_list"].append(float(entry["r_await"]))
    aggregated[key]["w_await_list"].append(float(entry["w_await"]))
    aggregated[key]["aqu_sz_list"].append(float(entry["aqu_sz"]))

# Write aggregated CSV
with open("$AGGREGATED_CSV", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow([
        "time_bucket", "node", "device",
        "avg_r_s", "avg_rkb_s", "avg_r_await", "max_r_await",
        "avg_w_s", "avg_wkb_s", "avg_w_await", "max_w_await",
        "avg_aqu_sz", "max_aqu_sz", "avg_util", "sample_count"
    ])

    for (bucket, node, device), data in sorted(aggregated.items()):
        count = data["count"]
        if count == 0:
            continue

        avg_r_s = data["r_s_sum"] / count
        avg_rkb_s = data["rkb_s_sum"] / count
        avg_r_await = data["r_await_sum"] / count
        max_r_await = max(data["r_await_list"]) if data["r_await_list"] else 0
        avg_w_s = data["w_s_sum"] / count
        avg_wkb_s = data["wkb_s_sum"] / count
        avg_w_await = data["w_await_sum"] / count
        max_w_await = max(data["w_await_list"]) if data["w_await_list"] else 0
        avg_aqu_sz = data["aqu_sz_sum"] / count
        max_aqu_sz = max(data["aqu_sz_list"]) if data["aqu_sz_list"] else 0
        avg_util = data["util_sum"] / count

        writer.writerow([
            bucket, node, device,
            f"{avg_r_s:.2f}", f"{avg_rkb_s:.2f}", f"{avg_r_await:.2f}", f"{max_r_await:.2f}",
            f"{avg_w_s:.2f}", f"{avg_wkb_s:.2f}", f"{avg_w_await:.2f}", f"{max_w_await:.2f}",
            f"{avg_aqu_sz:.2f}", f"{max_aqu_sz:.2f}", f"{avg_util:.2f}", count
        ])

print(f"Generated {len(aggregated)} aggregated entries")
AGGREGATE_EOF
fi

# Generate summary
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
    f.write("iostat Analysis Summary\n")
    f.write("===========================================\n\n")
    f.write(f"Source: $JOB_DIR\n")
    f.write(f"Total samples: {len(entries)}\n\n")

    if not entries:
        f.write("No entries found.\n")
        exit(0)

    # Get unique nodes and devices
    nodes = sorted(set(e["node"] for e in entries))
    devices = sorted(set(e["device"] for e in entries))

    f.write(f"Nodes: {len(nodes)}\n")
    f.write(f"Devices: {', '.join(devices)}\n\n")

    # Per-node, per-device statistics
    f.write("=== Per-Node Device Statistics ===\n\n")

    node_device_stats = defaultdict(lambda: {
        "r_await_list": [], "w_await_list": [],
        "r_s_list": [], "w_s_list": [],
        "rkb_s_list": [], "wkb_s_list": [],
        "aqu_sz_list": [], "util_list": []
    })

    for e in entries:
        key = (e["node"], e["device"])
        node_device_stats[key]["r_await_list"].append(float(e["r_await"]))
        node_device_stats[key]["w_await_list"].append(float(e["w_await"]))
        node_device_stats[key]["r_s_list"].append(float(e["r_s"]))
        node_device_stats[key]["w_s_list"].append(float(e["w_s"]))
        node_device_stats[key]["rkb_s_list"].append(float(e["rkb_s"]))
        node_device_stats[key]["wkb_s_list"].append(float(e["wkb_s"]))
        node_device_stats[key]["aqu_sz_list"].append(float(e["aqu_sz"]))
        node_device_stats[key]["util_list"].append(float(e["util"]))

    # Header
    f.write(f"{'Node':<12} {'Device':<10} {'r_await':>10} {'w_await':>10} {'aqu_sz':>10} {'util':>8} {'rkB/s':>10} {'wkB/s':>10}\n")
    f.write(f"{'':12} {'':10} {'(avg/max)':>10} {'(avg/max)':>10} {'(avg/max)':>10} {'(avg)':>8} {'(avg)':>10} {'(avg)':>10}\n")
    f.write("-" * 90 + "\n")

    for (node, device) in sorted(node_device_stats.keys()):
        stats = node_device_stats[(node, device)]
        r_await_avg = sum(stats["r_await_list"]) / len(stats["r_await_list"]) if stats["r_await_list"] else 0
        r_await_max = max(stats["r_await_list"]) if stats["r_await_list"] else 0
        w_await_avg = sum(stats["w_await_list"]) / len(stats["w_await_list"]) if stats["w_await_list"] else 0
        w_await_max = max(stats["w_await_list"]) if stats["w_await_list"] else 0
        aqu_sz_avg = sum(stats["aqu_sz_list"]) / len(stats["aqu_sz_list"]) if stats["aqu_sz_list"] else 0
        aqu_sz_max = max(stats["aqu_sz_list"]) if stats["aqu_sz_list"] else 0
        util_avg = sum(stats["util_list"]) / len(stats["util_list"]) if stats["util_list"] else 0
        rkb_s_avg = sum(stats["rkb_s_list"]) / len(stats["rkb_s_list"]) if stats["rkb_s_list"] else 0
        wkb_s_avg = sum(stats["wkb_s_list"]) / len(stats["wkb_s_list"]) if stats["wkb_s_list"] else 0

        f.write(f"{node:<12} {device:<10} {r_await_avg:>5.1f}/{r_await_max:<4.0f} {w_await_avg:>5.1f}/{w_await_max:<4.0f} ")
        f.write(f"{aqu_sz_avg:>5.1f}/{aqu_sz_max:<4.0f} {util_avg:>8.1f} {rkb_s_avg:>10.0f} {wkb_s_avg:>10.0f}\n")

    # Identify slow nodes (high r_await or w_await)
    f.write("\n=== Slow Nodes Analysis ===\n")

    # Find nodes with high r_await (> 10ms average)
    slow_nodes = []
    for (node, device) in sorted(node_device_stats.keys()):
        if device.startswith("loop"):
            continue
        stats = node_device_stats[(node, device)]
        r_await_avg = sum(stats["r_await_list"]) / len(stats["r_await_list"]) if stats["r_await_list"] else 0
        r_await_max = max(stats["r_await_list"]) if stats["r_await_list"] else 0
        if r_await_avg > 10 or r_await_max > 100:
            slow_nodes.append((node, device, r_await_avg, r_await_max))

    if slow_nodes:
        f.write("\nNodes with high read latency (r_await > 10ms avg or > 100ms max):\n")
        for node, device, avg, max_val in sorted(slow_nodes, key=lambda x: x[3], reverse=True):
            f.write(f"  {node}/{device}: avg={avg:.1f}ms, max={max_val:.1f}ms\n")
    else:
        f.write("\nNo nodes with abnormally high read latency detected.\n")

    # Queue depth analysis
    f.write("\n=== Queue Depth Analysis ===\n")
    high_queue_nodes = []
    for (node, device) in sorted(node_device_stats.keys()):
        if device.startswith("loop"):
            continue
        stats = node_device_stats[(node, device)]
        aqu_sz_max = max(stats["aqu_sz_list"]) if stats["aqu_sz_list"] else 0
        aqu_sz_avg = sum(stats["aqu_sz_list"]) / len(stats["aqu_sz_list"]) if stats["aqu_sz_list"] else 0
        if aqu_sz_max > 50:
            high_queue_nodes.append((node, device, aqu_sz_avg, aqu_sz_max))

    if high_queue_nodes:
        f.write("\nNodes with high queue depth (aqu-sz > 50):\n")
        for node, device, avg, max_val in sorted(high_queue_nodes, key=lambda x: x[3], reverse=True):
            f.write(f"  {node}/{device}: avg={avg:.1f}, max={max_val:.1f}\n")
    else:
        f.write("\nNo nodes with high queue depth detected.\n")

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
