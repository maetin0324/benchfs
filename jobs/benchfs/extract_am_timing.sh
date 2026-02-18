#!/bin/bash
# Extract AM (Active Message) timing data from server/client logs
# This script parses AM_SEND_TIMING and AM_WAIT_MSG_TIMING log entries

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULT_DIR="${1:-.}"

echo "Extracting AM timing data from: $RESULT_DIR"

# Create output directory
OUTPUT_DIR="$RESULT_DIR"
mkdir -p "$OUTPUT_DIR"

# Find only server and client log files (not all .log files)
SERVER_LOGS=$(find "$RESULT_DIR" -maxdepth 2 \( -name "server_*.log" -o -name "benchfs_server*.log" \) 2>/dev/null || true)
CLIENT_LOGS=$(find "$RESULT_DIR" -maxdepth 2 \( -name "client_*.log" -o -name "benchfs_client*.log" -o -name "ior_*.log" \) 2>/dev/null || true)

ALL_LOGS="$SERVER_LOGS $CLIENT_LOGS"
ALL_LOGS=$(echo "$ALL_LOGS" | tr ' ' '\n' | grep -v '^$' | sort -u)

LOG_COUNT=$(echo "$ALL_LOGS" | grep -c '.' || echo 0)
echo "Found $LOG_COUNT server/client log files"

if [ "$LOG_COUNT" -eq 0 ]; then
    echo "No server/client log files found in $RESULT_DIR"
    echo "No AM timing data to extract"
    exit 0
fi

# Use grep to pre-filter and extract only relevant lines (much faster)
echo "Extracting AM_SEND_TIMING and AM_WAIT_MSG_TIMING entries..."

AM_SEND_TMP=$(mktemp)
AM_WAIT_TMP=$(mktemp)

trap "rm -f $AM_SEND_TMP $AM_WAIT_TMP" EXIT

# Extract matching lines with filename prefix
for logfile in $ALL_LOGS; do
    if [ -f "$logfile" ]; then
        grep "AM_SEND_TIMING" "$logfile" 2>/dev/null >> "$AM_SEND_TMP" || true
        grep "AM_WAIT_MSG_TIMING" "$logfile" 2>/dev/null >> "$AM_WAIT_TMP" || true
    fi
done

AM_SEND_COUNT=$(wc -l < "$AM_SEND_TMP" | tr -d ' ')
AM_WAIT_COUNT=$(wc -l < "$AM_WAIT_TMP" | tr -d ' ')

echo "Found $AM_SEND_COUNT AM_SEND_TIMING entries"
echo "Found $AM_WAIT_COUNT AM_WAIT_MSG_TIMING entries"

if [ "$AM_SEND_COUNT" -eq 0 ] && [ "$AM_WAIT_COUNT" -eq 0 ]; then
    echo "No AM timing entries found"
    exit 0
fi

# Process with Python
export AM_SEND_TMP AM_WAIT_TMP OUTPUT_DIR

python3 << 'EOF'
import os
import re
import csv
from collections import defaultdict

am_send_tmp = os.environ['AM_SEND_TMP']
am_wait_tmp = os.environ['AM_WAIT_TMP']
output_dir = os.environ['OUTPUT_DIR']

# Patterns
ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
node_pattern = re.compile(r'^\[([a-zA-Z]+\d+)\]')
timestamp_pattern = re.compile(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+)')
kv_pattern = re.compile(r'(\w+)=(?:"([^"]+)"|(\S+))')

am_send_records = []
am_wait_records = []

# Parse AM_SEND_TIMING
with open(am_send_tmp, 'r', errors='ignore') as f:
    for line in f:
        line = ansi_escape.sub('', line)

        node_match = node_pattern.match(line)
        node = node_match.group(1) if node_match else 'unknown'

        ts_match = timestamp_pattern.search(line)
        timestamp = ts_match.group(1) if ts_match else ''

        # Determine log_type from content
        log_type = 'server'  # AM_SEND on server is reply send

        kv_matches = kv_pattern.findall(line)
        record = {
            'timestamp': timestamp,
            'node': node,
            'log_type': log_type,
            'op': '',
            'am_id': 0,
            'header_bytes': 0,
            'data_bytes': 0,
            'iov_count': 0,
            'need_reply': '',
            'activate_us': 0,
            'send_us': 0,
            'total_us': 0,
        }
        for key, quoted_val, unquoted_val in kv_matches:
            val = quoted_val if quoted_val else unquoted_val
            if key in record:
                if key in ['am_id', 'header_bytes', 'data_bytes', 'iov_count', 'activate_us', 'send_us', 'total_us']:
                    try:
                        record[key] = int(val)
                    except:
                        pass
                else:
                    record[key] = val
        am_send_records.append(record)

# Parse AM_WAIT_MSG_TIMING
with open(am_wait_tmp, 'r', errors='ignore') as f:
    for line in f:
        line = ansi_escape.sub('', line)

        node_match = node_pattern.match(line)
        node = node_match.group(1) if node_match else 'unknown'

        ts_match = timestamp_pattern.search(line)
        timestamp = ts_match.group(1) if ts_match else ''

        log_type = 'server'  # AM_WAIT on server is request receive

        kv_matches = kv_pattern.findall(line)
        record = {
            'timestamp': timestamp,
            'node': node,
            'log_type': log_type,
            'stream_id': 0,
            'has_msg': '',
            'wait_connect_us': 0,
            'wait_us': 0,
            'total_us': 0,
        }
        for key, quoted_val, unquoted_val in kv_matches:
            val = quoted_val if quoted_val else unquoted_val
            if key in record:
                if key in ['stream_id', 'wait_connect_us', 'wait_us', 'total_us']:
                    try:
                        record[key] = int(val)
                    except:
                        pass
                else:
                    record[key] = val
        am_wait_records.append(record)

print(f"Parsed {len(am_send_records)} AM_SEND_TIMING records")
print(f"Parsed {len(am_wait_records)} AM_WAIT_MSG_TIMING records")

# Write AM_SEND raw CSV
if am_send_records:
    am_send_csv = os.path.join(output_dir, 'am_send_timing_raw.csv')
    with open(am_send_csv, 'w', newline='') as f:
        fieldnames = ['timestamp', 'node', 'log_type', 'op', 'am_id', 'header_bytes', 'data_bytes', 'iov_count', 'need_reply', 'activate_us', 'send_us', 'total_us']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in sorted(am_send_records, key=lambda x: x['timestamp']):
            writer.writerow(r)
    print(f"Written: {am_send_csv}")

# Write AM_WAIT raw CSV
if am_wait_records:
    am_wait_csv = os.path.join(output_dir, 'am_wait_timing_raw.csv')
    with open(am_wait_csv, 'w', newline='') as f:
        fieldnames = ['timestamp', 'node', 'log_type', 'stream_id', 'has_msg', 'wait_connect_us', 'wait_us', 'total_us']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in sorted(am_wait_records, key=lambda x: x['timestamp']):
            writer.writerow(r)
    print(f"Written: {am_wait_csv}")

# Generate summary
summary_file = os.path.join(output_dir, 'am_timing_summary.txt')
with open(summary_file, 'w') as f:
    f.write("=" * 60 + "\n")
    f.write("AM (Active Message) Timing Summary\n")
    f.write("=" * 60 + "\n\n")

    if am_send_records:
        f.write("AM_SEND_TIMING Statistics:\n")
        f.write("-" * 40 + "\n")

        # Group by node
        by_node = defaultdict(list)
        for r in am_send_records:
            by_node[r['node']].append(r)

        for node in sorted(by_node.keys()):
            records = by_node[node]
            total_vals = [r['total_us'] for r in records]
            send_vals = [r['send_us'] for r in records]

            n = len(records)
            avg_total = sum(total_vals) / n if n > 0 else 0
            avg_send = sum(send_vals) / n if n > 0 else 0

            f.write(f"\n[{node}]:\n")
            f.write(f"  Count: {n}\n")
            f.write(f"  Avg total_us: {avg_total:.0f}\n")
            f.write(f"  Avg send_us: {avg_send:.0f}\n")
            if total_vals:
                f.write(f"  Min/Max total_us: {min(total_vals)} / {max(total_vals)}\n")

        # Overall
        f.write("\n" + "-" * 40 + "\n")
        f.write("Overall AM_SEND:\n")
        total_vals = [r['total_us'] for r in am_send_records]
        n = len(total_vals)
        if n > 0:
            avg = sum(total_vals) / n
            f.write(f"  Count: {n}\n")
            f.write(f"  Avg total_us: {avg:.0f}\n")
            f.write(f"  Min/Max: {min(total_vals)} / {max(total_vals)}\n")

    if am_wait_records:
        f.write("\n\nAM_WAIT_MSG_TIMING Statistics:\n")
        f.write("-" * 40 + "\n")

        # Group by node
        by_node = defaultdict(list)
        for r in am_wait_records:
            by_node[r['node']].append(r)

        for node in sorted(by_node.keys()):
            records = by_node[node]
            total_vals = [r['total_us'] for r in records]
            wait_vals = [r['wait_us'] for r in records]

            n = len(records)
            avg_total = sum(total_vals) / n if n > 0 else 0
            avg_wait = sum(wait_vals) / n if n > 0 else 0

            f.write(f"\n[{node}]:\n")
            f.write(f"  Count: {n}\n")
            f.write(f"  Avg total_us: {avg_total:.0f}\n")
            f.write(f"  Avg wait_us: {avg_wait:.0f}\n")
            if total_vals:
                f.write(f"  Min/Max total_us: {min(total_vals)} / {max(total_vals)}\n")

        # Overall
        f.write("\n" + "-" * 40 + "\n")
        f.write("Overall AM_WAIT:\n")
        total_vals = [r['total_us'] for r in am_wait_records]
        n = len(total_vals)
        if n > 0:
            avg = sum(total_vals) / n
            f.write(f"  Count: {n}\n")
            f.write(f"  Avg total_us: {avg:.0f}\n")
            f.write(f"  Min/Max: {min(total_vals)} / {max(total_vals)}\n")

print(f"Written: {summary_file}")

# Print summary to stdout
with open(summary_file, 'r') as f:
    print(f.read())
EOF

echo "AM timing extraction complete"
