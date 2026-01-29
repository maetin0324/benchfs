#!/bin/bash
# ==============================================================================
# extract_server_rpc_timing.sh - Extract server-side RPC timing logs to CSV
# ==============================================================================
# This script parses BenchFS server logs to extract detailed RPC timing information
# for READ and WRITE operations, enabling performance analysis.
#
# Usage:
#   ./extract_server_rpc_timing.sh <job_results_dir> [--aggregate <interval_sec>] [--no-aggregate]
#
# Output files (in job_results_dir):
#   - server_rpc_timing_raw.csv: Raw timing data per RPC call
#   - server_rpc_timing_aggregated.csv: Aggregated timing data
#   - server_rpc_timing_summary.txt: Summary statistics
# ==============================================================================

set -euo pipefail

# Parse arguments
if [ $# -lt 1 ]; then
    echo "Usage: $0 <job_results_dir> [--aggregate <interval_sec>] [--no-aggregate]"
    exit 1
fi

JOB_DIR="$1"
shift

# Default: aggregate by 1 second
AGGREGATE_INTERVAL=1
DO_AGGREGATE=true

while [ $# -gt 0 ]; do
    case "$1" in
        --aggregate)
            AGGREGATE_INTERVAL="$2"
            DO_AGGREGATE=true
            shift 2
            ;;
        --no-aggregate)
            DO_AGGREGATE=false
            shift
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

# Output files
RAW_CSV="$JOB_DIR/server_rpc_timing_raw.csv"
AGGREGATED_CSV="$JOB_DIR/server_rpc_timing_aggregated.csv"
SUMMARY_FILE="$JOB_DIR/server_rpc_timing_summary.txt"

echo "Extracting server RPC timing logs from: $JOB_DIR"

python3 << 'PYTHON_SCRIPT' - "$JOB_DIR" "$RAW_CSV" "$AGGREGATED_CSV" "$SUMMARY_FILE" "$AGGREGATE_INTERVAL" "$DO_AGGREGATE"
import sys
import os
import re
import glob
from collections import defaultdict
from datetime import datetime

job_dir = sys.argv[1]
raw_csv = sys.argv[2]
aggregated_csv = sys.argv[3]
summary_file = sys.argv[4]
aggregate_interval = int(sys.argv[5])
do_aggregate = sys.argv[6].lower() == 'true'

# Find server log files
log_patterns = [
    os.path.join(job_dir, "**/benchfsd_stdout.log"),
    os.path.join(job_dir, "**/benchfsd_stderr.log"),
    os.path.join(job_dir, "**/benchfs_server.log"),
    os.path.join(job_dir, "**/server*.log"),
]

log_files = []
for pattern in log_patterns:
    log_files.extend(glob.glob(pattern, recursive=True))

# Deduplicate and filter
log_files = list(set(log_files))
log_files = [f for f in log_files if os.path.isfile(f)]

if not log_files:
    print(f"WARNING: No log files found in {job_dir}")
    sys.exit(0)

print(f"Found {len(log_files)} log files")

# ANSI escape code stripper
ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')

def strip_ansi(text):
    return ansi_escape.sub('', text)

# Regex to extract node name from line start: [bnode077] or [anode001]
node_pattern = re.compile(r'^\[([a-zA-Z]+\d+)\]')

# Pattern to extract key-value pairs (handles both quoted and unquoted values)
kv_pattern = re.compile(r'(\w+)=(?:"([^"]+)"|(\d+))')

# Data storage
rpc_data = []
chunk_io_data = []

for log_file in log_files:
    print(f"  Processing: {log_file}")

    try:
        with open(log_file, 'r', errors='replace') as f:
            for line in f:
                # Strip ANSI codes
                clean_line = strip_ansi(line)

                # Extract node from line start
                node_match = node_pattern.match(clean_line)
                node = node_match.group(1) if node_match else 'unknown'

                # Check for RPC_TIMING logs
                if 'RPC_TIMING_READ' in clean_line or 'RPC_TIMING_WRITE' in clean_line:
                    rpc_type = 'READ' if 'RPC_TIMING_READ' in clean_line else 'WRITE'

                    # Extract timestamp
                    ts_match = re.search(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+)Z?', clean_line)
                    if not ts_match:
                        continue

                    timestamp_str = ts_match.group(1)

                    # Extract all key-value pairs
                    kvs = {}
                    for match in kv_pattern.finditer(clean_line):
                        key = match.group(1)
                        value = match.group(2) if match.group(2) else match.group(3)
                        kvs[key] = value

                    try:
                        # Parse timestamp
                        ts = timestamp_str
                        if '.' in ts:
                            ts_parts = ts.split('.')
                            ts = ts_parts[0] + '.' + ts_parts[1][:6]

                        try:
                            timestamp = datetime.strptime(ts, '%Y-%m-%dT%H:%M:%S.%f')
                        except ValueError:
                            timestamp = datetime.strptime(ts, '%Y-%m-%dT%H:%M:%S')

                        entry = {
                            'timestamp': timestamp,
                            'timestamp_str': timestamp_str,
                            'node': node,
                            'rpc_type': rpc_type,
                            'chunk_id': int(kvs.get('chunk_id', 0)),
                            'total_us': int(kvs.get('total_us', 0)),
                            'parse_us': int(kvs.get('parse_us', 0)),
                            'buffer_acquire_us': int(kvs.get('buffer_acquire_us', 0)),
                            'io_read_us': int(kvs.get('io_read_us', 0)),
                            'recv_data_us': int(kvs.get('recv_data_us', 0)),
                            'io_write_us': int(kvs.get('io_write_us', 0)),
                            'response_construct_us': int(kvs.get('response_construct_us', 0)),
                            'reply_us': int(kvs.get('reply_us', 0)),
                            'bytes': int(kvs.get('bytes', 0)),
                        }
                        rpc_data.append(entry)
                    except (ValueError, KeyError) as e:
                        pass

                # Check for CHUNK_IO_TIMING logs
                elif 'CHUNK_IO_TIMING' in clean_line:
                    # Extract timestamp
                    ts_match = re.search(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+)Z?', clean_line)
                    if not ts_match:
                        continue

                    timestamp_str = ts_match.group(1)

                    # Extract key-value pairs
                    kvs = {}
                    for match in kv_pattern.finditer(clean_line):
                        key = match.group(1)
                        value = match.group(2) if match.group(2) else match.group(3)
                        kvs[key] = value

                    op_type = kvs.get('op_type', 'UNKNOWN')

                    try:
                        ts = timestamp_str
                        if '.' in ts:
                            ts_parts = ts.split('.')
                            ts = ts_parts[0] + '.' + ts_parts[1][:6]

                        try:
                            timestamp = datetime.strptime(ts, '%Y-%m-%dT%H:%M:%S.%f')
                        except ValueError:
                            timestamp = datetime.strptime(ts, '%Y-%m-%dT%H:%M:%S')

                        entry = {
                            'timestamp': timestamp,
                            'timestamp_str': timestamp_str,
                            'node': node,
                            'op_type': op_type,
                            'chunk_index': int(kvs.get('chunk_index', 0)),
                            'bytes': int(kvs.get('bytes_read', kvs.get('bytes_written', 0))),
                            'open_us': int(kvs.get('open_us', 0)),
                            'read_us': int(kvs.get('read_us', 0)),
                            'write_us': int(kvs.get('write_us', 0)),
                            'close_us': int(kvs.get('close_us', 0)),
                            'total_us': int(kvs.get('total_us', 0)),
                        }
                        chunk_io_data.append(entry)
                    except (ValueError, KeyError):
                        pass

    except Exception as e:
        print(f"  WARNING: Error reading {log_file}: {e}")

print(f"\nExtracted {len(rpc_data)} RPC timing entries")
print(f"Extracted {len(chunk_io_data)} chunk IO timing entries")

if not rpc_data and not chunk_io_data:
    print("No timing data found in logs. Make sure RUST_LOG includes 'debug' level.")
    # Create empty files
    with open(raw_csv, 'w') as f:
        f.write("timestamp,node,rpc_type,chunk_id,total_us,parse_us,buffer_acquire_us,io_read_us,recv_data_us,io_write_us,response_construct_us,reply_us,bytes\n")
    with open(aggregated_csv, 'w') as f:
        f.write("time_bucket,node,rpc_type,count,avg_total_us,avg_io_us,avg_buffer_acquire_us,avg_reply_us,p50_total_us,p95_total_us,p99_total_us,total_bytes,throughput_mib_s\n")
    with open(summary_file, 'w') as f:
        f.write("No timing data found in logs.\n")
    sys.exit(0)

# Sort by timestamp
rpc_data.sort(key=lambda x: x['timestamp'])
chunk_io_data.sort(key=lambda x: x['timestamp'])

# Write raw RPC CSV
print(f"\nWriting raw RPC timing CSV: {raw_csv}")
with open(raw_csv, 'w') as f:
    f.write("timestamp,node,rpc_type,chunk_id,total_us,parse_us,buffer_acquire_us,io_read_us,recv_data_us,io_write_us,response_construct_us,reply_us,bytes\n")
    for entry in rpc_data:
        f.write(f"{entry['timestamp_str']},{entry['node']},{entry['rpc_type']},{entry['chunk_id']},"
                f"{entry['total_us']},{entry['parse_us']},{entry['buffer_acquire_us']},"
                f"{entry['io_read_us']},{entry['recv_data_us']},{entry['io_write_us']},"
                f"{entry['response_construct_us']},{entry['reply_us']},{entry['bytes']}\n")

# Also write chunk IO timing if available
if chunk_io_data:
    chunk_io_csv = os.path.join(os.path.dirname(raw_csv), "chunk_io_timing_raw.csv")
    print(f"Writing chunk IO timing CSV: {chunk_io_csv}")
    with open(chunk_io_csv, 'w') as f:
        f.write("timestamp,node,op_type,chunk_index,bytes,open_us,read_us,write_us,close_us,total_us\n")
        for entry in chunk_io_data:
            f.write(f"{entry['timestamp_str']},{entry['node']},{entry['op_type']},{entry['chunk_index']},"
                    f"{entry['bytes']},{entry['open_us']},{entry['read_us']},{entry['write_us']},"
                    f"{entry['close_us']},{entry['total_us']}\n")

# Aggregation
if do_aggregate and rpc_data:
    print(f"\nAggregating by {aggregate_interval}s intervals...")

    # Get base timestamp
    base_ts = rpc_data[0]['timestamp']

    # Group by time bucket, node, and rpc_type
    buckets = defaultdict(list)
    for entry in rpc_data:
        delta = (entry['timestamp'] - base_ts).total_seconds()
        bucket = int(delta // aggregate_interval) * aggregate_interval
        key = (bucket, entry['node'], entry['rpc_type'])
        buckets[key].append(entry)

    # Write aggregated CSV
    print(f"Writing aggregated RPC timing CSV: {aggregated_csv}")
    with open(aggregated_csv, 'w') as f:
        f.write("time_bucket,node,rpc_type,count,avg_total_us,avg_io_us,avg_buffer_acquire_us,avg_reply_us,"
                "p50_total_us,p95_total_us,p99_total_us,total_bytes,throughput_mib_s\n")

        for (bucket, node, rpc_type), entries in sorted(buckets.items()):
            count = len(entries)
            if count == 0:
                continue

            total_us_list = [e['total_us'] for e in entries]
            io_us_list = [e['io_read_us'] if e['rpc_type'] == 'READ' else e['io_write_us'] for e in entries]
            buffer_acquire_list = [e['buffer_acquire_us'] for e in entries]
            reply_list = [e['reply_us'] for e in entries]
            bytes_list = [e['bytes'] for e in entries]

            avg_total = sum(total_us_list) / count
            avg_io = sum(io_us_list) / count
            avg_buffer = sum(buffer_acquire_list) / count
            avg_reply = sum(reply_list) / count

            total_us_sorted = sorted(total_us_list)
            p50_idx = min(int(count * 0.5), count - 1)
            p95_idx = min(int(count * 0.95), count - 1)
            p99_idx = min(int(count * 0.99), count - 1)
            p50 = total_us_sorted[p50_idx]
            p95 = total_us_sorted[p95_idx]
            p99 = total_us_sorted[p99_idx]

            total_bytes = sum(bytes_list)
            throughput_mib_s = (total_bytes / (1024 * 1024)) / aggregate_interval if aggregate_interval > 0 else 0

            f.write(f"{bucket},{node},{rpc_type},{count},{avg_total:.1f},{avg_io:.1f},{avg_buffer:.1f},"
                    f"{avg_reply:.1f},{p50},{p95},{p99},{total_bytes},{throughput_mib_s:.2f}\n")

# Write summary
print(f"\nWriting summary: {summary_file}")
with open(summary_file, 'w') as f:
    f.write("=" * 60 + "\n")
    f.write("Server RPC Timing Summary\n")
    f.write("=" * 60 + "\n\n")

    # Separate READ and WRITE
    read_entries = [e for e in rpc_data if e['rpc_type'] == 'READ']
    write_entries = [e for e in rpc_data if e['rpc_type'] == 'WRITE']

    f.write(f"Total RPC entries: {len(rpc_data)}\n")
    f.write(f"  READ: {len(read_entries)}\n")
    f.write(f"  WRITE: {len(write_entries)}\n\n")

    for rpc_type, entries in [('READ', read_entries), ('WRITE', write_entries)]:
        if not entries:
            continue

        f.write(f"\n{'=' * 40}\n")
        f.write(f"{rpc_type} Operation Statistics\n")
        f.write(f"{'=' * 40}\n\n")

        # Calculate statistics
        total_us_list = [e['total_us'] for e in entries]
        io_us_list = [e['io_read_us'] if rpc_type == 'READ' else e['io_write_us'] for e in entries]
        buffer_list = [e['buffer_acquire_us'] for e in entries]
        reply_list = [e['reply_us'] for e in entries]

        def stats(name, data):
            if not data:
                f.write(f"{name}: No data\n\n")
                return
            data_sorted = sorted(data)
            n = len(data)
            avg = sum(data) / n
            p50_idx = min(int(n * 0.5), n - 1)
            p95_idx = min(int(n * 0.95), n - 1)
            p99_idx = min(int(n * 0.99), n - 1)
            p50 = data_sorted[p50_idx]
            p95 = data_sorted[p95_idx]
            p99 = data_sorted[p99_idx]
            max_val = max(data)
            min_val = min(data)

            f.write(f"{name}:\n")
            f.write(f"  avg: {avg:.1f} us\n")
            f.write(f"  p50: {p50} us\n")
            f.write(f"  p95: {p95} us\n")
            f.write(f"  p99: {p99} us\n")
            f.write(f"  min: {min_val} us\n")
            f.write(f"  max: {max_val} us\n\n")

        stats("Total latency", total_us_list)
        stats("IO operation", io_us_list)
        stats("Buffer acquisition", buffer_list)
        stats("Reply send", reply_list)

        # Per-node breakdown
        f.write("Per-node breakdown:\n")
        node_data = defaultdict(list)
        for e in entries:
            node_data[e['node']].append(e['total_us'])

        for node in sorted(node_data.keys()):
            data = node_data[node]
            if not data:
                continue
            avg = sum(data) / len(data)
            data_sorted = sorted(data)
            p99_idx = min(int(len(data) * 0.99), len(data) - 1)
            p99 = data_sorted[p99_idx]
            f.write(f"  {node}: count={len(data)}, avg={avg:.1f}us, p99={p99}us\n")

    # Compare READ vs WRITE
    if read_entries and write_entries:
        f.write(f"\n{'=' * 40}\n")
        f.write("READ vs WRITE Comparison\n")
        f.write(f"{'=' * 40}\n\n")

        read_avg = sum(e['total_us'] for e in read_entries) / len(read_entries)
        write_avg = sum(e['total_us'] for e in write_entries) / len(write_entries)

        read_io_avg = sum(e['io_read_us'] for e in read_entries) / len(read_entries)
        write_io_avg = sum(e['io_write_us'] for e in write_entries) / len(write_entries)

        f.write(f"Total latency:\n")
        f.write(f"  READ avg:  {read_avg:.1f} us\n")
        f.write(f"  WRITE avg: {write_avg:.1f} us\n")
        if write_avg > 0:
            f.write(f"  Ratio (READ/WRITE): {read_avg/write_avg:.2f}x\n\n")
        else:
            f.write(f"  Ratio (READ/WRITE): N/A (WRITE avg is 0)\n\n")

        f.write(f"IO operation latency:\n")
        f.write(f"  READ avg:  {read_io_avg:.1f} us\n")
        f.write(f"  WRITE avg: {write_io_avg:.1f} us\n")
        if write_io_avg > 0:
            f.write(f"  Ratio (READ/WRITE): {read_io_avg/write_io_avg:.2f}x\n\n")
        else:
            f.write(f"  Ratio (READ/WRITE): N/A (WRITE IO avg is 0)\n\n")

    # Chunk IO timing summary if available
    if chunk_io_data:
        f.write(f"\n{'=' * 40}\n")
        f.write("Chunk IO Timing (open/read/write/close breakdown)\n")
        f.write(f"{'=' * 40}\n\n")

        read_io = [e for e in chunk_io_data if 'READ' in e['op_type']]
        write_io = [e for e in chunk_io_data if 'WRITE' in e['op_type']]

        for op_type, entries in [('READ_CHUNK_FIXED', read_io), ('WRITE_CHUNK_FIXED', write_io)]:
            if not entries:
                continue

            f.write(f"\n{op_type} (n={len(entries)}):\n")
            open_avg = sum(e['open_us'] for e in entries) / len(entries)
            io_key = 'read_us' if 'READ' in op_type else 'write_us'
            io_avg = sum(e.get(io_key, 0) for e in entries) / len(entries)
            close_avg = sum(e['close_us'] for e in entries) / len(entries)
            total_avg = sum(e['total_us'] for e in entries) / len(entries)

            if total_avg > 0:
                f.write(f"  open:  {open_avg:.1f} us ({100*open_avg/total_avg:.1f}%)\n")
                f.write(f"  io:    {io_avg:.1f} us ({100*io_avg/total_avg:.1f}%)\n")
                f.write(f"  close: {close_avg:.1f} us ({100*close_avg/total_avg:.1f}%)\n")
                f.write(f"  total: {total_avg:.1f} us\n")
            else:
                f.write(f"  open:  {open_avg:.1f} us\n")
                f.write(f"  io:    {io_avg:.1f} us\n")
                f.write(f"  close: {close_avg:.1f} us\n")
                f.write(f"  total: {total_avg:.1f} us\n")

print("\nExtraction complete!")
PYTHON_SCRIPT

echo ""
echo "Generated files:"
if [ -f "$RAW_CSV" ]; then
    echo "  - $RAW_CSV ($(wc -l < "$RAW_CSV") lines)"
fi
if [ -f "$AGGREGATED_CSV" ]; then
    echo "  - $AGGREGATED_CSV ($(wc -l < "$AGGREGATED_CSV") lines)"
fi
if [ -f "$SUMMARY_FILE" ]; then
    echo "  - $SUMMARY_FILE"
fi
if [ -f "$JOB_DIR/chunk_io_timing_raw.csv" ]; then
    echo "  - $JOB_DIR/chunk_io_timing_raw.csv ($(wc -l < "$JOB_DIR/chunk_io_timing_raw.csv") lines)"
fi
