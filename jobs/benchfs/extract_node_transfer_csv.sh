#!/bin/bash
# ==============================================================================
# extract_node_transfer_csv.sh - Extract per-node transfer data from logs
# ==============================================================================
# This script extracts NODE_TRANSFER log entries from benchmark logs and
# outputs them as CSV for time-series analysis of load distribution.
#
# Log format expected (from tracing):
#   2026-01-22T10:00:00.123456 INFO node_transfer: NODE_TRANSFER op=WRITE target_node=bnode115 bytes=1048576 chunk_index=0 is_local=true
#
# Usage:
#   ./extract_node_transfer_csv.sh <job_results_dir> [--aggregate <interval_sec>]
#
# Directory structure expected:
#   <job_results_dir>/
#     ior_results/         <- IOR output logs
#     benchfsd_logs/       <- BenchFS daemon logs
#     diagnostics/         <- Collected diagnostics
#
# Output:
#   - node_transfer_raw.csv: Raw per-operation data
#   - node_transfer_aggregated.csv: Aggregated by time interval and node (if --aggregate)
#   - node_transfer_summary.txt: Human-readable summary
# ==============================================================================

set -euo pipefail

# Parse arguments
if [ $# -lt 1 ]; then
    echo "Usage: $0 <job_results_dir> [--aggregate <interval_sec>]"
    echo ""
    echo "Options:"
    echo "  --aggregate <sec>  Aggregate transfers per time interval (default: no aggregation)"
    echo ""
    echo "Example:"
    echo "  $0 /path/to/results/benchfs/2026.01.20-22.32.01-debug_large/2026.01.20-22.32.58-531604.nqsv-16 --aggregate 1"
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
IOR_DIR="${JOB_DIR}/ior_results"
BENCHFSD_DIR="${JOB_DIR}/benchfsd_logs"
DIAG_DIR="${JOB_DIR}/diagnostics"

# Output files (in job directory)
RAW_CSV="${JOB_DIR}/node_transfer_raw.csv"
AGGREGATED_CSV="${JOB_DIR}/node_transfer_aggregated.csv"
SUMMARY_TXT="${JOB_DIR}/node_transfer_summary.txt"

echo "==========================================="
echo "Extracting NODE_TRANSFER data from logs"
echo "==========================================="
echo "Job directory: $JOB_DIR"
echo "IOR logs:      $IOR_DIR"
echo "BenchFS logs:  $BENCHFSD_DIR"
echo "Diagnostics:   $DIAG_DIR"
echo ""

# Initialize raw CSV with header
echo "timestamp,client_node,op,target_node,bytes,chunk_index,is_local" > "$RAW_CSV"

# Function to parse a single log file (optimized for large files)
parse_log_file() {
    local default_client="$1"
    local log_file="$2"

    if [ ! -f "$log_file" ]; then
        return
    fi

    # Use grep + sed + gawk pipeline for efficient processing of large files
    # Log format examples:
    #   [bnode002] 2026-01-22T09:41:48.889096Z INFO  node_transfer:...: NODE_TRANSFER op="WRITE" target_node=node_13 bytes=4194304 chunk_index=0 is_local=false
    #   2026-01-22T10:00:00.123456 INFO node_transfer: NODE_TRANSFER op=WRITE target_node=bnode115 bytes=1048576 chunk_index=0 is_local=true
    # Note: grep returns 1 if no matches, so we use || true to avoid script exit
    { grep "NODE_TRANSFER" "$log_file" 2>/dev/null || true; } | \
    sed 's/\x1b\[[0-9;]*m//g' | \
    gawk -v default_client="$default_client" '
BEGIN { OFS="," }
{
    # Client from [bnode002] prefix or default
    client = default_client
    if (match($0, /\[([a-z]+[0-9]+)\]/)) {
        client = substr($0, RSTART+1, RLENGTH-2)
    }

    # Timestamp (ISO format with Z suffix)
    timestamp = "unknown"
    if (match($0, /[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]+Z/)) {
        timestamp = substr($0, RSTART, RLENGTH)
    }

    # op="WRITE" or op=WRITE
    op = ""
    if (match($0, /op="[A-Z]+"/)) {
        op = substr($0, RSTART+4, RLENGTH-5)
    } else if (match($0, /op=[A-Z]+ /)) {
        op = substr($0, RSTART+3, RLENGTH-4)
    }

    # target_node=node_13
    target = ""
    if (match($0, /target_node=[a-zA-Z0-9_]+/)) {
        target = substr($0, RSTART+12, RLENGTH-12)
    }

    # bytes=4194304
    bytes = ""
    if (match($0, /bytes=[0-9]+/)) {
        bytes = substr($0, RSTART+6, RLENGTH-6)
    }

    # chunk_index=0
    chunk = "0"
    if (match($0, /chunk_index=[0-9]+/)) {
        chunk = substr($0, RSTART+12, RLENGTH-12)
    }

    # is_local=true/false
    is_local = "unknown"
    if (match($0, /is_local=true/)) {
        is_local = "true"
    } else if (match($0, /is_local=false/)) {
        is_local = "false"
    }

    if (op != "" && target != "" && bytes != "") {
        print timestamp, client, op, target, bytes, chunk, is_local
    }
}'
}

# Process logs from various directories
echo "Processing logs..."
log_count=0

# 1. Process IOR results logs
if [ -d "$IOR_DIR" ]; then
    echo ""
    echo "=== IOR Results Directory ==="
    for log_file in "${IOR_DIR}"/*.log "${IOR_DIR}"/*.txt; do
        if [ -f "$log_file" ]; then
            # Try to extract client node from filename
            client_node=$(basename "$log_file" | grep -oE '[a-z]+[0-9]+' | head -1 || echo "ior_client")
            echo "  Processing: $log_file"
            parse_log_file "$client_node" "$log_file" >> "$RAW_CSV"
            ((log_count++)) || true
        fi
    done
fi

# 2. Process BenchFS daemon logs
if [ -d "$BENCHFSD_DIR" ]; then
    echo ""
    echo "=== BenchFS Daemon Logs ==="
    # Process all run directories
    for run_dir in "${BENCHFSD_DIR}"/run_*/; do
        if [ -d "$run_dir" ]; then
            for log_file in "${run_dir}"*.log; do
                if [ -f "$log_file" ]; then
                    echo "  Processing: $log_file"
                    parse_log_file "benchfsd" "$log_file" >> "$RAW_CSV"
                    ((log_count++)) || true
                fi
            done
        fi
    done
    # Also check root benchfsd_logs directory
    for log_file in "${BENCHFSD_DIR}"/*.log; do
        if [ -f "$log_file" ]; then
            echo "  Processing: $log_file"
            parse_log_file "benchfsd" "$log_file" >> "$RAW_CSV"
            ((log_count++)) || true
        fi
    done
fi

# 3. Process per-node diagnostics (if present)
if [ -d "$DIAG_DIR" ]; then
    echo ""
    echo "=== Diagnostics Directory ==="
    for node_dir in "$DIAG_DIR"/*/; do
        if [ ! -d "$node_dir" ]; then
            continue
        fi

        node=$(basename "$node_dir")

        # Skip if not a node directory
        if [[ ! "$node" =~ ^[a-z]+[0-9]+$ ]]; then
            continue
        fi

        # Process all log files in the node directory
        for log_file in "${node_dir}"*.log "${node_dir}"benchfs*.txt "${node_dir}"client*.log; do
            if [ -f "$log_file" ]; then
                echo "  Processing: $log_file"
                parse_log_file "$node" "$log_file" >> "$RAW_CSV"
                ((log_count++)) || true
            fi
        done
    done

    # Also check for combined logs in the root diagnostics directory
    for log_file in "${DIAG_DIR}"/*.log "${DIAG_DIR}"/client*.txt; do
        if [ -f "$log_file" ]; then
            # Try to extract client node from filename
            client_node=$(basename "$log_file" | grep -oE '[a-z]+[0-9]+' | head -1 || echo "unknown")
            echo "  Processing: $log_file (client: $client_node)"
            parse_log_file "$client_node" "$log_file" >> "$RAW_CSV"
            ((log_count++)) || true
        fi
    done
fi

echo ""
echo "Processed $log_count log files"

# Count entries
entry_count=$(tail -n +2 "$RAW_CSV" | wc -l)
echo "Extracted $entry_count NODE_TRANSFER entries"

# Generate aggregated CSV if requested
if [ "$AGGREGATE_INTERVAL" -gt 0 ]; then
    echo ""
    echo "Aggregating by ${AGGREGATE_INTERVAL}s intervals..."

    # Create aggregated CSV
    echo "time_bucket,client_node,op,target_node,total_bytes,transfer_count" > "$AGGREGATED_CSV"

    # Aggregate using awk
    tail -n +2 "$RAW_CSV" | awk -F',' -v interval="$AGGREGATE_INTERVAL" '
    BEGIN {
        OFS = ","
    }
    {
        # Parse timestamp to seconds (simplified - assumes ISO format or epoch)
        ts = $1

        # For ISO format, extract seconds from start of day
        if (match(ts, /[0-9]{2}:[0-9]{2}:[0-9]{2}/)) {
            h = substr(ts, RSTART, 2)
            m = substr(ts, RSTART+3, 2)
            s = substr(ts, RSTART+6, 2)
            total_sec = h * 3600 + m * 60 + s
        } else if (match(ts, /^[0-9]+/)) {
            total_sec = int(ts)
        } else {
            total_sec = NR  # fallback to line number
        }

        bucket = int(total_sec / interval) * interval

        key = bucket "," $2 "," $3 "," $4
        bytes[key] += $5
        count[key]++
    }
    END {
        for (key in bytes) {
            print key, bytes[key], count[key]
        }
    }' | sort -t',' -k1,1n -k2,2 -k3,3 -k4,4 >> "$AGGREGATED_CSV"

    agg_count=$(tail -n +2 "$AGGREGATED_CSV" | wc -l)
    echo "Generated $agg_count aggregated entries"
fi

# Generate summary
echo "" > "$SUMMARY_TXT"
echo "===========================================" >> "$SUMMARY_TXT"
echo "NODE_TRANSFER Analysis Summary" >> "$SUMMARY_TXT"
echo "===========================================" >> "$SUMMARY_TXT"
echo "" >> "$SUMMARY_TXT"
echo "Source: $JOB_DIR" >> "$SUMMARY_TXT"
echo "Total transfers: $entry_count" >> "$SUMMARY_TXT"
echo "" >> "$SUMMARY_TXT"

# Summary by operation type
echo "=== By Operation Type ===" >> "$SUMMARY_TXT"
tail -n +2 "$RAW_CSV" | awk -F',' '
{
    op[$3]++
    bytes[$3] += $5
}
END {
    printf "%-10s %12s %18s\n", "Op", "Count", "Total Bytes"
    printf "%-10s %12s %18s\n", "----", "-----", "-----------"
    for (o in op) {
        printf "%-10s %12d %18d\n", o, op[o], bytes[o]
    }
}' >> "$SUMMARY_TXT"
echo "" >> "$SUMMARY_TXT"

# Summary by target node
echo "=== By Target Node ===" >> "$SUMMARY_TXT"
tail -n +2 "$RAW_CSV" | awk -F',' '
{
    node[$4]++
    bytes[$4] += $5
}
END {
    printf "%-15s %12s %18s %12s\n", "Target Node", "Count", "Total Bytes", "Avg Bytes"
    printf "%-15s %12s %18s %12s\n", "-----------", "-----", "-----------", "---------"
    for (n in node) {
        avg = bytes[n] / node[n]
        printf "%-15s %12d %18d %12.0f\n", n, node[n], bytes[n], avg
    }
}' | sort -t',' -k3 -rn >> "$SUMMARY_TXT"
echo "" >> "$SUMMARY_TXT"

# Summary by client node
echo "=== By Client Node ===" >> "$SUMMARY_TXT"
tail -n +2 "$RAW_CSV" | awk -F',' '
{
    node[$2]++
    bytes[$2] += $5
}
END {
    printf "%-15s %12s %18s\n", "Client Node", "Count", "Total Bytes"
    printf "%-15s %12s %18s\n", "-----------", "-----", "-----------"
    for (n in node) {
        printf "%-15s %12d %18d\n", n, node[n], bytes[n]
    }
}' | sort -t',' -k3 -rn >> "$SUMMARY_TXT"
echo "" >> "$SUMMARY_TXT"

# Load distribution imbalance check
echo "=== Load Distribution Analysis ===" >> "$SUMMARY_TXT"
tail -n +2 "$RAW_CSV" | awk -F',' '
{
    bytes[$4] += $5
}
END {
    n = 0
    total = 0
    for (node in bytes) {
        n++
        total += bytes[node]
        vals[n] = bytes[node]
    }

    if (n == 0) {
        print "No data"
        exit
    }

    avg = total / n

    # Calculate std deviation
    var = 0
    for (i = 1; i <= n; i++) {
        var += (vals[i] - avg)^2
    }
    var = var / n
    stddev = sqrt(var)

    # Find min/max
    min = vals[1]; max = vals[1]
    for (i = 2; i <= n; i++) {
        if (vals[i] < min) min = vals[i]
        if (vals[i] > max) max = vals[i]
    }

    imbalance = (max - min) / avg * 100

    printf "Nodes: %d\n", n
    printf "Total bytes: %d\n", total
    printf "Average per node: %.0f\n", avg
    printf "Std deviation: %.0f (%.1f%%)\n", stddev, (stddev/avg)*100
    printf "Min: %.0f, Max: %.0f\n", min, max
    printf "Imbalance (max-min)/avg: %.1f%%\n", imbalance

    if (imbalance > 50) {
        print "\n*** WARNING: High load imbalance detected! ***"
    }
}' >> "$SUMMARY_TXT"

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
