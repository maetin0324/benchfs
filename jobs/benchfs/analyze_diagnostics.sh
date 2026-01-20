#!/bin/bash
# ==============================================================================
# analyze_diagnostics.sh - Analyze pre-benchmark node diagnostics
# ==============================================================================
# This script analyzes fio benchmark results from node diagnostics
# and identifies nodes with potential I/O performance issues.
#
# Usage:
#   ./analyze_diagnostics.sh <diagnostics_dir> [--threshold <ratio>]
#
# Arguments:
#   diagnostics_dir  - Directory containing node diagnostic results
#   --threshold      - READ/WRITE ratio threshold for flagging nodes (default: 5.0)
#
# Output:
#   - diagnostics_summary.csv: Summary of all node performance
#   - anomalous_nodes.txt: List of nodes with potential issues
#
# Example:
#   ./analyze_diagnostics.sh /path/to/job_output/diagnostics --threshold 3.0
# ==============================================================================

set -euo pipefail

# Default threshold for READ/WRITE ratio (nodes above this are flagged)
THRESHOLD=5.0

# Parse arguments
if [ $# -lt 1 ]; then
    echo "Usage: $0 <diagnostics_dir> [--threshold <ratio>]"
    echo ""
    echo "Arguments:"
    echo "  diagnostics_dir  - Directory containing node diagnostic results"
    echo "  --threshold      - READ/WRITE ratio threshold (default: 5.0)"
    exit 1
fi

DIAG_DIR="$1"
shift

while [ $# -gt 0 ]; do
    case "$1" in
        --threshold)
            THRESHOLD="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

if [ ! -d "$DIAG_DIR" ]; then
    echo "ERROR: Diagnostics directory not found: $DIAG_DIR"
    exit 1
fi

# Check for required tools
if ! command -v jq &> /dev/null; then
    echo "ERROR: jq is required but not installed"
    exit 1
fi

# Output files
SUMMARY_CSV="${DIAG_DIR}/diagnostics_summary.csv"
ANOMALOUS_FILE="${DIAG_DIR}/anomalous_nodes.txt"

# Initialize output files
echo "node,read_bw_mib,write_bw_mib,read_write_ratio,read_iops,write_iops,read_latency_us,write_latency_us,status" > "$SUMMARY_CSV"
> "$ANOMALOUS_FILE"

echo "=========================================="
echo "Node Diagnostics Analysis"
echo "=========================================="
echo "Diagnostics directory: $DIAG_DIR"
echo "READ/WRITE ratio threshold: $THRESHOLD"
echo ""

# Process each node directory
anomalous_count=0
total_count=0

for node_dir in "$DIAG_DIR"/*/; do
    if [ ! -d "$node_dir" ]; then
        continue
    fi

    node=$(basename "$node_dir")

    # Skip if not a node directory
    if [[ ! "$node" =~ ^[a-z]+[0-9]+$ ]]; then
        continue
    fi

    total_count=$((total_count + 1))

    # Parse fio READ results
    read_bw="N/A"
    read_iops="N/A"
    read_lat="N/A"
    if [ -f "${node_dir}/fio_read.json" ]; then
        read_bw=$(jq -r '.jobs[0].read.bw_bytes // 0' "${node_dir}/fio_read.json" 2>/dev/null)
        read_iops=$(jq -r '.jobs[0].read.iops // 0' "${node_dir}/fio_read.json" 2>/dev/null)
        read_lat=$(jq -r '.jobs[0].read.lat_ns.mean // 0' "${node_dir}/fio_read.json" 2>/dev/null)

        # Convert to MiB/s and microseconds
        if [ "$read_bw" != "null" ] && [ "$read_bw" != "0" ]; then
            read_bw_mib=$(awk "BEGIN {printf \"%.2f\", $read_bw / 1024 / 1024}")
        else
            read_bw_mib="0.00"
        fi

        if [ "$read_lat" != "null" ] && [ "$read_lat" != "0" ]; then
            read_lat_us=$(awk "BEGIN {printf \"%.2f\", $read_lat / 1000}")
        else
            read_lat_us="0.00"
        fi

        if [ "$read_iops" == "null" ]; then
            read_iops="0"
        fi
    else
        read_bw_mib="0.00"
        read_lat_us="0.00"
        read_iops="0"
    fi

    # Parse fio WRITE results
    write_bw="N/A"
    write_iops="N/A"
    write_lat="N/A"
    if [ -f "${node_dir}/fio_write.json" ]; then
        write_bw=$(jq -r '.jobs[0].write.bw_bytes // 0' "${node_dir}/fio_write.json" 2>/dev/null)
        write_iops=$(jq -r '.jobs[0].write.iops // 0' "${node_dir}/fio_write.json" 2>/dev/null)
        write_lat=$(jq -r '.jobs[0].write.lat_ns.mean // 0' "${node_dir}/fio_write.json" 2>/dev/null)

        # Convert to MiB/s and microseconds
        if [ "$write_bw" != "null" ] && [ "$write_bw" != "0" ]; then
            write_bw_mib=$(awk "BEGIN {printf \"%.2f\", $write_bw / 1024 / 1024}")
        else
            write_bw_mib="0.00"
        fi

        if [ "$write_lat" != "null" ] && [ "$write_lat" != "0" ]; then
            write_lat_us=$(awk "BEGIN {printf \"%.2f\", $write_lat / 1000}")
        else
            write_lat_us="0.00"
        fi

        if [ "$write_iops" == "null" ]; then
            write_iops="0"
        fi
    else
        write_bw_mib="0.00"
        write_lat_us="0.00"
        write_iops="0"
    fi

    # Calculate READ/WRITE ratio
    if [ "$(awk "BEGIN {print ($read_bw_mib > 0)}")" == "1" ] && [ "$(awk "BEGIN {print ($write_bw_mib > 0)}")" == "1" ]; then
        ratio=$(awk "BEGIN {printf \"%.2f\", $write_bw_mib / $read_bw_mib}")
    else
        ratio="N/A"
    fi

    # Determine status
    status="NORMAL"
    if [ "$ratio" != "N/A" ]; then
        is_anomalous=$(awk "BEGIN {print ($ratio > $THRESHOLD)}")
        if [ "$is_anomalous" == "1" ]; then
            status="SLOW_READ"
            anomalous_count=$((anomalous_count + 1))
            echo "$node" >> "$ANOMALOUS_FILE"
        fi
    else
        status="NO_DATA"
    fi

    # Output to CSV
    echo "${node},${read_bw_mib},${write_bw_mib},${ratio},${read_iops},${write_iops},${read_lat_us},${write_lat_us},${status}" >> "$SUMMARY_CSV"
done

# Sort the CSV by ratio (descending) - keep header at top
if [ -f "$SUMMARY_CSV" ]; then
    header=$(head -1 "$SUMMARY_CSV")
    tail -n +2 "$SUMMARY_CSV" | sort -t',' -k4 -rn > "${SUMMARY_CSV}.tmp"
    echo "$header" > "$SUMMARY_CSV"
    cat "${SUMMARY_CSV}.tmp" >> "$SUMMARY_CSV"
    rm -f "${SUMMARY_CSV}.tmp"
fi

# Print summary
echo ""
echo "=========================================="
echo "Summary"
echo "=========================================="
echo "Total nodes analyzed: $total_count"
echo "Anomalous nodes (ratio > $THRESHOLD): $anomalous_count"
echo ""

if [ $anomalous_count -gt 0 ]; then
    echo "WARNING: The following nodes have slow READ performance:"
    echo "  (WRITE/READ ratio > $THRESHOLD)"
    echo ""
    cat "$ANOMALOUS_FILE"
    echo ""
fi

# Print table
echo "Node Performance Summary:"
echo ""
printf "%-12s %12s %12s %12s %s\n" "Node" "READ (MiB/s)" "WRITE (MiB/s)" "W/R Ratio" "Status"
printf "%-12s %12s %12s %12s %s\n" "----" "------------" "-------------" "---------" "------"

# Read CSV and print as table
tail -n +2 "$SUMMARY_CSV" | while IFS=',' read -r node read_bw write_bw ratio read_iops write_iops read_lat write_lat status; do
    if [ "$status" == "SLOW_READ" ]; then
        printf "%-12s %12s %12s %12s %s\n" "$node" "$read_bw" "$write_bw" "$ratio" "***SLOW***"
    else
        printf "%-12s %12s %12s %12s %s\n" "$node" "$read_bw" "$write_bw" "$ratio" "$status"
    fi
done

echo ""
echo "=========================================="
echo "Output files:"
echo "  Summary CSV: $SUMMARY_CSV"
echo "  Anomalous nodes: $ANOMALOUS_FILE"
echo "=========================================="

# Exit with non-zero if anomalous nodes found
if [ $anomalous_count -gt 0 ]; then
    exit 1
fi
exit 0
