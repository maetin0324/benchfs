#!/bin/bash
# ==============================================================================
# analyze_diagnostics.sh - Analyze pre-benchmark node diagnostics
# ==============================================================================
# This script analyzes fio benchmark results and iostat data from node diagnostics
# and identifies nodes with potential I/O performance issues.
#
# Usage:
#   ./analyze_diagnostics.sh <diagnostics_dir> [--threshold <ratio>] [--rawait-threshold <ms>]
#
# Arguments:
#   diagnostics_dir      - Directory containing node diagnostic results
#   --threshold          - READ/WRITE ratio threshold for flagging nodes (default: 5.0)
#   --rawait-threshold   - r_await threshold in ms for HIGH_LATENCY warning (default: 10.0)
#   --mf-threshold       - Multi-file READ bandwidth threshold in MiB/s (default: 500)
#
# Output:
#   - diagnostics_summary.csv: Summary of all node performance
#   - anomalous_nodes.txt: List of nodes with potential issues
#
# Warning Conditions:
#   - SLOW_READ: WRITE/READ ratio > threshold (single file fio test)
#   - HIGH_LATENCY: md0 or nvme r_await > rawait-threshold ms
#   - MULTIFILE_SLOW: Multi-file READ bandwidth < mf-threshold MiB/s
#   - RAID_REBUILDING: RAID array is rebuilding/recovering
#
# Example:
#   ./analyze_diagnostics.sh /path/to/job_output/diagnostics --threshold 3.0 --rawait-threshold 5.0
# ==============================================================================

set -euo pipefail

# Default thresholds
THRESHOLD=5.0                # READ/WRITE ratio threshold
RAWAIT_THRESHOLD=10.0        # r_await threshold in ms
MF_THRESHOLD=500             # Multi-file READ bandwidth threshold in MiB/s

# Parse arguments
if [ $# -lt 1 ]; then
    echo "Usage: $0 <diagnostics_dir> [--threshold <ratio>] [--rawait-threshold <ms>] [--mf-threshold <mib_s>]"
    echo ""
    echo "Arguments:"
    echo "  diagnostics_dir      - Directory containing node diagnostic results"
    echo "  --threshold          - WRITE/READ ratio threshold (default: 5.0)"
    echo "  --rawait-threshold   - r_await threshold in ms (default: 10.0)"
    echo "  --mf-threshold       - Multi-file READ bandwidth threshold in MiB/s (default: 500)"
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
        --rawait-threshold)
            RAWAIT_THRESHOLD="$2"
            shift 2
            ;;
        --mf-threshold)
            MF_THRESHOLD="$2"
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

# Initialize output files with expanded header
echo "node,read_bw_mib,write_bw_mib,read_write_ratio,read_iops,write_iops,read_latency_us,write_latency_us,multifile_read_bw_mib,multifile_read_lat_us,md0_rawait_ms,nvme0_rawait_ms,nvme1_rawait_ms,raid_status,status" > "$SUMMARY_CSV"
> "$ANOMALOUS_FILE"

echo "=========================================="
echo "Node Diagnostics Analysis"
echo "=========================================="
echo "Diagnostics directory: $DIAG_DIR"
echo "Thresholds:"
echo "  WRITE/READ ratio: $THRESHOLD"
echo "  r_await: ${RAWAIT_THRESHOLD} ms"
echo "  Multi-file READ: ${MF_THRESHOLD} MiB/s"
echo ""

# Process each node directory
anomalous_count=0
high_latency_count=0
multifile_slow_count=0
raid_rebuild_count=0
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

    # ==========================================
    # Parse single-file fio READ results
    # ==========================================
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

    # ==========================================
    # Parse single-file fio WRITE results
    # ==========================================
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

    # ==========================================
    # Parse multi-file fio READ results
    # ==========================================
    mf_read_bw_mib="0.00"
    mf_read_lat_us="0.00"
    if [ -f "${node_dir}/fio_multifile_read.json" ]; then
        mf_read_bw=$(jq -r '.jobs[0].read.bw_bytes // 0' "${node_dir}/fio_multifile_read.json" 2>/dev/null)
        mf_read_lat=$(jq -r '.jobs[0].read.lat_ns.mean // 0' "${node_dir}/fio_multifile_read.json" 2>/dev/null)

        if [ "$mf_read_bw" != "null" ] && [ "$mf_read_bw" != "0" ]; then
            mf_read_bw_mib=$(awk "BEGIN {printf \"%.2f\", $mf_read_bw / 1024 / 1024}")
        fi

        if [ "$mf_read_lat" != "null" ] && [ "$mf_read_lat" != "0" ]; then
            mf_read_lat_us=$(awk "BEGIN {printf \"%.2f\", $mf_read_lat / 1000}")
        fi
    fi

    # ==========================================
    # Parse iostat r_await values
    # ==========================================
    md0_rawait="N/A"
    nvme0_rawait="N/A"
    nvme1_rawait="N/A"
    if [ -f "${node_dir}/iostat_baseline.txt" ]; then
        # iostat -x output format varies by version
        # Common format: Device r/s rkB/s rrqm/s %rrqm r_await rareq-sz ...
        # Get the last sample for each device (most recent)

        # md0 r_await
        md0_line=$(grep "^md0" "${node_dir}/iostat_baseline.txt" 2>/dev/null | tail -1)
        if [ -n "$md0_line" ]; then
            # Try to find r_await column - typically column 6 in extended iostat
            md0_rawait=$(echo "$md0_line" | awk '{print $6}')
            # Validate it's a number
            if ! [[ "$md0_rawait" =~ ^[0-9.]+$ ]]; then
                md0_rawait="N/A"
            fi
        fi

        # nvme0n1 r_await
        nvme0_line=$(grep "^nvme0n1" "${node_dir}/iostat_baseline.txt" 2>/dev/null | tail -1)
        if [ -n "$nvme0_line" ]; then
            nvme0_rawait=$(echo "$nvme0_line" | awk '{print $6}')
            if ! [[ "$nvme0_rawait" =~ ^[0-9.]+$ ]]; then
                nvme0_rawait="N/A"
            fi
        fi

        # nvme1n1 r_await
        nvme1_line=$(grep "^nvme1n1" "${node_dir}/iostat_baseline.txt" 2>/dev/null | tail -1)
        if [ -n "$nvme1_line" ]; then
            nvme1_rawait=$(echo "$nvme1_line" | awk '{print $6}')
            if ! [[ "$nvme1_rawait" =~ ^[0-9.]+$ ]]; then
                nvme1_rawait="N/A"
            fi
        fi
    fi

    # ==========================================
    # Parse RAID status
    # ==========================================
    raid_status="UNKNOWN"
    if [ -f "${node_dir}/mdstat.txt" ]; then
        if grep -q "No RAID" "${node_dir}/mdstat.txt" 2>/dev/null; then
            raid_status="NO_RAID"
        elif grep -qE "rebuilding|recovery|resync" "${node_dir}/mdstat.txt" 2>/dev/null; then
            raid_status="REBUILDING"
        elif grep -q "active" "${node_dir}/mdstat.txt" 2>/dev/null; then
            raid_status="ACTIVE"
        fi
    fi

    # ==========================================
    # Calculate WRITE/READ ratio
    # ==========================================
    if [ "$(awk "BEGIN {print ($read_bw_mib > 0)}")" == "1" ] && [ "$(awk "BEGIN {print ($write_bw_mib > 0)}")" == "1" ]; then
        ratio=$(awk "BEGIN {printf \"%.2f\", $write_bw_mib / $read_bw_mib}")
    else
        ratio="N/A"
    fi

    # ==========================================
    # Determine status with multiple warning conditions
    # ==========================================
    status_list=()

    # Check WRITE/READ ratio
    if [ "$ratio" != "N/A" ]; then
        is_slow_read=$(awk "BEGIN {print ($ratio > $THRESHOLD)}")
        if [ "$is_slow_read" == "1" ]; then
            status_list+=("SLOW_READ")
            anomalous_count=$((anomalous_count + 1))
        fi
    fi

    # Check r_await (high latency)
    is_high_latency=0
    for rawait_val in "$md0_rawait" "$nvme0_rawait" "$nvme1_rawait"; do
        if [ "$rawait_val" != "N/A" ]; then
            is_high=$(awk "BEGIN {print ($rawait_val > $RAWAIT_THRESHOLD)}")
            if [ "$is_high" == "1" ]; then
                is_high_latency=1
                break
            fi
        fi
    done
    if [ $is_high_latency -eq 1 ]; then
        status_list+=("HIGH_LATENCY")
        high_latency_count=$((high_latency_count + 1))
    fi

    # Check multi-file READ performance
    if [ "$(awk "BEGIN {print ($mf_read_bw_mib > 0)}")" == "1" ]; then
        is_mf_slow=$(awk "BEGIN {print ($mf_read_bw_mib < $MF_THRESHOLD)}")
        if [ "$is_mf_slow" == "1" ]; then
            status_list+=("MULTIFILE_SLOW")
            multifile_slow_count=$((multifile_slow_count + 1))
        fi
    fi

    # Check RAID status
    if [ "$raid_status" == "REBUILDING" ]; then
        status_list+=("RAID_REBUILDING")
        raid_rebuild_count=$((raid_rebuild_count + 1))
    fi

    # Combine status
    if [ ${#status_list[@]} -eq 0 ]; then
        status="NORMAL"
    else
        status=$(IFS="|"; echo "${status_list[*]}")
        echo "$node: $status" >> "$ANOMALOUS_FILE"
    fi

    # Output to CSV
    echo "${node},${read_bw_mib},${write_bw_mib},${ratio},${read_iops},${write_iops},${read_lat_us},${write_lat_us},${mf_read_bw_mib},${mf_read_lat_us},${md0_rawait},${nvme0_rawait},${nvme1_rawait},${raid_status},${status}" >> "$SUMMARY_CSV"
done

# Sort the CSV by status (anomalous first) then by ratio (descending) - keep header at top
if [ -f "$SUMMARY_CSV" ]; then
    header=$(head -1 "$SUMMARY_CSV")
    # Sort: first by status (NORMAL last), then by ratio descending
    tail -n +2 "$SUMMARY_CSV" | sort -t',' -k15,15 -k4 -rn > "${SUMMARY_CSV}.tmp"
    echo "$header" > "$SUMMARY_CSV"
    cat "${SUMMARY_CSV}.tmp" >> "$SUMMARY_CSV"
    rm -f "${SUMMARY_CSV}.tmp"
fi

# ==========================================
# Print summary
# ==========================================
echo ""
echo "=========================================="
echo "Summary"
echo "=========================================="
echo "Total nodes analyzed: $total_count"
echo ""
echo "Issue breakdown:"
echo "  SLOW_READ (ratio > $THRESHOLD): $anomalous_count"
echo "  HIGH_LATENCY (r_await > ${RAWAIT_THRESHOLD}ms): $high_latency_count"
echo "  MULTIFILE_SLOW (MF_READ < ${MF_THRESHOLD} MiB/s): $multifile_slow_count"
echo "  RAID_REBUILDING: $raid_rebuild_count"
echo ""

total_issues=$((anomalous_count + high_latency_count + multifile_slow_count + raid_rebuild_count))
if [ $total_issues -gt 0 ]; then
    echo "WARNING: The following nodes have potential issues:"
    echo ""
    cat "$ANOMALOUS_FILE"
    echo ""
fi

# Print detailed table
echo "Node Performance Summary:"
echo ""
printf "%-12s %8s %8s %6s %8s %10s %10s %10s %s\n" \
    "Node" "READ" "WRITE" "W/R" "MF_READ" "md0_raw" "nvme0_raw" "nvme1_raw" "Status"
printf "%-12s %8s %8s %6s %8s %10s %10s %10s %s\n" \
    "----" "----" "-----" "---" "-------" "-------" "---------" "---------" "------"

# Read CSV and print as table
tail -n +2 "$SUMMARY_CSV" | while IFS=',' read -r node read_bw write_bw ratio read_iops write_iops read_lat write_lat mf_read_bw mf_read_lat md0_raw nvme0_raw nvme1_raw raid_stat status; do
    # Format status for display
    display_status="$status"
    if [[ "$status" != "NORMAL" ]]; then
        display_status="***${status}***"
    fi

    printf "%-12s %8s %8s %6s %8s %10s %10s %10s %s\n" \
        "$node" "$read_bw" "$write_bw" "$ratio" "$mf_read_bw" "$md0_raw" "$nvme0_raw" "$nvme1_raw" "$display_status"
done

echo ""
echo "=========================================="
echo "Output files:"
echo "  Summary CSV: $SUMMARY_CSV"
echo "  Anomalous nodes: $ANOMALOUS_FILE"
echo "=========================================="

# Exit with non-zero if any issues found
if [ $total_issues -gt 0 ]; then
    exit 1
fi
exit 0
