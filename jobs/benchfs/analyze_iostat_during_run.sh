#!/bin/bash
# ==============================================================================
# analyze_iostat_during_run.sh - Analyze iostat data collected during benchmark
# ==============================================================================
# This script analyzes iostat_during_run*.txt files to identify nodes with
# storage performance issues during the actual benchmark run.
#
# Key metrics analyzed:
#   - r_await: Average read request latency
#   - w_await: Average write request latency
#   - r/s: Read IOPS
#   - w/s: Write IOPS
#   - aqu-sz: Average queue size
#   - %util: Device utilization
#
# Usage:
#   ./analyze_iostat_during_run.sh <diagnostics_dir> [--rawait-threshold <ms>]
#
# Output:
#   - iostat_analysis.csv: Detailed analysis per node
#   - iostat_summary.txt: Human-readable summary
# ==============================================================================

set -euo pipefail

# Default thresholds
RAWAIT_THRESHOLD=5.0    # r_await threshold in ms for warning
WAWAIT_THRESHOLD=50.0   # w_await threshold in ms for warning
UTIL_THRESHOLD=95.0     # %util threshold for saturation warning

# Parse arguments
if [ $# -lt 1 ]; then
    echo "Usage: $0 <diagnostics_dir> [--rawait-threshold <ms>]"
    exit 1
fi

DIAG_DIR="$1"
shift

while [ $# -gt 0 ]; do
    case "$1" in
        --rawait-threshold)
            RAWAIT_THRESHOLD="$2"
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

# Output files
ANALYSIS_CSV="${DIAG_DIR}/iostat_during_run_analysis.csv"
SUMMARY_TXT="${DIAG_DIR}/iostat_during_run_summary.txt"

echo "=========================================="
echo "Analyzing iostat_during_run data"
echo "=========================================="
echo "Directory: $DIAG_DIR"
echo "r_await threshold: ${RAWAIT_THRESHOLD}ms"
echo ""

# Initialize CSV with header
echo "node,phase,device,samples,avg_r_await,max_r_await,avg_w_await,max_w_await,avg_util,max_util,avg_aqu_sz,status" > "$ANALYSIS_CSV"

# Function to analyze a single iostat file
analyze_iostat_file() {
    local node="$1"
    local iostat_file="$2"
    local run_id="$3"

    if [ ! -f "$iostat_file" ]; then
        return
    fi

    # Extract md0, nvme0n1, nvme1n1 data
    # iostat -x format: Device r/s rkB/s rrqm/s %rrqm r_await rareq-sz w/s wkB/s wrqm/s %wrqm w_await wareq-sz d/s dkB/s drqm/s %drqm d_await dareq-sz f/s f_await aqu-sz %util
    #                   $1     $2  $3    $4     $5    $6      $7       $8  $9    $10    $11   $12     $13      $14 $15   $16    $17   $18     $19      $20 $21     $22    $23

    for device in md0 nvme0n1 nvme1n1; do
        # Extract lines for this device, skip header/empty lines
        local data=$(grep "^${device}" "$iostat_file" 2>/dev/null | awk '{print $6, $12, $22, $23}')
        # $6 = r_await, $12 = w_await, $22 = aqu-sz, $23 = %util

        if [ -z "$data" ]; then
            continue
        fi

        # Calculate statistics
        # Input columns: $1=r_await, $2=w_await, $3=aqu-sz, $4=%util
        local stats=$(echo "$data" | awk '
        BEGIN {
            count=0;
            sum_rawait=0; max_rawait=0;
            sum_wawait=0; max_wawait=0;
            sum_util=0; max_util=0;
            sum_aqu=0; max_aqu=0;
        }
        {
            count++;
            # r_await
            if ($1 != "" && $1 ~ /^[0-9.]+$/) {
                sum_rawait += $1;
                if ($1 > max_rawait) max_rawait = $1;
            }
            # w_await
            if ($2 != "" && $2 ~ /^[0-9.]+$/) {
                sum_wawait += $2;
                if ($2 > max_wawait) max_wawait = $2;
            }
            # aqu-sz
            if ($3 != "" && $3 ~ /^[0-9.]+$/) {
                sum_aqu += $3;
                if ($3 > max_aqu) max_aqu = $3;
            }
            # %util
            if ($4 != "" && $4 ~ /^[0-9.]+$/) {
                sum_util += $4;
                if ($4 > max_util) max_util = $4;
            }
        }
        END {
            if (count > 0) {
                printf "%d %.2f %.2f %.2f %.2f %.2f %.2f %.2f",
                    count, sum_rawait/count, max_rawait,
                    sum_wawait/count, max_wawait,
                    sum_util/count, max_util, sum_aqu/count
            } else {
                print "0 0 0 0 0 0 0 0"
            }
        }')

        local samples=$(echo "$stats" | awk '{print $1}')
        local avg_rawait=$(echo "$stats" | awk '{print $2}')
        local max_rawait=$(echo "$stats" | awk '{print $3}')
        local avg_wawait=$(echo "$stats" | awk '{print $4}')
        local max_wawait=$(echo "$stats" | awk '{print $5}')
        local avg_util=$(echo "$stats" | awk '{print $6}')
        local max_util=$(echo "$stats" | awk '{print $7}')
        local avg_aqu=$(echo "$stats" | awk '{print $8}')

        # Determine status
        local status="NORMAL"
        local is_high_rawait=$(awk "BEGIN {print ($avg_rawait > $RAWAIT_THRESHOLD)}")
        local is_saturated=$(awk "BEGIN {print ($avg_util > $UTIL_THRESHOLD)}")
        local is_high_queue=$(awk "BEGIN {print ($avg_aqu > 10)}")

        if [ "$is_high_rawait" == "1" ]; then
            status="HIGH_READ_LATENCY"
        fi
        if [ "$is_saturated" == "1" ]; then
            if [ "$status" == "NORMAL" ]; then
                status="SATURATED"
            else
                status="${status}|SATURATED"
            fi
        fi
        if [ "$is_high_queue" == "1" ]; then
            if [ "$status" == "NORMAL" ]; then
                status="HIGH_QUEUE"
            else
                status="${status}|HIGH_QUEUE"
            fi
        fi

        echo "${node},run${run_id},${device},${samples},${avg_rawait},${max_rawait},${avg_wawait},${max_wawait},${avg_util},${max_util},${avg_aqu},${status}" >> "$ANALYSIS_CSV"
    done
}

# Function to identify READ vs WRITE phases in iostat data
analyze_iostat_by_phase() {
    local node="$1"
    local iostat_file="$2"

    if [ ! -f "$iostat_file" ]; then
        return
    fi

    # Create temp file for analysis
    local tmp_file=$(mktemp)

    # Extract md0 data with line numbers to preserve order
    grep -n "^md0" "$iostat_file" 2>/dev/null | while IFS=: read -r lineno rest; do
        # Parse iostat line: md0 r/s rkB/s rrqm/s %rrqm r_await rareq-sz w/s wkB/s wrqm/s %wrqm w_await wareq-sz d/s dkB/s drqm/s %drqm d_await dareq-sz f/s f_await aqu-sz %util
        local r_s=$(echo "$rest" | awk '{print $2}')
        local rkb_s=$(echo "$rest" | awk '{print $3}')
        local r_await=$(echo "$rest" | awk '{print $6}')
        local w_s=$(echo "$rest" | awk '{print $8}')
        local wkb_s=$(echo "$rest" | awk '{print $9}')
        local w_await=$(echo "$rest" | awk '{print $12}')
        local util=$(echo "$rest" | awk '{print $22}')

        # Determine phase based on read/write activity
        local phase="IDLE"
        local r_active=$(awk "BEGIN {print ($rkb_s > 10000)}")
        local w_active=$(awk "BEGIN {print ($wkb_s > 10000)}")

        if [ "$r_active" == "1" ] && [ "$w_active" == "0" ]; then
            phase="READ"
        elif [ "$w_active" == "1" ] && [ "$r_active" == "0" ]; then
            phase="WRITE"
        elif [ "$r_active" == "1" ] && [ "$w_active" == "1" ]; then
            phase="MIXED"
        fi

        echo "$lineno $phase $r_await $w_await $rkb_s $wkb_s $util"
    done > "$tmp_file"

    # Analyze READ phase
    local read_stats=$(grep " READ " "$tmp_file" 2>/dev/null | awk '
    BEGIN { count=0; sum=0; max=0; }
    {
        count++;
        sum += $3;
        if ($3 > max) max = $3;
    }
    END {
        if (count > 0) printf "%d %.2f %.2f", count, sum/count, max;
        else print "0 0 0";
    }')

    local read_samples=$(echo "$read_stats" | awk '{print $1}')
    local read_avg=$(echo "$read_stats" | awk '{print $2}')
    local read_max=$(echo "$read_stats" | awk '{print $3}')

    # Analyze WRITE phase
    local write_stats=$(grep " WRITE " "$tmp_file" 2>/dev/null | awk '
    BEGIN { count=0; sum=0; max=0; }
    {
        count++;
        sum += $4;
        if ($4 > max) max = $4;
    }
    END {
        if (count > 0) printf "%d %.2f %.2f", count, sum/count, max;
        else print "0 0 0";
    }')

    local write_samples=$(echo "$write_stats" | awk '{print $1}')
    local write_avg=$(echo "$write_stats" | awk '{print $2}')
    local write_max=$(echo "$write_stats" | awk '{print $3}')

    rm -f "$tmp_file"

    echo "${node},READ_PHASE,md0,${read_samples},${read_avg},${read_max},0,0,0,0,0,$([ $(awk "BEGIN {print ($read_avg > $RAWAIT_THRESHOLD)}") == "1" ] && echo "HIGH_LATENCY" || echo "NORMAL")"
    echo "${node},WRITE_PHASE,md0,${write_samples},0,0,${write_avg},${write_max},0,0,0,NORMAL"
}

# Process each node
echo "Processing nodes..."
problem_nodes=()

for node_dir in "$DIAG_DIR"/*/; do
    if [ ! -d "$node_dir" ]; then
        continue
    fi

    node=$(basename "$node_dir")

    # Skip if not a node directory
    if [[ ! "$node" =~ ^[a-z]+[0-9]+$ ]]; then
        continue
    fi

    # Process iostat_during_run files
    for iostat_file in "${node_dir}"/iostat_during_run*.txt; do
        if [ -f "$iostat_file" ]; then
            run_id=$(echo "$iostat_file" | grep -oP 'run\K[0-9]+' || echo "0")
            analyze_iostat_file "$node" "$iostat_file" "$run_id"
        fi
    done
done

# Generate summary
echo "" > "$SUMMARY_TXT"
echo "=========================================="  >> "$SUMMARY_TXT"
echo "iostat During Run Analysis Summary"         >> "$SUMMARY_TXT"
echo "=========================================="  >> "$SUMMARY_TXT"
echo ""                                           >> "$SUMMARY_TXT"
echo "Threshold: r_await > ${RAWAIT_THRESHOLD}ms = HIGH_READ_LATENCY" >> "$SUMMARY_TXT"
echo ""                                           >> "$SUMMARY_TXT"

# Find problem nodes (high r_await during run)
echo "=== Nodes with High Read Latency ===" >> "$SUMMARY_TXT"
echo "" >> "$SUMMARY_TXT"

# Sort by max_r_await descending and show top issues
tail -n +2 "$ANALYSIS_CSV" | grep -v "NORMAL$" | sort -t',' -k6 -rn | head -20 | while IFS=',' read -r node phase device samples avg_r max_r avg_w max_w avg_u max_u aqu status; do
    printf "%-12s %-12s %-10s avg_r_await=%-8s max=%-8s status=%s\n" "$node" "$phase" "$device" "$avg_r" "$max_r" "$status" >> "$SUMMARY_TXT"
done

echo "" >> "$SUMMARY_TXT"
echo "=== Per-Node md0 Read Latency Comparison ===" >> "$SUMMARY_TXT"
echo "" >> "$SUMMARY_TXT"
printf "%-12s %10s %10s %10s\n" "Node" "avg_r_await" "max_r_await" "status" >> "$SUMMARY_TXT"
printf "%-12s %10s %10s %10s\n" "----" "----------" "----------" "------" >> "$SUMMARY_TXT"

# Extract md0 data only, sorted by avg_r_await
tail -n +2 "$ANALYSIS_CSV" | grep ",md0," | sort -t',' -k5 -rn | while IFS=',' read -r node phase device samples avg_r max_r avg_w max_w avg_u max_u aqu status; do
    printf "%-12s %10s %10s %10s\n" "$node" "$avg_r" "$max_r" "$status" >> "$SUMMARY_TXT"
done

echo "" >> "$SUMMARY_TXT"
echo "=== NVMe Device Comparison (Read Latency Difference) ===" >> "$SUMMARY_TXT"
echo "" >> "$SUMMARY_TXT"
echo "Comparing nvme0n1 vs nvme1n1 r_await per node:" >> "$SUMMARY_TXT"
echo "" >> "$SUMMARY_TXT"
printf "%-12s %12s %12s %12s %s\n" "Node" "nvme0_r_await" "nvme1_r_await" "diff_ratio" "status" >> "$SUMMARY_TXT"
printf "%-12s %12s %12s %12s %s\n" "----" "------------" "------------" "----------" "------" >> "$SUMMARY_TXT"

# Compare nvme0n1 and nvme1n1 for each node
for node_dir in "$DIAG_DIR"/*/; do
    node=$(basename "$node_dir")
    if [[ ! "$node" =~ ^[a-z]+[0-9]+$ ]]; then
        continue
    fi

    nvme0_avg=$(grep "^${node}," "$ANALYSIS_CSV" | grep ",nvme0n1," | head -1 | cut -d',' -f5)
    nvme1_avg=$(grep "^${node}," "$ANALYSIS_CSV" | grep ",nvme1n1," | head -1 | cut -d',' -f5)

    if [ -n "$nvme0_avg" ] && [ -n "$nvme1_avg" ] && [ "$nvme0_avg" != "0.00" ]; then
        diff_ratio=$(awk "BEGIN {printf \"%.1f\", $nvme1_avg / $nvme0_avg}")
        status="NORMAL"
        if [ $(awk "BEGIN {print ($diff_ratio > 5.0)}") == "1" ]; then
            status="***NVME1_SLOW***"
        fi
        printf "%-12s %12s %12s %12s %s\n" "$node" "$nvme0_avg" "$nvme1_avg" "${diff_ratio}x" "$status" >> "$SUMMARY_TXT"
    fi
done | sort -t'x' -k4 -rn >> "$SUMMARY_TXT"

echo ""
echo "=========================================="
echo "Analysis complete"
echo "=========================================="
echo "Output files:"
echo "  CSV:     $ANALYSIS_CSV"
echo "  Summary: $SUMMARY_TXT"
echo ""
cat "$SUMMARY_TXT"
