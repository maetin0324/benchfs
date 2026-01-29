#!/bin/bash
# ==============================================================================
# extract.sh - Run all extraction scripts for benchmark analysis
# ==============================================================================
# This script runs all CSV extraction scripts for comprehensive benchmark
# analysis. By default, it aggregates data by 1-second intervals.
#
# Usage:
#   ./extract.sh <job_results_dir> [--aggregate <interval_sec>] [--no-aggregate]
#
# Options:
#   --aggregate <sec>   Aggregate data per time interval (default: 1 second)
#   --no-aggregate      Disable aggregation (raw data only)
#
# Example:
#   ./extract.sh /path/to/results/benchfs/2026.01.23-14.36.23-debug_large/...
#   ./extract.sh /path/to/results/... --aggregate 5
#   ./extract.sh /path/to/results/... --no-aggregate
#
# Output files (in job_results_dir):
#   - node_transfer_raw.csv, node_transfer_aggregated.csv, node_transfer_summary.txt
#   - io_depth_raw.csv, io_depth_aggregated.csv, io_depth_summary.txt
#   - io_timing_raw.csv, io_timing_aggregated.csv, io_timing_summary.txt
#   - rpc_transfer_raw.csv, rpc_transfer_aggregated.csv, rpc_transfer_summary.txt
#   - iostat_raw.csv, iostat_aggregated.csv, iostat_summary.txt
# ==============================================================================

set -euo pipefail

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Parse arguments
if [ $# -lt 1 ]; then
    echo "Usage: $0 <job_results_dir> [--aggregate <interval_sec>] [--no-aggregate]"
    echo ""
    echo "Options:"
    echo "  --aggregate <sec>   Aggregate data per time interval (default: 1 second)"
    echo "  --no-aggregate      Disable aggregation (raw data only)"
    echo ""
    echo "Example:"
    echo "  $0 /path/to/results/benchfs/2026.01.23-14.36.23-debug_large/..."
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

echo "=============================================="
echo "Running all extraction scripts"
echo "=============================================="
echo "Job directory: $JOB_DIR"
if [ "$DO_AGGREGATE" = true ]; then
    echo "Aggregation:   ${AGGREGATE_INTERVAL}s intervals"
else
    echo "Aggregation:   disabled"
fi
echo ""

# Build aggregate option
if [ "$DO_AGGREGATE" = true ]; then
    AGGREGATE_OPT="--aggregate $AGGREGATE_INTERVAL"
else
    AGGREGATE_OPT=""
fi

# Track success/failure
FAILED=()

# Run each extraction script
run_script() {
    local script_name="$1"
    local script_path="${SCRIPT_DIR}/${script_name}"

    echo ""
    echo "=============================================="
    echo "Running: $script_name"
    echo "=============================================="

    if [ ! -f "$script_path" ]; then
        echo "WARNING: Script not found: $script_path"
        FAILED+=("$script_name (not found)")
        return 1
    fi

    if [ ! -x "$script_path" ]; then
        chmod +x "$script_path"
    fi

    if "$script_path" "$JOB_DIR" $AGGREGATE_OPT; then
        echo "SUCCESS: $script_name completed"
        return 0
    else
        echo "ERROR: $script_name failed"
        FAILED+=("$script_name")
        return 1
    fi
}

# Run all extraction scripts
run_script "extract_node_transfer_csv.sh" || true
run_script "extract_io_depth_csv.sh" || true
run_script "extract_io_timing_csv.sh" || true
run_script "extract_rpc_transfer_csv.sh" || true
run_script "extract_iostat_csv.sh" || true
run_script "extract_server_rpc_timing.sh" || true
run_script "extract_am_timing.sh" || true

# Summary
echo ""
echo "=============================================="
echo "Extraction Complete"
echo "=============================================="
echo "Output directory: $JOB_DIR"
echo ""

# List generated files
echo "Generated files:"
for csv in "$JOB_DIR"/*.csv "$JOB_DIR"/*.txt; do
    if [ -f "$csv" ]; then
        size=$(du -h "$csv" | cut -f1)
        echo "  $(basename "$csv") ($size)"
    fi
done

# Report failures
if [ ${#FAILED[@]} -gt 0 ]; then
    echo ""
    echo "WARNING: The following scripts had issues:"
    for f in "${FAILED[@]}"; do
        echo "  - $f"
    done
    exit 1
fi

echo ""
echo "All extraction scripts completed successfully."
