#!/bin/bash
#PBS -q gold
#PBS -A NBB
#PBS -l select=1
#PBS -l walltime=0:15:00

# FIO Baseline Benchmark for Sirius NVMe
# Establishes raw NVMe performance to compare against BenchFS results.
#
# Usage:
#   qsub sirius-fio-baseline.sh
#
# Tests:
#   1. Sequential write/read with single large file (ideal case)
#   2. Many small files with open/close per IO (simulating BenchFS behavior)
#   3. Fixed buffers with io_uring (matching BenchFS io_uring config)

set -euo pipefail

echo "=========================================="
echo "FIO Baseline Benchmark (Sirius NVMe)"
echo "=========================================="
echo "Hostname: $(hostname)"
echo "PBS_JOBID: ${PBS_JOBID}"
echo "Date: $(date)"
echo ""

# Detect scratch directory
SCRATCH=""
if [ -d "/scr/${PBS_JOBID}" ]; then
    SCRATCH="/scr/${PBS_JOBID}"
else
    for n in 0 1 2 3; do
        candidate="/scr${n}/${PBS_JOBID}"
        if [ -d "$candidate" ]; then
            SCRATCH="$candidate"
            break
        fi
    done
fi

if [ -z "$SCRATCH" ]; then
    echo "ERROR: No scratch directory found for job ${PBS_JOBID}"
    exit 1
fi

echo "Scratch directory: $SCRATCH"
echo ""

# Check fio availability
if ! command -v fio >/dev/null 2>&1; then
    echo "ERROR: fio not found. Trying to load module..."
    module load fio 2>/dev/null || true
    if ! command -v fio >/dev/null 2>&1; then
        echo "ERROR: fio still not available after module load attempt"
        echo "Install fio or load the correct module"
        exit 1
    fi
fi
echo "fio version: $(fio --version)"
echo ""

# Output directory
RESULTS_DIR="${SCRATCH}/fio_results"
mkdir -p "${RESULTS_DIR}"

# Cleanup function
cleanup() {
    echo ""
    echo "Cleaning up test files..."
    rm -rf "${SCRATCH}/fio_test" "${SCRATCH}/fio_chunks" "${SCRATCH}/fio_fixed" "${RESULTS_DIR}" 2>/dev/null || true
}
trap cleanup EXIT

# ============================================================================
# Test 1: Single large file sequential write/read (ideal case)
# This is the theoretical maximum for the NVMe drive.
# ============================================================================
echo "=========================================="
echo "Test 1: Sequential write (single large file, io_uring)"
echo "=========================================="
fio --name=seq_write \
    --ioengine=io_uring --iodepth=128 --bs=4m \
    --direct=1 --numjobs=1 --size=16g --rw=write \
    --filename="${SCRATCH}/fio_test" \
    --group_reporting \
    --output="${RESULTS_DIR}/test1_seq_write.json" --output-format=json
echo ""

echo "=========================================="
echo "Test 1: Sequential read (single large file, io_uring)"
echo "=========================================="
fio --name=seq_read \
    --ioengine=io_uring --iodepth=128 --bs=4m \
    --direct=1 --numjobs=1 --size=16g --rw=read \
    --filename="${SCRATCH}/fio_test" \
    --group_reporting \
    --output="${RESULTS_DIR}/test1_seq_read.json" --output-format=json
echo ""

# Cleanup test 1 file
rm -f "${SCRATCH}/fio_test"

# ============================================================================
# Test 2: Many small files with open/close per IO (simulating BenchFS)
# This simulates BenchFS behavior where each chunk is a separate file
# and file handles are opened/closed for every operation.
# ============================================================================
echo "=========================================="
echo "Test 2: Many small files write (4096 x 4MB files, open/close per IO)"
echo "=========================================="
mkdir -p "${SCRATCH}/fio_chunks"
fio --name=small_files_write \
    --ioengine=io_uring --iodepth=32 --bs=4m \
    --direct=1 --numjobs=1 --nrfiles=4096 --filesize=4m \
    --openfiles=1 --rw=write \
    --directory="${SCRATCH}/fio_chunks" \
    --group_reporting \
    --output="${RESULTS_DIR}/test2_small_files_write.json" --output-format=json
echo ""

echo "=========================================="
echo "Test 2: Many small files read (4096 x 4MB files, open/close per IO)"
echo "=========================================="
fio --name=small_files_read \
    --ioengine=io_uring --iodepth=32 --bs=4m \
    --direct=1 --numjobs=1 --nrfiles=4096 --filesize=4m \
    --openfiles=1 --rw=read \
    --directory="${SCRATCH}/fio_chunks" \
    --group_reporting \
    --output="${RESULTS_DIR}/test2_small_files_read.json" --output-format=json
echo ""

# Cleanup test 2 files
rm -rf "${SCRATCH}/fio_chunks"

# ============================================================================
# Test 3: Fixed buffers (matching BenchFS io_uring config)
# Tests io_uring with registered buffers and registered files.
# ============================================================================
echo "=========================================="
echo "Test 3: Fixed buffers write (io_uring fixedbufs + registerfiles)"
echo "=========================================="
fio --name=fixed_buf_write \
    --ioengine=io_uring --iodepth=128 --bs=4m \
    --direct=1 --numjobs=1 --size=16g --rw=write \
    --fixedbufs=1 --registerfiles=1 \
    --filename="${SCRATCH}/fio_fixed" \
    --group_reporting \
    --output="${RESULTS_DIR}/test3_fixed_buf_write.json" --output-format=json
echo ""

echo "=========================================="
echo "Test 3: Fixed buffers read (io_uring fixedbufs + registerfiles)"
echo "=========================================="
fio --name=fixed_buf_read \
    --ioengine=io_uring --iodepth=128 --bs=4m \
    --direct=1 --numjobs=1 --size=16g --rw=read \
    --fixedbufs=1 --registerfiles=1 \
    --filename="${SCRATCH}/fio_fixed" \
    --group_reporting \
    --output="${RESULTS_DIR}/test3_fixed_buf_read.json" --output-format=json
echo ""

# Cleanup test 3 file
rm -f "${SCRATCH}/fio_fixed"

# ============================================================================
# Test 4: Multiple concurrent jobs (simulating multi-process BenchFS server)
# ============================================================================
echo "=========================================="
echo "Test 4: Multi-job write (4 jobs, simulating multi-process server)"
echo "=========================================="
fio --name=multi_write \
    --ioengine=io_uring --iodepth=128 --bs=4m \
    --direct=1 --numjobs=4 --size=4g --rw=write \
    --filename="${SCRATCH}/fio_test" \
    --group_reporting \
    --output="${RESULTS_DIR}/test4_multi_write.json" --output-format=json
echo ""

echo "=========================================="
echo "Test 4: Multi-job read (4 jobs, simulating multi-process server)"
echo "=========================================="
fio --name=multi_read \
    --ioengine=io_uring --iodepth=128 --bs=4m \
    --direct=1 --numjobs=4 --size=4g --rw=read \
    --filename="${SCRATCH}/fio_test" \
    --group_reporting \
    --output="${RESULTS_DIR}/test4_multi_read.json" --output-format=json
echo ""

# ============================================================================
# Summary
# ============================================================================
echo "=========================================="
echo "FIO Baseline Summary"
echo "=========================================="
echo ""

# Extract bandwidth from JSON results
extract_bw() {
    local file="$1"
    local op="$2"  # "read" or "write"
    if [ -f "$file" ]; then
        # Extract bw_bytes from the JSON and convert to MiB/s
        python3 -c "
import json, sys
with open('$file') as f:
    data = json.load(f)
    bw = data['jobs'][0]['${op}']['bw_bytes']
    iops = data['jobs'][0]['${op}']['iops']
    lat_mean = data['jobs'][0]['${op}']['lat_ns']['mean']
    print(f'  BW: {bw/1024/1024:.1f} MiB/s ({bw/1024/1024/1024:.2f} GiB/s)')
    print(f'  IOPS: {iops:.0f}')
    print(f'  Lat (mean): {lat_mean/1000:.1f} us')
" 2>/dev/null || echo "  (could not parse JSON results)"
    else
        echo "  (result file not found)"
    fi
}

echo "Test 1: Sequential single file (ideal case)"
echo "  Write:"
extract_bw "${RESULTS_DIR}/test1_seq_write.json" "write"
echo "  Read:"
extract_bw "${RESULTS_DIR}/test1_seq_read.json" "read"
echo ""

echo "Test 2: Many small files (BenchFS-like open/close pattern)"
echo "  Write:"
extract_bw "${RESULTS_DIR}/test2_small_files_write.json" "write"
echo "  Read:"
extract_bw "${RESULTS_DIR}/test2_small_files_read.json" "read"
echo ""

echo "Test 3: Fixed buffers (io_uring registered buffers)"
echo "  Write:"
extract_bw "${RESULTS_DIR}/test3_fixed_buf_write.json" "write"
echo "  Read:"
extract_bw "${RESULTS_DIR}/test3_fixed_buf_read.json" "read"
echo ""

echo "Test 4: Multi-job (4 concurrent jobs)"
echo "  Write:"
extract_bw "${RESULTS_DIR}/test4_multi_write.json" "write"
echo "  Read:"
extract_bw "${RESULTS_DIR}/test4_multi_read.json" "read"
echo ""

# Copy results to a persistent location if OUTPUT_DIR is set
if [ -n "${OUTPUT_DIR:-}" ]; then
    mkdir -p "${OUTPUT_DIR}"
    cp -r "${RESULTS_DIR}"/* "${OUTPUT_DIR}/" 2>/dev/null || true
    echo "Results copied to: ${OUTPUT_DIR}"
fi

echo "=========================================="
echo "FIO baseline benchmark completed"
echo "=========================================="
