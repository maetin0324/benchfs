#!/usr/bin/env python3
"""
Analyze io_uring read/write timing logs from BenchFS benchfsd.

This script parses log files containing read_fixed_direct and write_fixed_direct
timing information and outputs statistical summaries.

Log format:
[node] timestamp DEBUG benchfs::storage::iouring:path: read_fixed_direct: N bytes in TIME from fd=FD at offset=OFFSET (BW MiB/s)
[node] timestamp DEBUG benchfs::storage::iouring:path: write_fixed_direct: N bytes in TIME to fd=FD at offset=OFFSET (BW MiB/s)

Usage:
    python analyze_io_timing.py <log_file> [--output csv_file] [--by-node]
"""

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path

import polars as pl


@dataclass
class IoTimingEntry:
    node: str
    timestamp: str
    operation: str  # 'read' or 'write'
    bytes: int
    time_us: float  # microseconds
    fd: int
    offset: int
    bandwidth_mib_s: float


def parse_time_string(time_str: str) -> float:
    """Parse time string like '14.144622ms' or '657.705µs' to microseconds."""
    time_str = time_str.strip()
    if time_str.endswith("ms"):
        return float(time_str[:-2]) * 1000  # ms to us
    elif time_str.endswith("µs") or time_str.endswith("us"):
        return float(time_str[:-2])
    elif time_str.endswith("\xb5s"):  # UTF-8 µ (micro sign)
        return float(time_str[:-2])
    elif time_str.endswith("s") and not time_str.endswith("ms") and not time_str.endswith("ns"):
        return float(time_str[:-1]) * 1_000_000  # s to us
    elif time_str.endswith("ns"):
        return float(time_str[:-2]) / 1000  # ns to us
    else:
        raise ValueError(f"Unknown time format: {time_str}")


def parse_log_file(log_path: Path) -> list[IoTimingEntry]:
    """Parse log file and extract I/O timing entries."""
    entries = []

    # Pattern for read_fixed_direct and write_fixed_direct
    # [bnode045] 2026-01-16T09:53:50.226585Z DEBUG benchfs::storage::iouring:src/storage/iouring.rs:189: read_fixed_direct: 4194304 bytes in 8.872384ms from fd=188516 at offset=0 (450.84 MiB/s)
    pattern = re.compile(
        r"\[([^\]]+)\]\s+"  # node
        r"(\S+)\s+"  # timestamp
        r"DEBUG\s+benchfs::storage::iouring:\S+:\s+"  # log prefix (module:file:line:)
        r"(read|write)_fixed_direct:\s+"  # operation
        r"(\d+)\s+bytes\s+in\s+"  # bytes
        r"(\S+)\s+"  # time
        r"(?:from|to)\s+fd=(\d+)\s+"  # fd
        r"at\s+offset=(\d+)\s+"  # offset
        r"\(([0-9.]+)\s+MiB/s\)"  # bandwidth
    )

    with open(log_path, "r") as f:
        for line in f:
            match = pattern.search(line)
            if match:
                node = match.group(1)
                timestamp = match.group(2)
                operation = match.group(3)
                bytes_count = int(match.group(4))
                time_str = match.group(5)
                fd = int(match.group(6))
                offset = int(match.group(7))
                bandwidth = float(match.group(8))

                try:
                    time_us = parse_time_string(time_str)
                    entries.append(
                        IoTimingEntry(
                            node=node,
                            timestamp=timestamp,
                            operation=operation,
                            bytes=bytes_count,
                            time_us=time_us,
                            fd=fd,
                            offset=offset,
                            bandwidth_mib_s=bandwidth,
                        )
                    )
                except ValueError as e:
                    print(f"Warning: {e}", file=sys.stderr)

    return entries


def analyze_entries(entries: list[IoTimingEntry], by_node: bool = False) -> None:
    """Analyze and print statistics for I/O timing entries."""
    if not entries:
        print("No entries found in log file.")
        return

    # Convert to polars DataFrame
    df = pl.DataFrame(
        {
            "node": [e.node for e in entries],
            "operation": [e.operation for e in entries],
            "bytes": [e.bytes for e in entries],
            "time_us": [e.time_us for e in entries],
            "bandwidth_mib_s": [e.bandwidth_mib_s for e in entries],
        }
    )

    print("=" * 80)
    print("I/O Timing Analysis")
    print("=" * 80)
    print(f"Total entries: {len(entries)}")
    print()

    # Overall statistics by operation
    print("-" * 80)
    print("Overall Statistics by Operation")
    print("-" * 80)

    stats = (
        df.group_by("operation")
        .agg(
            pl.len().alias("count"),
            pl.col("bytes").sum().alias("total_bytes"),
            pl.col("time_us").mean().alias("mean_time_us"),
            pl.col("time_us").median().alias("median_time_us"),
            pl.col("time_us").min().alias("min_time_us"),
            pl.col("time_us").max().alias("max_time_us"),
            pl.col("time_us").std().alias("std_time_us"),
            pl.col("time_us").quantile(0.01).alias("p1_time_us"),
            pl.col("time_us").quantile(0.05).alias("p5_time_us"),
            pl.col("time_us").quantile(0.95).alias("p95_time_us"),
            pl.col("time_us").quantile(0.99).alias("p99_time_us"),
            pl.col("bandwidth_mib_s").mean().alias("mean_bw_mib_s"),
            pl.col("bandwidth_mib_s").median().alias("median_bw_mib_s"),
            pl.col("bandwidth_mib_s").min().alias("min_bw_mib_s"),
            pl.col("bandwidth_mib_s").max().alias("max_bw_mib_s"),
        )
        .sort("operation")
    )

    for row in stats.iter_rows(named=True):
        op = row["operation"]
        print(f"\n{op.upper()} Operations:")
        print(f"  Count:        {row['count']:,}")
        print(f"  Total bytes:  {row['total_bytes'] / (1024**3):.2f} GiB")
        print()
        print(f"  Time (µs):")
        print(f"    Mean:       {row['mean_time_us']:,.2f}")
        print(f"    Median:     {row['median_time_us']:,.2f}")
        print(f"    Min:        {row['min_time_us']:,.2f}")
        print(f"    Max:        {row['max_time_us']:,.2f}")
        print(f"    Std:        {row['std_time_us']:,.2f}")
        print(f"    P1:         {row['p1_time_us']:,.2f}")
        print(f"    P5:         {row['p5_time_us']:,.2f}")
        print(f"    P95:        {row['p95_time_us']:,.2f}")
        print(f"    P99:        {row['p99_time_us']:,.2f}")
        print()
        print(f"  Bandwidth (MiB/s):")
        print(f"    Mean:       {row['mean_bw_mib_s']:,.2f}")
        print(f"    Median:     {row['median_bw_mib_s']:,.2f}")
        print(f"    Min:        {row['min_bw_mib_s']:,.2f}")
        print(f"    Max:        {row['max_bw_mib_s']:,.2f}")

    if by_node:
        print()
        print("-" * 80)
        print("Statistics by Node and Operation")
        print("-" * 80)

        node_stats = (
            df.group_by(["node", "operation"])
            .agg(
                pl.len().alias("count"),
                pl.col("time_us").mean().alias("mean_time_us"),
                pl.col("time_us").median().alias("median_time_us"),
                pl.col("bandwidth_mib_s").mean().alias("mean_bw_mib_s"),
            )
            .sort(["node", "operation"])
        )

        print(node_stats)

    # Time distribution histogram (text-based)
    print()
    print("-" * 80)
    print("Time Distribution (log scale buckets)")
    print("-" * 80)

    for op in ["read", "write"]:
        op_df = df.filter(pl.col("operation") == op)
        if op_df.height == 0:
            continue

        print(f"\n{op.upper()}:")

        # Create log-scale buckets
        buckets = [0, 100, 500, 1000, 5000, 10000, 50000, 100000, float("inf")]
        bucket_labels = [
            "0-100µs",
            "100-500µs",
            "500µs-1ms",
            "1-5ms",
            "5-10ms",
            "10-50ms",
            "50-100ms",
            ">100ms",
        ]

        times = op_df["time_us"].to_list()
        total = len(times)

        for i, label in enumerate(bucket_labels):
            count = sum(1 for t in times if buckets[i] <= t < buckets[i + 1])
            pct = count / total * 100 if total > 0 else 0
            bar = "#" * int(pct / 2)
            print(f"  {label:12s}: {count:6d} ({pct:5.1f}%) {bar}")


def export_to_csv(entries: list[IoTimingEntry], output_path: Path) -> None:
    """Export entries to CSV file."""
    df = pl.DataFrame(
        {
            "node": [e.node for e in entries],
            "timestamp": [e.timestamp for e in entries],
            "operation": [e.operation for e in entries],
            "bytes": [e.bytes for e in entries],
            "time_us": [e.time_us for e in entries],
            "fd": [e.fd for e in entries],
            "offset": [e.offset for e in entries],
            "bandwidth_mib_s": [e.bandwidth_mib_s for e in entries],
        }
    )
    df.write_csv(output_path)
    print(f"\nExported {len(entries)} entries to {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Analyze io_uring read/write timing logs"
    )
    parser.add_argument("log_file", type=Path, help="Path to log file")
    parser.add_argument(
        "--output", "-o", type=Path, help="Output CSV file path (optional)"
    )
    parser.add_argument(
        "--by-node", action="store_true", help="Show statistics broken down by node"
    )

    args = parser.parse_args()

    if not args.log_file.exists():
        print(f"Error: Log file not found: {args.log_file}", file=sys.stderr)
        sys.exit(1)

    print(f"Parsing {args.log_file}...")
    entries = parse_log_file(args.log_file)

    analyze_entries(entries, by_node=args.by_node)

    if args.output:
        export_to_csv(entries, args.output)


if __name__ == "__main__":
    main()
