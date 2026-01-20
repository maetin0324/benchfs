#!/usr/bin/env python3
"""
Analyze io_uring submit io_depth logs from BenchFS benchfsd.

This script parses log files containing io_uring submit information
and outputs statistical summaries of io_depth (batch size).

Log format:
[node] timestamp DEBUG pluvio_uring::reactor:path: io_uring: submitting N SQEs (io_depth=N)

Usage:
    python analyze_io_depth.py <log_file> [--output csv_file] [--by-node]
"""

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path

import polars as pl


@dataclass
class IoDepthEntry:
    node: str
    timestamp: str
    io_depth: int


def parse_log_file(log_path: Path) -> list[IoDepthEntry]:
    """Parse log file and extract io_depth entries."""
    entries = []

    # Pattern for io_uring submit
    # [bnode044] 2026-01-16T09:51:36.192122Z DEBUG pluvio_uring::reactor:lib/pluvio/pluvio_uring/src/reactor.rs:152: io_uring: submitting 1 SQEs (io_depth=1)
    pattern = re.compile(
        r"\[([^\]]+)\]\s+"  # node
        r"(\S+)\s+"  # timestamp
        r"DEBUG\s+pluvio_uring::reactor:\S+:\s+"  # log prefix (module:file:line:)
        r"io_uring:\s+submitting\s+(\d+)\s+SQEs\s+"  # SQE count
        r"\(io_depth=(\d+)\)"  # io_depth
    )

    with open(log_path, "r") as f:
        for line in f:
            match = pattern.search(line)
            if match:
                node = match.group(1)
                timestamp = match.group(2)
                io_depth = int(match.group(4))

                entries.append(
                    IoDepthEntry(
                        node=node,
                        timestamp=timestamp,
                        io_depth=io_depth,
                    )
                )

    return entries


def analyze_entries(entries: list[IoDepthEntry], by_node: bool = False) -> None:
    """Analyze and print statistics for io_depth entries."""
    if not entries:
        print("No entries found in log file.")
        return

    # Convert to polars DataFrame
    df = pl.DataFrame(
        {
            "node": [e.node for e in entries],
            "io_depth": [e.io_depth for e in entries],
        }
    )

    print("=" * 80)
    print("io_depth (SQ Batch Size) Analysis")
    print("=" * 80)
    print(f"Total submit events: {len(entries)}")
    print(f"Total SQEs submitted: {df['io_depth'].sum():,}")
    print()

    # Overall statistics
    print("-" * 80)
    print("Overall io_depth Statistics")
    print("-" * 80)

    stats = df.select(
        pl.col("io_depth").count().alias("count"),
        pl.col("io_depth").sum().alias("total_sqes"),
        pl.col("io_depth").mean().alias("mean"),
        pl.col("io_depth").median().alias("median"),
        pl.col("io_depth").min().alias("min"),
        pl.col("io_depth").max().alias("max"),
        pl.col("io_depth").std().alias("std"),
        pl.col("io_depth").quantile(0.25).alias("p25"),
        pl.col("io_depth").quantile(0.50).alias("p50"),
        pl.col("io_depth").quantile(0.75).alias("p75"),
        pl.col("io_depth").quantile(0.90).alias("p90"),
        pl.col("io_depth").quantile(0.95).alias("p95"),
        pl.col("io_depth").quantile(0.99).alias("p99"),
    )

    row = stats.row(0, named=True)
    print(f"  Count:        {row['count']:,}")
    print(f"  Total SQEs:   {row['total_sqes']:,}")
    print(f"  Mean:         {row['mean']:.2f}")
    print(f"  Median:       {row['median']:.2f}")
    print(f"  Min:          {row['min']}")
    print(f"  Max:          {row['max']}")
    print(f"  Std:          {row['std']:.2f}")
    print()
    print("  Percentiles:")
    print(f"    P25:        {row['p25']:.1f}")
    print(f"    P50:        {row['p50']:.1f}")
    print(f"    P75:        {row['p75']:.1f}")
    print(f"    P90:        {row['p90']:.1f}")
    print(f"    P95:        {row['p95']:.1f}")
    print(f"    P99:        {row['p99']:.1f}")

    # io_depth distribution
    print()
    print("-" * 80)
    print("io_depth Distribution")
    print("-" * 80)

    depth_counts = (
        df.group_by("io_depth").agg(pl.len().alias("count")).sort("io_depth")
    )

    total = len(entries)

    # Show distribution for common values
    print("\nDistribution by io_depth value:")
    for row in depth_counts.iter_rows(named=True):
        depth = row["io_depth"]
        count = row["count"]
        pct = count / total * 100
        bar = "#" * int(pct / 2)
        print(f"  io_depth={depth:3d}: {count:6d} ({pct:5.1f}%) {bar}")

    # Bucket distribution
    print("\nDistribution by io_depth bucket:")
    buckets = [(1, 1), (2, 4), (5, 8), (9, 16), (17, 32), (33, 64), (65, 128), (129, 512)]
    bucket_labels = ["1", "2-4", "5-8", "9-16", "17-32", "33-64", "65-128", "129-512"]

    depths = df["io_depth"].to_list()
    for (lo, hi), label in zip(buckets, bucket_labels):
        count = sum(1 for d in depths if lo <= d <= hi)
        pct = count / total * 100 if total > 0 else 0
        bar = "#" * int(pct / 2)
        print(f"  {label:8s}: {count:6d} ({pct:5.1f}%) {bar}")

    if by_node:
        print()
        print("-" * 80)
        print("Statistics by Node")
        print("-" * 80)

        node_stats = (
            df.group_by("node")
            .agg(
                pl.len().alias("count"),
                pl.col("io_depth").sum().alias("total_sqes"),
                pl.col("io_depth").mean().alias("mean"),
                pl.col("io_depth").median().alias("median"),
                pl.col("io_depth").max().alias("max"),
            )
            .sort("node")
        )

        print(node_stats)

    # Check for low io_depth (potential issue indicator)
    low_depth_count = sum(1 for d in depths if d == 1)
    low_depth_pct = low_depth_count / total * 100 if total > 0 else 0

    print()
    print("-" * 80)
    print("Performance Insights")
    print("-" * 80)

    if low_depth_pct > 50:
        print(
            f"  WARNING: {low_depth_pct:.1f}% of submits have io_depth=1 (low batching)"
        )
        print("  This may indicate suboptimal io_uring utilization.")
        print("  Consider:")
        print("    - Increasing submit_depth threshold")
        print("    - Reducing wait_submit_timeout")
        print("    - Checking if I/O operations are serialized")
    elif low_depth_pct > 30:
        print(f"  Note: {low_depth_pct:.1f}% of submits have io_depth=1")
        print("  Moderate batching efficiency.")
    else:
        print(f"  Good: Only {low_depth_pct:.1f}% of submits have io_depth=1")
        print("  io_uring batching appears efficient.")

    avg_depth = sum(depths) / len(depths) if depths else 0
    if avg_depth < 4:
        print(f"\n  WARNING: Average io_depth is low ({avg_depth:.2f})")
        print("  io_uring is not being used efficiently for batching.")
    else:
        print(f"\n  Average io_depth: {avg_depth:.2f}")


def export_to_csv(entries: list[IoDepthEntry], output_path: Path) -> None:
    """Export entries to CSV file."""
    df = pl.DataFrame(
        {
            "node": [e.node for e in entries],
            "timestamp": [e.timestamp for e in entries],
            "io_depth": [e.io_depth for e in entries],
        }
    )
    df.write_csv(output_path)
    print(f"\nExported {len(entries)} entries to {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Analyze io_uring submit io_depth logs")
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
