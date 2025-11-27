#!/usr/bin/env python3
"""
Analyze BenchFS IOR benchmark results.

This script parses IOR JSON output files and generates summary reports.
"""

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import List, Dict, Any, Optional


def fix_ior_json(content: str) -> str:
    """Fix common IOR JSON output issues."""
    content = re.sub(r'\]\s*\]\s*,\s*"max":', r'], "max":', content)
    content = re.sub(r'\}\s*\]\s*\]\s*,\s*"summary":', r'}], "summary":', content)
    content = re.sub(r',\s*\}', '}', content)
    content = re.sub(r',\s*\]', ']', content)
    return content


def load_ior_json(filepath: Path) -> Optional[Dict[str, Any]]:
    """Load and fix IOR JSON file."""
    try:
        with open(filepath, 'r') as f:
            content = f.read()
        fixed = fix_ior_json(content)
        return json.loads(fixed)
    except (json.JSONDecodeError, FileNotFoundError) as e:
        print(f"Warning: Failed to parse {filepath}: {e}", file=sys.stderr)
        return None


def load_job_metadata(filepath: Path) -> Optional[Dict[str, Any]]:
    """Load job metadata JSON file."""
    try:
        with open(filepath, 'r') as f:
            return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        return None


def extract_results(ior_data: Dict[str, Any], metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Extract key metrics from IOR data."""
    result = {
        "version": ior_data.get("Version", "unknown"),
        "began": ior_data.get("Began", "unknown"),
    }

    # Extract from summary
    if "summary" in ior_data:
        for entry in ior_data["summary"]:
            op = entry.get("operation", "")
            if op == "write":
                result["write_MiBps"] = entry.get("bwMaxMIB", 0)
                result["write_iops"] = entry.get("OPsMax", 0)
            elif op == "read":
                result["read_MiBps"] = entry.get("bwMaxMIB", 0)
                result["read_iops"] = entry.get("OPsMax", 0)

            # Get common parameters from first entry
            if "numTasks" not in result:
                result["numTasks"] = entry.get("numTasks", 0)
                result["tasksPerNode"] = entry.get("tasksPerNode", 0)
                result["blockSize"] = entry.get("blockSize", 0)
                result["transferSize"] = entry.get("transferSize", 0)

    # Add metadata if available
    if metadata:
        result["nnodes"] = metadata.get("nnodes", 0)
        result["benchfs_chunk_size"] = metadata.get("benchfs_chunk_size", 0)
        result["runid"] = metadata.get("runid", 0)
        result["jobid"] = metadata.get("jobid", "")

    return result


def find_result_files(base_dir: Path) -> List[Dict[str, Path]]:
    """Find all IOR result files in the directory structure."""
    results = []

    for job_dir in sorted(base_dir.glob("*-*-*")):  # TIMESTAMP-JOBID-NNODES pattern
        if not job_dir.is_dir():
            continue

        ior_results_dir = job_dir / "ior_results"
        if not ior_results_dir.exists():
            continue

        for ior_file in sorted(ior_results_dir.glob("ior_result_*.json")):
            # Extract runid from filename
            match = re.search(r'ior_result_(\d+)\.json$', ior_file.name)
            if not match:
                continue

            runid = match.group(1)
            metadata_file = job_dir / f"job_metadata_{runid}.json"

            results.append({
                "ior_file": ior_file,
                "metadata_file": metadata_file if metadata_file.exists() else None,
                "job_dir": job_dir,
            })

    return results


def print_table(results: List[Dict[str, Any]], format: str = "table"):
    """Print results in specified format."""
    if not results:
        print("No results found.")
        return

    if format == "json":
        print(json.dumps(results, indent=2))
        return

    if format == "csv":
        # CSV header
        keys = ["jobid", "runid", "nnodes", "numTasks", "tasksPerNode",
                "blockSize", "transferSize", "benchfs_chunk_size",
                "write_MiBps", "read_MiBps", "write_iops", "read_iops"]
        print(",".join(keys))
        for r in results:
            print(",".join(str(r.get(k, "")) for k in keys))
        return

    # Table format
    print("\n" + "=" * 100)
    print("BenchFS IOR Benchmark Results")
    print("=" * 100)
    print(f"{'JobID':<12} {'Run':>4} {'Nodes':>5} {'Tasks':>6} {'PPN':>4} "
          f"{'Block':>8} {'Xfer':>6} {'Chunk':>8} "
          f"{'Write(MiB/s)':>14} {'Read(MiB/s)':>14}")
    print("-" * 100)

    for r in results:
        block_mb = r.get("blockSize", 0) / (1024 * 1024)
        xfer_mb = r.get("transferSize", 0) / (1024 * 1024)
        chunk_mb = r.get("benchfs_chunk_size", 0) / (1024 * 1024)

        print(f"{str(r.get('jobid', ''))[:12]:<12} "
              f"{r.get('runid', 0):>4} "
              f"{r.get('nnodes', 0):>5} "
              f"{r.get('numTasks', 0):>6} "
              f"{r.get('tasksPerNode', 0):>4} "
              f"{block_mb:>7.0f}M "
              f"{xfer_mb:>5.0f}M "
              f"{chunk_mb:>7.0f}M "
              f"{r.get('write_MiBps', 0):>14.2f} "
              f"{r.get('read_MiBps', 0):>14.2f}")

    print("=" * 100)

    # Summary
    if len(results) > 1:
        write_vals = [r.get("write_MiBps", 0) for r in results if r.get("write_MiBps")]
        read_vals = [r.get("read_MiBps", 0) for r in results if r.get("read_MiBps")]

        if write_vals:
            print(f"\nWrite: min={min(write_vals):.2f}, max={max(write_vals):.2f}, "
                  f"avg={sum(write_vals)/len(write_vals):.2f} MiB/s")
        if read_vals:
            print(f"Read:  min={min(read_vals):.2f}, max={max(read_vals):.2f}, "
                  f"avg={sum(read_vals)/len(read_vals):.2f} MiB/s")


def main():
    parser = argparse.ArgumentParser(description="Analyze BenchFS IOR benchmark results")
    parser.add_argument("path", nargs="?", default=".",
                        help="Path to results directory or specific IOR JSON file")
    parser.add_argument("-f", "--format", choices=["table", "json", "csv"],
                        default="table", help="Output format")
    parser.add_argument("--fix-only", action="store_true",
                        help="Only fix JSON files in place, don't analyze")
    args = parser.parse_args()

    path = Path(args.path)

    if args.fix_only:
        # Fix JSON files in place
        if path.is_file():
            files = [path]
        else:
            files = list(path.rglob("ior_result_*.json"))

        for f in files:
            with open(f, 'r') as fp:
                content = fp.read()
            fixed = fix_ior_json(content)
            try:
                # Validate and pretty print
                data = json.loads(fixed)
                with open(f, 'w') as fp:
                    json.dump(data, fp, indent=2)
                print(f"Fixed: {f}")
            except json.JSONDecodeError as e:
                print(f"Failed to fix {f}: {e}", file=sys.stderr)
        return

    # Analyze results
    if path.is_file():
        # Single file
        ior_data = load_ior_json(path)
        if ior_data:
            result = extract_results(ior_data)
            print_table([result], args.format)
    else:
        # Directory
        file_infos = find_result_files(path)
        if not file_infos:
            # Try looking in subdirectories
            for subdir in path.iterdir():
                if subdir.is_dir():
                    file_infos.extend(find_result_files(subdir))

        results = []
        for info in file_infos:
            ior_data = load_ior_json(info["ior_file"])
            metadata = load_job_metadata(info["metadata_file"]) if info["metadata_file"] else None

            if ior_data:
                result = extract_results(ior_data, metadata)
                results.append(result)

        print_table(results, args.format)


if __name__ == "__main__":
    main()
