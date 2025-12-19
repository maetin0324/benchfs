#!/usr/bin/env python3
"""
Repair corrupted perf.data files where the data size field is 0.

This typically happens when 'perf record' is terminated without proper cleanup,
leaving the header's data_size field uninitialized (0).

The script:
1. Parses the perf.data header structure
2. Calculates the correct data size based on file size
3. Creates a repaired copy of the file with the correct data_size

Usage:
    python repair_perf_data.py <input_file> [output_file]

If output_file is not specified, it defaults to <input_file>.repaired
"""

import struct
import sys
import os
from pathlib import Path


# perf.data magic number
PERF_MAGIC = b'PERFILE2'

# Header field offsets (all little-endian uint64)
HEADER_MAGIC_OFFSET = 0
HEADER_SIZE_OFFSET = 8
ATTR_SIZE_OFFSET = 16
ATTRS_OFFSET = 24
ATTRS_SIZE_OFFSET = 32
DATA_OFFSET = 40
DATA_SIZE_OFFSET = 48
EVENT_TYPES_OFFSET = 56
EVENT_TYPES_SIZE_OFFSET = 64
FLAGS_OFFSET = 72

# Feature bit definitions (from Linux kernel tools/perf/util/header.h)
HEADER_FEATURES = {
    0: "HEADER_TRACING_DATA",
    1: "HEADER_BUILD_ID",
    2: "HEADER_HOSTNAME",
    3: "HEADER_OSRELEASE",
    4: "HEADER_VERSION",
    5: "HEADER_ARCH",
    6: "HEADER_NRCPUS",
    7: "HEADER_CPUDESC",
    8: "HEADER_CPUID",
    9: "HEADER_TOTAL_MEM",
    10: "HEADER_CMDLINE",
    11: "HEADER_EVENT_DESC",
    12: "HEADER_CPU_TOPOLOGY",
    13: "HEADER_NUMA_TOPOLOGY",
    14: "HEADER_BRANCH_STACK",
    15: "HEADER_PMU_MAPPINGS",
    16: "HEADER_GROUP_DESC",
    17: "HEADER_AUXTRACE",
    18: "HEADER_STAT",
    19: "HEADER_CACHE",
    20: "HEADER_SAMPLE_TIME",
    21: "HEADER_MEM_TOPOLOGY",
    22: "HEADER_CLOCKID",
    23: "HEADER_DIR_FORMAT",
    24: "HEADER_BPF_PROG_INFO",
    25: "HEADER_BPF_BTF",
    26: "HEADER_COMPRESSED",
    27: "HEADER_CPU_PMU_CAPS",
    28: "HEADER_CLOCK_DATA",
    29: "HEADER_HYBRID_TOPOLOGY",
    30: "HEADER_PMU_CAPS",
}


def read_uint64(data, offset):
    """Read a little-endian uint64 from data at offset."""
    return struct.unpack('<Q', data[offset:offset+8])[0]


def write_uint64(data, offset, value):
    """Write a little-endian uint64 to data at offset."""
    packed = struct.pack('<Q', value)
    data[offset:offset+8] = packed


def parse_header(data):
    """Parse the perf.data header and return a dict of fields."""
    magic = data[HEADER_MAGIC_OFFSET:HEADER_MAGIC_OFFSET+8]
    if magic != PERF_MAGIC:
        raise ValueError(f"Invalid magic: {magic}, expected {PERF_MAGIC}")

    header = {
        'magic': magic,
        'header_size': read_uint64(data, HEADER_SIZE_OFFSET),
        'attr_size': read_uint64(data, ATTR_SIZE_OFFSET),
        'attrs_offset': read_uint64(data, ATTRS_OFFSET),
        'attrs_size': read_uint64(data, ATTRS_SIZE_OFFSET),
        'data_offset': read_uint64(data, DATA_OFFSET),
        'data_size': read_uint64(data, DATA_SIZE_OFFSET),
        'event_types_offset': read_uint64(data, EVENT_TYPES_OFFSET),
        'event_types_size': read_uint64(data, EVENT_TYPES_SIZE_OFFSET),
        'flags': read_uint64(data, FLAGS_OFFSET),
    }

    # Read additional flags if present (flags0, flags1 at offset 80, 88)
    if len(data) >= 96:
        header['flags0'] = read_uint64(data, 80)
        header['flags1'] = read_uint64(data, 88)

    return header


def get_enabled_features(flags):
    """Get list of enabled features from flags."""
    features = []
    for bit, name in HEADER_FEATURES.items():
        if flags & (1 << bit):
            features.append(name)
    return features


def find_feature_section_start(file_path, data_offset, file_size):
    """
    Try to find where the feature sections start by looking for patterns.

    Feature sections are stored after the data section. Each feature has
    a perf_file_section header (offset + size, 16 bytes each).

    This is a heuristic approach - we scan backwards from end of file
    looking for the feature section headers.
    """
    # For a simple fix, assume all data from data_offset to end of file is data
    # This works if there are no feature sections, or if they're corrupted too
    return file_size


def calculate_data_size(file_path, header):
    """Calculate the correct data size."""
    file_size = os.path.getsize(file_path)
    data_offset = header['data_offset']

    # Simple approach: assume data extends to end of file
    # This is correct if perf didn't write feature sections
    # (which is likely the case if it was terminated abnormally)
    data_size = file_size - data_offset

    print(f"  File size: {file_size:,} bytes")
    print(f"  Data offset: {data_offset:,} bytes (0x{data_offset:x})")
    print(f"  Calculated data size: {data_size:,} bytes")

    return data_size


def repair_perf_data(input_path, output_path=None):
    """Repair a corrupted perf.data file."""
    input_path = Path(input_path)
    if output_path is None:
        output_path = input_path.with_suffix('.data.repaired')
    else:
        output_path = Path(output_path)

    print(f"Repairing: {input_path}")
    print(f"Output: {output_path}")
    print()

    # Read the header (first 256 bytes should be enough)
    with open(input_path, 'rb') as f:
        header_data = bytearray(f.read(256))

    # Parse header
    try:
        header = parse_header(header_data)
    except ValueError as e:
        print(f"Error: {e}")
        return False

    print("Current header values:")
    print(f"  Magic: {header['magic']}")
    print(f"  Header size: {header['header_size']} bytes")
    print(f"  Attr size: {header['attr_size']} bytes")
    print(f"  Attrs offset: {header['attrs_offset']} (0x{header['attrs_offset']:x})")
    print(f"  Attrs size: {header['attrs_size']} bytes")
    print(f"  Data offset: {header['data_offset']} (0x{header['data_offset']:x})")
    print(f"  Data size: {header['data_size']} bytes (0x{header['data_size']:x})")
    print(f"  Event types offset: {header['event_types_offset']}")
    print(f"  Event types size: {header['event_types_size']}")
    print(f"  Flags: 0x{header['flags']:x}")
    print()

    # Show enabled features
    features = get_enabled_features(header['flags'])
    print(f"  Enabled features ({len(features)}):")
    for f in features[:10]:  # Show first 10
        print(f"    - {f}")
    if len(features) > 10:
        print(f"    ... and {len(features) - 10} more")
    print()

    if header['data_size'] != 0:
        print("Warning: data_size is not 0, file may not be corrupted in the expected way")
        response = input("Continue anyway? [y/N]: ")
        if response.lower() != 'y':
            return False

    # Calculate correct data size
    print("Calculating correct data size...")
    new_data_size = calculate_data_size(input_path, header)

    if new_data_size <= 0:
        print(f"Error: Calculated data size is invalid: {new_data_size}")
        return False

    print()
    print(f"Fixing data_size: {header['data_size']} -> {new_data_size}")

    # Update the header
    write_uint64(header_data, DATA_SIZE_OFFSET, new_data_size)

    # Verify the fix
    fixed_header = parse_header(header_data)
    print(f"Verified new data_size: {fixed_header['data_size']}")
    print()

    # Write the repaired file
    print(f"Writing repaired file to: {output_path}")

    # Copy the file with the fixed header
    with open(input_path, 'rb') as src:
        with open(output_path, 'wb') as dst:
            # Write fixed header
            dst.write(header_data[:256])

            # Skip the original header and copy the rest
            src.seek(256)

            # Copy in chunks
            chunk_size = 64 * 1024 * 1024  # 64MB chunks
            total_copied = 256
            file_size = os.path.getsize(input_path)

            while True:
                chunk = src.read(chunk_size)
                if not chunk:
                    break
                dst.write(chunk)
                total_copied += len(chunk)
                progress = total_copied / file_size * 100
                print(f"\r  Progress: {progress:.1f}% ({total_copied:,} / {file_size:,} bytes)", end='')

            print()

    print()
    print("Repair complete!")
    print()
    print("To verify, run:")
    print(f"  perf report -i '{output_path}' --stdio | head -50")

    return True


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) > 2 else None

    if not os.path.exists(input_path):
        print(f"Error: File not found: {input_path}")
        sys.exit(1)

    success = repair_perf_data(input_path, output_path)
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
