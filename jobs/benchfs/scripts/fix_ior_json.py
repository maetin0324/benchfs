#!/usr/bin/env python3
"""
Fix IOR JSON output format issues.

IOR 4.0.0 has a bug in JSON output where the Results array has an extra closing bracket.
This script fixes the JSON to make it valid.
"""

import sys
import re
import json


def fix_ior_json(content: str) -> str:
    """Fix common IOR JSON output issues."""

    # Issue 1: Double closing bracket after Results array: ]] -> ]
    # Pattern: "Results": [ ... ]] , "max":
    content = re.sub(r'\]\s*\]\s*,\s*"max":', r'], "max":', content)

    # Issue 2: Extra closing bracket at the end of tests array
    # Pattern: }]] , "summary": -> }] , "summary":
    content = re.sub(r'\}\s*\]\s*\]\s*,\s*"summary":', r'}], "summary":', content)

    # Issue 3: Trailing comma before closing brace
    content = re.sub(r',\s*\}', '}', content)

    # Issue 4: Trailing comma before closing bracket
    content = re.sub(r',\s*\]', ']', content)

    return content


def main():
    if len(sys.argv) < 2:
        print("Usage: fix_ior_json.py <input.json> [output.json]", file=sys.stderr)
        print("       If output is not specified, prints to stdout", file=sys.stderr)
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None

    with open(input_file, 'r') as f:
        content = f.read()

    fixed_content = fix_ior_json(content)

    # Validate the fixed JSON
    try:
        data = json.loads(fixed_content)
        # Pretty print if valid
        fixed_content = json.dumps(data, indent=2)
    except json.JSONDecodeError as e:
        print(f"Warning: JSON still invalid after fix: {e}", file=sys.stderr)

    if output_file:
        with open(output_file, 'w') as f:
            f.write(fixed_content)
        print(f"Fixed JSON written to {output_file}", file=sys.stderr)
    else:
        print(fixed_content)


if __name__ == '__main__':
    main()
