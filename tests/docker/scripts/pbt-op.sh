#!/bin/bash
# Property-based test file operation script
# This script executes file operations on BenchFS for PBT testing

set -e

BENCHFS_MOUNT="/benchfs"
BENCHFS_CLIENT="/usr/local/bin/benchfs_client"

# Ensure mount directory exists
mkdir -p "$BENCHFS_MOUNT"

op="$1"
shift

case "$op" in
    create)
        path="$1"
        # Create parent directories if needed
        dir=$(dirname "$path")
        mkdir -p "$dir" 2>/dev/null || true
        # Create empty file
        touch "$path"
        echo "OK"
        ;;

    write)
        path="$1"
        offset="$2"
        data_b64="$3"
        # Decode base64 data and write at offset
        dir=$(dirname "$path")
        mkdir -p "$dir" 2>/dev/null || true
        # Use dd to write at specific offset
        echo "$data_b64" | base64 -d | dd of="$path" bs=1 seek="$offset" conv=notrunc 2>/dev/null
        echo "OK"
        ;;

    read)
        path="$1"
        offset="$2"
        length="$3"
        # Read data at offset and encode as base64
        if [ -f "$path" ]; then
            dd if="$path" bs=1 skip="$offset" count="$length" 2>/dev/null | base64 -w 0
        else
            echo ""
        fi
        ;;

    delete)
        path="$1"
        rm -f "$path"
        echo "OK"
        ;;

    truncate)
        path="$1"
        size="$2"
        truncate -s "$size" "$path"
        echo "OK"
        ;;

    rename)
        old_path="$1"
        new_path="$2"
        # Create parent directory for new path
        dir=$(dirname "$new_path")
        mkdir -p "$dir" 2>/dev/null || true
        mv "$old_path" "$new_path"
        echo "OK"
        ;;

    stat)
        path="$1"
        if [ -f "$path" ]; then
            stat -c %s "$path"
        else
            echo "0"
            exit 1
        fi
        ;;

    *)
        echo "Unknown operation: $op" >&2
        exit 1
        ;;
esac
