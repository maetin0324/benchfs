#!/usr/bin/env bash
# Point BenchFS at a TOML profile by setting BENCHFS_CONFIG.
#
# Usage:
#   source jobs/profiles/load_profile.sh jobs/profiles/locusta_default.toml
#
# Behavior:
# - Resolves the profile path to an absolute path and exports
#   BENCHFS_CONFIG so benchfsd_mpi / libbenchfs.so pick it up.
# - This script previously parsed the TOML and exported each key as a
#   separate BENCHFS_* env var. That layer was removed when the codebase
#   migrated to single-source-of-truth TOML loading via
#   `RuntimeConfig::global()`.

set -eu

_load_profile_impl() {
  local profile="$1"
  if [[ ! -f "$profile" ]]; then
    echo "load_profile: '$profile' not found" >&2
    return 1
  fi
  # Resolve to absolute path so mpirun -x BENCHFS_CONFIG works from any
  # peer node's CWD.
  local abs
  abs="$(cd "$(dirname "$profile")" && pwd)/$(basename "$profile")"
  export BENCHFS_CONFIG="$abs"
  echo "load_profile: BENCHFS_CONFIG=$abs"
}

# Detect invocation style.
if [[ "${BASH_SOURCE[0]:-}" != "$0" ]]; then
  _load_profile_impl "$1"
else
  _load_profile_impl "$1"
fi
