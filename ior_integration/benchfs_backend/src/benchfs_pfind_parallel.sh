#!/bin/bash
# benchfs_pfind_parallel.sh — io500 external-script wrapper that fans out
# multiple benchfs_pfind copies across cluster nodes for parallel
# directory traversal.
#
# io500 calls this once (from its rank 0 popen). We launch N copies via
# ssh on the first N distinct hosts in PBS_NODEFILE, partition the work
# by `BENCHFS_PFIND_RANK / BENCHFS_PFIND_SIZE`, and aggregate the
# `MATCHED <hits>/<total>` outputs back into a single line.
#
# Why ssh instead of mpirun: nested mpirun inside io500's outer mpirun
# context triggers OpenMPI PMIx deadlock (see pfind.c comment). ssh
# launches each pfind as a plain child process — no PMIx, no nested
# wireup. Each pfind connects to its co-located BenchFS daemon via the
# standalone-daemon SHM (BENCHFS_LOCUSTA_DAEMON_NAME).
#
# Required env (inherited from io500 mpirun):
#   PBS_NODEFILE                 — list of all vnodes (one per line)
#   BENCHFS_REGISTRY_DIR         — locusta registry
#   BENCHFS_LOCUSTA_STANDALONE_DAEMON=1
#   BENCHFS_SERVERS_PER_HOST     — vnodes per host (for SHM name)
#
# Optional:
#   BENCHFS_PFIND_NRANKS         — number of parallel pfind processes
#                                  (default: min(10, distinct hosts))
#   BENCHFS_PFIND_BIN            — path to benchfs_pfind binary
#                                  (default: <script_dir>/../bin/benchfs_pfind)
#   BENCHFS_PFIND_TMPDIR         — where to stash per-rank output
#                                  (default: /tmp)

set -u

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# Look in ../bin first (relative to this script in src/), fall back to script_dir
PFIND_BIN_DEFAULT="$SCRIPT_DIR/../bin/benchfs_pfind"
if [ ! -x "$PFIND_BIN_DEFAULT" ]; then
    PFIND_BIN_DEFAULT="$SCRIPT_DIR/benchfs_pfind"
fi
PFIND_BIN="${BENCHFS_PFIND_BIN:-$PFIND_BIN_DEFAULT}"
TMPDIR_BASE="${BENCHFS_PFIND_TMPDIR:-/tmp}"

if [ ! -x "$PFIND_BIN" ]; then
    echo "ERROR: pfind binary not found at $PFIND_BIN" >&2
    exit 1
fi
if [ -z "${PBS_NODEFILE:-}" ]; then
    echo "ERROR: PBS_NODEFILE not set" >&2
    exit 1
fi

# One unique host per phys node (PBS_NODEFILE has one entry per vnode,
# but vnodes on the same phys share hostname).
mapfile -t HOSTS < <(sort -u "$PBS_NODEFILE")
NHOSTS=${#HOSTS[@]}
NRANKS="${BENCHFS_PFIND_NRANKS:-$NHOSTS}"
if [ "$NRANKS" -gt "$NHOSTS" ]; then
    NRANKS="$NHOSTS"
fi

# Stash output dir unique per invocation. Keep on failure so we can
# inspect each rank's stderr — io500 retries the find phase otherwise
# and the original hang context vanishes.
RUN_ID="$$_$(date +%s%N)"
WORK_DIR="${BENCHFS_PFIND_KEEPDIR:-$TMPDIR_BASE/benchfs_pfind_$RUN_ID}"
mkdir -p "$WORK_DIR"
PFIND_PERSIST_LOGS="${BENCHFS_PFIND_PERSIST_LOGS:-1}"
if [ "$PFIND_PERSIST_LOGS" = "0" ]; then
    trap 'rm -rf "$WORK_DIR" 2>/dev/null || true' EXIT
fi
echo "[pfind-parallel] WORK_DIR=$WORK_DIR" >&2

echo "[pfind-parallel] launching $NRANKS ranks across hosts: ${HOSTS[*]:0:$NRANKS}" >&2

ENV_VARS=(
    "BENCHFS_REGISTRY_DIR=${BENCHFS_REGISTRY_DIR:-}"
    "BENCHFS_LOCUSTA_STANDALONE_DAEMON=${BENCHFS_LOCUSTA_STANDALONE_DAEMON:-0}"
    "BENCHFS_UCX_STANDALONE_DAEMON=${BENCHFS_UCX_STANDALONE_DAEMON:-0}"
    "BENCHFS_SERVERS_PER_HOST=${BENCHFS_SERVERS_PER_HOST:-1}"
    "BENCHFS_CONFIG=${BENCHFS_CONFIG:-}"
    "LD_LIBRARY_PATH=${LD_LIBRARY_PATH:-}"
    "PATH=$PATH"
)

# Pick the SHM-name prefix that matches whichever daemon was started.
# UCX daemon mode (2026-06-01): /benchfs_ucx_daemon_<host>_<vlr>
# locusta daemon mode: /benchfs_daemon_<host>_<vlr>
DAEMON_PREFIX="benchfs_daemon"
DAEMON_NAME_VAR="BENCHFS_LOCUSTA_DAEMON_NAME"
if [ "${BENCHFS_UCX_STANDALONE_DAEMON:-0}" = "1" ]; then
    DAEMON_PREFIX="benchfs_ucx_daemon"
    DAEMON_NAME_VAR="BENCHFS_UCX_DAEMON_NAME"
fi

# Launch each rank on its host via ssh in background.
PIDS=()
for ((i=0; i<NRANKS; i++)); do
    HOST="${HOSTS[$i]}"
    OUT="$WORK_DIR/pfind.$i.out"
    ERR="$WORK_DIR/pfind.$i.err"
    DAEMON_NAME="/${DAEMON_PREFIX}_$(echo "$HOST" | cut -d. -f1)_0"
    (
        ssh -o BatchMode=yes -o StrictHostKeyChecking=no "$HOST" \
            "env ${ENV_VARS[*]} ${DAEMON_NAME_VAR}=$DAEMON_NAME BENCHFS_PFIND_RANK=$i BENCHFS_PFIND_SIZE=$NRANKS '$PFIND_BIN' $*" \
            > "$OUT" 2> "$ERR"
    ) &
    PIDS+=($!)
done

# Wait for all with a hard cap so a single stuck pfind rank can't
# block the entire io500 sequence. The find phase is allowed to skew
# results but it must not stall the rest of the test.
FAIL=0
PFIND_DEADLINE_SEC="${BENCHFS_PFIND_DEADLINE_SEC:-300}"
START_TS=$(date +%s)
for pid in "${PIDS[@]}"; do
    REMAINING=$(( PFIND_DEADLINE_SEC - ( $(date +%s) - START_TS ) ))
    if [ "$REMAINING" -le 0 ]; then
        REMAINING=1
    fi
    if ! timeout "${REMAINING}s" tail --pid="$pid" -f /dev/null 2>/dev/null; then
        echo "[pfind-parallel] pid=$pid timeout after ${PFIND_DEADLINE_SEC}s, killing" >&2
        kill -9 "$pid" 2>/dev/null || true
        FAIL=1
        continue
    fi
    if ! wait "$pid"; then
        FAIL=1
    fi
done

# Aggregate MATCHED <hits>/<total> from each rank's output
TOTAL_HITS=0
TOTAL_FILES=0
NMATCHED=0
for ((i=0; i<NRANKS; i++)); do
    OUT="$WORK_DIR/pfind.$i.out"
    if [ ! -s "$OUT" ]; then
        echo "[pfind-parallel] rank $i (${HOSTS[$i]}): no output" >&2
        cat "$WORK_DIR/pfind.$i.err" >&2 2>/dev/null
        continue
    fi
    LINE=$(grep "^MATCHED " "$OUT" | tail -1)
    if [ -z "$LINE" ]; then
        echo "[pfind-parallel] rank $i (${HOSTS[$i]}): no MATCHED line" >&2
        cat "$WORK_DIR/pfind.$i.err" >&2 2>/dev/null | head -5
        continue
    fi
    HITS=$(echo "$LINE" | awk -F'/' '{print $1}' | awk '{print $2}')
    TOTAL=$(echo "$LINE" | awk -F'/' '{print $2}')
    TOTAL_HITS=$((TOTAL_HITS + HITS))
    TOTAL_FILES=$((TOTAL_FILES + TOTAL))
    NMATCHED=$((NMATCHED + 1))
done

if [ "$NMATCHED" -eq 0 ]; then
    echo "ERROR: no ranks produced MATCHED output" >&2
    exit 1
fi

echo "[pfind-parallel] aggregated from $NMATCHED/$NRANKS ranks: hits=$TOTAL_HITS total=$TOTAL_FILES" >&2
# io500 parses this last line: must be "MATCHED <hits>/<total>".
echo "MATCHED $TOTAL_HITS/$TOTAL_FILES"
exit $FAIL
