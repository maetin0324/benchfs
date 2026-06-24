#!/usr/bin/env bash
# 4-way ablation submitter: (locusta, UCX) × (iouring, posix) @ 10 phys.
# Each variant is submitted as an independent PBS job; the cluster runs them
# serially because of `place=exclhost` on the same select=40 vnode set.

set -euo pipefail
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Common knobs — match the 10-phys sub34 (29392, TOTAL 376.35) baseline.
common_env=(
  PBS_QUEUE=gen
  PBS_ACCOUNT=NBBG
  SELECT_NODES=40
  ELAPSTIM_REQ=02:00:00
  CLIENT_NNODES=10
  BEST_PPN=20
  BEST_BLOCK=900g
  BENCHFS_METADATA_XATTR_SIZE=true
  BENCHFS_CENTRAL_PARENT_INDEX=1
  BENCHFS_LOCUSTA_ARENA_SIZE=268435456
  BENCHFS_LOCUSTA_MAX_INFLIGHT=128
  BENCHFS_LOCUSTA_PRE_ALLOC_PEER=64
  BENCHFS_LOCUSTA_HANDSHAKE=udp
  BENCHFS_RPC_TIMEOUT=120
  BENCHFS_IOURING_SQ_POLL_MS=0
  SKIP_SWEEP=1
  FINAL_STONEWALL=300
  IO500_IOR_HARD_SEGMENTS=1200000
  IO500_MDTEST_EASY_N=1000000
  IO500_MDTEST_HARD_N=400000
  IO500_RND4K_RUN=TRUE
  PMIX_MCA_gds=hash
)

submit_variant() {
  local net="$1"        # locusta or ucx
  local store="$2"      # iouring or posix
  local label="abl_${net}_${store}"
  echo
  echo "=== Submitting ${label} ==="
  local locusta_daemon="0"
  local ucx_daemon="0"
  case "${net}" in
    locusta) locusta_daemon=1 ;;
    ucx)     ucx_daemon=1 ;;
  esac
  env \
    "${common_env[@]}" \
    BENCHFS_BACKEND="${net}" \
    BENCHFS_STORAGE_BACKEND="${store}" \
    BENCHFS_LOCUSTA_STANDALONE_DAEMON="${locusta_daemon}" \
    BENCHFS_UCX_STANDALONE_DAEMON="${ucx_daemon}" \
    LABEL="${label}" \
    bash "${SCRIPT_DIR}/sirius-io500.sh"
}

# Order: locusta first (proven), then UCX (more fragile).
submit_variant locusta iouring
submit_variant locusta posix
submit_variant ucx     iouring
submit_variant ucx     posix
