#!/bin/bash
# Submit the single-node ASAN debug job for mdtest-hard heap corruption.
set -euo pipefail
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE:-$0}" )" &> /dev/null && pwd )"
PROJECT_ROOT="$( cd "${SCRIPT_DIR}/../.." && pwd )"

JOB_FILE="${SCRIPT_DIR}/sirius-io500-asan-debug-job.sh"
TIMESTAMP="$(date +%Y.%m.%d-%H.%M.%S)"
OUTPUT_DIR="${PROJECT_ROOT}/results/io500/${TIMESTAMP}-sirius-asan-debug"

export PROJECT_ROOT
export OUTPUT_DIR
export SCRIPT_DIR

# 1 physical node (4 vnodes). Short walltime — we just want to see the
# first ASAN report.
qsub -q mcrp -A NBB -l select=4 -l place=exclhost \
     -l walltime=00:20:00 \
     -v SCRRAID=no -V "${JOB_FILE}"
