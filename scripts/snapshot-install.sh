#!/usr/bin/env bash
# Snapshot the pipeline checkout's current tracked-files state into a
# pushed snapshot ref (via scripts/snapshot.sh), then `pip install`
# from that ref into the active environment.
#
# Identical tree state -> identical snapshot SHA -> cache hits on repeat runs.
#
# Usage: scripts/snapshot-install.sh <pipeline-name> <pipeline-dir> [pip-extra-args...]

set -euo pipefail

if [ $# -lt 2 ]; then
    echo "usage: $0 <pipeline-name> <pipeline-dir> [pip-extra-args...]" >&2
    exit 2
fi

PIPELINE="$1"
PROJECT_DIR="$2"
shift 2

SCRIPT_DIR=$(dirname "$0")
REF=$("$SCRIPT_DIR/snapshot.sh" "$PIPELINE" "$PROJECT_DIR")

pip install --force-reinstall "$@" "git+file://$PROJECT_DIR@$REF"
