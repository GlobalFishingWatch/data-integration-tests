#!/usr/bin/env bash
# Surgical removal of a single snapshot ref locally and on origin.
#
# Intended only for secret-leak remediation -- snapshots normally live forever
# by design (bytes-scale storage in a hidden namespace).
#
# Each snapshot commit's message records the parent SHA as
#   "dit snapshot of <40-char-sha>"
# so `git show <snapshot>` preserves the reproduce context even before the
# `dit_runs.pipeline_commit_parent` column (M-pivot-3) lands.
#
# Usage: scripts/clean-snapshot.sh <pipeline-dir> <ref>
#
# <ref> may be:
#   - full:  refs/dit-snapshots/<pipeline>/<sha>
#   - short: exactly 12 hex chars (the script deduces the pipeline from the
#            pipeline-dir's basename; any other shape is rejected to guard
#            against accidental deletion).

set -euo pipefail

if [ $# -ne 2 ]; then
    echo "usage: $0 <pipeline-dir> <ref>" >&2
    exit 2
fi

PROJECT_DIR="$1"
REF="$2"

if [ ! -d "$PROJECT_DIR/.git" ]; then
    echo "error: $PROJECT_DIR is not a git repository" >&2
    exit 2
fi

cd "$PROJECT_DIR"

case "$REF" in
    refs/dit-snapshots/*)
        FULL_REF="$REF"
        ;;
    *)
        # Short form must be exactly 12 hex chars -- this is what snapshot.sh
        # emits. Reject anything else to make accidental deletion via typo or
        # branch-name harder.
        if ! [[ "$REF" =~ ^[0-9a-f]{12}$ ]]; then
            echo "error: short REF must be exactly 12 hex chars (got: $REF)" >&2
            echo "       or pass the full ref: refs/dit-snapshots/<pipeline>/<sha>" >&2
            exit 2
        fi
        FULL_REF="refs/dit-snapshots/$(basename "$PROJECT_DIR")/$REF"
        ;;
esac

REMOVED_LOCAL=0
REMOVED_REMOTE=0

if git rev-parse --verify --quiet "$FULL_REF" >/dev/null; then
    git update-ref -d "$FULL_REF"
    REMOVED_LOCAL=1
fi

if git ls-remote --exit-code origin "$FULL_REF" >/dev/null 2>&1; then
    git push origin ":$FULL_REF"
    REMOVED_REMOTE=1
fi

echo "$FULL_REF: removed-local=$REMOVED_LOCAL removed-remote=$REMOVED_REMOTE"
