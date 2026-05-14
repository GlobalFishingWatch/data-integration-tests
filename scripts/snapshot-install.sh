#!/usr/bin/env bash
# Snapshot tracked changes of a pipeline checkout via `git stash create`
# (working tree untouched), anchor on a dit-snapshot-<epoch> branch so git
# GC won't reclaim it, then `pip install --force-reinstall` from that ref.
#
# Untracked files are NOT captured (git stash create limitation). Run
# `git add -A` in the pipeline repo first if you need them in the snapshot.
#
# Usage: scripts/snapshot-install.sh <project-dir> [pip-extra-args...]

set -euo pipefail

if [ $# -lt 1 ]; then
    echo "usage: $0 <project-dir> [pip-extra-args...]" >&2
    exit 2
fi

PROJECT_DIR="$1"
shift

if [ ! -d "$PROJECT_DIR/.git" ]; then
    echo "error: $PROJECT_DIR is not a git repository" >&2
    exit 2
fi

ORIG_PWD=$(pwd)
cd "$PROJECT_DIR"

STASH=$(git stash create "dit-snapshot" 2>/dev/null || true)
if [ -n "$STASH" ]; then
    REF="$STASH"
    BR="dit-snapshot-$(date +%s)"
    git branch "$BR" "$STASH"
    echo "snapshotted $PROJECT_DIR to branch: $BR ($REF)"
else
    REF=$(git rev-parse HEAD)
    echo "$PROJECT_DIR working tree clean; using HEAD: $REF"
fi

cd "$ORIG_PWD"

pip install --force-reinstall "$@" "git+file://$PROJECT_DIR@$REF"
