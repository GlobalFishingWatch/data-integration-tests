#!/usr/bin/env bash
# Remove all dit-snapshot-* local branches from each given project directory.
#
# Usage: scripts/clean-snapshots.sh <project-dir>...

set -euo pipefail

for dir in "$@"; do
    if [ ! -d "$dir/.git" ]; then
        continue
    fi
    cd "$dir"
    branches=$(git for-each-ref --format='%(refname:short)' 'refs/heads/dit-snapshot-*')
    if [ -n "$branches" ]; then
        echo "$dir: removing $branches"
        # shellcheck disable=SC2086
        git branch -D $branches
    fi
    cd - >/dev/null
done
