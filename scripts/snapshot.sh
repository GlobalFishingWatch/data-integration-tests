#!/usr/bin/env bash
# Build a deterministic, orphan snapshot commit of the pipeline checkout's
# current tracked-files state, push it to origin under
# refs/dit-snapshots/<pipeline>/<commit-short-sha>, and print the resulting
# ref (or HEAD SHA, if the working tree is clean) to stdout.
#
# Properties:
#   - Identical tree state -> identical commit SHA -> idempotent push.
#   - Orphan commit (no parent) so the SHA is purely a function of the tree,
#     not the user's branch history. Rebasing the user's branch doesn't
#     invalidate the cache; pushing the snapshot doesn't propagate any
#     unpushed HEAD ancestors via git's reachability rules.
#   - Parent SHA is recorded in the commit message ("dit snapshot of <sha>")
#     so the reproduce context survives even if the snapshot ref is later
#     deleted (e.g. by scripts/clean-snapshot.sh for secret remediation).
#
# Usage: scripts/snapshot.sh <pipeline-name> <pipeline-dir>

set -euo pipefail

if [ $# -ne 2 ]; then
    echo "usage: $0 <pipeline-name> <pipeline-dir>" >&2
    exit 2
fi

PIPELINE="$1"
PROJECT_DIR="$2"

if [ ! -d "$PROJECT_DIR/.git" ]; then
    echo "error: $PROJECT_DIR is not a git repository" >&2
    exit 2
fi

cd "$PROJECT_DIR"

PARENT_SHA=$(git rev-parse HEAD)

# Clean tree -> nothing to snapshot; return HEAD directly.
if git diff --quiet && git diff --cached --quiet; then
    echo "$PARENT_SHA"
    exit 0
fi

# Build a temp index seeded from HEAD; stage tracked-file modifications
# against it (same dance `git stash create` does internally). Keeps the
# user's real index untouched.
TMP_INDEX=$(mktemp)
trap 'rm -f "$TMP_INDEX"' EXIT

GIT_INDEX_FILE="$TMP_INDEX" git read-tree HEAD
GIT_INDEX_FILE="$TMP_INDEX" git add -u
TREE_SHA=$(GIT_INDEX_FILE="$TMP_INDEX" git write-tree)

# Frozen author/committer dates AND identities + --no-gpg-sign:
# commit SHA must be a pure function of the tree for content-addressability.
SNAPSHOT_SHA=$(
    GIT_AUTHOR_DATE="1970-01-01T00:00:00Z" \
    GIT_COMMITTER_DATE="1970-01-01T00:00:00Z" \
    GIT_AUTHOR_NAME=dit \
    GIT_AUTHOR_EMAIL=dit@local \
    GIT_COMMITTER_NAME=dit \
    GIT_COMMITTER_EMAIL=dit@local \
    git -c commit.gpgsign=false commit-tree --no-gpg-sign \
        -m "dit snapshot of $PARENT_SHA" \
        "$TREE_SHA"
)

REF="refs/dit-snapshots/$PIPELINE/${SNAPSHOT_SHA:0:12}"

{
    echo "dit snapshot for $PIPELINE:"
    echo "  ref:       $REF"
    echo "  snapshot:  $SNAPSHOT_SHA"
    echo "  parent:    $PARENT_SHA"
    echo "  caveats:"
    echo "    - tracked files only; 'git add -A' first if untracked files matter"
    echo "    - this run writes unreviewed_code=TRUE to dit_runs"
    echo "    - if your changes touch worker code, also build+push a custom --worker-image"
    echo "    - auto-push requires git-push permission on $PIPELINE's origin"
} >&2

git update-ref "$REF" "$SNAPSHOT_SHA"

if git ls-remote --exit-code origin "$REF" >/dev/null 2>&1; then
    echo "  (ref already present on origin -- skipping push)" >&2
else
    git push origin "$REF:$REF" >&2
fi

echo "$REF"
