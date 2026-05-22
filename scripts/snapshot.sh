#!/usr/bin/env bash
# Build a deterministic, orphan snapshot commit of the pipeline checkout's
# current TRACKED-FILES state, push it to origin under
# refs/dit-snapshots/<pipeline>/<commit-short-sha>, and print the resulting
# ref (or HEAD SHA, if the working tree is clean) to stdout.
#
# SECURITY: this script PUSHES TO ORIGIN AUTOMATICALLY. The pipeline repo's
# origin may be a public GitHub repository. Treat every snapshot as if it
# will be publicly visible:
#
#   - Only TRACKED files (modifications + deletions against HEAD) are
#     captured. Files you haven't `git add`-ed are NOT captured. If you
#     keep credentials / .env / one-off datasets as untracked files in the
#     pipeline checkout, they stay out of the snapshot.
#   - If you have already tracked a file that contains secrets (e.g. you
#     once `git commit`-ed a credentials file by mistake), the snapshot
#     WILL include any modification to it. Untrack the file first.
#   - If you've added a NEW file you want included in the snapshot, you
#     must `git commit` it first (no `git add` shortcut — see the docstring
#     below on why). This is the deliberate safety default; the
#     alternative (`git add -A` in the temp index) would automatically
#     push any rogue file in the working tree.
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
# `git status --porcelain` emits one line per modified, staged, OR untracked
# (non-gitignored) path. Empty output = nothing the snapshot would capture.
# Plain `git diff` would miss untracked files and produce the wrong answer
# now that we use `git add -A` in the temp-index dance below.
if [ -z "$(git status --porcelain)" ]; then
    echo "$PARENT_SHA"
    exit 0
fi

# Build a temp index seeded from HEAD; stage tracked-file modifications +
# deletions against it. Keeps the user's real index untouched. `mktemp -t`
# is the portable form (works on macOS where bare `mktemp` errors).
#
# WHY `git add -u` (NOT `-A`): security default. `-A` would push every
# non-gitignored file in the working tree to origin, including any rogue
# `.env`, `sa.json`, downloaded test dataset, or one-off artifact the user
# happens to have lying around. `-u` confines the snapshot to files the
# user has explicitly chosen to track. The cost — silent drop of brand-new
# files — surfaces immediately as a failed/wrong Dataflow run; credential
# leaks may not surface for weeks. We take the louder failure mode.
TMP_INDEX=$(mktemp -t dit-snapshot-index.XXXXXXXX)
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

# Show the user exactly which tracked paths will be in the snapshot.
# Last-chance visual review before the auto-push goes out — a `.env`
# accidentally promoted to tracked status, or a credentials file added in
# error, surfaces here rather than after the fact on origin.
CHANGED_PATHS=$(git diff --name-only HEAD || true)

{
    echo "dit snapshot for $PIPELINE:"
    echo "  ref:       $REF"
    echo "  snapshot:  $SNAPSHOT_SHA"
    echo "  parent:    $PARENT_SHA"
    echo "  pushing tracked-file changes:"
    if [ -n "$CHANGED_PATHS" ]; then
        echo "$CHANGED_PATHS" | sed 's/^/    /'
    else
        echo "    (no tracked-file changes — snapshot tree is identical to HEAD)"
    fi
    echo "  safety notes:"
    echo "    - tracked files only; untracked files NEVER snapshotted (commit first to include)"
    echo "    - SNAPSHOT IS PUSHED TO origin AUTOMATICALLY; origin may be a public repo"
    echo "    - if a path above contains secrets, ABORT (Ctrl-C now) and untrack it"
    echo "    - if your changes touch worker code, also build+push a custom --worker-image"
    echo "    - auto-push requires git-push permission on $PIPELINE's origin"
} >&2

git update-ref "$REF" "$SNAPSHOT_SHA"

# If the remote already has the ref, it must point at the same SHA we just
# computed (the ref is content-addressable). A divergence means either a
# 12-char prefix collision (astronomically rare) or a manual overwrite —
# either way, refusing to silently install from a local-only ref that
# disagrees with origin is the safer failure mode.
REMOTE_SHA=$(git ls-remote origin "$REF" 2>/dev/null | awk '{print $1}')
if [ -n "$REMOTE_SHA" ]; then
    if [ "$REMOTE_SHA" != "$SNAPSHOT_SHA" ]; then
        echo "error: $REF exists on origin at $REMOTE_SHA but local snapshot is $SNAPSHOT_SHA" >&2
        echo "       refusing to install from a local-only ref that disagrees with origin." >&2
        echo "       delete the divergent ref (scripts/clean-snapshot.sh) or use a longer prefix." >&2
        exit 1
    fi
    echo "  (ref already present on origin at the same SHA -- skipping push)" >&2
else
    git push origin "$REF:$REF" >&2
fi

echo "$REF"
