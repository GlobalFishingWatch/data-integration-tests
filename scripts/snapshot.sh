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
#   - The capture boundary is "files already tracked in HEAD". The temp
#     index is seeded from HEAD; `git add -u` against that index updates
#     entries that exist in HEAD with their current working-tree content
#     (or removes them if deleted). New files are NOT in HEAD, so they
#     don't enter the temp index, so they don't enter the snapshot —
#     regardless of whether they're `git add`-ed in the user's real index.
#   - Practical consequence: credentials/.env/one-off datasets you keep
#     untracked stay out of the snapshot. To include a new file, you must
#     `git commit` it first — the staging step alone isn't enough.
#   - If you have already tracked a file that contains secrets (e.g. you
#     once `git commit`-ed a credentials file by mistake), the snapshot
#     WILL include any subsequent modification. Untrack it via
#     `git rm --cached <file>` + `.gitignore` + commit the removal.
#   - This is the deliberate safety default. The alternative (`git add -A`
#     in the temp index) would automatically push any non-gitignored file
#     in the working tree — rejected because the convenience win is
#     dwarfed by the credential-leak surface area against a potentially-
#     public origin.
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

if [ ! -e "$PROJECT_DIR/.git" ]; then
    # `-e` (entry exists) instead of `-d` (directory) so worktrees are
    # accepted too -- in a worktree, .git is a file pointing back at the
    # main repo's .git/worktrees/<name> directory. git commands work
    # identically against worktrees; the dir-only check was an over-strict
    # heuristic that broke ``scripts/snapshot.sh <pipeline> <worktree>``
    # for the pipe-segment workflow which builds a v5.0.3 worktree to
    # isolate its repin edit from the user's main checkout.
    echo "error: $PROJECT_DIR is not a git repository (no .git entry)" >&2
    exit 2
fi

cd "$PROJECT_DIR"

PARENT_SHA=$(git rev-parse HEAD)
HEAD_TREE=$(git rev-parse "HEAD^{tree}")

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

# If the computed tree is identical to HEAD's tree, nothing the snapshot
# would capture has changed -- return HEAD directly. This catches BOTH
# (a) a truly clean working tree and (b) a working tree with only
# untracked files (those don't enter the temp index, so the resulting
# tree equals HEAD's). Either way: no need to run the scanner or push.
# Using the tree comparison rather than `git status --porcelain` keeps
# the early-exit aligned with the actual `add -u` capture boundary.
if [ "$TREE_SHA" = "$HEAD_TREE" ]; then
    echo "$PARENT_SHA"
    exit 0
fi

# Pre-push secret scanner. Defense-in-depth on top of the `add -u`
# tracked-only default: catches newly-introduced credential-shaped
# content in a tracked file before it lands on origin (e.g. a developer
# pasted an API key into a config.py mid-debug). Gitleaks is the
# de-facto standard pattern-based scanner (and is pre-baked into ditbox).
#
# Override only with deliberate intent — secrets in a tracked file are
# a credential rotation event, not a flag-bypass event.
#
#   DIT_SKIP_SECRET_SCAN=1   skip the scan entirely (logs a loud bypass
#                            banner; intended for known false positives
#                            and for environments where installing
#                            gitleaks isn't an option)
if [ -n "${DIT_SKIP_SECRET_SCAN:-}" ]; then
    echo "  WARNING: secret scan BYPASSED via DIT_SKIP_SECRET_SCAN" >&2
    echo "  WARNING: you are personally vouching that no credential-shaped" >&2
    echo "  WARNING: content is about to land on $PIPELINE's origin." >&2
elif ! command -v gitleaks >/dev/null 2>&1; then
    echo "error: gitleaks not installed; refusing to auto-push without a secret scan" >&2
    echo "       (the snapshot push is a known credential-leak surface area;" >&2
    echo "       defense-in-depth requires this scanner)" >&2
    echo "" >&2
    echo "       install: https://github.com/gitleaks/gitleaks/releases" >&2
    echo "       or override (NOT recommended; rotate any leaked credential afterwards):" >&2
    echo "         export DIT_SKIP_SECRET_SCAN=1" >&2
    exit 1
else
    SCAN_DIR=$(mktemp -d -t dit-snapshot-scan.XXXXXXXX)
    # Extend the EXIT trap so SCAN_DIR is cleaned up too.
    trap 'rm -f "$TMP_INDEX"; rm -rf "$SCAN_DIR"' EXIT
    GIT_INDEX_FILE="$TMP_INDEX" git checkout-index --prefix="$SCAN_DIR/" -a
    if ! gitleaks detect --source "$SCAN_DIR" --no-git --no-banner --redact --exit-code 1 >&2; then
        echo "" >&2
        echo "error: gitleaks detected credential-shaped content in the snapshot tree." >&2
        echo "       review the findings above, untrack the offending file(s), and re-run." >&2
        echo "       to override after careful review (rotate any flagged secret afterwards):" >&2
        echo "         export DIT_SKIP_SECRET_SCAN=1" >&2
        exit 1
    fi
    echo "  gitleaks scan passed" >&2
fi

# Frozen author/committer dates AND identities + disabled GPG signing:
# commit SHA must be a pure function of the tree for content-addressability.
# `-c commit.gpgsign=false` is the portable disable (works on any git
# version with config-override support); `--no-gpg-sign` would be belt-
# and-suspenders but isn't supported by all git versions.
SNAPSHOT_SHA=$(
    GIT_AUTHOR_DATE="1970-01-01T00:00:00Z" \
    GIT_COMMITTER_DATE="1970-01-01T00:00:00Z" \
    GIT_AUTHOR_NAME=dit \
    GIT_AUTHOR_EMAIL=dit@local \
    GIT_COMMITTER_NAME=dit \
    GIT_COMMITTER_EMAIL=dit@local \
    git -c commit.gpgsign=false commit-tree \
        -m "dit snapshot of $PARENT_SHA" \
        "$TREE_SHA"
)

REF="refs/dit-snapshots/$PIPELINE/${SNAPSHOT_SHA:0:12}"

# Show the user exactly which paths will be in the snapshot. Diff the
# SNAPSHOT TREE against HEAD (not the working tree against HEAD) so the
# banner matches the snapshot's capture boundary precisely — a file
# staged in the user's real index but not in the snapshot (e.g. a new
# untracked file) must NOT appear here, or the "what will be pushed"
# promise is false. Last-chance visual review before the auto-push: a
# credentials file accidentally promoted to tracked status surfaces here
# rather than after the fact on origin.
CHANGED_PATHS=$(git diff --name-only HEAD "$TREE_SHA" || true)

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

# If the remote already has the ref, it must point at the same SHA we just
# computed (the ref is content-addressable). A divergence means either a
# 12-char prefix collision (astronomically rare) or a manual overwrite —
# either way, refusing to silently install from a local-only ref that
# disagrees with origin is the safer failure mode.
#
# Check BEFORE `git update-ref` so a divergent-remote run doesn't leave
# behind a local-only ref pointing at the divergent SHA (undermines the
# "refusing to install" message and creates cleanup work for the user).
REMOTE_SHA=$(git ls-remote origin "$REF" 2>/dev/null | awk '{print $1}')
if [ -n "$REMOTE_SHA" ] && [ "$REMOTE_SHA" != "$SNAPSHOT_SHA" ]; then
    echo "error: $REF exists on origin at $REMOTE_SHA but local snapshot is $SNAPSHOT_SHA" >&2
    echo "       refusing to install from a local-only ref that disagrees with origin." >&2
    echo "       investigate the manual overwrite (or 12-char collision) and remove" >&2
    echo "       the divergent ref with: make clean-snapshot PIPELINE=<name> REF=$REF" >&2
    exit 1
fi

git update-ref "$REF" "$SNAPSHOT_SHA"

if [ -n "$REMOTE_SHA" ]; then
    echo "  (ref already present on origin at the same SHA -- skipping push)" >&2
else
    git push origin "$REF:$REF" >&2
fi

echo "$REF"
