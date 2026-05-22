"""Smoke tests for scripts/snapshot.sh + scripts/clean-snapshot.sh.

The shell scripts implement the M-pivot-1 deterministic orphan snapshot
mechanism. These tests exercise the high-value invariants:

  - clean tree -> stdout is HEAD SHA, no ref created;
  - dirty tree -> stdout is refs/dit-snapshots/<pipeline>/<12-char>; the
    snapshot commit is orphan; commit message records the parent SHA;
  - re-running against the same dirty tree produces the same SHA and skips
    the push (idempotency / content-addressable property);
  - the snapshot SHA is the same regardless of the wall-clock at invocation
    (committer/author dates frozen to epoch 0);
  - clean-snapshot removes the ref from both local and remote.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SNAPSHOT_SCRIPT = REPO_ROOT / "scripts" / "snapshot.sh"
CLEAN_SCRIPT = REPO_ROOT / "scripts" / "clean-snapshot.sh"


def _git(*args: str, cwd: Path, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", *args], cwd=str(cwd), check=check, capture_output=True, text=True
    )


@pytest.fixture
def pipeline_repo(tmp_path: Path) -> Path:
    """A pipeline-shaped checkout with a bare origin and one initial commit."""
    origin = tmp_path / "origin.git"
    work = tmp_path / "pipe-gaps"
    _git("init", "--bare", "-q", str(origin), cwd=tmp_path)
    _git("init", "-q", "-b", "main", str(work), cwd=tmp_path)
    _git("config", "user.email", "test@dit.local", cwd=work)
    _git("config", "user.name", "test", cwd=work)
    _git("config", "commit.gpgsign", "false", cwd=work)
    _git("remote", "add", "origin", str(origin), cwd=work)
    (work / "README.md").write_text("hello\n")
    _git("add", ".", cwd=work)
    _git("commit", "-q", "-m", "initial", cwd=work)
    _git("push", "-q", "-u", "origin", "main", cwd=work)
    return work


def _run_snapshot(repo: Path, pipeline: str = "pipe-gaps") -> tuple[str, str]:
    proc = subprocess.run(
        [str(SNAPSHOT_SCRIPT), pipeline, str(repo)],
        check=True,
        capture_output=True,
        text=True,
    )
    return proc.stdout.strip(), proc.stderr


def _run_clean(repo: Path, ref: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [str(CLEAN_SCRIPT), str(repo), ref],
        check=True,
        capture_output=True,
        text=True,
    )


def test_snapshot_clean_tree_returns_head(pipeline_repo: Path) -> None:
    head = _git("rev-parse", "HEAD", cwd=pipeline_repo).stdout.strip()
    out, _ = _run_snapshot(pipeline_repo)
    assert out == head


def test_snapshot_dirty_tree_creates_orphan_ref(pipeline_repo: Path) -> None:
    (pipeline_repo / "README.md").write_text("hello dirty\n")
    head = _git("rev-parse", "HEAD", cwd=pipeline_repo).stdout.strip()

    ref, stderr = _run_snapshot(pipeline_repo)

    assert ref.startswith("refs/dit-snapshots/pipe-gaps/")
    short = ref.rsplit("/", 1)[-1]
    assert len(short) == 12, f"expected 12-char short SHA, got {short!r}"

    sha = _git("rev-parse", ref, cwd=pipeline_repo).stdout.strip()
    assert sha.startswith(short)

    # Orphan: rev-list --parents shows one entry (just the commit SHA, no parents).
    parents = (
        _git("rev-list", "--parents", "-n1", sha, cwd=pipeline_repo)
        .stdout.strip()
        .split()
    )
    assert parents == [sha], f"expected orphan commit, got parents={parents[1:]!r}"

    # Commit message records the parent SHA verbatim (40 chars).
    subject = _git("show", "-s", "--format=%s", sha, cwd=pipeline_repo).stdout.strip()
    assert subject == f"dit snapshot of {head}"

    # Commit metadata is frozen to epoch 0 / dit identity — required for the
    # "two snapshots of the same tree at different wall-clock times resolve
    # to the same SHA" property. A regression here would re-introduce
    # non-determinism via committer/author timestamp drift.
    fmt = "%at|%ct|%an|%ae|%cn|%ce"
    meta = _git("show", "-s", f"--format={fmt}", sha, cwd=pipeline_repo).stdout.strip()
    author_ts, committer_ts, an, ae, cn, ce = meta.split("|")
    assert author_ts == "0", f"author timestamp must be epoch 0, got {author_ts!r}"
    assert committer_ts == "0", f"committer timestamp must be epoch 0, got {committer_ts!r}"
    assert an == "dit" and ae == "dit@local"
    assert cn == "dit" and ce == "dit@local"

    # Banner reaches stderr.
    assert "dit snapshot for pipe-gaps" in stderr


def test_snapshot_idempotent_same_dirty_tree(pipeline_repo: Path) -> None:
    (pipeline_repo / "README.md").write_text("hello dirty\n")

    ref1, stderr1 = _run_snapshot(pipeline_repo)
    ref2, stderr2 = _run_snapshot(pipeline_repo)

    assert ref1 == ref2, "same dirty tree must produce identical ref"
    # First run pushes; second sees the ref on origin and skips.
    assert "already present on origin" in stderr2
    assert "already present on origin" not in stderr1


def test_snapshot_different_trees_different_refs(pipeline_repo: Path) -> None:
    (pipeline_repo / "README.md").write_text("variant-A\n")
    ref_a, _ = _run_snapshot(pipeline_repo)

    (pipeline_repo / "README.md").write_text("variant-B\n")
    ref_b, _ = _run_snapshot(pipeline_repo)

    assert ref_a != ref_b


def test_snapshot_ref_pushed_to_origin(pipeline_repo: Path) -> None:
    (pipeline_repo / "README.md").write_text("hello dirty\n")
    ref, _ = _run_snapshot(pipeline_repo)
    proc = _git("ls-remote", "--exit-code", "origin", ref, cwd=pipeline_repo)
    assert ref in proc.stdout


def test_snapshot_user_index_untouched(pipeline_repo: Path) -> None:
    """The temp-index dance must not pollute the real index."""
    (pipeline_repo / "README.md").write_text("hello dirty\n")
    status_before = _git("status", "--porcelain", cwd=pipeline_repo).stdout

    _run_snapshot(pipeline_repo)

    status_after = _git("status", "--porcelain", cwd=pipeline_repo).stdout
    assert status_before == status_after


def test_clean_snapshot_removes_local_and_remote(pipeline_repo: Path) -> None:
    (pipeline_repo / "README.md").write_text("hello dirty\n")
    ref, _ = _run_snapshot(pipeline_repo)

    _git("rev-parse", ref, cwd=pipeline_repo)  # exists locally
    _git("ls-remote", "--exit-code", "origin", ref, cwd=pipeline_repo)  # exists remotely

    proc = _run_clean(pipeline_repo, ref)
    assert "removed-local=1" in proc.stdout
    assert "removed-remote=1" in proc.stdout

    assert (
        _git(
            "rev-parse", "--verify", "--quiet", ref, cwd=pipeline_repo, check=False
        ).returncode
        != 0
    )
    assert (
        _git(
            "ls-remote", "--exit-code", "origin", ref, cwd=pipeline_repo, check=False
        ).returncode
        != 0
    )


def test_clean_snapshot_idempotent_on_missing_ref(pipeline_repo: Path) -> None:
    """Cleaning a ref that doesn't exist anywhere is a quiet no-op."""
    proc = _run_clean(
        pipeline_repo, "refs/dit-snapshots/pipe-gaps/0123456789ab"
    )
    assert "removed-local=0" in proc.stdout
    assert "removed-remote=0" in proc.stdout


def test_clean_snapshot_accepts_short_sha(pipeline_repo: Path) -> None:
    (pipeline_repo / "README.md").write_text("hello dirty\n")
    ref, _ = _run_snapshot(pipeline_repo)
    short = ref.rsplit("/", 1)[-1]

    proc = _run_clean(pipeline_repo, short)
    assert "removed-local=1" in proc.stdout
    assert "removed-remote=1" in proc.stdout


def test_clean_snapshot_rejects_invalid_short_ref(pipeline_repo: Path) -> None:
    """Destructive: reject anything that isn't 12 hex chars or a full ref."""
    proc = subprocess.run(
        [str(CLEAN_SCRIPT), str(pipeline_repo), "not-a-sha"],
        capture_output=True,
        text=True,
    )
    assert proc.returncode != 0
    assert "must be exactly 12 hex chars" in proc.stderr


def test_snapshot_captures_untracked_files(pipeline_repo: Path) -> None:
    """With `git add -A` in the temp index, brand-new files land in the tree."""
    new_file = pipeline_repo / "src" / "new_module.py"
    new_file.parent.mkdir(parents=True)
    new_file.write_text("def f(): pass\n")

    ref, _ = _run_snapshot(pipeline_repo)
    sha = _git("rev-parse", ref, cwd=pipeline_repo).stdout.strip()

    # `git ls-tree -r` should list the new path.
    tree_listing = _git("ls-tree", "-r", "--name-only", sha, cwd=pipeline_repo).stdout
    assert "src/new_module.py" in tree_listing.split()


def test_snapshot_excludes_gitignored_files(pipeline_repo: Path) -> None:
    """`.gitignore`'d paths must not enter the snapshot tree — guards against
    accidental secret/credential leakage via auto-snapshot."""
    (pipeline_repo / ".gitignore").write_text(".env\n")
    _git("add", ".gitignore", cwd=pipeline_repo)
    _git("commit", "-q", "-m", "gitignore", cwd=pipeline_repo)

    (pipeline_repo / ".env").write_text("SECRET=hunter2\n")
    (pipeline_repo / "README.md").write_text("hello dirty\n")

    ref, _ = _run_snapshot(pipeline_repo)
    sha = _git("rev-parse", ref, cwd=pipeline_repo).stdout.strip()

    tree_listing = _git("ls-tree", "-r", "--name-only", sha, cwd=pipeline_repo).stdout
    assert ".env" not in tree_listing.split()


def test_snapshot_fails_on_ref_divergence(pipeline_repo: Path) -> None:
    """If the remote already has the snapshot ref pointing at a different SHA,
    snapshot.sh must refuse to install from a divergent local-only ref."""
    (pipeline_repo / "README.md").write_text("hello dirty\n")
    ref, _ = _run_snapshot(pipeline_repo)

    # Simulate divergence: overwrite the remote ref to point at HEAD instead.
    head = _git("rev-parse", "HEAD", cwd=pipeline_repo).stdout.strip()
    _git("push", "origin", f"{head}:{ref}", "--force", cwd=pipeline_repo)
    # Also drop the local ref so update-ref re-creates it cleanly on the next
    # run; this isolates the test to the "remote disagrees" path.
    _git("update-ref", "-d", ref, cwd=pipeline_repo)

    proc = subprocess.run(
        [str(SNAPSHOT_SCRIPT), "pipe-gaps", str(pipeline_repo)],
        capture_output=True,
        text=True,
    )
    assert proc.returncode != 0
    assert "exists on origin at" in proc.stderr
    assert "refusing to install" in proc.stderr
