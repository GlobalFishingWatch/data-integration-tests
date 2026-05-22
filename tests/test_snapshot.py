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

import os
import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SNAPSHOT_SCRIPT = REPO_ROOT / "scripts" / "snapshot.sh"
CLEAN_SCRIPT = REPO_ROOT / "scripts" / "clean-snapshot.sh"

# Most tests bypass the pre-push secret scanner so they don't require
# gitleaks installed on the test runner. A focused test below exercises the
# scan path explicitly (skipped if gitleaks isn't available).
SKIP_SCAN_ENV = {**os.environ, "DIT_SKIP_SECRET_SCAN": "1"}

_GITLEAKS_AVAILABLE = shutil.which("gitleaks") is not None
requires_gitleaks = pytest.mark.skipif(
    not _GITLEAKS_AVAILABLE,
    reason="gitleaks not installed on test runner",
)


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


def _run_snapshot(
    repo: Path, pipeline: str = "pipe-gaps", env: dict[str, str] | None = None
) -> tuple[str, str]:
    proc = subprocess.run(
        [str(SNAPSHOT_SCRIPT), pipeline, str(repo)],
        check=True,
        capture_output=True,
        text=True,
        env=env if env is not None else SKIP_SCAN_ENV,
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


def test_snapshot_excludes_untracked_files(pipeline_repo: Path) -> None:
    """SECURITY default: untracked files MUST NOT enter the snapshot tree.

    With `git add -u` in the temp-index dance, brand-new files (untracked OR
    `git add`-ed-but-uncommitted in the user's real index) stay out of the
    snapshot. This is the deliberate safety default — auto-push means
    untracked must mean unseen, otherwise a stray `.env` / `sa.json` /
    one-off dataset could land on origin without the user noticing.

    The cost (silent drop of new files the user wanted in the snapshot)
    surfaces immediately as a wrong/failed Dataflow run; the alternative
    failure mode (credentials on origin) may not surface for weeks.
    """
    # An untracked file that the user would expect to be part of the test.
    new_file = pipeline_repo / "src" / "new_module.py"
    new_file.parent.mkdir(parents=True)
    new_file.write_text("def f(): pass\n")

    # Some tracked-file modifications to ensure the snapshot path is taken.
    (pipeline_repo / "README.md").write_text("hello dirty\n")

    ref, _ = _run_snapshot(pipeline_repo)
    sha = _git("rev-parse", ref, cwd=pipeline_repo).stdout.strip()

    tree_paths = (
        _git("ls-tree", "-r", "--name-only", sha, cwd=pipeline_repo)
        .stdout.split()
    )
    assert "src/new_module.py" not in tree_paths


def test_snapshot_excludes_staged_but_uncommitted_files(pipeline_repo: Path) -> None:
    """SECURITY: even files the user has `git add`-ed (staged in the real
    index) must NOT enter the snapshot if they aren't yet in HEAD.

    The temp index is seeded from HEAD, so `git add -u` against it only
    updates entries that exist in HEAD. A staged-but-uncommitted new file
    isn't in HEAD -> not in temp index -> not in snapshot tree.
    """
    new_file = pipeline_repo / "src" / "staged_new.py"
    new_file.parent.mkdir(parents=True)
    new_file.write_text("# staged but not committed\n")
    _git("add", "src/staged_new.py", cwd=pipeline_repo)

    (pipeline_repo / "README.md").write_text("hello dirty\n")

    ref, _ = _run_snapshot(pipeline_repo)
    sha = _git("rev-parse", ref, cwd=pipeline_repo).stdout.strip()

    tree_paths = (
        _git("ls-tree", "-r", "--name-only", sha, cwd=pipeline_repo)
        .stdout.split()
    )
    assert "src/staged_new.py" not in tree_paths


def test_snapshot_excludes_credential_shaped_untracked_file(pipeline_repo: Path) -> None:
    """A representative credential-shaped untracked file (.env in the
    pipeline root) must not be captured even if .gitignore doesn't mention
    it. With `git add -u`, gitignore status is irrelevant — anything not
    tracked stays out.
    """
    (pipeline_repo / ".env").write_text("GOOGLE_APPLICATION_CREDENTIALS=/tmp/sa.json\n")
    (pipeline_repo / "README.md").write_text("hello dirty\n")

    ref, _ = _run_snapshot(pipeline_repo)
    sha = _git("rev-parse", ref, cwd=pipeline_repo).stdout.strip()

    tree_paths = (
        _git("ls-tree", "-r", "--name-only", sha, cwd=pipeline_repo)
        .stdout.split()
    )
    assert ".env" not in tree_paths


def test_snapshot_banner_lists_changed_paths(pipeline_repo: Path) -> None:
    """Banner shows the user exactly which tracked paths are about to be
    pushed, so a tracked credential file (or any surprise) can be spotted
    before the snapshot lands on origin."""
    (pipeline_repo / "README.md").write_text("hello dirty\n")
    _, stderr = _run_snapshot(pipeline_repo)
    assert "pushing tracked-file changes:" in stderr
    assert "README.md" in stderr


def test_snapshot_refuses_without_gitleaks_or_bypass(
    pipeline_repo: Path,
) -> None:
    """When gitleaks isn't on PATH and no bypass env var is set, the
    snapshot must refuse to push. Defense-in-depth invariant: the auto-push
    can't happen without either a scanner or an explicit opt-out."""
    (pipeline_repo / "README.md").write_text("hello dirty\n")
    env_no_scan = {k: v for k, v in os.environ.items() if k != "DIT_SKIP_SECRET_SCAN"}
    env_no_scan["PATH"] = "/usr/bin:/bin"  # strip any user-local gitleaks

    proc = subprocess.run(
        [str(SNAPSHOT_SCRIPT), "pipe-gaps", str(pipeline_repo)],
        capture_output=True,
        text=True,
        env=env_no_scan,
    )
    assert proc.returncode != 0
    assert "gitleaks not installed" in proc.stderr
    assert "refusing to auto-push without a secret scan" in proc.stderr


def test_snapshot_bypass_env_var_logs_loud_banner(pipeline_repo: Path) -> None:
    """The DIT_SKIP_SECRET_SCAN bypass must emit a loud WARNING banner so
    the user (and anyone reading CI logs) can spot it. Quiet bypass would
    let the scanner be silently disabled, defeating the safety story."""
    (pipeline_repo / "README.md").write_text("hello dirty\n")
    _, stderr = _run_snapshot(pipeline_repo)
    assert "secret scan BYPASSED via DIT_SKIP_SECRET_SCAN" in stderr
    assert "personally vouching" in stderr


@requires_gitleaks
def test_snapshot_blocks_when_scanner_finds_credential(pipeline_repo: Path) -> None:
    """End-to-end: a recognisable credential-shaped string in a TRACKED
    file makes the scan fail, the push gets blocked, and the user sees a
    clear remediation pointer."""
    # Commit a config file first so the credential modification lands in
    # a tracked file (the `add -u` path actually captures it).
    cfg = pipeline_repo / "config.py"
    cfg.write_text("# config\n")
    _git("add", "config.py", cwd=pipeline_repo)
    _git("commit", "-q", "-m", "add config", cwd=pipeline_repo)
    _git("push", "-q", "origin", "main", cwd=pipeline_repo)

    # Introduce a GitHub-PAT-shaped string. AWS dummy keys like
    # AKIAIOSFODNN7EXAMPLE are on gitleaks' built-in allowlist (they're
    # the canonical example keys in AWS docs); `ghp_<40-chars>` is not
    # allowlisted and triggers the GitHub personal access token rule.
    cfg.write_text(
        'GITHUB_TOKEN = "ghp_1234567890abcdef1234567890abcdef123456"\n'
    )

    env_with_scan = {k: v for k, v in os.environ.items() if k != "DIT_SKIP_SECRET_SCAN"}
    proc = subprocess.run(
        [str(SNAPSHOT_SCRIPT), "pipe-gaps", str(pipeline_repo)],
        capture_output=True,
        text=True,
        env=env_with_scan,
    )
    assert proc.returncode != 0
    assert "gitleaks detected" in proc.stderr or "leaks found" in proc.stderr.lower()

    # Ref must NOT have been created locally — the scan happens before
    # the snapshot commit is built.
    refs = _git(
        "for-each-ref",
        "--format=%(refname)",
        "refs/dit-snapshots/",
        cwd=pipeline_repo,
    ).stdout
    assert refs == ""


@requires_gitleaks
def test_snapshot_allows_clean_tree_through_scanner(pipeline_repo: Path) -> None:
    """The scan path is enabled but the change is benign — push proceeds."""
    (pipeline_repo / "README.md").write_text("hello dirty (no secrets here)\n")

    env_with_scan = {k: v for k, v in os.environ.items() if k != "DIT_SKIP_SECRET_SCAN"}
    proc = subprocess.run(
        [str(SNAPSHOT_SCRIPT), "pipe-gaps", str(pipeline_repo)],
        check=True,
        capture_output=True,
        text=True,
        env=env_with_scan,
    )
    ref = proc.stdout.strip()
    assert ref.startswith("refs/dit-snapshots/pipe-gaps/")
    assert "gitleaks scan passed" in proc.stderr


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
        env=SKIP_SCAN_ENV,
    )
    assert proc.returncode != 0
    assert "exists on origin at" in proc.stderr
    assert "refusing to install" in proc.stderr
