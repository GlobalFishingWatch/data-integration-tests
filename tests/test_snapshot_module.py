"""Unit tests for dit.snapshot.resolve_pipeline_commit + git_info.

These cover the orchestration logic (env override / clean / dirty-docker /
dirty-dataflow-snapshot / require-clean) with git + the snapshot script
mocked. The shell script itself (scripts/snapshot.sh) is exercised
end-to-end in test_snapshot.py.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from dit import snapshot
from dit.git_info import git_info


# --------------------------------------------------------------------------
# git_info — against a real temp repo
# --------------------------------------------------------------------------

def _git(*args: str, cwd: Path) -> None:
    subprocess.run(["git", *args], cwd=str(cwd), check=True, capture_output=True)


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    _git("init", "-q", "-b", "main", str(tmp_path), cwd=tmp_path)
    _git("config", "user.email", "t@dit.local", cwd=tmp_path)
    _git("config", "user.name", "t", cwd=tmp_path)
    (tmp_path / "f.txt").write_text("one\n")
    _git("add", ".", cwd=tmp_path)
    _git("commit", "-q", "-m", "init", cwd=tmp_path)
    return tmp_path


def test_git_info_clean(repo: Path) -> None:
    short, dirty = git_info(str(repo))
    assert short and not dirty


def test_git_info_dirty_on_tracked_modification(repo: Path) -> None:
    (repo / "f.txt").write_text("two\n")
    _, dirty = git_info(str(repo))
    assert dirty is True


def test_git_info_untracked_is_not_dirty(repo: Path) -> None:
    """Matches the snapshot capture boundary: untracked files don't count."""
    (repo / "new.txt").write_text("untracked\n")
    _, dirty = git_info(str(repo))
    assert dirty is False


# --------------------------------------------------------------------------
# resolve_pipeline_commit — git + snapshot mocked
# --------------------------------------------------------------------------

def test_env_override_classifies_via_is_unreviewed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(snapshot.ENV_PIPELINE_COMMIT, "deadbeef")
    monkeypatch.delenv(snapshot.ENV_UNREVIEWED, raising=False)  # force the live-check path
    calls = []
    monkeypatch.setattr(snapshot, "git_info", lambda d: calls.append("git_info") or ("x", False))
    monkeypatch.setattr(snapshot, "create_snapshot", lambda d, p: calls.append("snap"))
    seen = []
    monkeypatch.setattr(snapshot, "is_unreviewed", lambda c, d: seen.append(c) or True)

    commit, unreviewed = snapshot.resolve_pipeline_commit(
        "/repo", "pipe-gaps", runner="dataflow"
    )
    # The override commit is recorded and classified via is_unreviewed.
    assert (commit, unreviewed) == ("deadbeef", True)
    assert seen == ["deadbeef"]
    assert calls == [], "env override must not touch git_info or the snapshot script"


def test_env_override_uses_dit_unreviewed_false(monkeypatch: pytest.MonkeyPatch) -> None:
    """Cloud path: the laptop-resolved DIT_UNREVIEWED is used verbatim, WITHOUT
    calling is_unreviewed (which can't fetch origin/main in the builder)."""
    monkeypatch.setenv(snapshot.ENV_PIPELINE_COMMIT, "6cd6706")
    monkeypatch.setenv(snapshot.ENV_UNREVIEWED, "false")
    monkeypatch.setattr(
        snapshot, "is_unreviewed",
        lambda c, d: pytest.fail("must use DIT_UNREVIEWED, not the live check"),
    )
    assert snapshot.resolve_pipeline_commit(
        "/repo", "pipe-gaps", runner="dataflow"
    ) == ("6cd6706", False)


def test_env_override_uses_dit_unreviewed_true(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(snapshot.ENV_PIPELINE_COMMIT, "abc1234")
    monkeypatch.setenv(snapshot.ENV_UNREVIEWED, "true")
    monkeypatch.setattr(snapshot, "is_unreviewed", lambda c, d: pytest.fail("must not call"))
    assert snapshot.resolve_pipeline_commit(
        "/repo", "pipe-gaps", runner="dataflow"
    ) == ("abc1234", True)


def test_env_override_invalid_dit_unreviewed_falls_back(monkeypatch: pytest.MonkeyPatch) -> None:
    """An unparseable DIT_UNREVIEWED falls back to the live check (don't trust junk)."""
    monkeypatch.setenv(snapshot.ENV_PIPELINE_COMMIT, "abc1234")
    monkeypatch.setenv(snapshot.ENV_UNREVIEWED, "maybe")
    seen = []
    monkeypatch.setattr(snapshot, "is_unreviewed", lambda c, d: seen.append(c) or False)
    assert snapshot.resolve_pipeline_commit(
        "/repo", "pipe-gaps", runner="dataflow"
    ) == ("abc1234", False)
    assert seen == ["abc1234"]


def test_clean_tree_classifies_via_is_unreviewed(monkeypatch: pytest.MonkeyPatch) -> None:
    """A clean checkout's reviewed/unreviewed status comes from is_unreviewed
    (the ancestor-of-main check) -- clean main is reviewed, a clean feature
    branch is unreviewed."""
    monkeypatch.delenv(snapshot.ENV_PIPELINE_COMMIT, raising=False)
    monkeypatch.setattr(snapshot, "git_info", lambda d: ("abc1234", False))
    snapped = []
    monkeypatch.setattr(snapshot, "create_snapshot", lambda d, p: snapped.append((d, p)))

    # Clean + on main -> reviewed.
    monkeypatch.setattr(snapshot, "is_unreviewed", lambda c, d: False)
    assert snapshot.resolve_pipeline_commit("/repo", "pipe-gaps", runner="dataflow") == ("abc1234", False)

    # Clean + unmerged feature branch -> unreviewed (the case the dirty-only gate missed).
    monkeypatch.setattr(snapshot, "is_unreviewed", lambda c, d: True)
    assert snapshot.resolve_pipeline_commit("/repo", "pipe-gaps", runner="dataflow") == ("abc1234", True)

    assert snapped == [], "clean tree must not snapshot"


def test_dirty_docker_no_snapshot(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(snapshot.ENV_PIPELINE_COMMIT, raising=False)
    monkeypatch.setattr(snapshot, "git_info", lambda d: ("abc1234", True))
    snapped = []
    monkeypatch.setattr(snapshot, "create_snapshot", lambda d, p: snapped.append((d, p)))
    monkeypatch.setattr(snapshot, "is_unreviewed", lambda c, d: pytest.fail("dirty is known unreviewed; no ancestor check"))

    commit, unreviewed = snapshot.resolve_pipeline_commit(
        "/repo", "pipe-gaps", runner="docker"
    )
    # Docker runs the working tree directly; dirty -> known unreviewed, no push.
    assert (commit, unreviewed) == ("abc1234", True)
    assert snapped == [], "docker runner must not snapshot/push"


def test_dirty_dataflow_require_clean_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(snapshot.ENV_PIPELINE_COMMIT, raising=False)
    monkeypatch.setattr(snapshot, "git_info", lambda d: ("abc1234", True))
    monkeypatch.setattr(snapshot, "create_snapshot", lambda d, p: pytest.fail("must not snapshot"))

    with pytest.raises(SystemExit, match="require-clean"):
        snapshot.resolve_pipeline_commit(
            "/repo", "pipe-gaps", runner="dataflow", require_clean=True
        )


def test_dirty_dataflow_snapshots_and_returns_snapshot_sha(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(snapshot.ENV_PIPELINE_COMMIT, raising=False)
    monkeypatch.setattr(snapshot, "git_info", lambda d: ("abc1234", True))
    monkeypatch.setattr(
        snapshot, "create_snapshot",
        lambda d, p: "refs/dit-snapshots/pipe-gaps/0123456789ab",
    )
    # The rev-parse --short of the snapshot ref.
    monkeypatch.setattr(
        snapshot.subprocess, "run",
        lambda *a, **k: subprocess.CompletedProcess(a, 0, stdout="0123456789ab\n", stderr=""),
    )

    commit, unreviewed = snapshot.resolve_pipeline_commit(
        "/repo", "pipe-gaps", runner="dataflow"
    )
    assert (commit, unreviewed) == ("0123456789ab", True)


def test_snapshot_parent_extracts_from_commit_message(repo: Path) -> None:
    """A snapshot-shaped commit message yields the recorded parent SHA."""
    parent = git_info(str(repo))[0]
    # Build an orphan commit with the snapshot message shape.
    tree = subprocess.run(
        ["git", "rev-parse", "HEAD^{tree}"], cwd=str(repo),
        check=True, capture_output=True, text=True,
    ).stdout.strip()
    full_parent = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=str(repo),
        check=True, capture_output=True, text=True,
    ).stdout.strip()
    snap = subprocess.run(
        ["git", "commit-tree", "-m", f"dit snapshot of {full_parent}", tree],
        cwd=str(repo), check=True, capture_output=True, text=True,
    ).stdout.strip()
    assert snapshot.snapshot_parent(snap, str(repo)) == full_parent
    # A regular commit (HEAD, "init") returns None.
    assert snapshot.snapshot_parent(parent, str(repo)) is None


def test_snapshot_parent_none_on_unreadable_commit(repo: Path) -> None:
    assert snapshot.snapshot_parent("0000000000000000000000000000000000000000", str(repo)) is None


def test_snapshot_parent_rejects_option_like_commit(repo: Path) -> None:
    """A commit-ish starting with '-' must not be passed to git as a flag
    (option injection / provenance corruption). Returns None up front."""
    assert snapshot.snapshot_parent("--output=/tmp/x", str(repo)) is None
    assert snapshot.snapshot_parent("-n1", str(repo)) is None


def test_snapshot_parent_rejects_malformed_sha(repo: Path) -> None:
    """A snapshot-prefixed message whose payload isn't a 40-char hex SHA must
    NOT be recorded — guards pipeline_commit_parent against junk."""
    tree = subprocess.run(
        ["git", "rev-parse", "HEAD^{tree}"], cwd=str(repo),
        check=True, capture_output=True, text=True,
    ).stdout.strip()
    snap = subprocess.run(
        ["git", "commit-tree", "-m", "dit snapshot of not-a-real-sha", tree],
        cwd=str(repo), check=True, capture_output=True, text=True,
    ).stdout.strip()
    assert snapshot.snapshot_parent(snap, str(repo)) is None


def test_create_snapshot_raises_without_script(monkeypatch: pytest.MonkeyPatch) -> None:
    """Non-editable install (no scripts/ dir) -> clear error rather than a
    confusing FileNotFoundError. Such installs are always a committed ref
    (clean), so this path is defensive."""
    monkeypatch.setattr(snapshot, "snapshot_script", lambda: Path("/nonexistent/snapshot.sh"))
    with pytest.raises(RuntimeError, match="editable dit install"):
        snapshot.create_snapshot("/repo", "pipe-gaps")


# --------------------------------------------------------------------------
# is_unreviewed — against a real repo with origin/main
# --------------------------------------------------------------------------

@pytest.fixture
def repo_with_main(tmp_path: Path) -> Path:
    """Work repo with a bare origin holding `main`; HEAD == origin/main."""
    origin = tmp_path / "origin.git"
    work = tmp_path / "work"
    _git("init", "--bare", "-q", str(origin), cwd=tmp_path)
    _git("init", "-q", "-b", "main", str(work), cwd=tmp_path)
    _git("config", "user.email", "t@dit.local", cwd=work)
    _git("config", "user.name", "t", cwd=work)
    _git("remote", "add", "origin", str(origin), cwd=work)
    (work / "f.txt").write_text("one\n")
    _git("add", ".", cwd=work)
    _git("commit", "-q", "-m", "init", cwd=work)
    _git("push", "-q", "-u", "origin", "main", cwd=work)
    return work


def test_is_unreviewed_main_commit_is_reviewed(repo_with_main: Path) -> None:
    head = git_info(str(repo_with_main))[0]
    assert snapshot.is_unreviewed(head, str(repo_with_main)) is False


def test_is_unreviewed_unmerged_branch_is_unreviewed(repo_with_main: Path) -> None:
    """A clean feature branch (committed, not merged to main) is unreviewed --
    the case the old dirty-only gate missed."""
    _git("checkout", "-q", "-b", "feature", cwd=repo_with_main)
    (repo_with_main / "f.txt").write_text("two\n")
    _git("commit", "-aqm", "feature change", cwd=repo_with_main)
    feature_sha = git_info(str(repo_with_main))[0]
    assert snapshot.is_unreviewed(feature_sha, str(repo_with_main)) is True


def test_is_unreviewed_snapshot_orphan_short_circuits(repo_with_main: Path) -> None:
    """A dit snapshot (orphan) is unreviewed without any fetch/merge-base."""
    tree = subprocess.run(
        ["git", "rev-parse", "HEAD^{tree}"], cwd=str(repo_with_main),
        check=True, capture_output=True, text=True,
    ).stdout.strip()
    head_full = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=str(repo_with_main),
        check=True, capture_output=True, text=True,
    ).stdout.strip()
    snap = subprocess.run(
        ["git", "commit-tree", "-m", f"dit snapshot of {head_full}", tree],
        cwd=str(repo_with_main), check=True, capture_output=True, text=True,
    ).stdout.strip()
    assert snapshot.is_unreviewed(snap, str(repo_with_main)) is True


def test_is_unreviewed_fetch_failure_defaults_unreviewed(tmp_path: Path) -> None:
    """No origin to fetch -> build-when-unsure -> treat as unreviewed."""
    work = tmp_path / "noremote"
    _git("init", "-q", "-b", "main", str(work), cwd=tmp_path)
    _git("config", "user.email", "t@dit.local", cwd=work)
    _git("config", "user.name", "t", cwd=work)
    (work / "f.txt").write_text("one\n")
    _git("add", ".", cwd=work)
    _git("commit", "-q", "-m", "init", cwd=work)
    head = git_info(str(work))[0]
    assert snapshot.is_unreviewed(head, str(work)) is True
