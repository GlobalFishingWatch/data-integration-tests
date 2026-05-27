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

def test_env_override_short_circuits(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(snapshot.ENV_PIPELINE_COMMIT, "deadbeef")
    calls = []
    monkeypatch.setattr(snapshot, "git_info", lambda d: calls.append(d) or ("x", False))
    monkeypatch.setattr(snapshot, "create_snapshot", lambda d, p: calls.append("snap"))

    commit, unreviewed = snapshot.resolve_pipeline_commit(
        "/repo", "pipe-gaps", runner="dataflow"
    )
    assert (commit, unreviewed) == ("deadbeef", True)
    assert calls == [], "env override must not touch git or the snapshot script"


def test_clean_tree_returns_head_not_unreviewed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(snapshot.ENV_PIPELINE_COMMIT, raising=False)
    monkeypatch.setattr(snapshot, "git_info", lambda d: ("abc1234", False))
    snapped = []
    monkeypatch.setattr(snapshot, "create_snapshot", lambda d, p: snapped.append((d, p)))

    commit, unreviewed = snapshot.resolve_pipeline_commit(
        "/repo", "pipe-gaps", runner="dataflow"
    )
    assert (commit, unreviewed) == ("abc1234", False)
    assert snapped == [], "clean tree must not snapshot"


def test_dirty_docker_no_snapshot(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(snapshot.ENV_PIPELINE_COMMIT, raising=False)
    monkeypatch.setattr(snapshot, "git_info", lambda d: ("abc1234", True))
    snapped = []
    monkeypatch.setattr(snapshot, "create_snapshot", lambda d, p: snapped.append((d, p)))

    commit, unreviewed = snapshot.resolve_pipeline_commit(
        "/repo", "pipe-gaps", runner="docker"
    )
    # Docker runs the working tree directly; record as unreviewed, no push.
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
