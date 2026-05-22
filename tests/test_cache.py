"""Tests for the pure (non-BQ-touching) parts of `dit.cache`.

BQ-touching tests (read_cache / verify_tables_exist / write_cache /
expires_at_for / cancel_run) live in a separate file once those land —
they need either a BQ emulator or mocked client and aren't worth that
plumbing for the scaffold milestone.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from dit.cache import (
    CacheKey,
    canonicalise_params,
    compute_cache_key,
    resolve_worker_image_to_digest,
    sha1_of_workflow_file,
)


# --------------------------------------------------------------------------
# compute_cache_key
# --------------------------------------------------------------------------

def _key(
    pipeline_commit: str = "abc123",
    worker_image_digest: str = "gcr.io/foo/bar@sha256:0011",
    workflow_file_sha1: str = "def456",
    params: dict[str, Any] | None = None,
) -> CacheKey:
    return CacheKey(
        pipeline_commit=pipeline_commit,
        worker_image_digest=worker_image_digest,
        workflow_file_sha1=workflow_file_sha1,
        params=params if params is not None else {"start": "2020-01-01", "end": "2020-12-31"},
    )


def test_compute_cache_key_is_deterministic():
    assert compute_cache_key(_key()) == compute_cache_key(_key())


def test_compute_cache_key_independent_of_params_dict_ordering():
    a = _key(params={"start": "2020-01-01", "end": "2020-12-31"})
    b = _key(params={"end": "2020-12-31", "start": "2020-01-01"})
    assert compute_cache_key(a) == compute_cache_key(b)


def test_compute_cache_key_changes_when_pipeline_commit_changes():
    assert compute_cache_key(_key(pipeline_commit="abc123")) != \
           compute_cache_key(_key(pipeline_commit="abc124"))


def test_compute_cache_key_changes_when_worker_image_digest_changes():
    assert compute_cache_key(_key(worker_image_digest="gcr.io/foo/bar@sha256:0011")) != \
           compute_cache_key(_key(worker_image_digest="gcr.io/foo/bar@sha256:0022"))


def test_compute_cache_key_changes_when_workflow_file_sha1_changes():
    # The dit-side cache buster: editing the workflow file must invalidate.
    assert compute_cache_key(_key(workflow_file_sha1="def456")) != \
           compute_cache_key(_key(workflow_file_sha1="def457"))


def test_compute_cache_key_changes_when_any_param_changes():
    assert compute_cache_key(_key(params={"start": "2020-01-01"})) != \
           compute_cache_key(_key(params={"start": "2020-01-02"}))


def test_compute_cache_key_is_sha256_hex():
    h = compute_cache_key(_key())
    assert len(h) == 64
    assert all(c in "0123456789abcdef" for c in h)


# --------------------------------------------------------------------------
# canonicalise_params
# --------------------------------------------------------------------------

def test_canonicalise_params_sorts_keys():
    assert list(canonicalise_params({"b": 1, "a": 2}).keys()) == ["a", "b"]


def test_canonicalise_params_sorts_list_values():
    # ssvids-shaped: order doesn't affect output, so we sort to stabilise
    # the cache key.
    assert canonicalise_params({"ssvids": ["999", "111", "222"]})["ssvids"] == \
           ["111", "222", "999"]


def test_canonicalise_params_preserves_tuple_order():
    # Tuples signal "order matters" -- preserve it.
    assert canonicalise_params({"modes": ("1_bf", "2_bfd")})["modes"] == ["1_bf", "2_bfd"]


def test_canonicalise_params_passes_scalars_through():
    p = canonicalise_params({"start": "2020-01-01", "tail_days": 4, "good": True, "x": None})
    assert p == {"good": True, "start": "2020-01-01", "tail_days": 4, "x": None}


# --------------------------------------------------------------------------
# sha1_of_workflow_file
# --------------------------------------------------------------------------

def test_sha1_of_workflow_file(tmp_path: Path):
    f = tmp_path / "wf.py"
    f.write_text("print('hello')\n")
    h = sha1_of_workflow_file(f)
    assert len(h) == 40
    # Same content -> same hash.
    assert sha1_of_workflow_file(f) == h


def test_sha1_of_workflow_file_changes_on_edit(tmp_path: Path):
    f = tmp_path / "wf.py"
    f.write_text("a = 1\n")
    h1 = sha1_of_workflow_file(f)
    f.write_text("a = 2\n")
    assert sha1_of_workflow_file(f) != h1


def test_sha1_changes_for_docstring_edit(tmp_path: Path):
    # Documented behaviour: a docstring edit DOES bust the cache (we treat
    # all workflow-file bytes as significant). Tolerable; refinement via a
    # BEHAVIOUR_VERSION constant is deferred.
    f = tmp_path / "wf.py"
    f.write_text('"""docstring v1."""\n\ndef main(): pass\n')
    h1 = sha1_of_workflow_file(f)
    f.write_text('"""docstring v2."""\n\ndef main(): pass\n')
    assert sha1_of_workflow_file(f) != h1


# --------------------------------------------------------------------------
# resolve_worker_image_to_digest
# --------------------------------------------------------------------------

def test_resolve_worker_image_to_digest_passes_digest_form_through():
    digest_form = "gcr.io/foo/bar@sha256:0011aabbccddeeff"
    # No network call -- direct return.
    assert resolve_worker_image_to_digest(digest_form) == digest_form


# Tag-form resolution would require a real gcloud call; no test for it
# here. Manual verification at M2 / first real workflow integration:
#   resolve_worker_image_to_digest("us-central1-docker.pkg.dev/.../pipe-gaps:v0.9.6")
# should return the same image ref with @sha256:... appended.
