"""Tests for the pipe-segment temp-dataset overlay (``docker/overlays/pipe-segment-temp-dataset``).

No image is built here. What is checked:

* the read counter of ``verify_overlay.py`` (pure; it runs inside the image at
  build time, where it fails the build when upstream adds a BigQuery read);
* each patch against the read counts ``verify_overlay.py`` expects for it:
  the patch adds ``**temp_dataset_kwargs()`` once per expected read, in the
  expected modules and in no other;
* that ``build.sh`` knows every patch, and that the workflow's default image
  tag names the patch that builds it;
* opt-in, when a pipe-segment checkout with the two tags is found: each patch
  applies to its own tag and not to the other, and the patched tree passes
  the static check. Skipped otherwise, as in CI.
"""

from __future__ import annotations

import hashlib
import importlib.util
import re
import shutil
import subprocess
from pathlib import Path

import pytest

from workflows.pipe_segment import ais
from workflows.pipe_segment import identity_match_key as imk

OVERLAY = Path(__file__).resolve().parents[1] / ais.OVERLAY_DIR
PATCH_V510 = "temp_dataset.patch"
PATCH_V503 = "temp_dataset_v5.0.3.patch"
#: The tag each patch is written against.
PATCH_TAGS = {PATCH_V510: "v5.1.0", PATCH_V503: "v5.0.3"}
HELPER_FILE = "src/pipe_segment/_dit_overlay.py"


def _load_verify():
    spec = importlib.util.spec_from_file_location("verify_overlay", OVERLAY / "verify_overlay.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


verify = _load_verify()


def _added_lines_by_file(patch: str) -> dict[str, list[str]]:
    """The lines each file gains in a patch, by path."""
    added: dict[str, list[str]] = {}
    current = None
    for line in (OVERLAY / patch).read_text().splitlines():
        if line.startswith("+++ b/"):
            current = line.removeprefix("+++ b/")
            added[current] = []
        elif line.startswith("+") and not line.startswith("+++") and current:
            added[current].append(line[1:])
    return added


# --------------------------------------------------------------------------
# the read counter
# --------------------------------------------------------------------------

@pytest.mark.parametrize(("source", "expected"), [
    ("x = 1", (0, 0)),
    # The three spellings pipe-segment uses, unpatched.
    ("beam.io.ReadFromBigQuery(query=q, use_standard_sql=True)", (1, 0)),
    ("io.ReadFromBigQuery(query=q)", (1, 0)),
    ("ReadFromBigQuery(query=q)", (1, 0)),
    # Patched.
    ("beam.io.ReadFromBigQuery(query=q, **temp_dataset_kwargs())", (1, 1)),
    ("beam.io.ReadFromBigQuery(query=q, **other, **temp_dataset_kwargs())", (1, 1)),
    ("io.ReadFromBigQuery(\n    query=q,\n    **temp_dataset_kwargs(),\n)", (1, 1)),
    # Another ** argument is not the helper.
    ("beam.io.ReadFromBigQuery(query=q, **self.read_from_bigquery_kwargs)", (1, 0)),
    # A reference to the class is not a read: only calls count.
    ("m = beam.io.ReadFromBigQuery.Method.EXPORT", (0, 0)),
    ("beam.io.ReadFromBigQuery(query=q, method=beam.io.ReadFromBigQuery.Method.EXPORT)", (1, 0)),
    # A mention in a comment or a docstring is not a read.
    ('"""Extra arguments for Beam\'s ``ReadFromBigQuery``."""\n# ReadFromBigQuery(', (0, 0)),
    ("a = beam.io.ReadFromBigQuery(query=1, **temp_dataset_kwargs())\n"
     "b = beam.io.ReadFromBigQuery(query=2)", (2, 1)),
])
def test_bigquery_reads_counts_calls_and_patched_calls(source, expected) -> None:
    assert verify.bigquery_reads(source) == expected


def test_uncovered_reads_names_the_entry_points_the_patch_does_not_cover() -> None:
    assert verify.uncovered_reads("beam.io.ReadAllFromBigQuery()") == ["ReadAllFromBigQuery"]
    assert verify.uncovered_reads("beam.io.ReadFromBigQuery(query=q)") == []


def _package(tmp_path: Path, files: dict[str, str]) -> Path:
    for rel, source in files.items():
        path = tmp_path / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(source)
    return tmp_path


_PATCHED = "beam.io.ReadFromBigQuery(query=q, **temp_dataset_kwargs())\n"
_UNPATCHED = "beam.io.ReadFromBigQuery(query=q)\n"


def _expected_package(patch: str) -> dict[str, str]:
    return {rel: _PATCHED * reads for rel, reads in verify.EXPECTED_READS_BY_PATCH[patch].items()}


@pytest.mark.parametrize("patch", sorted(PATCH_TAGS))
def test_check_static_passes_on_exactly_the_expected_reads(patch, tmp_path) -> None:
    verify.check_static(_package(tmp_path, {**_expected_package(patch), "pipeline.py": "x = 1\n"}),
                        patch)


@pytest.mark.parametrize(("change", "message"), [
    # A read added upstream in a module the patch never touched.
    ({"transform/read_something_new.py": _UNPATCHED}, "reads per module differ"),
    # A second read added to a patched module.
    ({"transform/read_messages.py": _PATCHED + _UNPATCHED}, "reads per module differ"),
    # A read the patch should cover but does not.
    ({"transform/read_fragments.py": _UNPATCHED}, "reads without"),
    # A patched module's read removed upstream: the patch is stale.
    ({"transform/satellite_offsets.py": "x = 1\n"}, "reads per module differ"),
    ({"pipeline.py": "beam.io.ReadAllFromBigQuery()\n"}, "does not cover"),
])
def test_check_static_fails_when_the_reads_change(change, message, tmp_path) -> None:
    with pytest.raises(AssertionError, match=message):
        verify.check_static(_package(tmp_path, {**_expected_package(PATCH_V510), **change}),
                            PATCH_V510)


def test_the_newer_generation_has_one_more_read() -> None:
    """v5.1.0 added the read of the prior segments, the incremental segmenter's state."""
    old, new = (verify.EXPECTED_READS_BY_PATCH[p] for p in (PATCH_V503, PATCH_V510))

    assert set(new) - set(old) == {"transform/read_prior_segments.py"}
    assert sum(old.values()) == 4 and sum(new.values()) == 5


# --------------------------------------------------------------------------
# the patches against the counts
# --------------------------------------------------------------------------

def test_every_patch_in_the_directory_is_known_everywhere() -> None:
    patches = sorted(p.name for p in OVERLAY.glob("*.patch"))

    assert patches == sorted(PATCH_TAGS) == sorted(verify.EXPECTED_READS_BY_PATCH)
    build = (OVERLAY / "build.sh").read_text()
    # build.sh tries the newest generation first.
    assert f"PATCHES=({PATCH_V510} {PATCH_V503})" in build
    assert '--patch "$patch"' in build
    assert "gcr.io/world-fishing-827/dit/pipe-segment:" in build


@pytest.mark.parametrize("patch", sorted(PATCH_TAGS))
def test_each_patch_patches_exactly_the_reads_the_self_check_expects(patch) -> None:
    added = _added_lines_by_file(patch)
    expected = verify.EXPECTED_READS_BY_PATCH[patch]

    # The helper, plus one module per expected entry and no other file.
    assert set(added) == {HELPER_FILE, *(f"src/pipe_segment/{rel}" for rel in expected)}
    for rel, reads in expected.items():
        lines = added[f"src/pipe_segment/{rel}"]
        assert sum("**temp_dataset_kwargs()" in line for line in lines) == reads, rel
        assert lines.count("from pipe_segment._dit_overlay import temp_dataset_kwargs") == 1, rel
    helper = "\n".join(added[HELPER_FILE])
    assert 'ENV = "DIT_BQ_TEMP_DATASET"' in helper
    assert "def temp_dataset_kwargs() -> dict:" in helper


def test_both_patches_add_the_same_helper() -> None:
    assert _added_lines_by_file(PATCH_V510)[HELPER_FILE] == _added_lines_by_file(
        PATCH_V503)[HELPER_FILE]


def test_the_helper_reads_the_variable_the_workflow_sets() -> None:
    assert ais.TEMP_DATASET_ENV == "DIT_BQ_TEMP_DATASET"
    assert f'ENV = "{ais.TEMP_DATASET_ENV}"' in "\n".join(
        _added_lines_by_file(PATCH_V510)[HELPER_FILE])


def test_the_default_image_tag_names_the_patch_that_builds_it() -> None:
    """``build.sh`` tags an image ``<label>-<commit7>-temp-dataset-<patch8>``.
    Editing the default version's patch changes the tag, so the constant must
    follow. Both pipe-segment workflows share the default image."""
    default_patch = next(p for p, tag in PATCH_TAGS.items() if tag == ais.DEFAULT_PIPELINE_VERSION)
    patch8 = hashlib.sha256((OVERLAY / default_patch).read_bytes()).hexdigest()[:8]
    tag = ais.DEFAULT_OVERLAY_IMAGE.rsplit(":", 1)[1]

    assert default_patch == PATCH_V510
    assert re.fullmatch(rf"{re.escape(ais.DEFAULT_PIPELINE_VERSION)}-[0-9a-f]{{7}}"
                        rf"-temp-dataset-{patch8}", tag)
    assert imk.DEFAULT_IMAGE_TAG == ais.DEFAULT_IMAGE_TAG == ais.DEFAULT_OVERLAY_IMAGE


def _image_of(patch: str) -> str:
    """The image ``build.sh`` names for the tag ``patch`` is written against,
    with the commits of the two tags (f4d39fe, 4ddde03)."""
    commit7 = {"v5.0.3": "f4d39fe", "v5.1.0": "4ddde03"}[PATCH_TAGS[patch]]
    patch8 = hashlib.sha256((OVERLAY / patch).read_bytes()).hexdigest()[:8]
    return f"dit/pipe-segment:{PATCH_TAGS[patch]}-{commit7}-temp-dataset-{patch8}"


def test_the_readme_lists_the_image_of_each_patch() -> None:
    readme = (OVERLAY / "README.md").read_text()

    assert "dit/" + ais.DEFAULT_OVERLAY_IMAGE.rsplit("/", 1)[1] == _image_of(PATCH_V510)
    # The default (the version the AIS DAG pins) and the older one the VMS DAG pins.
    for patch in PATCH_TAGS:
        assert _image_of(patch) in readme and patch in readme


# --------------------------------------------------------------------------
# opt-in: the patches against a real pipe-segment checkout
# --------------------------------------------------------------------------

def _checkout_with_tags() -> Path | None:
    repo = Path(imk.DEFAULT_PIPELINE_DIR)
    if shutil.which("git") is None or not (repo / ".git").exists():
        return None
    for tag in PATCH_TAGS.values():
        found = subprocess.run(
            ["git", "-C", str(repo), "rev-parse", "--verify", "--quiet", f"{tag}^{{commit}}"],
            capture_output=True,
        )
        if found.returncode != 0:
            return None
    return repo


needs_checkout = pytest.mark.skipif(
    _checkout_with_tags() is None,
    reason="no pipe-segment checkout with the tags v5.0.3 and v5.1.0",
)


def _export(repo: Path, tag: str, dest: Path) -> Path:
    dest.mkdir()
    archive = subprocess.run(
        ["git", "-C", str(repo), "archive", tag, "src/pipe_segment"],
        check=True, capture_output=True,
    ).stdout
    subprocess.run(["tar", "-x", "-C", str(dest)], input=archive, check=True)
    return dest


def _applies(tree: Path, patch: str) -> bool:
    return subprocess.run(
        ["git", "apply", "--check", str(OVERLAY / patch)], cwd=tree, capture_output=True,
    ).returncode == 0


@needs_checkout
@pytest.mark.parametrize("patch", sorted(PATCH_TAGS))
def test_each_patch_applies_to_its_tag_only_and_covers_every_read(patch, tmp_path) -> None:
    repo = _checkout_with_tags()
    other = next(p for p in PATCH_TAGS if p != patch)
    tree = _export(repo, PATCH_TAGS[patch], tmp_path / "tree")

    assert _applies(tree, patch)
    assert not _applies(tree, other)
    # Unpatched, the self-check fails; patched, it passes.
    with pytest.raises(AssertionError, match="reads without"):
        verify.check_static(tree / "src/pipe_segment", patch)
    subprocess.run(["git", "apply", str(OVERLAY / patch)], cwd=tree, check=True)
    verify.check_static(tree / "src/pipe_segment", patch)


@needs_checkout
def test_the_default_image_tag_names_the_commit_of_productions_version() -> None:
    commit = subprocess.run(
        ["git", "-C", str(_checkout_with_tags()), "rev-parse", "--short=7",
         f"{ais.DEFAULT_PIPELINE_VERSION}^{{commit}}"],
        check=True, capture_output=True, text=True,
    ).stdout.strip()

    assert f":{ais.DEFAULT_PIPELINE_VERSION}-{commit}-temp-dataset-" in ais.DEFAULT_OVERLAY_IMAGE
