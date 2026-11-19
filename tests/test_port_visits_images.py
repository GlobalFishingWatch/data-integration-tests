"""Tests for the image guard of ``workflows/port_visits/ais.py``.

The default image is a DIT overlay of the pipe-anchorages version production
runs. The shared harness would replace a default worker image with a kaniko
build of an unreviewed commit, but that build has no temp-dataset overlay, so
its Dataflow job would fail. The workflow therefore never auto-builds and
refuses, before any cloud call, to run unreviewed code on a default image.

No git, gcloud, docker, BQ or Dataflow calls: every boundary is patched.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from dit import workflow as dit_workflow
from workflows.port_visits import ais as mod

OVERLAY_OF_COMMIT = "gcr.io/world-fishing-827/dit/pipe-anchorages:fix-abc1234-temp-dataset-905dc761"


def _args(*cli: str):
    return mod.parse_args(list(cli))


# --------------------------------------------------------------------------
# which default images a run executes
# --------------------------------------------------------------------------

@pytest.mark.parametrize(("cli", "expected"), [
    # Dataflow: the submitter builds the graph, the workers run it.
    ((), ("--image-tag", "--worker-image")),
    (("--image-tag", OVERLAY_OF_COMMIT), ("--worker-image",)),
    (("--worker-image", OVERLAY_OF_COMMIT), ("--image-tag",)),
    (("--image-tag", OVERLAY_OF_COMMIT, "--worker-image", OVERLAY_OF_COMMIT), ()),
    # --build-from-source replaces the submitter image, not the workers'.
    (("--build-from-source",), ("--worker-image",)),
    (("--build-from-source", "--worker-image", OVERLAY_OF_COMMIT), ()),
    # DirectRunner has no workers.
    (("--runner", "docker"), ("--image-tag",)),
    (("--runner", "docker", "--image-tag", OVERLAY_OF_COMMIT), ()),
    (("--runner", "docker", "--build-from-source"), ()),
    # A compare-only run executes no image at all.
    (("--skip-pipelines",), ()),
])
def test_default_images_executed(cli, expected):
    assert mod._default_images_executed(_args(*cli)) == expected


# --------------------------------------------------------------------------
# check_images
# --------------------------------------------------------------------------

@pytest.mark.parametrize("cli", [
    (),
    ("--build-from-source",),
    ("--runner", "docker"),
    ("--image-tag", OVERLAY_OF_COMMIT),
    ("--worker-image", OVERLAY_OF_COMMIT),
])
def test_unreviewed_code_on_a_default_image_is_refused(cli):
    with pytest.raises(SystemExit) as refusal:
        mod.check_images(_args(*cli), "abc1234", True)

    message = str(refusal.value)
    assert "abc1234" in message
    assert mod.DEFAULT_OVERLAY_IMAGE in message
    # The way out: build an overlay image of the commit, pass it as both flags.
    assert f"{mod.OVERLAY_DIR}/build.sh" in message
    assert "--image-tag" in message and "--worker-image" in message
    assert "--runner docker --build-from-source" in message


def test_the_refusal_names_the_flags_still_at_the_default():
    with pytest.raises(SystemExit, match="default image for --image-tag and --worker-image:"):
        mod.check_images(_args(), "abc1234", True)
    with pytest.raises(SystemExit, match="default image for --worker-image:"):
        mod.check_images(_args("--image-tag", OVERLAY_OF_COMMIT), "abc1234", True)


@pytest.mark.parametrize("cli", [
    ("--image-tag", OVERLAY_OF_COMMIT, "--worker-image", OVERLAY_OF_COMMIT),
    ("--runner", "docker", "--build-from-source"),
    ("--runner", "docker", "--image-tag", OVERLAY_OF_COMMIT),
    ("--skip-pipelines",),
])
def test_unreviewed_code_runs_when_no_default_image_is_executed(cli):
    mod.check_images(_args(*cli), "abc1234", True)


@pytest.mark.parametrize("cli", [(), ("--runner", "docker"), ("--build-from-source",)])
def test_reviewed_code_runs_on_the_default_image(cli):
    mod.check_images(_args(*cli), "767770f", False)


def test_the_overlay_directory_named_in_the_refusal_exists():
    from pathlib import Path

    overlay = Path(__file__).resolve().parents[1] / mod.OVERLAY_DIR
    assert (overlay / "build.sh").is_file()
    assert (overlay / "temp_dataset.patch").is_file()


# --------------------------------------------------------------------------
# main(): the guard runs inside resolve_run_context, nothing is auto-built
# --------------------------------------------------------------------------

def _run_main(*cli: str, commit: str = "abc1234", unreviewed: bool):
    """``main()`` with every boundary patched; returns the mocks to assert on."""
    with (
        patch.object(dit_workflow, "resolve_pipeline_commit",
                     return_value=(commit, unreviewed)),
        patch.object(dit_workflow, "snapshot_parent", return_value=None),
        patch.object(dit_workflow, "dit_commit", return_value="ditsha0"),
        patch.object(dit_workflow, "ensure_pipeline_image") as ensure,
        patch.object(dit_workflow, "resolve_worker_image_to_digest",
                     side_effect=lambda image: f"{image}@sha256:w") as worker_digest,
        patch.object(mod, "resolve_worker_image_to_digest",
                     side_effect=lambda image: f"{image}@sha256:s") as submitter_digest,
        patch.object(mod.dit_workflow, "run_with_cache",
                     side_effect=lambda *a, **k: k["output_fqn"]) as run,
    ):
        try:
            rc = mod.main(["--modes", mod.MODE_BF, "--skip-comparisons", *cli])
        except SystemExit as exc:
            rc = exc
    return rc, ensure, worker_digest, submitter_digest, run


def test_main_refuses_unreviewed_code_before_any_cloud_call():
    rc, ensure, worker_digest, submitter_digest, run = _run_main(unreviewed=True)

    assert isinstance(rc, SystemExit) and f"{mod.OVERLAY_DIR}/build.sh" in str(rc)
    ensure.assert_not_called()
    worker_digest.assert_not_called()
    submitter_digest.assert_not_called()
    run.assert_not_called()


def test_main_never_auto_builds_even_when_the_run_goes_ahead():
    # Unreviewed code, both images given: the harness would not build anyway.
    # Unreviewed code on the docker runner with the working tree: it would
    # have built a worker image nobody pulls.
    for cli in (
        ("--image-tag", OVERLAY_OF_COMMIT, "--worker-image", OVERLAY_OF_COMMIT),
        ("--runner", "docker", "--build-from-source"),
    ):
        rc, ensure, _, _, run = _run_main(*cli, unreviewed=True)

        assert rc == 0
        ensure.assert_not_called()
        run.assert_called_once()


def test_main_runs_reviewed_code_on_the_default_overlay_image():
    rc, ensure, worker_digest, submitter_digest, run = _run_main(
        commit="767770f", unreviewed=False,
    )

    assert rc == 0
    ensure.assert_not_called()
    worker_digest.assert_called_once_with(mod.DEFAULT_OVERLAY_IMAGE)
    submitter_digest.assert_called_once_with(mod.DEFAULT_OVERLAY_IMAGE)
    args = run.call_args.kwargs["execute_kwargs"]["args"]
    assert args.worker_image == args.image_tag == mod.DEFAULT_OVERLAY_IMAGE


def test_main_warns_that_build_from_source_on_dataflow_has_no_overlay(caplog):
    with caplog.at_level("WARNING", logger=mod.logger.name):
        rc, *_ = _run_main(
            "--build-from-source", "--worker-image", OVERLAY_OF_COMMIT, unreviewed=True,
        )

    assert rc == 0
    assert any("without the temp-dataset overlay" in r.getMessage() for r in caplog.records)


def test_main_does_not_warn_about_build_from_source_on_the_docker_runner(caplog):
    with caplog.at_level("WARNING", logger=mod.logger.name):
        rc, *_ = _run_main("--runner", "docker", "--build-from-source", unreviewed=True)

    assert rc == 0
    assert not any("temp-dataset overlay" in r.getMessage() for r in caplog.records)
