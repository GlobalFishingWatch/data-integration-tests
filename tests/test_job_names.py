import pytest

from dit.job_names import MAX_JOB_NAME, make_job_name, to_safe_for_job_name


def test_to_safe_lowercases_and_replaces():
    assert to_safe_for_job_name("Foo_Bar.Baz") == "foo-bar-baz"


def test_to_safe_replaces_arbitrary_unsafe_chars():
    assert to_safe_for_job_name("foo bar/baz@qux") == "foo-bar-baz-qux"


def test_to_safe_collapses_repeated_hyphens():
    assert to_safe_for_job_name("foo___bar...baz") == "foo-bar-baz"


def test_to_safe_strips_edges():
    assert to_safe_for_job_name("_foo_") == "foo"


def test_to_safe_empty_on_all_unsafe():
    assert to_safe_for_job_name("___") == ""


def test_make_job_name_minimal_shape():
    name = make_job_name(repo="pipe-gaps", step="detect", experiment_id="solo-abc")
    assert name == "dit-pipe-gaps-detect-solo-abc"


def test_make_job_name_with_mode_and_iteration():
    name = make_job_name(
        repo="anchorages-pipeline",
        step="thin",
        experiment_id="exp-1",
        mode="1_bf",
        iteration=2,
        total_iterations=5,
    )
    assert name == "dit-anchorages-pipeline-thin-exp-1-1-bf-2-5"


def test_make_job_name_with_binding():
    name = make_job_name(
        repo="r", step="s", experiment_id="e", binding="before", mode="1_bf",
        iteration=1, total_iterations=1,
    )
    assert name == "dit-r-s-e-before-1-bf-1-1"


def test_make_job_name_iteration_skipped_when_only_one_part_provided():
    name = make_job_name(
        repo="r", step="s", experiment_id="e", iteration=2,
    )
    assert name == "dit-r-s-e"


def test_make_job_name_truncates_experiment_id_when_too_long():
    long_exp = "x" * 100
    name = make_job_name(
        repo="anchorages-pipeline", step="visits",
        experiment_id=long_exp, mode="1_bf", iteration=1, total_iterations=1,
    )
    assert len(name) == MAX_JOB_NAME
    assert name.startswith("dit-anchorages-pipeline-visits-")
    assert name.endswith("-1-bf-1-1")


def test_make_job_name_respects_custom_max_len():
    name = make_job_name(
        repo="r", step="s", experiment_id="x" * 50, mode="m",
        max_len=20,
    )
    assert len(name) == 20


def test_make_job_name_raises_when_fixed_parts_already_overflow():
    # The fixed+tail parts alone exceed max_len -> we can't fit any
    # experiment_id, so the function raises rather than truncating the
    # load-bearing tail (which would also risk a trailing hyphen).
    with pytest.raises(ValueError, match="cannot fit job name"):
        make_job_name(
            repo="very-long-repo-name",
            step="very-long-step-name",
            experiment_id="anything",
            mode="m",
            iteration=1,
            total_iterations=1,
            max_len=20,
        )
