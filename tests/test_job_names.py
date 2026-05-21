from dit.job_names import MAX_JOB_NAME, make_job_name, to_safe_for_job_name


def test_to_safe_lowercases_and_replaces():
    assert to_safe_for_job_name("Foo_Bar.Baz") == "foo-bar-baz"


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
