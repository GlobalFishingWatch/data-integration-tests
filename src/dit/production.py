"""Find a production pipeline run, to pin its inputs and output.

A production-baseline test (PIPELINE-4703) runs only a candidate version and
compares it with the table production wrote. It needs the moments production
read its inputs and finished writing, so it can pin both with time travel.
Table metadata cannot supply them: a production job updates its output
table's description (and so ``lastModifiedTime``) at job START, before it
writes. The Dataflow job itself is the reliable record: its creation time
precedes every read, and its terminal ``Done`` time follows the write.

Import-light on purpose (no BigQuery client): gcloud via ``subprocess``, in
the style of ``dit.cache._discover_dataflow_jobs``.
"""
from __future__ import annotations

import dataclasses
import json
import logging
import subprocess
from datetime import date, datetime, timedelta, timezone

logger = logging.getLogger(__name__)

#: ``state`` of a successfully finished job in ``gcloud dataflow jobs list``.
DONE_STATE = "Done"

#: Format of ``creationTime`` / ``stateTime`` in ``gcloud dataflow jobs list``
#: JSON output (UTC, second precision).
_GCLOUD_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"


@dataclasses.dataclass(frozen=True)
class ProductionRun:
    """One finished production Dataflow job.

    ``create_time`` precedes the job's BigQuery reads, so it pins the inputs
    the job read; ``end_time`` (when the job reached ``Done``) follows its
    output write, so it pins the output.
    """

    job_id: str
    name: str
    create_time: datetime
    end_time: datetime


def _parse_gcloud_time(value: str) -> datetime:
    return datetime.strptime(value, _GCLOUD_TIME_FORMAT).replace(tzinfo=timezone.utc)


def find_dataflow_job(
    *, project: str, region: str, name: str, created_after: date,
) -> ProductionRun:
    """The latest successfully finished Dataflow job named exactly ``name``.

    Retries and cleared tasks reuse a job name, so several jobs can match;
    the latest ``Done`` one by creation time is the run whose output is in
    the table. Raises ``RuntimeError`` when gcloud fails or no matching job
    is ``Done`` (still running, failed, or never ran), naming what was found.
    """
    result = subprocess.run(
        [
            "gcloud", "dataflow", "jobs", "list",
            f"--project={project}",
            f"--region={region}",
            f"--created-after={created_after.isoformat()}",
            f"--filter=name={name}",
            "--format=json",
        ],
        capture_output=True, text=True, check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"gcloud dataflow jobs list failed for {name} in {project} "
            f"(rc={result.returncode}): {result.stderr.strip()}"
        )
    try:
        jobs = json.loads(result.stdout or "[]")
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"could not parse gcloud jobs-list output for {name}") from exc

    matching = [j for j in jobs if j.get("name") == name]
    done = sorted(
        (j for j in matching if j.get("state") == DONE_STATE),
        key=lambda j: j["creationTime"],
    )
    if not done:
        states = ", ".join(f"{j.get('id')}={j.get('state')}" for j in matching) or "none"
        raise RuntimeError(
            f"no finished ({DONE_STATE}) Dataflow job named {name} in {project} "
            f"since {created_after}; found: {states}"
        )
    job = done[-1]
    run = ProductionRun(
        job_id=job["id"],
        name=job["name"],
        create_time=_parse_gcloud_time(job["creationTime"]),
        end_time=_parse_gcloud_time(job["stateTime"]),
    )
    logger.info(
        "production run %s (%s): created %s, done %s",
        run.name, run.job_id, run.create_time.isoformat(), run.end_time.isoformat(),
    )
    return run


def default_created_after(production_date: date) -> date:
    """Lower bound for the job search: the job for day D runs a few days
    after D, never before it."""
    return production_date - timedelta(days=1)
