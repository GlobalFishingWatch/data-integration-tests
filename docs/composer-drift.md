# The composer drift check

DIT copies by hand what production runs: each step's command line, its image version, its input tables. Nothing updates those copies when production changes. The drift check compares them with production's DAGs and reports where they differ. It changes nothing.

This page explains how the check works and how to use it. The dated findings of the spike that led to it, and the result per workflow, are in [`composer-snapshots-2026-10.md`](composer-snapshots-2026-10.md).

- [Run it](#run-it)
- [Where the production side comes from: `dit.composer`](#where-the-production-side-comes-from-ditcomposer)
- [What is compared: `dit.drift`](#what-is-compared-ditdrift)
- [Expected differences](#expected-differences)
- [Verdicts and exit codes](#verdicts-and-exit-codes)
- [Output](#output)
- [Frozen specs](#frozen-specs)
- [Adding a spec](#adding-a-spec)
- [The public-repo constraint](#the-public-repo-constraint)
- [What the check caught](#what-the-check-caught)
- [What it does not cover](#what-it-does-not-cover)

---

## Run it

```bash
git -C ../composer-dags-production fetch origin      # the check never fetches
dit run workflows/composer_drift.py
dit run workflows/composer_drift.py --workflow port_visits/ais --detail
dit run workflows/composer_drift.py --strict
```

| Option | Meaning |
|---|---|
| `--composer-dir` | The composer-dags-production checkout. Default: `$DIT_COMPOSER_DAGS_DIR`, else `$PROJECTS/composer-dags-production`, else a sibling of this repo. |
| `--ref` | The composer ref to read. Default `origin/main`. |
| `--workflow NAME` | Check only this spec. Repeatable. An unknown name is an error. |
| `--detail` | After the summary, print each workflow's full report. Prints production values. |
| `--strict` | Exit 1 on any drift, also in workflows not yet expected in sync. |
| `--frozen` | Also check the frozen specs. |

The check is offline. It runs no Dataflow job, no docker container and no BigQuery query. It only runs `git show` and `git rev-parse` on the composer checkout.

Run it without `DIT_*` overrides in the shell. DIT's side is what the workflows would run in this environment, so an override such as `DIT_DATAFLOW_REGION` shows up as a difference.

Nothing runs the check on a schedule. It is run by hand.

---

## Where the production side comes from: `dit.composer`

composer-dags-production keeps one generated file per DAG under `tests/render_dags/snapshots/`. Each holds one literal assignment, `rendered_dag = {...}`: the DAG rendered for one fixed run, with every task's image, command and full argument list. composer's own CI renders the DAG and compares it with the file, so a snapshot on composer's main describes main.

`src/dit/composer.py` reads these files as data.

| Step | How | Function |
|---|---|---|
| Resolve the ref to a commit | `git rev-parse` | `resolve_ref` |
| Read the file | `git show <commit>:<path>`. No checkout, so the composer working tree is never touched. | `read_snapshot_source` |
| Parse it | `ast.parse`, then `ast.literal_eval` on the assignment's value. The file is never executed. A value that is not a pure literal is rejected. | `parse_rendered_dag` |
| Build the model | `RenderedDag` with `RenderedTask`s: task id, image, image name and version, argument list | `dag_from_snapshot`, `load_dag` |

DIT never imports composer's code or airflow. That is a working agreement, and it is what keeps DIT's dependencies independent of composer's.

### Reading an argument list

`parse_argv` splits an argument list into positionals (the entrypoint and the command words) and flags with values. It has no knowledge of a pipeline's own parser, so it takes two hints:

- `bare`: flags that take no value;
- `multi`: flags that take several values after one mention (gfw-common's `--labels k=v k=v`).

`diff_flags` compares two parsed lists by flag name and value and returns four groups: same, only left, only right, differing.

### Dates and placeholders

A snapshot is one run, so its dates are concrete. To compare with DIT, a spec builds DIT's tokens **for the same run dates**. `task_placeholders(task)` gives those dates by name:

| Placeholder | Meaning |
|---|---|
| `start_date` | The date the run's data interval starts |
| `end_date` | The last day inside the interval (for inclusive end dates) |
| `end_date_exclusive` | The date the interval ends (for exclusive end dates) |
| `execution_date` | The logical date, only where it differs from the start |

`templatize` goes the other way: it replaces a snapshot's dates by `{placeholder}` where that is unambiguous. A date that fits several placeholders is left as written and reported. In a one-day interval the first and the last day are the same date, so daily snapshots cannot settle which convention a flag uses. A date right after a letter (`_v20240117`) is a table version and is never replaced.

### Looking at a DAG

```bash
dit composer-dag core_ais_v5 --task-prefix port_visits --templated
```

prints the snapshot's tasks as JSON (`src/dit/cli.py`, `composer_dag`). Use it to find task ids when writing a spec. It prints production values: local reading only.

### Limits of a snapshot

- It is **one run**. A flag production passes only on some dates is not seen. A Jinja conditional shows only the branch that run takes.
- **Argument order is not verified** by composer's CI, which compares lists without order. Pairing `--flag value` by position is sound only because the files are generated, not edited.
- Only what a snapshot contains is checked. It says nothing about environment variables or resources it leaves out.

More: [`composer-snapshots-2026-10.md`](composer-snapshots-2026-10.md) § 4.

---

## What is compared: `dit.drift`

`src/dit/drift.py` holds the spec types, the comparison and the report formatting. It uses the standard library and `dit.composer` only.

### Specs

A workflow declares what it mirrors in a file next to itself, `workflows/<pkg>/composer_drift.py`. The file exposes `SPECS`.

**`WorkflowSpec`** (frozen dataclass): one workflow against one snapshot.

| Field | Meaning |
|---|---|
| `name`, `workflow` | The spec's name (`port_visits/ais`) and the workflow file |
| `snapshot` | The composer snapshot it mirrors (`core_ais_v5`) |
| `steps` | One `StepSpec` per step |
| `dit_owned` | Flags DIT sets to its own values on purpose |
| `accepted` | Accepted differences for the whole workflow: flag name to reason |
| `equivalents` | Spellings of a value that mean the same |
| `multi_value`, `bare` | Hints for `parse_argv`, applied to both sides |
| `field_checks` | Extra checks of hand-copied values that are not flags |
| `in_sync` | Whether the workflow is expected to match production today |
| `note` | Why it is not expected in sync, or what the check leaves out |
| `composer_ref` | Freezes the spec at one composer commit (see [Frozen specs](#frozen-specs)) |

**`StepSpec`** (frozen dataclass): one workflow step against one composer task.

| Field | Meaning |
|---|---|
| `step`, `task_id` | DIT's step name and the composer task id |
| `image` | DIT's default image for the step |
| `version` | The pipeline version inside the image, when the tag does not say it (an overlay image) |
| `tokens` | A function from the run's dates to the tokens DIT would run: entrypoint, command words, flags |
| `command` | Instead of `tokens`, when DIT's tokens cannot be built offline: only the entrypoint and command words |
| `accepted` | Accepted differences for this step only |
| `note` | What the comparison leaves out for this step |

Exactly one of `tokens` and `command` is given. No spec uses `command` today.

DIT's tokens come from **the workflow's own builders**. A spec calls the same pure function the workflow calls to build a step's command line, with the workflow's defaults and the snapshot run's dates. So the check compares what the workflow would really hand its runner. The tests pin that the two are the same.

### Three levels per step

Each step is checked on three levels, reported separately (`check_step`).

| Level | DIT's side | Production's side |
|---|---|---|
| Version | Image name and pipeline version of DIT's default image | Image name and tag of the task |
| Command | Entrypoint and command words | The same, from the task's argument list |
| Flags | Every flag DIT emits, by name and value | Every flag of the task |

When the command differs, the flag differences are still listed, but marked **indicative only**. The two sides then belong to different generations of the pipeline's command line, so names and values do not line up. The counts show the size of the port, not a list of defects.

---

## Expected differences

DIT is not production. Some flags must differ. A spec states which, and why. Everything else that differs is drift.

| Kind | Meaning | Examples | Compared? |
|---|---|---|---|
| DIT-owned | DIT sets its own value on purpose | Output tables, job name, labels, service account, GCS paths, subnetwork, worker cap, image path | No, not even for presence |
| Accepted | Production passes it and DIT does not, or DIT sets another value. Each has a written reason. | Lineage options; a staging input table | No. The reason is printed. |
| Equivalent values | Two spellings that mean the same, mapped to one form before comparing | `--runner`: `DataflowRunner` and `dataflow` | Yes, after mapping |

Shared sets in `dit.drift`:

- `DATAFLOW_OWNED_FLAGS`: the flags every Dataflow workflow of DIT owns. Each spec adds its own output-table flags.
- `DATAFLOW_ACCEPTED_DIFFERENCES`: production Dataflow flags DIT leaves out, with reasons. Lineage stays off in DIT so that DIT's tables never enter production's lineage graph.
- `RUNNER_EQUIVALENTS`, `LABELS_MULTI_VALUE`.

An expected flag that only one side passes is marked `[production only]` or `[DIT only]` in the detail.

One cost is worth knowing. A flag listed as DIT-owned is invisible to the check. If the table one step hands to the next is DIT-owned, the check cannot see how the chain is wired. Workflow tests pin that instead.

---

## Verdicts and exit codes

`in_sync` on a spec says whether the workflow is expected to match production today.

| `in_sync` | Drift found | Verdict line |
|---|---|---|
| yes | no | `in sync` |
| yes | yes | `DRIFT (expected in sync)` |
| no | yes | `drift (known: not expected in sync yet)` |
| no | no | `no drift (can be marked in sync)` |

| Exit code | When (`drift.exit_code`) |
|---|---|
| 0 | Every workflow marked in sync has no drift beyond its expected differences |
| 1 | A workflow marked in sync drifts. With `--strict`: any workflow drifts. |
| 2 | A ref, a snapshot or a task could not be read. The other workflows are still reported. |

Drift in a workflow that is not marked in sync is known work. Its spec is the acceptance test for porting that workflow to production's current version. When the port passes, the spec is set to `in_sync=True`, and from then on drift fails the check.

---

## Output

The default output is a summary: one verdict line per workflow, then one row per step with the version, the command and three counts. The counts are flags only production passes, flags only DIT passes, and flags whose values differ, all without the expected differences. A `*` marks counts that are indicative only.

The summary holds versions, command names, task ids and counts. It holds no argument values.

**Example.** This is how the summary looked for one workflow on the evening of 2026-11-20 (see [What the check caught](#what-the-check-caught)). It is rebuilt from the format in `drift.format_summary` and from values already in this repository. It is not a captured log, and the commit is left out.

```
composer-dags-production @ <commit>

  step                  composer task                   version (DIT -> production)  command  only prod  only DIT  differ

pipe_segment/identity_match_key: DRIFT (expected in sync)
  segment               segment.segment                 DIFF v5.0.3 -> v5.1.0        ok       1          0         0
  segment_identity      segment.segment_identity_daily  DIFF v5.0.3 -> v5.1.0        ok       0          0         0
  segment_vessel_daily  segment.segment_vessel_daily    DIFF v5.0.3 -> v5.1.0        ok       0          0         0
  segment_info          segment.segment_info            DIFF v5.0.3 -> v5.1.0        ok       0          0         0
  vessel_info           segment.vessel_info             DIFF v5.0.3 -> v5.1.0        ok       0          0         0
  segment_vessel        segment.segment_vessel          DIFF v5.0.3 -> v5.1.0        ok       0          0         0
```

Read it as: all six steps run an older pipeline version than production; the commands are the same; one flag that production passes to `segment` is missing in DIT. The exit code was 1.

`--detail` adds, per workflow, each field check and, per step, the flags on both sides with their values (`drift.format_report`).

---

## Frozen specs

A cross-version run may use an older pipeline version as its base. DIT then runs the workflow's base plus a declared adjustment for that version ([`framework.md`](framework.md) § 10). The normal drift check only covers the newest interface. The older layer would go unchecked.

A frozen spec closes that gap. It sets `composer_ref` to the composer commit at which production last ran the older version. The spec is read there, whatever `--ref` says. Base plus adjustment must be in sync at that commit.

Production cannot move a fixed commit. So a frozen spec can only drift because DIT changed. It is a regression test of DIT, and it is treated as one:

- it is **not** in the default output, which answers "does DIT mirror production today";
- `--frozen` adds the frozen specs, and `--workflow` can name one;
- the opt-in test `tests/test_composer_drift_live.py` checks each of them, and skips when the checkout lacks the commit.

One frozen spec exists: pipe-segment v5.0.3, at the commit before the DAG moved to v5.1.0 (`workflows/pipe_segment/adjustments.py`, `FROZEN_AT`).

A frozen spec and its adjustment are kept only while their version is production's current or previous one.

---

## Adding a spec

1. Find the snapshot and the task ids: `dit composer-dag <snapshot> --task-prefix <group>`.
2. Make sure each step's command line comes from a pure function of the parsed arguments. If it is built inline, extract it, and keep the emitted tokens unchanged.
3. Write `workflows/<pkg>/composer_drift.py`:
   - a token builder per step;
   - the DIT-owned flags: the workflow's outputs, plus `drift.DATAFLOW_OWNED_FLAGS` for a Dataflow workflow;
   - the accepted differences, each with a reason;
   - `SPECS`.
4. Add the module's `SPECS` to `workflows/composer_drift.py`.
5. Add tests in `tests/test_composer_drift.py`: the steps' commands, and that the tokens equal what the workflow hands its runner.
6. Run the check with `--detail` and read every difference. Fix the workflow, or accept the difference with a reason.
7. When nothing is left, set `in_sync=True`.

Accept a difference only with a reason someone else can check. "Production passes it" is not a reason to leave it out; "DIT's tables must not enter production's lineage graph" is.

---

## The public-repo constraint

This repository is public. composer-dags-production is not. So:

- **No composer content is committed here.** No snapshot, no argument list, no table name or service account read from one.
- **Tests use synthetic DAGs.** For most specs they are built from DIT's own tokens, which states "production runs what DIT runs".
- **The summary is safe to share; the detail is not.** `--detail` and `dit composer-dag` print production's values. Do not paste them into this repository, a commit message or a pull request.
- **One opt-in test module reads a real checkout** when it finds one (`tests/test_composer_drift_live.py`). It asserts structure, that port visits is in sync, and the frozen specs. It fails when production changes its port-visits tasks. That is the signal, not a flaky test.

A future checked-in parameter file would need an allow-list for the same reason: versions, commands and flag names, never infrastructure values.

---

## What the check caught

**2026-11-20: production's move to pipe-segment v5.1.0.**

- On 2026-11-20 at 17:26 UTC, composer-dags-production PR #3145 was merged. It set the AIS v3 DAG's segment group to pipe-segment v5.1.0 and added `--incremental` to the `segment` task.
- That evening the check exited 1. Both pipe-segment specs reported the version `v5.0.3 -> v5.1.0` on every step and `--incremental` as a production-only flag on `segment`. The record of that run is the Plan-changelog entry "2026-11-20 — Mode equivalence for the segmenter" in [`CLAUDE.md`](../CLAUDE.md); the run's output was not kept, so the wording here is reconstructed from that entry, not copied from the output.
- DIT followed the same day. Both pipe-segment workflows now default to an overlay image of v5.1.0, and the specs are in sync again.

Two limits of that story:

- The check was run by hand that day. Nothing would have run it otherwise.
- It shows what the DAG is configured for. Whether a production run had executed with it was not checked.

**Before the general check existed** there were two misses in a row, which is why it was built. DIT's port-visits workflow followed production's pinned version while the pipeline's main branch changed its command line; nobody noticed for months. Then production moved to the new version and DIT stayed behind; only a manual audit on 2026-11-19 found it.

Status per workflow, and the list of what differs in the workflows not yet in sync: [`composer-snapshots-2026-10.md`](composer-snapshots-2026-10.md) § 8d and [`README.md`](../README.md) § Roadmap, item 7b.

---

## What it does not cover

- **Whether a pipeline accepts its flags.** The check compares argument lists. It runs nothing.
- **Inputs, beyond the flags.** DIT's tokens use the workflow's default tables. A staging table differs from production's by design, and a spec accepts that per input.
- **DIT-owned flags.** They are not compared.
- **Anything outside the argument list:** environment variables, steps production runs and DIT does not.
- **Production itself.** The check reads DAG configuration, not job history. A claim about what production did needs the data and the job history.
- **Wrappers.** The cross-version wrappers have no spec of their own. They run the workflows that have one.
