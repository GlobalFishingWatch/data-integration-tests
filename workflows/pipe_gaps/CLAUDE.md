# dit/workflows/pipe_gaps — workflow-author notes

The user-facing page (versions, modes, commands, run history) is
[`README.md`](README.md). This file holds the rules for changing the
workflows.

## Reprocess-to-end contract (load-bearing)

`pipe-gaps detect` is NOT designed for arbitrary mid-range backfills. Any
reprocess implicitly commits to overwriting the `raw_gaps` SCD-2 table
from `start_date` to `+∞`; sub-range backfills leak (rows past your
chosen `end_date` get deleted but never re-emitted until the next full
reprocess). The implementation lives in
`pipe-gaps/src/pipe_gaps/assets/queries/gaps_delete.sql.j2` — the
pre-write delete is unbounded on the right, which is the mechanism
by which a reprocess takes ownership of the tail:

```sql
DELETE FROM `<gaps table>`
WHERE (is_closed = TRUE  AND DATE(end_timestamp)   >= '<start_date>')
   OR (is_closed = FALSE AND DATE(start_timestamp) >= '<start_date>');
```

The query is the same at v0.10.0 (DIT's default worker image) and at
v0.17.1 (production's AIS v5 DAG), read with `git show` on 2026-11-25.

**This is by design.** Workflows here MUST respect it.

### Concrete rules for workflow authors

1. **Every "reprocess" / "recovery" / "backfill-after-something" stage
   ends at `--end` (the most-recent-data day).** Don't model a workflow
   that reprocesses an inner sub-range and expects pipe-gaps to leave
   surrounding rows alone — pipe-gaps doesn't, and the resulting
   `raw_gaps` state will diverge from the oracle even though the bug
   isn't in pipe-gaps' detection logic.

2. **An "outage" stage that's NOT a reprocess (i.e. a daily-DAG-cadence
   run that happens DURING the outage window) is allowed to have a
   narrow `end`.** Those represent the live daily DAG, which legitimately
   runs `[d-W, d]` windows where `d` is today's date. The constraint is
   strictly about the *reprocess* stage(s) — anything that's meant to
   "fix" prior output must extend to `--end`.

3. **`outage_recovery.py` Stage 3 (recovery) must end at `--end`.**
   Today this is satisfied: the recovery range is `[outage_start -
   recovery_buffer_days, end]` (the recovery stage runs `pipe-gaps
   detect --start_date <recovery_start> --end_date <end>` against the
   pinned copy or, with `--no-pin`, the live source), which always
   terminates at `end`. Keep it
   that way in any redesign. The same applies to any future "recovery"
   / "fix" stage in any sibling workflow.

4. **`mode_equivalence.py`'s `bfd` / `bftruncate` / `mutate_recover`
   modes already terminate at `--end`.** Their multi-day-tail loops
   reach `end` on the last iteration. Don't introduce a mode whose
   final iteration falls short of `end`.

### Concrete rules for oracle design

The oracle is the "what would a clean single-shot run produce?" answer
the staged path is compared against. Per the contract above:

- The oracle is **always** `pipe-gaps detect --date-range start,end+1`
  (single run, full range, frozen source). The workflow's `--end` is
  INCLUSIVE; pipe-gaps' `--date-range` upper bound is EXCLUSIVE
  (`messages.sql.j2` filters `DATE(timestamp) < end_date`), so
  `execute_outage_oracle` — like every staged stage — passes
  `end + 1 day`. (Before 2026-06-10 the workflow passed the inclusive
  dates verbatim and silently dropped each stage's final day.)
- A staged path that diverges from the oracle indicates a real bug in
  pipe-gaps' detection logic (e.g. the `get_first_message_inside_range`
  close-path issue documented in `docs/context.md`), NOT a
  contract-violation by the workflow.
- If you find yourself wanting an oracle that's "the staged path's last
  range run as a single-shot", you're designing a sub-range backfill —
  go back to step 1 above.

### Worked example

For `outage_recovery.py` defaults (one-day outage on `2020-12-29`).
Workflow dates are inclusive; the pipeline ranges below show the
exclusive upper bound actually passed (`inclusive end + 1`):

```
Stage 1 (initial backfill):  pipe-gaps detect [2020-01-01, 2020-12-29)   → processes through 12-28
Stage 2 (post-outage):       pipe-gaps detect [2020-12-30, 2021-01-01)   → processes 12-30..12-31
Stage 3 (recovery):          pipe-gaps detect [2020-12-29, 2021-01-01)   ← reaches --end ✓
Oracle:                      pipe-gaps detect [2020-01-01, 2021-01-01)
```

Stage 3 starts at `outage_start` because the default
`--recovery-buffer-days` is 0 (the discriminating regression-test
setting — see the flag's help). A non-zero buffer shifts the recovery
start earlier (`outage_start - buffer`), which also widens the
pre-write DELETE enough to sweep the orphaned open-v1 seeds and mask
the MAP-1676 leftover class.

With `--synthetic-outage`, Stage 2 instead runs
`[2020-12-28, 2021-01-01)` against the outage-filtered view — a range
load that SPANS the missing day, modelling production's rolling-window
load (the detector bridges from the last 12-28 message to the first
12-30 message and emits the incorrect over-threshold gap). Without the
spanning range, filtering the source is a no-op: the stages would be
hiding rows their date ranges never query (issue #59).

Stage 3 reaches `--end` (`2020-12-31`, the most-recent-data day for
the staging cohort). After Stage 3, `raw_gaps_last_versions` should
equal the oracle's `_last_versions` view; any divergence is a
detection-logic bug worth investigating.

A counter-example that would be wrong:

```
Stage 3 (WRONG):  pipe-gaps detect [2020-12-29, 2020-12-31)  ← stops before --end
```

This would delete every gap whose end/start was on `2020-12-29` or
later (including the rows for `2020-12-31`), then only re-emit rows
through `2020-12-30`. The `2020-12-31` rows that Stage 2 wrote are
gone, and the comparison to the oracle would FAIL — but for a
contract-violation reason, not a pipe-gaps bug. Don't do this.

### What this contract is NOT

- **Not a constraint on `start_date`.** Stage 1's start is freely
  chooseable. So is Stage 3's `outage_start - recovery_buffer_days`.
  The contract is about how far the reprocess EXTENDS (always to
  `--end`), not where it begins.

- **Not specific to `outage_recovery.py`.** Any future pipe-gaps
  workflow that includes a reprocess stage must obey the same shape.

- **Not enforced in code today.** The workflow's argparse doesn't reject
  a degenerate config that would violate the contract. That's a future
  guard worth adding if a foot-gun ever materialises; for now, code
  review on PRs in this directory is the enforcement.

## Source pinning in `outage_recovery.py` (2026-11-19, PIPELINE-4705)

`outage_recovery.py` copies its sources at run start
(`dit.bq.pin_source_into_experiment`). Every stage and the oracle read
the copies. `mode_equivalence.py` does not pin.

### What the copies must hold

The copies are windowed by what pipe-gaps reads. Read from pipe-gaps
v0.10.0, the default worker image:

- **Messages.** `messages.sql.j2` filters `DATE(timestamp) >= start_date
  AND DATE(timestamp) < end_date`. The start it gets is not the run's
  start: `DetectGapsConfig.messages_query_start_date` subtracts
  `ceil(n_hours_before / 24)` days. A job over `[s, e)` reads the
  messages of `[s - lookback, e)`.
- **Segments.** The same query reads `seg_id` from the segments table
  with no date filter. The whole table is read, so the whole table is
  copied.
- **Open gaps.** The side input reads the run's own output table, not a
  source.

So the messages copy covers `[--start - lookback, --end]`
(`_messages_window`). Stage 1 and the oracle start at `--start`, and
every job ends at `--end`.

### Rules for workflow authors

1. **A new stage must read inside the window, or the window must
   grow.** A stage that starts before `--start`, or ends after `--end`,
   would read fewer rows from the copy than from the source, without an
   error. `test_the_messages_window_covers_what_every_stage_and_the_oracle_read`
   checks the stages there are; extend it with any new one.
2. **Re-read the pipe-gaps SQL when the default worker image moves.**
   The window encodes v0.10.0's lookback. `messages.sql.j2` is the same
   at v0.17.1, but a later change to the lookback, or a new date-less
   read of the messages, would make the copy too small.
3. **The cache key holds the copies' names.** Anything new that changes
   what a job reads must show in the key: either through the names, or
   as a param of its own.
4. **`--synthetic-outage` filters the copy, not the source.** Stages 1
   and 2 read the view; stage 3 and the oracle read the unfiltered
   copy. With `--no-pin` the view filters the live source.

### Worked example

Default run, `--n-hours-before 12`:

```
messages copy:  DATE(timestamp) in [2019-12-31, 2021-01-01)   4.41 GB (the whole staging table)
segments copy:  all rows                                       4.1 MB
```

Names (`<stamp>` is the run's start time in UTC, e.g. `20261119t101500z`):

```
dit_exp_<experiment>_outage_messages_positions_20191231_20210101_<stamp>
dit_exp_<experiment>_outage_segs_activity_all_<stamp>
```

`--reuse-pin <stamp>` reads these again. It needs the same
`--experiment-id`, `--start`, `--end`, `--n-hours-before` and source
table names, since all of them are in the names. The outage dates and
the recovery buffer may differ.
