# `workflows/encounters/`

dit's encounters workflows. Both cover the **generation** half only
(`create_raw_encounters` → `merge_encounters` in `encounters_pipeline`); the
`product_events_encounter` publication step belongs to pipe-events and is out
of scope. Full onboarding audit:
[`docs/encounters-onboarding-2026-07.md`](../../docs/encounters-onboarding-2026-07.md).

| file | source target | default window |
|---|---|---|
| `ais.py` | AIS staging cohort (`pipe_ais_test_202408290000`) | 2020, year-wide |
| `vms.py` | VMS prod (`gfw-int-vms-v3` + `global-fishing-watch`) | 7 days |

`vms.py` reuses `ais.run(args)` wholesale — the two differ only in defaults and
help text. The CLI is deliberately *not* shared, so `--help` never prints AIS
defaults for a VMS run; a parser-parity test guards the resulting drift risk.

## Running

Both need an image exposing `--temp_dataset`, which published `pipe-encounters`
does not. Until the upstream patch lands, point at the dit overlay image:

```
--image-tag   gcr.io/world-fishing-827/dit/encounters:v4.4.0-temp-dataset-d2536aaf
--worker-image gcr.io/world-fishing-827/dit/encounters:v4.4.0-temp-dataset-d2536aaf
```

Use `--binding-name before|after` when running two variants concurrently —
without it, runs sharing an `--experiment-id` collide on the Dataflow job name.
`--ssvid-filter` is accepted on **both** steps and is the cheapest way to shrink
a run.

## Interpreting results

### Cross-tenant self-encounters (VMS)

**The same physical vessel reported by two VMS tenants can "encounter itself".**
Each tenant's feed yields a distinct `vessel_id`/`ssvid`, so the pipeline has no
way to tell they are one vessel, and their two tracks sit on top of each other.
Observed shapes: `JOE TURNER (PNG) × JOE TURNER (PLW)`,
`LAUTARO (PAN) × LAUTARO (ECU)`.

Measured on a 2025 full-year VMS run: **~3.9% of encounters have identical
shipnames on both sides, and ~96% of those are cross-tenant.** `ssvid` and
`vessel_id` never matched across sides, which is exactly why the pipeline does
not filter them.

This is **pre-existing and not caused by any dit change** — dit only made it
visible. Two consequences:

- When quantifying encounter counts, consider excluding
  `v1_shipname = v2_shipname AND v1_tenant <> v2_tenant`, or at least reporting
  with and without.
- It is an identity-resolution gap worth raising with the encounters team
  independently of anything dit is testing.

### `encounter_id` is a content hash

`md5("encounter|<seg_1>|<seg_2>|<start_time>")`. Unique (verified), but because
it derives from content, a content difference produces a *different id* — so
table-check reports only-in-A / only-in-B rather than field-level diffs. Do not
read only-in-X rows as missing data.

### Which table to trust for which bug class

- **raw_encounters** — built incrementally, so it detects *incrementality* bugs.
- **encounters (merged)** — rebuilt `WRITE_TRUNCATE` every call, so it is blind
  to incrementality but a strong detector of *non-determinism*.

A green merged result means "recompute is reproducible", not "the modes agree".

### A green comparison is only as good as the target

AIS staging carries 142 vessels over one year and yields 2–3 co-located pairs
per day; gaps of exactly one hour occur once per ~269k there versus 5.48% of all
gaps on VMS. Some behaviour is simply unreachable on the AIS cohort. Before
reading IDENTICAL as a pass, check the target could have shown a difference at
all — see [`README.md` § Source data targets](../../README.md#source-data-targets).
