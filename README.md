# Synthetic health warehouse

An analytics pipeline over synthetic patient data, built to work through dbt and Airflow on problems that make their features necessary rather than decorative: claims that arrive weeks after the encounter they bill for, records that get restated, and patient attributes that change over time.

## The shape of it

```
Synthea CSVs ──→ loader ──→ raw ──→ staging ──→ marts
                (windowed)         (1 model     (dims, facts,
                                    per source)  analytics)
```

The loader is the load-bearing piece. It takes a date window and lands only the records belonging to it. Most of what makes that awkward is already in the export: claims fan out over encounters and are billed days or weeks after the service. What the export lacks, the loader injects and records in an injection log, and everything downstream is measured against that log: distortions the pipeline should absorb silently are verified by reconciling totals, and distortions it should reject are verified by a named test failing.

Data comes from [Synthea](https://github.com/synthetichealth/synthea), run once with its CSV export committed to the repository. No Java is needed to run this project. It covers 554 patients over 31,824 encounters and 60,828 claims, running to the end of 2025; `data/README.md` records the exact command that produced it and what a modeller needs to know about its shape.

## Versions

Pinned deliberately. Airflow 3 changed scheduling semantics and `catchup` defaults, so version drift mid-project is an avoidable way to lose an afternoon.

| Component | Version |
|---|---|
| Python | 3.13 |
| DuckDB | 1.5.5 |
| dbt-core | 1.12.0 |
| dbt-duckdb | 1.10.1 |
| Airflow | not yet chosen |
| astronomer-cosmos | not yet chosen |

Python and the three data versions are locked in `uv.lock`. The Airflow and Cosmos rows are filled in when the Astro Runtime image is chosen, since the image decides them.

## Loading

The loader lands one half-open date window into `warehouse.duckdb`, so that adjacent windows tile the way an Airflow data interval does:

```sh
uv run python -m loader.land --window-start 2025-11-03 --window-end 2025-11-04
```

Re-running a window replaces it rather than adding to it, so a backfill can be repeated, and a window can be re-landed inside a larger one that was already loaded, without double-counting. Replacing a window means deleting it first, and the whole window is one transaction so that a load killed partway leaves it as it was rather than emptying it.

### One window, two notions of what belongs to it

Encounters and the conditions and medications recorded at them are selected by **service date**: the window holds what happened in it.

Claims come from Synthea's own `claims.csv`, one row per real claim linked to its encounter, and are selected by the date they were **billed**. An encounter bills 1 to 8 of them, averaging 7.97 for an inpatient stay against 1.45 for an ambulatory visit, and they share a billing date, so a visit is billed as a unit. Their amounts apportion the encounter's cost, with the last claim taking the remainder rather than its own rounded share, so per-encounter totals reconcile exactly despite the fan-out. That fan-out is the correctness problem the grain tests exist to catch.

The billing lag is the export's own and is right-skewed the way a real one is: median 0, 95th percentile 6 days, and a tail out to 100. So `fct_claim`'s lookback is not a number we know by construction; it is a percentile to be estimated from a distribution, accepting a miss rate, which is the production problem rather than a simulation of it.

The one distortion the export lacks is restatement: it contains no `ADJUSTMENT` transactions, so no claim ever re-arrives amended. The loader injects that alone, at 5% of claims, 1 to 30 days later under the same `claim_id` with an adjusted amount. `raw.claims` is therefore an append log of submissions rather than one row per claim, and collapsing it is `fct_claim`'s job. Every injected value is a pure function of the claim id and a fixed seed, computed with `md5`, so a claim lands in the same window whatever order a backfill runs in. `meta.injection_log` records only what was injected, and `raw.claims` carries no restatement flag, so nothing downstream can identify an amendment except by merging on the claim id.

One consequence worth expecting: the leading edge of a backfill is ragged, because claims billed then may be for services that predate the range and were never landed.

### A source that mutates, so a snapshot has something to observe

`raw.patients_current` holds every patient as they stood at the **window end**, replaced outright on every run. It exists because a dbt snapshot detects change by comparing a source against what it saw last time, and Synthea hands over history directly: dated rows in a file that never changes. The table throws that history away so the snapshot can rediscover it one window at a time, which is what most operational source systems look like anyway.

What changes is the payer, resolved from `payer_transitions.csv`, which is the export's own coverage history rather than anything injected. Landing the window ending 2025-11-01 and then the one ending 2025-12-01 moves four patients: Medicaid to Cigna, Humana to Medicare, Cigna to Aetna, and Medicare to uninsured. Death is the other change, and a deceased patient stays in the table with a flag set, so the snapshot sees a change rather than a row vanishing.

It deliberately carries only attributes that can be evaluated as of a date. Marital status, address, income and healthcare expenses are current values fixed at generation time, so stamping them onto a row dated years earlier would assert something false and make any date-aware join to `dim_patient` confidently wrong. Age is left out for a different reason: it is derivable from the birthdate and the date being asked about, and versioning it would add a dimension row per patient per year, burying the changes that matter.

**The snapshot has to run per window.** Because the table is replaced rather than accumulated, it only ever holds the latest as-of date. A backfill that runs the loader sixty times and `dbt snapshot` once at the end captures one state and loses the other fifty-nine.

### What backs this up, and how that differs from production

Nothing backs up `warehouse.duckdb`, because it is derived rather than authoritative. The committed CSVs and the loader reproduce any window on demand, so recovery means re-running the loader, not restoring a file. Reverting the code is git's job and the data follows from it.

That works here for a reason that does not hold in production: **the source is immutable and complete**. Every record already exists in `data/`, and the window is the pretence that it does not. Re-landing last March in a year's time returns exactly what it returns today.

A real source accumulates and mutates, so re-reading a past window can legitimately return something different, or nothing at all if the source has aged the records out. Once that is true, the raw layer holds the only copy of what arrived and stops being reproducible. The usual answer is an immutable landing zone, with extracts written once to object storage and never rewritten, and the warehouse loaded from those files rather than from the source. The committed CSVs are that landing zone in miniature, which is the only reason this project can get away with no backups at all.

Two smaller differences worth naming. A single DuckDB file has no point-in-time recovery, so the transaction guarantees a window is never half-written but nothing lets you read the warehouse as it stood an hour ago. And the loader manufactures restatements that a real extract would merely observe, because the export has none; the late arrivals, by contrast, are the export's own.

## Transforming

The dbt project is in `transform/`. Nothing is modelled yet: what exists is the source layer over the seven raw tables and the freshness check that guards them.

```sh
make freshness
```

**Freshness here measures the loader, not the data.** `_loaded_at` is wall-clock at the moment a row lands, so it answers whether the extract ran rather than whether the records are recent: backfilling a window from 2015 stamps every row with now and reports green. That is what makes it worth running as a precondition on the build rather than as a report after it. The thresholds warn at 24 hours and error at 48, so one missed daily run warns and two error.

One reading to expect. On the windowed tables freshness reports the last window that landed rows, not the last run, because the loader replaces a window rather than touching every row. `providers`, `payers`, and `patients_current` are rewritten whole each run, so they always read as current.

## What of this would survive in a real pipeline

Worth knowing before reading the loader, because the split does not run file by file. It runs through the middle of each one.

**Transfers unchanged.** The windowing contract, `land_window(start, end)` over a half-open interval, is exactly how a real extract task is parameterised from a scheduler's data interval. Delete-on-a-partition-predicate then insert, all inside one transaction, is the idempotency and atomicity pattern a real warehouse needs, differing only in dialect: `MERGE`, `INSERT OVERWRITE PARTITION`, a partition swap. Deciding *which* date a row belongs to, a condition by its parent encounter's service date and a claim by its billing date rather than its service date, is a real decision that quietly breaks incremental models when it is wrong. `_loaded_at` is ordinary ingestion metadata. Most of the tests, too: idempotency, atomicity under a killed load, and adjacent windows tiling are what an extract layer should be checked for anywhere.

**Would be deleted.** The restatement injection and `meta.injection_log`, which exist because the export contains no amendments and a merge would otherwise have nothing to collapse. Apportioning the encounter's cost across its claims, which is only needed because the 216 MB transactions file holding the real charges was left out. And `patients_current`'s as-of resolution, since a production source system already shows only current state and needs no help being flattened.

**Would be replaced.** Eight lines. Every `read_csv_auto` call is the seam where a real source connector goes, and nothing around them changes.

That makes the loader the most scaffolding-heavy part of the project, and it is now finished. The layers that follow are closer to shippable: the staging and mart models are the SQL anyone would write, `fct_claim`'s incremental merge and lookback are a production pattern with the lookback genuinely estimated from an observed distribution, and the Airflow DAG with its retries, pool, and backfill is orchestration as it would really be configured. What the loader buys is that those layers meet real problems — late arrival, fan-out, a mutating dimension — rather than clean data where `merge` and `catchup` would be decorative.

## Development

```sh
make            # list targets
make check-all  # lint, typecheck, tests
```

Requires [uv](https://docs.astral.sh/uv/). Python 3.13.

## Licence

MIT. The Synthea export is synthetic data generated by a public tool and carries no patient information, real or reconstructable.
