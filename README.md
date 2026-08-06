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

### What backs this up, and how that differs from production

Nothing backs up `warehouse.duckdb`, because it is derived rather than authoritative. The committed CSVs and the loader reproduce any window on demand, so recovery means re-running the loader, not restoring a file. Reverting the code is git's job and the data follows from it.

That works here for a reason that does not hold in production: **the source is immutable and complete**. Every record already exists in `data/`, and the window is the pretence that it does not. Re-landing last March in a year's time returns exactly what it returns today.

A real source accumulates and mutates, so re-reading a past window can legitimately return something different, or nothing at all if the source has aged the records out. Once that is true, the raw layer holds the only copy of what arrived and stops being reproducible. The usual answer is an immutable landing zone, with extracts written once to object storage and never rewritten, and the warehouse loaded from those files rather than from the source. The committed CSVs are that landing zone in miniature, which is the only reason this project can get away with no backups at all.

Two smaller differences worth naming. A single DuckDB file has no point-in-time recovery, so the transaction guarantees a window is never half-written but nothing lets you read the warehouse as it stood an hour ago. And the loader manufactures restatements that a real extract would merely observe, because the export has none; the late arrivals, by contrast, are the export's own.

## Development

```sh
make            # list targets
make check-all  # lint, typecheck, tests
```

Requires [uv](https://docs.astral.sh/uv/). Python 3.13.

## Licence

MIT. The Synthea export is synthetic data generated by a public tool and carries no patient information, real or reconstructable.
