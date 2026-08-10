# Synthetic health warehouse

An analytics pipeline over synthetic patient data, built to work through dbt and Airflow on problems that make their features necessary rather than decorative: claims that arrive weeks after the encounter they bill for, records that get restated, and patient attributes that change over time.

## The shape of it

```
Synthea export ──→ loader ──→ raw ──→ staging ──→ marts
                (windowed)         (1 model     (dims, facts,
                                    per source)  analytics)
```

The loader is the load-bearing piece. It takes a date window and lands only the records belonging to it. Most of what makes that awkward is already in the export: claims fan out over encounters and are billed days or weeks after the service. What the export lacks, the loader injects and records in an injection log, and everything downstream is measured against that log: distortions the pipeline should absorb silently are verified by reconciling totals, and distortions it should reject are verified by a named test failing.

Data comes from [Synthea](https://github.com/synthetichealth/synthea), run once with its export committed to the repository as Parquet. No Java is needed to run this project. It covers 554 patients over 31,824 encounters and 60,828 claims, running to the end of 2025; `data/README.md` records the exact command that produced it and what a modeller needs to know about its shape.

Synthea emits CSV. It is converted once to Parquet, which the loader reads about twenty times faster.

## Versions

Pinned deliberately. Airflow 3 changed scheduling semantics and `catchup` defaults, so version drift mid-project is an avoidable way to lose an afternoon.

| Component | Version |
|---|---|
| Python | 3.13 |
| DuckDB | 1.5.5 |
| dbt-core | 1.12.0 |
| dbt-duckdb | 1.10.1 |
| Airflow | 3.3.0, from Astro Runtime 3.3-2 |
| astronomer-cosmos | 1.15.1 |

Everything but Airflow is pinned in `pyproject.toml` and locked in `uv.lock`; the Airflow row follows from the runtime image, which owns it. The image installs the same `dbt` and `airflow` dependency groups the Makefile uses, rather than a separate `requirements.txt`, so there is no second place for a version to drift.

## Loading

The loader lands one half-open date window into `warehouse.duckdb`, so that adjacent windows tile the way an Airflow data interval does:

```sh
uv run python -m loader.land --window-start 2025-11-03 --window-end 2025-11-04
```

A populated warehouse takes a history load and then the scheduled windows, building after each so the snapshot sees every as-of date:

```sh
uv run python -m loader.land --window-start 1900-01-01 --window-end 2025-09-01 && make build
uv run python -m loader.land --window-start 2025-09-01 --window-end 2025-11-01 && make build
uv run python -m loader.land --window-start 2025-11-01 --window-end 2025-12-01 && make build
```

The first is the one-off load a pipeline runs on the day it is deployed, before the schedule takes over. It is not decoration: inpatient stays run at 30 to 50 a year, so the three scheduled months hold five of them, and a readmission mart built over those is empty with every test green.

Re-running a window replaces it rather than adding to it, so a backfill can be repeated, and a window can be re-landed inside a larger one that was already loaded, without double-counting. Replacing a window means deleting it first, and the whole window is one transaction so that a load killed partway leaves it as it was rather than emptying it.

### One window, two notions of what belongs to it

Encounters and the conditions and medications recorded at them are selected by **service date**: the window holds what happened in it.

Claims come from Synthea's own claims export, one row per real claim linked to its encounter, and are selected by the date they were **billed**. An encounter bills 1 to 46 of them, averaging 7.98 for an inpatient stay against 1.45 for an ambulatory visit, and they share a billing date, so a visit is billed as a unit. Their amounts apportion the encounter's cost, with the last claim taking the remainder rather than its own rounded share, so per-encounter totals reconcile exactly despite the fan-out. That fan-out is the correctness problem the grain tests exist to catch.

The billing lag is the export's own and is right-skewed the way a real one is: median 0, 95th percentile 6 days, and a tail out to 100. That tail is why `fct_claim` is keyed and filtered on the billing date rather than the service date: a claim can bill for a service three months gone, and only the billing date says when the warehouse actually saw it.

The one distortion the export lacks is restatement: it contains no `ADJUSTMENT` transactions, so no claim ever re-arrives amended. The loader injects that alone, at 5% of claims, 1 to 30 days later under the same `claim_id` with an adjusted amount. `raw.claims` is therefore an append log of submissions rather than one row per claim, and collapsing it is `fct_claim`'s job. Every injected value is a pure function of the claim id and a fixed seed, computed with `md5`, so a claim lands in the same window whatever order a backfill runs in. `meta.injection_log` records only what was injected, and `raw.claims` carries no restatement flag, so nothing downstream can identify an amendment except by merging on the claim id.

One consequence worth expecting: the leading edge of a backfill is ragged, because claims billed then may be for services that predate the range and were never landed.

### A source that mutates, so a snapshot has something to observe

`raw.patients_current` holds every patient as they stood at the **window end**, replaced outright on every run. It exists because a dbt snapshot detects change by comparing a source against what it saw last time, and Synthea hands over history directly: dated rows in a file that never changes. The table throws that history away so the snapshot can rediscover it one window at a time, which is what most operational source systems look like anyway.

What changes is the payer, resolved from the payer transitions export, which is the export's own coverage history rather than anything injected. Landing the window ending 2025-11-01 and then the one ending 2025-12-01 moves four patients: Medicaid to Cigna, Humana to Medicare, Cigna to Aetna, and Medicare to uninsured. Death is the other change, and a deceased patient stays in the table with a flag set, so the snapshot sees a change rather than a row vanishing.

It deliberately carries only attributes that can be evaluated as of a date. Marital status, address, income and healthcare expenses are current values fixed at generation time, so stamping them onto a row dated years earlier would assert something false and make any date-aware join to `dim_patient` confidently wrong. Age is left out for a different reason: it is derivable from the birthdate and the date being asked about, and versioning it would add a dimension row per patient per year, burying the changes that matter.

**The snapshot has to run per window.** Because the table is replaced rather than accumulated, it only ever holds the latest as-of date. A backfill that runs the loader sixty times and `dbt snapshot` once at the end captures one state and loses the other fifty-nine.

### What backs this up, and how that differs from production

Nothing backs up `warehouse.duckdb`, because it is derived rather than authoritative. The committed export and the loader reproduce any window on demand, so recovery means re-running the loader, not restoring a file. Reverting the code is git's job and the data follows from it.

That works here for a reason that does not hold in production: **the source is immutable and complete**. Every record already exists in `data/`, and the window is the pretence that it does not. Re-landing last March in a year's time returns exactly what it returns today.

A real source accumulates and mutates, so re-reading a past window can legitimately return something different, or nothing at all if the source has aged the records out. Once that is true, the raw layer holds the only copy of what arrived and stops being reproducible. The usual answer is an immutable landing zone, with extracts written once to object storage and never rewritten, and the warehouse loaded from those files rather than from the source. The committed Parquet files are that landing zone in miniature, which is the only reason this project can get away with no backups at all.

Two smaller differences worth naming. A single DuckDB file has no point-in-time recovery, so the transaction guarantees a window is never half-written but nothing lets you read the warehouse as it stood an hour ago. And the loader manufactures restatements that a real extract would merely observe, because the export has none; the late arrivals, by contrast, are the export's own.

## Transforming

The dbt project is in `transform/`. It declares the seven raw tables as sources, guards them with a freshness check, models each as a staging view, and builds a small star schema on top.

```sh
make freshness  # has the loader run recently enough
make build      # build the models and run their tests
make docs       # serve the model documentation and lineage graph
```

### Staging

One view per source table, renaming and casting only: no joins, no filters, no business logic. Codes are carried as text rather than the integers the export sniffed them into, because a SNOMED or RxNorm code is an identifier and the condition-grouping seed will key on it as one.

**`stg_claims` deliberately does not deduplicate.** `raw.claims` is an append log of submissions, so a restated claim appears twice under one `claim_id` with different amounts, and collapsing that is `fct_claim`'s job via a merge on the claim id. A staging model that quietly picked the latest arrival per claim would look entirely reasonable and would remove the thing the incremental model exists to demonstrate. Its `claim_id` therefore carries no `unique` test.

Two relationships are tested and a third deliberately is not. Conditions and medications must reference a landed encounter, and do by construction, since the loader selects them by joining to the window's encounters. Claims need not: a claim is windowed on its billing date, so it can bill for a service that predates the range and was never landed. That test warns rather than fails, and reports none over the windows CI builds: the history load covers every encounter those windows could bill for. A pipeline whose history began at the first scheduled window would see them, which is why the test stays at warn rather than being deleted for reporting zero.

Nothing tests a reference to `stg_patients_current`, because it would be wrong. The table is replaced each run and filtered to patients born by the window end, so landing an earlier window after a later one strands the encounters already there: the three windows in order, and then 1900-01-01 to 1950-01-01, leaves 27,061 of 31,611 encounters pointing at patients the table no longer holds. **A backfill therefore has to run its windows in ascending order**, which is a constraint on the DAG rather than on the models.

`--full-refresh` is not the way out of that one. The raw table itself holds the wrong as-of date, so rebuilding the models over it reproduces the wrong answer faithfully. Re-land the latest window instead. The distinction is worth keeping straight: where raw is complete and only a model is stale, a rebuild is the fix; where the loader has overwritten raw with an older state, only the loader can put it back.

**Freshness here measures the loader, not the data.** `_loaded_at` is wall-clock at the moment a row lands, so it answers whether the extract ran rather than whether the records are recent: backfilling a window from 2015 stamps every row with now and reports green. That is what makes it worth running as a precondition on the build rather than as a report after it. The thresholds warn at 24 hours and error at 48, so one missed daily run warns and two error.

One reading to expect. On the windowed tables freshness reports the last window that landed rows, not the last run, because the loader replaces a window rather than touching every row. `providers`, `payers`, and `patients_current` are rewritten whole each run, so they always read as current.

### Marts

Five models, materialised as tables rather than views because they are joined and aggregated far more often than they are built.

| Model | Grain |
|---|---|
| `dim_provider` | One row per clinician, all 650 of them, not only the 585 with an encounter in the landed window |
| `dim_payer` | One row per payer, including `NO_INSURANCE`, which is how the source records an uncovered patient |
| `fct_encounter` | One row per encounter |
| `fct_claim` | One row per claim, holding its latest submission |
| `fct_readmission` | One row per index admission, which is the denominator of the 30-day readmission rate |

**`fct_encounter` counts its children in CTEs rather than joining them in.** Conditions and medications each fan out from the encounter, at 1.58 and 2.02 rows apiece, so joining both directly turns 31,611 encounters into 61,812 rows and overstates `sum(total_cost)` more than fourfold, from £95.8m to £450.9m. The cost multiplies faster than the row count because the encounters that fan out hardest are the expensive ones. Aggregating each child to encounter grain first means it contributes one row and one number.

The version of that bug worth fearing is the one that adds a `group by`. It restores the grain exactly, so the row count and every cost reconcile, and only the counts are wrong: 37,254 conditions against a true 19,883. Uniqueness, grain, and relationship tests all pass over it. `transform/tests/assert_child_counts_reconcile.sql` totals the counts against the tables they came from, and is the only one of the 67 checks that catches it.

**No claim amounts here.** `raw.claims` is an append log, so summing it would count a restatement twice. Such a total would also go stale: encounters are windowed on service date and claims on billing date, so an encounter landed in September still has claims arriving in October. Consumers join `fct_claim` to `fct_encounter` rather than reading a rollup that was correct when it was built.

Both dimensions hold every member rather than only the referenced ones, so they do not change shape under the fact they are conformed against.

### fct_claim, the incremental model

The only incremental model, merging on `claim_id`. `raw.claims` is an append log, so a restated claim arrives a second time under the same id; the merge replaces the row rather than adding one, which is what turns 63,417 arrivals into 60,433 claims.

**The filter compares per claim, not against a table-wide high-water mark.** An arrival is new if it is later than the one this model already holds *for that claim*. The obvious alternative, `received_date > (select max(received_date) from {{ this }})`, prunes better and is sound only while windows land in ascending order and are never re-landed. It fails silently the moment they do not: landing the three windows in reverse leaves **422 claims of 60,433**, because every earlier arrival is behind the high-water mark the latest window set.

**So landing order does not matter.** Building the windows forward, and building them reversed, both produce 60,433 claims, identical to a full refresh. That is what a backfill needs, and it is a property of the comparison rather than of the schedule.

**One thing still needs `--full-refresh`,** and it is a property of `merge` rather than of the filter: a merge cannot delete. If a re-landed window drops an arrival the model has already merged, nothing removes the stale row, measured at five claims silently wrong with row counts matching throughout.

That the filter is on `received_date` at all is the decision everything else follows from. It is the date the claim arrived and the date `raw.claims` is partitioned by, so comparing against it is exact. Filtering on `service_date` instead would mean rebuilding whole service-date partitions to catch a claim billed 100 days late, and accepting a permanent miss rate wherever the rebuild window stopped short. That is the argument for `merge` on the claim id over `delete+insert` or `insert_overwrite`.

Every column describes a single arrival: its keys, its dates, its amounts. That is what makes the landing order irrelevant, since nothing in the row depends on having seen the arrivals before it. Anything summarising a claim across arrivals belongs to whatever has the whole history in front of it, which an incremental model reading forward does not.

One cost worth naming. A per-claim comparison cannot be pushed down to skip files or partitions, so the source is read in full on every build. At 63,417 arrivals that is free, and at a billion it would be the first thing to fix.

A date prune beside the comparison is not the fix, however obvious it looks. The prune runs first and discards rows before the comparison can protect them, so a 30-day one reintroduces the failure the comparison exists to prevent: a reversed backfill drops back to 422 claims of 60,433. What works is an exact bound rather than a guessed one, with the scheduler passing the window it is currently processing so the model reads that window and nothing else. Backfilling September then prunes to September, and the comparison still decides what is newer. That is what an Airflow data interval is for, and it is the next thing this project builds.

**`meta.injection_log` is declared as a source so the merge can be checked.** Keeping the wrong arrival is invisible to every structural test: the row count, the grain, and uniqueness are all correct whichever of the two you keep. Reconciling against the injection log is the only check that fails, and it does so for all 2,984 logged restatements.

### fct_readmission, the one that answers a question

Every other model reshapes a table. This one defines something: an index admission is an inpatient encounter discharged at least 30 days before the latest admission the warehouse holds, and a readmission is the earliest later inpatient admission for the same patient, 1 to 30 days after that discharge. 613 index admissions, 125 readmissions, and the rate is `sum(is_readmitted)` over `count(*)` computed by whoever asks rather than stored, so a cut by payer or by year needs no new model.

**The exclusions are where the judgement is.** A gap of 0 or less is a transfer, not a readmission: one rule covers the 22 stays beginning on the day of discharge and the 2 beginning before it, and admitting them would report 142. Day 30 counts, and that is not cosmetic either, because Synthea generates recurring admissions on near-monthly cycles: 32 of the 125 sit exactly on day 30, so reading the bound as 29 would drop a quarter of them. Admissions discharged within 30 days of the end of the data are excluded entirely, on the same reasoning that keeps claim rollups off `fct_encounter`. A rate over admissions that have not yet had time to be readmitted is biased downwards at the edge and gets trusted anyway.

**Its two singular tests catch the model drifting from the rules, not the rules being wrong.** Turning the left join into an inner one collapses the denominator into the numerator and reports a 100% rate with the grain, uniqueness, and null checks all green; `assert_readmission_denominator_is_complete` is the only thing that fails. Reversing the window order names the wrong encounter, and `assert_readmission_is_the_earliest_qualifying` catches it on a single row. But change the 30-day boundary in the model *and* in the test, which is how a definition actually gets edited, and all 89 dbt checks stay green over 142 readmissions. `scripts/check_warehouse.py readmissions-match-the-export` pins the counts against a separate implementation of the definition, and is what notices.

**Dates are read in UTC, pinned in `profiles.yml`.** The export's timestamps carry a zone, so a discharge falls on a different date depending on where dbt runs. Building the same warehouse in Sydney without the pin gives 127 readmissions instead of 125 and 387 differing rows, with every dbt check green. `loader.connect` pins the same setting for the same reason; this is that bug one layer up.

The number is not clinically meaningful and the README would be worse for pretending otherwise. Synthea does not model readmission behaviour, seven patients hold 20 or more inpatient stays each and account for 110 of the 125, and 79 of the inpatient encounters are a recurring detoxification regimen that a real measure would exclude as planned. The query is the deliverable; the rate is an artefact of the generator.

## Orchestrating

```sh
astro dev start     # Airflow on localhost:6563, needs Docker
```

One DAG, `health_warehouse`, lands a window and builds every model as its own task. It runs monthly because the loader's windows are monthly and Airflow's data interval is a half-open date range exactly as `land_window` takes it, so the DAG does no date arithmetic of its own.

Two settings carry the whole thing, and what each one is for was established by removing it and watching what the warehouse came out like.

**A `warehouse` pool of one slot, holding the loader and every dbt task.** DuckDB takes a single writer between processes, and both the loader and each of Cosmos's per-model dbt invocations are their own process. Without the pool they fight for the file, and the two ways that goes are not equally kind. When the warehouse already exists, the loser gets `Could not set lock on file` and fails loudly. When it does not, the lock DuckDB is contending for lives *inside the file it is about to create*, so two writers can both acquire it, both commit, and the second to close leaves behind a file the other's window is simply missing from. Both tasks report success, and each returns an accurate count of the rows it inserted, because each really did insert them.

The pool is named for the warehouse rather than for dbt because that is what it protects. A cold first backfill is exactly the case where the file does not exist yet.

**`max_active_runs=1`, which the pool does not give you.** The pool decides that one task writes at a time; it does not decide that a window is built before the next one lands. Raised to 3, `catchup` starts every interval at once, and because landing is quick and building is not, all three loaders take the pool first: `raw.patients_current` reaches the final window's as-of date within three seconds, before a single model has been built. Nothing is lost from raw, and every model rebuilt from it comes out byte-identical. The snapshot does not, because it is the one thing here that reads a state rather than a range. All three runs snapshot the same table, one as-of date is captured instead of three, and `dim_patient` comes out at 554 rows against 567, having lost every version row it should hold.

Every run reports success. The write guard stays quiet, correctly: the as-of date never ran backwards, it skipped. This is the same property as **the snapshot has to run per window** above, arrived at from the other direction, and it is the reason the schedule serialises whole runs rather than merely their writes.

Tests run as one task after every model rather than one per model. Cosmos attaches a test to the model it points at, so a `relationships` test on a conformed dimension would run before the fact that references it exists.

The backfill over three monthly intervals produces a warehouse byte-identical to landing the same three windows by hand, and clearing an interval and letting it re-run changes nothing.

## What of this would survive in a real pipeline

Worth knowing before reading the loader, because the split does not run file by file. It runs through the middle of each one.

**Transfers unchanged.** The windowing contract, `land_window(start, end)` over a half-open interval, is exactly how a real extract task is parameterised from a scheduler's data interval. Delete-on-a-partition-predicate then insert, all inside one transaction, is the idempotency and atomicity pattern a real warehouse needs, differing only in dialect: `MERGE`, `INSERT OVERWRITE PARTITION`, a partition swap. Deciding *which* date a row belongs to, a condition by its parent encounter's service date and a claim by its billing date rather than its service date, is a real decision that quietly breaks incremental models when it is wrong. `_loaded_at` is ordinary ingestion metadata. Most of the tests, too: idempotency, atomicity under a killed load, and adjacent windows tiling are what an extract layer should be checked for anywhere.

**Would be deleted.** The restatement injection and `meta.injection_log`, which exist because the export contains no amendments and a merge would otherwise have nothing to collapse. Apportioning the encounter's cost across its claims, which is only needed because the 216 MB transactions file holding the real charges was left out. And `patients_current`'s as-of resolution, since a production source system already shows only current state and needs no help being flattened.

**Would be replaced.** Eight lines. Every `read_parquet` call is the seam where a real source connector goes, and nothing around them changes.

That makes the loader the most scaffolding-heavy part of the project, and it is now finished. The layers that follow are closer to shippable: the staging and mart models are the SQL anyone would write, `fct_claim`'s incremental merge is a production pattern against a source that really does restate, and the Airflow DAG with its retries, pool, and backfill is orchestration as it would really be configured. What the loader buys is that those layers meet real problems, late arrival, fan-out, and a mutating dimension, rather than clean data where `merge` and `catchup` would be decorative.

## Development

```sh
make            # list targets
make check-all  # lint, typecheck, tests
```

Requires [uv](https://docs.astral.sh/uv/). Python 3.13.

## Licence

MIT. The Synthea export is synthetic data generated by a public tool and carries no patient information, real or reconstructable.
