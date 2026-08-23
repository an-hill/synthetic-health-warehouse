# Design notes

The reasoning behind the five decisions the [README](../README.md) lists, and the measurements that settled them. Every number here came from a run.

## Late arrival and restatement

Encounters are windowed on their service date. Claims are windowed on the date they were billed, a different window for the same visit.

The export holds no amendments, so the loader injects them: 5% of claims re-arrive 1 to 30 days later under the same `claim_id` with a changed amount, recorded in `meta.injection_log`. That makes `raw.claims` an append log, and collapsing 63,417 arrivals into 60,433 claims is `fct_claim`'s job, by a merge on the claim id.

The incremental filter compares each arrival against the row already held for that same claim. The obvious alternative is to compare against the newest `received_date` anywhere in the table, `received_date > (select max(received_date) from {{ this }})`, which reads far less data but is correct only while windows land in ascending order and are never re-landed. Landing the same three windows in reverse leaves **422 claims out of 60,433**, because the latest window pushes that one table-wide maximum forward and every arrival from an earlier window then looks too old to keep. The per-claim comparison gives 60,433 either way.

Keeping the wrong arrival is invisible to every structural test, since row count, grain, and uniqueness are correct whichever arrival survives. `meta.injection_log` is declared as a dbt source so that one test can reconcile the landed amounts against what was injected.

A merge cannot delete, so re-landing a window that drops an arrival leaves the stale row in place. Filtering cannot solve this, because only the scheduler knows that this interval has been processed before and needs overwriting. The solution would be to delete the interval's slice of the target before the merge inserts, an `insert_overwrite` sitting on top of the merge rather than replacing it. The cost is a model that depends on being handed the right window, and nothing here would notice a wrong one. Stated, not fixed.

## Fan-out and grain

Conditions and medications fan out from the encounter. Joining both into `fct_encounter` directly turns 31,611 encounters into 61,812 rows and takes `sum(total_cost)` from **$95.8m to $450.9m**. Aggregating each child to encounter grain first means it contributes one row and one number.

The "fix" worth fearing adds a `group by`. It restores the grain exactly, so the row count and every cost reconcile, and only the counts are wrong: 37,254 conditions against a true 19,883. Uniqueness, grain, and relationship tests all pass. `assert_child_counts_reconcile` totals the counts against the tables they came from, and it is the only check that catches this.

Claims fan out harder still, and their amounts apportion the encounter's cost. The last claim takes the remainder instead of its own rounded share, so per-encounter totals reconcile exactly. `fct_encounter` stores no claim rollup at all, because summing an append log would count a restatement twice and any total stored there would go stale as later claims arrive.

## Snapshots over a current-state source

A dbt snapshot detects change by comparing a source against what it saw last time, so it needs a source that mutates. Synthea hands over the opposite: dated history in a file that never changes. `raw.patients_current` throws that history away and holds every patient as they stood at the **window end**, replaced outright each run, and the snapshot rediscovers the history one window at a time. Most operational sources really do show current state alone. The export is the odd one.

Two things move. The payer changes, resolved from the export's own coverage history rather than anything injected, and patients die, with the row staying in the table and a flag set so the snapshot sees a change rather than a row vanishing.

Change is found by dbt's `check` strategy over an explicit column list, because the source carries no row-level modification time. The columns checked are birthdate, gender, race, ethnicity, and birthplace, none of which move in this export. They are carried because a real patient system corrects them, and a correction is dated by the day it arrives. Marital status, address, and income are left out because those change rather than get corrected, on a date a real system records and Synthea withholds: it gives one undated value per patient, and stamping that onto a row dated years earlier would make any date-aware join to `dim_patient` confidently wrong.

`dim_patient` is a type-2 slowly changing dimension, one row per version of a patient. `dbt_valid_from` and `dbt_valid_to` come from `_as_of_at`, the window end, handed to dbt as `updated_at`. A version is therefore stamped with the date its data was true rather than the moment the build ran, so three windows backfilled in one afternoon still date their versions a month apart.

What the dimension answers is what the warehouse knew about a patient on a date, rather than what was true on it. The history only starts when the pipeline does, and the facts rebuild from raw each run, so a 2024 encounter landing in 2025 cannot be excluded from a 2024 answer. Carrying both axes is bitemporal modelling, which is not attempted here.

A snapshot that has written a false row cannot be repaired, because `--full-refresh` rebuilds from current state and discards every version captured so far. Landing an earlier window after a later one closes the open version at a timestamp before it began, and nothing inside dbt says so. The `refuse_a_backwards_as_of_date` macro runs as a `pre_hook`, comparing the arriving as-of date against the history already held and failing the build before anything is written. Re-landing the latest window then recovers it.

## Defining a measure rather than reshaping a table

`fct_readmission` comes out at 613 index admissions and 125 readmissions. An index admission is an inpatient encounter discharged at least 30 days before the latest admission the warehouse holds. A readmission is the earliest later inpatient admission for the same patient, 1 to 30 days after that discharge. The rate itself is not stored, so a cut by payer or by year needs no new model.

Each boundary excludes something. A gap of 0 or less is a transfer, and admitting those would report 142. Day 30 counts because Synthea generates recurring admissions on near-monthly cycles, so 32 of the 125 sit exactly on it. Admissions discharged too near the end of the data are excluded entirely, because a rate over admissions that have not had time to be readmitted is biased downwards and gets trusted anyway. Four `unit_tests:` hold one boundary apiece against fixture rows.

Those unit tests restate the model's rules, so they catch the model drifting from its rules but never the rules being wrong. Editing a definition means changing the boundary in the model and in the test, and doing both leaves every dbt check green over 142 readmissions instead of 125. `scripts/check_warehouse.py` holds the answer as two constants counted from `data/encounters.parquet` independently of the model, and `make check-windows` fails when the model disagrees.

Synthea does not generate readmission behaviour, so the rate is an artefact of the generator rather than a clinical finding. Seven patients hold 20 or more inpatient stays and account for 110 of the 125, and another 79 inpatient encounters are a recurring detoxification regimen that a real measure would exclude as planned. The query is the deliverable.

Two constants only work because the export never changes. Over live data the counts move daily, so an expected value goes stale overnight. A real warehouse reconciles against something with its own authority instead, such as a published test deck for the measure or the figure a regulator calculates and sends back. The check still has to come from outside the definition it is checking.

`dim_patient` and `fct_readmission` carry enforced contracts, so a dropped column or a narrowed type fails the build naming it. The other four carry none, because nothing builds against them.

## Splitting the DAGs on idempotency

Every model rebuilds from raw and backfills freely, but the snapshot accumulates and cannot be repaired. Scheduling both from one DAG forces the weaker of the two onto the stronger.

The split is one Cosmos selector, `stg_patients_current+`, excluded by `health_warehouse` and selected by `patient_history`, so no node is built twice and no test runs in the DAG that lacks its model. The selector starts at the view the snapshot reads rather than at the snapshot itself. A boundary at `snap_patient` would leave the snapshot reading a relation the other DAG owns, and on a cold warehouse that relation is built after the snapshot has gone looking for it.

Both concurrency settings were established by removing them and watching what came out. A `warehouse` pool of one slot holds the loader and every dbt task, because DuckDB takes a single writer between processes. Where the warehouse already exists the loser fails loudly. Where it does not, the lock lives *inside the file about to be created*, so two writers both acquire it, both commit, and the second to close leaves a file the other's window is missing from. Both tasks report success, each with an accurate count of the rows it did insert.

`max_active_runs=1` is there for the snapshot alone. A monthly schedule never has two intervals at once, so it is dormant outside a hand-run backfill. Raised to 3, all three windows still land in ascending order, because the pool grants slots in logical-date order, and every model comes out byte-identical. The snapshot is the casualty: at 3 it captures one as-of date, and `dim_patient` comes out at 554 rows with every version gone, under three green runs.

![The warehouse_raw asset: health_warehouse produces it, patient_history consumes it, and three asset events each name the run they triggered.](img/patient_history_asset.png)

Backfilling the three monthly intervals from cold gives 564 patient versions dated 552, 8, and 4, identical to landing and building those windows one at a time, on four trials with no retries. Nothing in the wiring guarantees it. The asset releases the snapshot the moment `land` commits, and no dependency orders the consumer runs against the producer triggering them. It holds because one pool slot and one active run leave nothing to interleave. Widen either and the history goes first.

CI runs both DAGs with `airflow dags test` and then asserts on the warehouse they built. A run whose logical date falls outside the DAG's `end_date` succeeds having executed nothing at all.

## Timezone

Dates are read in UTC, pinned in `profiles.yml` and in the loader's connection, because the export's timestamps carry a zone. The identical warehouse built under `TZ=Australia/Sydney` gives 127 readmissions and 387 differing rows, with every check green.
