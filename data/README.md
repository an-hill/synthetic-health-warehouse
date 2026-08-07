# Synthea export

Synthetic patient records, generated once and committed.

**These files stand in for a live source system.** They are fixed so that a clone runs without Java in fifteen minutes, not because the thing being modelled is fixed. Nothing downstream should assume a column cannot change because this export does not change it.

## How this was generated

Release `master-branch-latest` of [`synthea-with-dependencies.jar`](https://github.com/synthetichealth/synthea/releases), published 2026-07-22, `sha256:7fdbc2951d305eebac1fa46b1027347efb7380e93053e39ca2ee86613beb84f9`.

```sh
java -jar synthea-with-dependencies.jar \
  -p 500 -s 1 -cs 1 -r 20260101 -e 20260101 \
  --exporter.csv.export=true \
  --exporter.csv.included_files=patients.csv,encounters.csv,conditions.csv,medications.csv,providers.csv,claims.csv,payer_transitions.csv,payers.csv \
  --exporter.baseDirectory=./output
```

`-e` is what pins the data, and the upstream README does not mention it. Synthea's `endTime` is initialised from `referenceTime` when the options object is constructed, so `-r` on its own arrives too late to move it and the simulation still runs to the wall clock: generating on a different day would produce a different export from the same seed.

`--exporter.csv.included_files` is the other one that matters. The default exports everything except `patient_expenses.csv`, which pulls in `observations.csv` and `claims_transactions.csv`. The second of those is 216 MB, ten times everything committed here, and holds the charge and payment lines; the amounts in this project come from the encounter instead, so it is left out.

**Reproducible in content, not byte for byte.** Regenerating with the command above returns exactly the same records, verified by comparing sorted contents, but not in the same order: Synthea exports from several threads and whichever finishes first writes first. Committing the output of a second run would therefore produce a large and entirely meaningless diff.

Left at their defaults: `exporter.years_of_history` (10) and `generate.only_alive_patients` (false). State defaults to Massachusetts, so every provider is a Massachusetts one.

## Converted to Parquet

Synthea emits CSV. The files committed here are that CSV converted once, with DuckDB 1.5.5:

```sh
uv run python -c "
import duckdb
for name in ('patients', 'encounters', 'conditions', 'medications', 'providers', 'claims', 'payer_transitions', 'payers'):
    duckdb.execute(f\"copy (select * from read_csv_auto('{name}.csv')) to '{name}.parquet' (format parquet, compression zstd)\")
"
```

The loader reads the export about twenty times faster this way. The types are the ones `read_csv_auto` inferred, now recorded in the file rather than re-sniffed on every read, which is what removes the schema detection cost entirely.

Converting was verified to change nothing: every table lands identically from either format, compared column by column.

**Every timestamp is `TIMESTAMP WITH TIME ZONE`,** in `encounters`, `medications`, `claims`, and `payer_transitions`. Casting one to a date resolves it in the reader's session zone, so which window a row belongs to would otherwise depend on the machine doing the reading: the September 2025 window holds 227 encounters read in UTC and 225 read an hour east of it. The loader pins its connection to UTC for that reason. `patients` and `conditions` carry plain dates and are not affected.

**The sniffer decided the types, and Parquet has frozen them.** It read `ZIP` as text, so the Massachusetts codes keep their leading zero, and the SNOMED and RxNorm codes as integers. No code in this export begins with a zero, so nothing was lost, but a regenerated export carrying one would lose it silently at conversion. Check before trusting a new export: `read_csv_auto` is the only thing standing between the CSV and what everything downstream believes.

## What is in it

| File | Rows | Grain |
|---|---|---|
| `patients.parquet` | 554 | One per patient |
| `encounters.parquet` | 31,824 | One per encounter |
| `conditions.parquet` | 19,993 | One per condition onset, 1.58 per encounter on average and up to 11 |
| `medications.parquet` | 29,004 | One per medication order, 2.02 per encounter on average |
| `providers.parquet` | 650 | One per clinician, of which 587 appear in `encounters` |
| `claims.parquet` | 60,828 | One per claim, linked to its encounter by `APPOINTMENTID` |
| `payer_transitions.parquet` | 20,693 | One per coverage period, 37.7 per patient |
| `payers.parquet` | 10 | One per payer |

554 patients rather than 500 because `-p` counts the living: the 54 with a `DEATHDATE` are exported on top of it.

Encounters run from 1915-10-27 to 2025-12-31, but 23,404 of the 31,824 fall in 2016 onwards. The tail is a consequence of `years_of_history`, which keeps an encounter outside the ten-year window whenever it carries a condition or medication that never stopped.

`ENCOUNTERCLASS` takes ten values: `ambulatory`, `wellness`, `outpatient`, `urgentcare`, `emergency`, `inpatient`, `home`, `virtual`, `snf`, and `hospice`. Inpatient encounters, which the readmission model is built on, are the thin one at 618 across the whole span, roughly 20 to 50 a year.

Conditions are coded in SNOMED CT throughout, using 250 distinct codes. That is the set the condition-grouping seed has to cover.

Claims fan out over encounters at 1.91 apiece, from 1 to 8, and the heavier classes carry more: an inpatient stay bills 7.97 on average against 1.45 for an ambulatory visit. Getting per-encounter cost right across that fan-out is what the grain tests exist to catch.

`LASTBILLEDDATE1` is a real billing date, distinct from `SERVICEDATE` and populated on every row. The lag between them is right-skewed the way a real one is: mean 0.77 days, median 0, 95th percentile 6, and a tail out to 100. All the claims for one encounter share a billing date, so a visit is always billed as a unit.

The export contains no `ADJUSTMENT` transactions, so no claim ever re-arrives amended. Restatement is the one distortion the loader injects, and the only thing `meta.injection_log` records.

`payer_transitions.parquet` is real insurance history, with `START_DATE` and `END_DATE` timestamps rather than the years the upstream data dictionary describes, and periods beginning on each patient's own anniversary rather than on 1 January. Only 1,565 of the 20,693 rows are an actual change of payer; the rest are renewals with the same one. Around 41 changes fall in 2025, spread over every month.

Coverage resolves cleanly: on any date a patient is alive, exactly one period covers them, with no gaps and no overlaps. The exception is the end of the data, where the simulation stops renewing: 3 living patients are uncovered by 2025-12-31 and 8 by 2026-01-01. That is a trailing edge to keep as-of dates inside, and the counterpart to the ragged leading edge of a claims backfill.
