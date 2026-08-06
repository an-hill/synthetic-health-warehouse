# Synthea export

Synthetic patient records, generated once and committed.

## How this was generated

Release `master-branch-latest` of [`synthea-with-dependencies.jar`](https://github.com/synthetichealth/synthea/releases), published 2026-07-22, `sha256:7fdbc2951d305eebac1fa46b1027347efb7380e93053e39ca2ee86613beb84f9`.

```sh
java -jar synthea-with-dependencies.jar \
  -p 500 -s 1 -cs 1 -r 20260101 -e 20260101 \
  --exporter.csv.export=true \
  --exporter.csv.included_files=patients.csv,encounters.csv,conditions.csv,medications.csv,providers.csv \
  --exporter.baseDirectory=./output
```

`-e` is what pins the data, and the upstream README does not mention it. Synthea's `endTime` is initialised from `referenceTime` when the options object is constructed, so `-r` on its own arrives too late to move it and the simulation still runs to the wall clock: generating on a different day would produce a different export from the same seed.

`--exporter.csv.included_files` is the other one that matters. The default exports everything except `patient_expenses.csv`, which pulls in `observations.csv` — larger than these five files combined, and unused here.

**Reproducible in content, not byte for byte.** Regenerating with the command above returns exactly the same records, verified by comparing sorted contents, but not in the same order: Synthea exports from several threads and whichever finishes first writes first. Committing the output of a second run would therefore produce a large and entirely meaningless diff.

Left at their defaults: `exporter.years_of_history` (10) and `generate.only_alive_patients` (false). State defaults to Massachusetts, so every provider is a Massachusetts one.

## What is in it

| File | Rows | Grain |
|---|---|---|
| `patients.csv` | 554 | One per patient |
| `encounters.csv` | 31,824 | One per encounter |
| `conditions.csv` | 19,993 | One per condition onset, 1.58 per encounter on average and up to 11 |
| `medications.csv` | 29,004 | One per medication order, 2.02 per encounter on average |
| `providers.csv` | 650 | One per clinician, of which 587 appear in `encounters` |

554 patients rather than 500 because `-p` counts the living: the 54 with a `DEATHDATE` are exported on top of it.

Encounters run from 1915-10-27 to 2025-12-31, but 23,404 of the 31,824 fall in 2016 onwards. The tail is a consequence of `years_of_history`, which keeps an encounter outside the ten-year window whenever it carries a condition or medication that never stopped.

`ENCOUNTERCLASS` takes ten values: `ambulatory`, `wellness`, `outpatient`, `urgentcare`, `emergency`, `inpatient`, `home`, `virtual`, `snf`, and `hospice`. Inpatient encounters, which the readmission model is built on, are the thin one at 618 across the whole span, roughly 20 to 50 a year.

Conditions are coded in SNOMED CT throughout, using 250 distinct codes. That is the set the condition-grouping seed has to cover.
