# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## The idea

An analytics pipeline over synthetic patient data, built to gain working experience with dbt and Airflow. The domain is chosen for its awkwardness: claims arrive late, records get restated, and patient attributes change. Those problems are what make incremental models, snapshots, and idempotent backfills necessary rather than decorative.

## The export is a stand-in

**`data/*.parquet` represents a live source system landing into a raw bucket. It is committed and immutable only because simulating a moving source is work this project has chosen not to do.** That is a concession to keeping a clone runnable in fifteen minutes, not a property of the thing being modelled.

Build as though the source restates rows, corrects columns, and changes shape between runs, because the real one would. **"This column cannot change, so nothing need handle it" is a fact about Synthea, not about what Synthea is imitating.** A patient system corrects birthdates and re-collects ethnicity as a matter of routine, which is why `snap_patient` checks every attribute rather than only the two this export moves.

Where the stand-in genuinely constrains a design, say so out loud rather than quietly designing around it. `land_as_of` resolving attributes at an arbitrary past date is the clearest case: a real source could not answer that question, which is the whole reason snapshots exist.

**Add structure when the code needs it, not before.** Directories, CI jobs, and README sections arrive with the work that requires them. Anything below is here because it exists today.

## Commands

`make` lists the targets. `make check-all` runs lint, typecheck, and tests.

dbt lives in its own dependency group so the loader stays runnable without it: `uv run --group dbt dbt build`. Airflow and Cosmos are the `airflow` group, which the Astro image installs and `make typecheck` needs so `ty` can resolve the DAG's imports.

**Run every piece of Python through `uv run python`,** including throwaway snippets written to check a behaviour. Bare `python3` on this machine is the system 3.9 with none of the project's dependencies, so anything run that way is testing a different environment than the code lives in, and will mislead you.

If a `make` target fails, reach for `uv run <command>` rather than a bare `python`, `pytest`, or `ruff`. The Makefile is a shortcut to those commands, not a separate way of running them.

## The dbt project

`transform/` holds it. `make build` builds and tests, `make freshness` checks the loader ran recently enough, `make docs` serves the lineage graph.

| Path | |
|---|---|
| `models/staging/` | One view per source table. Rename and cast only: no joins, no filters, no deduplication. |
| `models/marts/` | Dimensions and facts, materialised as tables by `dbt_project.yml`. |
| `snapshots/` | `snap_patient`, the type-2 history behind `dim_patient`. |
| `models/*/_models.yml` | Descriptions and generic tests, one per directory. |
| `models/staging/_sources.yml` | Both sources: `raw`, and `meta` for the loader's injection log. |
| `macros/` | `refuse_a_backwards_as_of_date`, the snapshot's write guard. |
| `packages.yml` | One package, `dbt_utils`, pinned by `package-lock.yml`. `make deps` installs it, and every target that compiles the project depends on it. |
| `transform/tests/` | Singular tests, each a query that must return no rows. Not `tests/`, which is pytest over the loader. |

Four things about dbt here that took finding:

- **`--project-dir` and `--profiles-dir` go after the subcommand.** `dbt --project-dir transform build` fails with `No such option`, and suggests `--deprecated-defer`.
- **A relative `path` in `profiles.yml` resolves against the invoking directory, not `--project-dir`,** and DuckDB creates whatever file it is handed, so a wrong one gives an empty database rather than an error. The Makefile exports an absolute `DBT_WAREHOUSE_PATH`; override it to point a check at a copy.
- **Generic test arguments nest under `arguments:`.** The older top-level form still runs, and says so only as a deprecation summary at the end of a build that otherwise reads clean.
- **dbt validates a source's own properties but accepts invented keys on its tables silently.** A mistyped `data_tests:` disables those tests without a word.

## Landing more than one window

Some models can only be exercised across windows: `fct_claim`'s merge needs a second landing to reach its incremental branch at all, and a snapshot needs one run per window, because `raw.patients_current` is replaced rather than accumulated and so only ever holds the newest as-of date.

```sh
uv run python -m loader.land --window-start 1900-01-01 --window-end 2025-09-01
make build
uv run python -m loader.land --window-start 2025-09-01 --window-end 2025-11-01
make build
uv run python -m loader.land --window-start 2025-11-01 --window-end 2025-12-01
make build
```

These are the boundaries CI uses. 2025-12-01 is where the payer history stops being renewed, so a window ending later shows the snapshot mostly lapses to null rather than switches between payers.

**The first window is the history load,** the one-off a pipeline runs on the day it is deployed, before the schedule takes over. It starts in 1900 because the export's first encounter is 1915-10-27, and the marts that measure rather than reshape need it: inpatient stays run at 30 to 50 a year, so the three scheduled months hold five of them and a readmission mart over them is empty with every test green.

To work against a copy instead of `warehouse.duckdb`, pass `database=` to `land_window` and set `DBT_WAREHOUSE_PATH` to the same file.

**Land windows in ascending order.** `raw.patients_current` is filtered to patients born by the window end, so an earlier window landed after a later one strands the encounters already there: the three windows above, then 1900-01-01 to 1950-01-01, leaves 27,061 of 31,611 encounters pointing at patients the table no longer holds. `--full-refresh` does not undo it, because the wrong as-of date is in raw and a rebuild reproduces it. Re-land the latest window instead.

The snapshot refuses it rather than absorbing it. A `pre_hook` on `snap_patient` compares the arriving as-of date against the history already held and fails the build when it runs backwards, so nothing is written and re-landing the latest window recovers exactly as it does for raw. That hook is the only reason the mistake stays recoverable: a snapshot that has written a false row cannot be repaired, because `--full-refresh` rebuilds from current state and discards every version captured so far.

## Airflow

`astro dev start` brings up Airflow on localhost:6563 and needs Docker running. `astro dev restart` rebuilds the image, which anything outside `dags/` and `include/` needs, since only those are bind-mounted.

Two DAGs, sharing `dags/warehouse.py`. `health_warehouse` lands a window and builds every model but the snapshot's; `patient_history` runs the snapshot and `dim_patient`, triggered by an `Asset` rather than a schedule and with `catchup=False`, because a snapshot accumulates where every other model rebuilds. They are split by one Cosmos selector, `stg_patients_current+`, excluded by the first and selected by the second, so no node is built twice and no test runs in the DAG that lacks its model. It starts at the view rather than at the snapshot so that the snapshot DAG owns everything it reads.

The warehouse lives at `include/warehouse.duckdb` under Airflow, separately from the `warehouse.duckdb` the Makefile builds at the root. `include/` is bind-mounted, so it survives a rebuild and can be read from the host.

The DAG's intervals are the scheduled windows only, so land the history into it once before starting Airflow, or `fct_readmission` is empty there while the root warehouse holds it:

```sh
uv run python -m loader.land --window-start 1900-01-01 --window-end 2025-09-01 --database include/warehouse.duckdb
```

Eight things about this stack that took finding:

- **`RenderConfig.exclude` is forwarded to the `TestBehavior.AFTER_ALL` task, `select` too.** That is what makes the DAG split safe: excluding `stg_patients_current+` removes `dim_patient`'s tests along with the model, rather than leaving them to run against a table this DAG never builds.
- **An outlet belongs on a task, not on `operator_args`,** which is global to the task group and would emit the asset from every model Cosmos generates. It sits on `land` because raw is the snapshot's only dependency.
- **A backfill produces one consumer run per asset event,** three for three intervals here, and nothing orders them against the producer that keeps triggering them. The single pool slot is what does: widen it, or `max_active_runs`, and all three snapshot whatever `patients_current` has reached by then.
- **Airflow 3 resolves a bare cron string to a `CronTriggerTimetable`, whose interval is `timedelta(0)`.** Every run would hand the loader a window with start equal to end. `CronDataIntervalTimetable` is the one that still spans the period, and a month cannot be expressed as the fixed `timedelta` the trigger timetable takes.
- **`end_date` bounds the logical date,** which that timetable sets to the interval *start*, so it admits the window beginning on that date rather than the one ending there.
- **Cosmos takes `env` in `operator_args`, not `default_args`.** It runs dbt from a temporary copy of the project, so without an absolute `DBT_WAREHOUSE_PATH` the profile's relative default silently creates an empty database in that copy.
- **`ProjectConfig.dbt_project_path` and `ExecutionConfig.dbt_project_path` are mutually exclusive,** and `LoadMode.DBT_MANIFEST` needs `manifest_path` given explicitly.
- **`ProjectConfig.install_dbt_deps` defaults to true, and resolves to false only while no `packages.yml` exists.** Adding one is therefore enough to make every dbt task fetch from the package hub at run time, in its temporary project copy. `install_dbt_deps=False` links the image's `dbt_packages` instead, which is why the Dockerfile installs them.

## Code conventions

**Google-style docstrings throughout.** Sections are `Args:`, `Returns:`, `Raises:`, `Yields:`, and `Example:`.

```python
def land_window(start: date, end: date, *, seed: int | None = None) -> LoadReport:
    """Land the records belonging to a date window, replacing anything already there.

    Args:
        start: First service date to land, inclusive.
        end: Last service date to land, exclusive.
        seed: Fixes the injected claim lags and restatements, so a window lands
            identically on every run. Defaults to nondeterministic injection.

    Returns:
        The counts landed per table, and the path of the injection log written.
    """
```

Do not repeat types in the docstring: the annotations are the source of truth and `ty` checks them, so a type written twice is a type that can disagree with itself. Describe what the argument means, not what it is.

**Keep docstrings as short as the function allows.** One line is the default, not the exception. Add a section only where a reader would otherwise guess wrong.

- Delete anything that restates the signature. `Returns: The result.` is noise.
- Describe the contract, not the implementation. A docstring that changes every time the body does is documenting the wrong thing.
- Skip the docstring entirely on self-evident private helpers rather than padding one out.

Ruff selects `D2` and `D4` with the Google convention, so docstring layout and section structure are checked, and `D417` catches an `Args:` section that omits a parameter. `D1` is not selected: nothing demands a docstring where none is warranted.

## Comments

The same discipline as docstrings, applied to config files, CI, SQL, and the Makefile.

A comment earns its place by recording a decision a reader could not reach alone: why this value, what breaks without it. One sentence is the budget, and most lines need none.

- Do not explain the tool. What `.PHONY` is for, what `is_incremental()` does, what `eol=lf` means: a reader can look these up, and the comment rots when the tool changes.
- Do not elaborate. The reason earns a sentence; the mechanism behind the reason almost never does.
- Do not record status. "No models yet" is true when written, wrong within a week, and nobody updates it.
- Do not comment an absence. Either give the reason or delete the line.
- If the rationale is longer than the config it explains, it belongs in your reply, not the file.

In dbt, a model's `description` in its YAML is the documentation and ships in `dbt docs`; a SQL comment is for the reader of the query alone. Grain, and the reasoning behind a lookback window or a materialisation, belong in the description.

## Spelling

British English in prose, per the global conventions. The exception is anything that is an identifier: Synthea's export filenames and columns (`ENCOUNTERCLASS`, `TOTAL_CLAIM_COST`), dbt and DuckDB keywords, and the model, column, and macro names built on them. Those keep the spelling they ship with, so a mart column stays `materialized` and `color` where the tool says so.
