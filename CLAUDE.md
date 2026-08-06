# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## The idea

An analytics pipeline over synthetic patient data, built to gain working experience with dbt and Airflow. The domain is chosen for its awkwardness: claims arrive late, records get restated, and patient attributes change. Those problems are what make incremental models, snapshots, and idempotent backfills necessary rather than decorative.

**Add structure when the code needs it, not before.** Directories, CI jobs, and README sections arrive with the work that requires them. Anything below is here because it exists today.

## Commands

`make` lists the targets. `make check-all` runs lint, typecheck, and tests.

dbt lives in its own dependency group so the loader stays runnable without it: `uv run --group dbt dbt build`.

**Run every piece of Python through `uv run python`,** including throwaway snippets written to check a behaviour. Bare `python3` on this machine is the system 3.9 with none of the project's dependencies, so anything run that way is testing a different environment than the code lives in, and will mislead you.

If a `make` target fails, reach for `uv run <command>` rather than a bare `python`, `pytest`, or `ruff`. The Makefile is a shortcut to those commands, not a separate way of running them.

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
