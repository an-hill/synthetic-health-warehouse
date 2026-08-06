"""Lands a date window of the Synthea export into the raw DuckDB schema."""

import argparse
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import cast

import duckdb

from loader import claims

DEFAULT_DATABASE = Path("warehouse.duckdb")
DEFAULT_SOURCE = Path("data")

# Conditions and medications carry their own START, which runs up to 1,028 days
# from the encounter they were recorded at. Windowing them on it would strand
# rows referencing an encounter that had not landed, so a window means the
# encounters serviced in it and everything recorded at them.
CHILD_TABLES = ("conditions", "medications")
WINDOWED_TABLES = ("encounters", *CHILD_TABLES)


@dataclass(frozen=True)
class LoadReport:
    """The window that was landed, and how many rows reached each raw table."""

    window_start: date
    window_end: date
    rows: dict[str, int]


def land_window(
    start: date,
    end: date,
    *,
    database: Path = DEFAULT_DATABASE,
    source: Path = DEFAULT_SOURCE,
    seed: int = claims.DEFAULT_SEED,
) -> LoadReport:
    """Land the records belonging to a date window into the raw schema.

    Encounters and their children are selected by service date; claims are
    selected by the date they were billed, which is days or weeks later, so one
    window holds two different notions of what belongs to it.

    Args:
        start: First date to land, inclusive.
        end: First date beyond the window, exclusive, so that adjacent windows
            tile without overlap or gap.
        database: DuckDB file to land into, created if absent.
        source: Directory holding the Synthea CSV export.
        seed: Salts the injected claim restatements. Fixed by default, because
            a repeated backfill has to be identical.

    Returns:
        The counts landed per table.

    Raises:
        ValueError: If the window is empty or inverted.
    """
    if end <= start:
        raise ValueError(f"window end {end} must be after window start {start}")

    with duckdb.connect(database) as con:
        con.execute("create schema if not exists raw")
        con.execute("create schema if not exists meta")
        _create_source_tables(con, source)
        claims.create_tables(con)

        con.execute(
            """
            create or replace temp table window_encounters as
            select Id, START::date as _service_date
            from read_csv_auto(?) where START::date >= ? and START::date < ?
            """,
            [_csv(source, "encounters"), start, end],
        )

        # One transaction for the whole window. Each table is deleted before it
        # is inserted, so a load that dies in between would otherwise leave the
        # window empty rather than replaced: a hole that reports no error and
        # that dbt would happily build clean models over.
        con.execute("begin transaction")
        rows = {name: _land(con, source, name, start, end) for name in WINDOWED_TABLES}
        rows["providers"] = _land_providers(con, source)
        rows["claims"] = claims.land_arrivals(
            con,
            claims_csv=_csv(source, "claims"),
            encounters_csv=_csv(source, "encounters"),
            start=start,
            end=end,
            seed=seed,
        )
        con.execute("commit")

    return LoadReport(window_start=start, window_end=end, rows=rows)


def _csv(source: Path, name: str) -> str:
    return str(source / f"{name}.csv")


def _create_source_tables(con: duckdb.DuckDBPyConnection, source: Path) -> None:
    """Create each raw table that mirrors a CSV, taking its columns and types from the file."""
    for name in WINDOWED_TABLES:
        con.execute(
            f"""
            create table if not exists raw.{name} as
            select *, now() as _loaded_at, null::date as _service_date
            from read_csv_auto(?) limit 0
            """,
            [_csv(source, name)],
        )


def _land(con: duckdb.DuckDBPyConnection, source: Path, name: str, start: date, end: date) -> int:
    """Land one windowed table, replacing whatever the window already held.

    The delete is what makes a backfill idempotent. It spans the window rather
    than matching a window identifier, so re-landing one day inside a month that
    was already loaded removes that day alone.
    """
    key = "Id" if name == "encounters" else "ENCOUNTER"
    con.execute(
        f"delete from raw.{name} where _service_date >= ? and _service_date < ?",
        [start, end],
    )
    con.execute(
        f"""
        insert into raw.{name}
        select s.*, now(), w._service_date
        from read_csv_auto(?) s join window_encounters w on s.{key} = w.Id
        """,
        [_csv(source, name)],
    )
    return _rowcount(con)


def _land_providers(con: duckdb.DuckDBPyConnection, source: Path) -> int:
    """Replace the whole provider dimension, so that landing one window is self-contained."""
    con.execute(
        """
        create or replace table raw.providers as
        select *, now() as _loaded_at from read_csv_auto(?)
        """,
        [_csv(source, "providers")],
    )
    return _rowcount(con)


def _rowcount(con: duckdb.DuckDBPyConnection) -> int:
    """Rows affected by the statement just executed."""
    return cast(tuple[int], con.fetchone())[0]


def main() -> None:
    """Land one window, named by command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--window-start", type=date.fromisoformat, required=True)
    parser.add_argument("--window-end", type=date.fromisoformat, required=True)
    args = parser.parse_args()

    report = land_window(args.window_start, args.window_end)
    print(f"{report.window_start} to {report.window_end}")
    for name, count in report.rows.items():
        print(f"  {name:<12} {count:>7,}")


if __name__ == "__main__":
    main()
