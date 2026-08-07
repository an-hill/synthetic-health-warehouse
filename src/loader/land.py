"""Lands a date window of the Synthea export into the raw DuckDB schema."""

import argparse
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import cast

import duckdb

from loader import claims, patients

DEFAULT_DATABASE = Path("warehouse.duckdb")
DEFAULT_SOURCE = Path("data")

# Conditions and medications carry their own START, which runs up to 1,028 days
# from the encounter they were recorded at. Windowing them on it would strand
# rows referencing an encounter that had not landed, so a window means the
# encounters serviced in it and everything recorded at them.
CHILD_TABLES = ("conditions", "medications")
WINDOWED_TABLES = ("encounters", *CHILD_TABLES)

# Small static dimensions, rewritten whole on every run so that landing one
# window is self-contained. Idempotent by construction, unlike the windowed
# tables, which had to be made so.
FULL_REPLACE_TABLES = ("providers", "payers")


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
        source: Directory holding the committed Synthea export.
        seed: Salts the injected claim restatements. Fixed by default, because
            a repeated backfill has to be identical.

    Returns:
        The counts landed per table.

    Raises:
        ValueError: If the window is empty or inverted.
    """
    if end <= start:
        raise ValueError(f"window end {end} must be after window start {start}")

    with connect(database) as con:
        con.execute("create schema if not exists raw")
        con.execute("create schema if not exists meta")
        _create_source_tables(con, source)
        claims.create_tables(con)
        patients.create_tables(con)

        # The key and the date alone. Selecting every column would make this
        # table as wide as its source, which costs nothing here and is the wrong
        # habit to carry to a source where it would.
        con.execute(
            """
            create or replace temp table window_encounters as
            select Id, START::date as _service_date
            from read_parquet(?) where START::date >= ? and START::date < ?
            """,
            [_export(source, "encounters"), start, end],
        )

        # One transaction for the whole window. Each table is deleted before it
        # is inserted, so a load that dies in between would otherwise leave the
        # window empty rather than replaced: a hole that reports no error and
        # that dbt would happily build clean models over.
        con.execute("begin transaction")
        rows = {name: _land(con, source, name, start, end) for name in WINDOWED_TABLES}
        rows |= {name: _land_whole(con, source, name) for name in FULL_REPLACE_TABLES}
        rows["claims"] = claims.land_arrivals(
            con,
            claims_path=_export(source, "claims"),
            encounters_path=_export(source, "encounters"),
            start=start,
            end=end,
            seed=seed,
        )
        rows["patients_current"] = patients.land_as_of(
            con,
            patients_path=_export(source, "patients"),
            transitions_path=_export(source, "payer_transitions"),
            as_of=end,
        )
        con.execute("commit")

    return LoadReport(window_start=start, window_end=end, rows=rows)


def connect(database: Path | str = ":memory:", *, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Open a connection whose date arithmetic does not depend on where it runs.

    The export's timestamps carry a time zone, so casting one to a date resolves
    it in the session's zone, which is what decides the window a row belongs to.
    Pinned at the connection rather than at each cast, because the default is
    inherited from the host and a predicate added later would inherit it too.
    """
    con = duckdb.connect(database, read_only=read_only)
    con.execute("set TimeZone='UTC'")
    return con


def _export(source: Path, name: str) -> str:
    return str(source / f"{name}.parquet")


def _create_source_tables(con: duckdb.DuckDBPyConnection, source: Path) -> None:
    """Create each raw table that mirrors an export file, taking its columns and types from it."""
    for name in WINDOWED_TABLES:
        con.execute(
            f"""
            create table if not exists raw.{name} as
            select *, now() as _loaded_at, null::date as _service_date
            from read_parquet(?) limit 0
            """,
            [_export(source, name)],
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
        from read_parquet(?) s join window_encounters w on s.{key} = w.Id
        """,
        [_export(source, name)],
    )
    return _rowcount(con)


def _land_whole(con: duckdb.DuckDBPyConnection, source: Path, name: str) -> int:
    """Replace a static dimension outright, taking its columns from the file."""
    con.execute(
        f"""
        create or replace table raw.{name} as
        select *, now() as _loaded_at from read_parquet(?)
        """,
        [_export(source, name)],
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
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    args = parser.parse_args()

    report = land_window(args.window_start, args.window_end, database=args.database)
    print(f"{report.window_start} to {report.window_end}")
    for name, count in report.rows.items():
        print(f"  {name:<17} {count:>7,}")


if __name__ == "__main__":
    main()
