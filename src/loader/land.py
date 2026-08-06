"""Lands a date window of the Synthea export into the raw DuckDB schema."""

import argparse
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import cast

import duckdb

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
) -> LoadReport:
    """Land the records belonging to a date window into the raw schema.

    Args:
        start: First service date to land, inclusive.
        end: First service date beyond the window, exclusive, so that adjacent
            windows tile without overlap or gap.
        database: DuckDB file to land into, created if absent.
        source: Directory holding the Synthea CSV export.

    Returns:
        The counts landed per table.

    Raises:
        ValueError: If the window is empty or inverted.
    """
    if end <= start:
        raise ValueError(f"window end {end} must be after window start {start}")

    with duckdb.connect(database) as con:
        con.execute("create schema if not exists raw")
        _create_tables(con, source)

        con.execute(
            """
            create or replace temp table window_encounters as
            select Id, START::date as _service_date
            from read_csv_auto(?) where START::date >= ? and START::date < ?
            """,
            [_csv(source, "encounters"), start, end],
        )

        rows = {name: _land(con, source, name) for name in WINDOWED_TABLES}
        rows["providers"] = _land_providers(con, source)

    return LoadReport(window_start=start, window_end=end, rows=rows)


def _csv(source: Path, name: str) -> str:
    return str(source / f"{name}.csv")


def _create_tables(con: duckdb.DuckDBPyConnection, source: Path) -> None:
    """Create each raw table from the source's own columns and types, adding the load metadata."""
    for name in WINDOWED_TABLES:
        con.execute(
            f"""
            create table if not exists raw.{name} as
            select *, now() as _loaded_at, null::date as _service_date
            from read_csv_auto(?) limit 0
            """,
            [_csv(source, name)],
        )


def _land(con: duckdb.DuckDBPyConnection, source: Path, name: str) -> int:
    """Land one windowed table, joining it to the window on the key that partitions it."""
    key = "Id" if name == "encounters" else "ENCOUNTER"
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
