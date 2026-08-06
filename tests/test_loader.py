"""Tests for the windowed loader.

Expectations are computed against the committed CSVs with an independent query
rather than hardcoded, so regenerating the export cannot silently rot them.
"""

from datetime import date
from pathlib import Path

import duckdb
import pytest

from loader.land import land_window

SOURCE = Path("data")

# Every day of 2025 carries encounters, so a single day is a real window rather
# than an empty one.
DAY = date(2025, 11, 3)
NEXT_DAY = date(2025, 11, 4)
DAY_AFTER = date(2025, 11, 5)


@pytest.fixture
def database(tmp_path: Path) -> Path:
    """Path to a DuckDB file that does not exist yet."""
    return tmp_path / "test.duckdb"


def expected_encounter_ids(start: date, end: date) -> set[str]:
    """The encounter ids a window should land, read straight from the source."""
    with duckdb.connect() as con:
        rows = con.execute(
            "select Id from read_csv_auto(?) where START::date >= ? and START::date < ?",
            [str(SOURCE / "encounters.csv"), start, end],
        ).fetchall()
    return {row[0] for row in rows}


def landed(database: Path, sql: str) -> list[tuple]:
    with duckdb.connect(database, read_only=True) as con:
        return con.execute(sql).fetchall()


def test_lands_exactly_the_encounters_serviced_in_the_window(database: Path) -> None:
    land_window(DAY, NEXT_DAY, database=database, source=SOURCE)

    ids = {row[0] for row in landed(database, "select Id from raw.encounters")}
    assert ids == expected_encounter_ids(DAY, NEXT_DAY)
    assert ids


def test_children_land_with_their_parent_encounter(database: Path) -> None:
    land_window(DAY, NEXT_DAY, database=database, source=SOURCE)

    for table in ("conditions", "medications"):
        orphans = landed(
            database,
            f"select count(*) from raw.{table} c "
            "where not exists (select 1 from raw.encounters e where e.Id = c.ENCOUNTER)",
        )
        assert orphans == [(0,)], f"{table} landed rows referencing an encounter that did not"


def test_child_service_date_is_the_parents_not_its_own(database: Path) -> None:
    land_window(DAY, NEXT_DAY, database=database, source=SOURCE)

    for table in ("conditions", "medications"):
        dates = landed(database, f"select distinct _service_date from raw.{table}")
        assert dates == [(DAY,)], f"{table} was partitioned on something other than the service date"


def test_adjacent_windows_tile_into_the_combined_window(database: Path, tmp_path: Path) -> None:
    land_window(DAY, NEXT_DAY, database=database, source=SOURCE)
    land_window(NEXT_DAY, DAY_AFTER, database=database, source=SOURCE)

    combined = tmp_path / "combined.duckdb"
    land_window(DAY, DAY_AFTER, database=combined, source=SOURCE)

    for table, columns in (("encounters", "Id"), ("conditions", "ENCOUNTER, CODE"), ("medications", "ENCOUNTER, CODE")):
        sql = f"select {columns} from raw.{table} order by all"
        assert landed(database, sql) == landed(combined, sql), table


def test_a_window_with_no_encounters_lands_nothing(database: Path) -> None:
    report = land_window(date(1900, 1, 1), date(1900, 1, 2), database=database, source=SOURCE)

    assert report.rows["encounters"] == 0
    assert landed(database, "select count(*) from raw.encounters") == [(0,)]


def test_report_counts_match_the_database(database: Path) -> None:
    report = land_window(DAY, NEXT_DAY, database=database, source=SOURCE)

    for table, count in report.rows.items():
        assert landed(database, f"select count(*) from raw.{table}") == [(count,)], table


def test_loaded_at_is_populated_and_timezone_aware(database: Path) -> None:
    land_window(DAY, NEXT_DAY, database=database, source=SOURCE)

    rows = landed(database, "select _loaded_at from raw.encounters")
    assert rows
    assert all(row[0].tzinfo is not None for row in rows)


def test_an_inverted_window_is_rejected(database: Path) -> None:
    with pytest.raises(ValueError, match="must be after"):
        land_window(NEXT_DAY, DAY, database=database, source=SOURCE)
