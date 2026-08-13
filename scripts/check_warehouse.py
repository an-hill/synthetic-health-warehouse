"""Assertions over a built warehouse, for properties no dbt test can carry.

Each of these is false after a single landing, which is a legitimate state, so a
singular test would leave `make build` red on a warehouse that is entirely
correct. They run after the second build instead.

Every check takes a connection and returns the reason it failed, or None if it
passed, which is what `sys.exit` wants.
"""

import argparse
import sys
from collections.abc import Callable
from datetime import date
from pathlib import Path

import duckdb

from loader.land import DEFAULT_DATABASE, connect

# The as-of date of the window landed out of order to watch snap_patient's
# pre-hook refuse it. Nothing carrying it should ever reach the snapshot.
BACKWARDS_AS_OF = date(2025, 10, 1)

# Patients whose attributes move across the three window ends that get landed:
# nine at 2025-11-01 and four at 2025-12-01, after which Synthea stops renewing
# coverage and a further window would record lapses rather than switches.
EXPECTED_VERSIONED_PATIENTS = 13

# The window ends the two DAG intervals land, and so the as-of date the snapshot
# captures after each. Not the three the CLI lands: the Airflow workflow says why.
EXPECTED_DAG_AS_OF_DATES = [date(2025, 10, 1), date(2025, 11, 1)]

# Counted from data/encounters.parquet independently of the model, so that a
# wrong rule inside it has something to disagree with. 617 inpatient stays land,
# of which 4 are discharged too near the end of the data to have been observed
# for 30 days.
EXPECTED_INDEX_ADMISSIONS = 613
EXPECTED_READMISSIONS = 125


def merge_reached_an_earlier_build(con: duckdb.DuckDBPyConnection) -> str | None:
    """Check a claim arrived in more than one landing, so the merge updated a row rather than only inserting."""
    crossed = _count(
        con,
        "select count(*) from (select claim_id from raw.claims group by 1 having count(distinct _loaded_at) > 1)",
    )
    print(f"{crossed} claims arrived in more than one landing")
    if crossed == 0:
        return "the second window restated nothing the first build had inserted"
    return None


def snapshot_captured_the_payer_changes(con: duckdb.DuckDBPyConnection) -> str | None:
    """Check the snapshot recorded a further version for every patient whose payer moved between the window ends."""
    versioned = _count(con, "select count(*) from (select patient_id from dim_patient group by 1 having count(*) > 1)")
    print(f"{versioned} patients hold more than one version")
    if versioned != EXPECTED_VERSIONED_PATIENTS:
        return f"expected {EXPECTED_VERSIONED_PATIENTS} patients to hold more than one version"
    return None


def nothing_written_from_a_backwards_window(con: duckdb.DuckDBPyConnection) -> str | None:
    """Check the snapshot refused the earlier as-of date rather than writing it.

    A red build is not the assertion. Without the pre-hook the build goes red
    anyway, at assert_patient_versions_tile, having already written the history
    the hook exists to prevent.
    """
    written = _count(con, "select count(*) from snap_patient where dbt_valid_from = ?", BACKWARDS_AS_OF)
    if written:
        return f"{written} versions were written dated {BACKWARDS_AS_OF}"
    return None


def snapshot_ran_once_per_window(con: duckdb.DuckDBPyConnection) -> str | None:
    """Check the asset-triggered DAG captured a version after every window the loader DAG landed.

    The only assertion here that touches patient_history's output at all. It is
    the count of as-of dates rather than of versions because a run executing the
    tasks one at a time cannot say anything about how a real backfill interleaves
    them, which is what the version counts in the README measure.
    """
    rows = con.execute("select distinct dbt_valid_from from snap_patient order by 1").fetchall()
    captured = [row[0].date() for row in rows]
    print("snapshot as-of dates: " + ", ".join(str(day) for day in captured))
    if captured != EXPECTED_DAG_AS_OF_DATES:
        return f"expected {[str(day) for day in EXPECTED_DAG_AS_OF_DATES]}"
    return None


def readmissions_match_the_export(con: duckdb.DuckDBPyConnection) -> str | None:
    """Check fct_readmission counts what the export holds, which its own tests cannot.

    The singular tests restate the model's rules, so they catch the model
    drifting from them and not the rules themselves being changed. Widening the
    gap to admit same-day transfers in the model alone fails
    assert_readmission_is_the_earliest_qualifying. Widening it in that test too,
    which is how a rule actually gets changed, leaves every dbt check green over
    142 readmissions rather than 125. A count fixed outside the project is the
    only thing that notices.
    """
    admissions = _count(con, "select count(*) from fct_readmission")
    readmissions = _count(con, "select count(*) from fct_readmission where is_readmitted")
    print(f"{readmissions} readmissions over {admissions} index admissions")
    if (admissions, readmissions) != (EXPECTED_INDEX_ADMISSIONS, EXPECTED_READMISSIONS):
        return f"expected {EXPECTED_READMISSIONS} over {EXPECTED_INDEX_ADMISSIONS}"
    return None


CHECKS: dict[str, Callable[[duckdb.DuckDBPyConnection], str | None]] = {
    "merge-reached-an-earlier-build": merge_reached_an_earlier_build,
    "snapshot-captured-the-payer-changes": snapshot_captured_the_payer_changes,
    "nothing-written-from-a-backwards-window": nothing_written_from_a_backwards_window,
    "snapshot-ran-once-per-window": snapshot_ran_once_per_window,
    "readmissions-match-the-export": readmissions_match_the_export,
}


def _count(con: duckdb.DuckDBPyConnection, query: str, *parameters: object) -> int:
    counted = con.execute(query, list(parameters)).fetchone()
    return counted[0] if counted else 0


def main() -> None:
    """Run one named check, exiting non-zero with its reason if it fails."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("check", choices=CHECKS)
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    args = parser.parse_args()

    with connect(args.database, read_only=True) as con:
        sys.exit(CHECKS[args.check](con))


if __name__ == "__main__":
    main()
