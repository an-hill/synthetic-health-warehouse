"""Resolves each patient as they stood at a window end, into a source that mutates.

A dbt snapshot detects change by comparing a source against what it saw last
time, so it needs a source that mutates. Synthea hands over history directly,
which is the opposite: dated rows in a file that never changes. This table
deliberately throws that history away and holds only the current state, letting
the snapshot rediscover the history one window at a time. Most operational
source systems really do show only current state; the export is the odd one.

What changes is the payer, taken from the payer transitions export, which is real
coverage history rather than anything injected. Death is the other.
"""

from datetime import date

import duckdb

# Attributes of the patients export deliberately left out. MARITAL, ADDRESS, CITY,
# ZIP, INCOME and HEALTHCARE_EXPENSES are current values fixed at generation
# time, so stamping them onto a row dated years earlier would assert something
# false, and a date-aware join to dim_patient would return a confidently wrong
# answer. AGE is excluded for a different reason: it is derivable from the
# birthdate and the date being asked about, and versioning it would add a
# dimension row per patient per year, burying the changes that matter.


def create_tables(con: duckdb.DuckDBPyConnection) -> None:
    """Create the patient snapshot source, which projects two export files rather than mirroring one."""
    con.execute("""
        create table if not exists raw.patients_current (
            patient_id varchar, birthdate date, gender varchar,
            race varchar, ethnicity varchar, birthplace varchar,
            payer_id varchar, is_deceased boolean, deceased_date date,
            _as_of_date date, _loaded_at timestamp with time zone
        )
    """)


def land_as_of(con: duckdb.DuckDBPyConnection, *, patients_path: str, transitions_path: str, as_of: date) -> int:
    """Replace the table with every patient as they stood on `as_of`.

    Replaced rather than accumulated, so the table only ever holds one as-of
    date. A backfill that runs this sixty times and `dbt snapshot` once at the
    end captures one state and loses the other fifty-nine.

    Args:
        con: Connection to land into, already inside the caller's transaction.
        patients_path: Path to the Synthea patients export.
        transitions_path: Path to the payer transitions export, which carries the
            coverage history the payer is resolved from.
        as_of: The date attributes are evaluated at, being the window end.

    Returns:
        The number of patients landed.
    """
    con.execute("delete from raw.patients_current")
    con.execute(
        """
        insert into raw.patients_current
        select
            p.Id, p.BIRTHDATE, p.GENDER, p.RACE, p.ETHNICITY, p.BIRTHPLACE,
            t.PAYER,
            p.DEATHDATE is not null and p.DEATHDATE <= $as_of as is_deceased,
            case when p.DEATHDATE <= $as_of then p.DEATHDATE end as deceased_date,
            $as_of, now()
        from read_parquet($patients_path) p
        -- Exactly one transition covers any date a patient is alive for, so this
        -- neither drops a patient nor duplicates one. A patient not yet born is
        -- absent entirely: their first appearance is a new dimension row, and a
        -- death is a change to an existing one rather than a disappearance the
        -- snapshot would have to be configured to notice.
        left join read_parquet($transitions_path) t
          on t.PATIENT = p.Id
         and t.START_DATE::date <= $as_of
         and t.END_DATE::date > $as_of
        where p.BIRTHDATE <= $as_of
        """,
        {"as_of": as_of, "patients_path": patients_path, "transitions_path": transitions_path},
    )

    counted = con.execute("select count(*) from raw.patients_current").fetchone()
    return counted[0] if counted else 0
