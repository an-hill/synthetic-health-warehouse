"""Tests for the windowed loader.

Expectations are computed against the committed CSVs with an independent query
rather than hardcoded, so regenerating the export cannot silently rot them.
"""

from datetime import date, timedelta
from pathlib import Path

import duckdb
import pytest

from loader import claims as claims_module
from loader import land as land_module
from loader.land import land_window

SOURCE = Path("data")
TABLES = (
    "raw.encounters",
    "raw.conditions",
    "raw.medications",
    "raw.providers",
    "raw.payers",
    "raw.claims",
    "raw.patients_current",
    "meta.injection_log",
)

# Wide enough to hold a useful number of billed claims and their restatements.
# An encounter's claims all share one billing date, so they always land together
# and never need a settling period to be complete.
CLAIM_WINDOW = (date(2025, 9, 1), date(2026, 1, 1))

# Payer coverage is complete through 2025-12-01 and thins after it, as the
# simulation stops renewing towards its end date. Both as-of dates sit inside
# the covered region so that a change means a real switch, not a record running
# out.
AS_OF_EARLIER = date(2025, 11, 1)
AS_OF_LATER = date(2025, 12, 1)

# Mid-history, where 38 patients have died and 16 have yet to. Every death in
# the export precedes the recent windows, so only a date like this can tell a
# death recorded on time from one recorded early.
AS_OF_MIDLIFE = date(2020, 1, 1)

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


def test_loading_the_same_window_twice_changes_nothing(database: Path) -> None:
    """_loaded_at is wall-clock and moves by design, so it is excluded rather than compared.

    Values are compared as text because DuckDB needs pytz installed to hand a
    timezone-aware timestamp to Python, and nothing outside these tests does.
    """

    def snapshot() -> dict[str, list[tuple]]:
        return {
            t: landed(database, f"select columns(* exclude (_loaded_at))::varchar from {t} order by all")
            for t in TABLES
        }

    land_window(DAY, NEXT_DAY, database=database, source=SOURCE)
    before = snapshot()

    land_window(DAY, NEXT_DAY, database=database, source=SOURCE)

    assert before == snapshot()


def test_a_load_that_fails_partway_leaves_the_window_untouched(database: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Without a transaction the delete would already have committed, emptying the window."""
    land_window(DAY, NEXT_DAY, database=database, source=SOURCE)
    before = {t: landed(database, f"select count(*) from {t}") for t in TABLES}

    real_land = land_module._land

    def die_after_deleting(con, source, name, start, end):
        """Reproduce a load killed between a table's delete and its insert."""
        if name == "medications":
            con.execute("delete from raw.medications")
            raise RuntimeError("load dies partway through the window")
        return real_land(con, source, name, start, end)

    monkeypatch.setattr(land_module, "_land", die_after_deleting)

    with pytest.raises(RuntimeError, match="dies partway"):
        land_window(DAY, NEXT_DAY, database=database, source=SOURCE)

    assert {t: landed(database, f"select count(*) from {t}") for t in TABLES} == before


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
    """The stored type is what matters: dbt source freshness reads the column, not a Python client."""
    land_window(DAY, NEXT_DAY, database=database, source=SOURCE)

    types = landed(
        database,
        "select data_type from information_schema.columns "
        "where table_schema = 'raw' and table_name = 'encounters' and column_name = '_loaded_at'",
    )
    assert types == [("TIMESTAMP WITH TIME ZONE",)]
    assert landed(database, "select count(*) from raw.encounters where _loaded_at is null") == [(0,)]


def test_an_inverted_window_is_rejected(database: Path) -> None:
    with pytest.raises(ValueError, match="must be after"):
        land_window(NEXT_DAY, DAY, database=database, source=SOURCE)


def test_claim_arrivals_do_not_depend_on_backfill_order(database: Path, tmp_path: Path) -> None:
    """The reason restatements are hashed from the claim id rather than drawn from a random stream."""
    october, november, december = date(2025, 10, 1), date(2025, 11, 1), date(2025, 12, 1)

    land_window(october, november, database=database, source=SOURCE)
    land_window(november, december, database=database, source=SOURCE)

    reversed_order = tmp_path / "reversed.duckdb"
    land_window(november, december, database=reversed_order, source=SOURCE)
    land_window(october, november, database=reversed_order, source=SOURCE)

    sql = "select columns(* exclude (_loaded_at))::varchar from raw.claims order by all"
    assert landed(database, sql) == landed(reversed_order, sql)
    assert landed(database, "select count(*) from raw.claims") > [(0,)]


def test_a_window_holds_the_claims_that_arrived_in_it(database: Path) -> None:
    land_window(DAY, NEXT_DAY, database=database, source=SOURCE)

    assert landed(database, "select distinct received_date from raw.claims") == [(DAY,)]
    assert landed(database, "select count(*) from raw.claims") > [(0,)]


def test_the_billing_lag_is_the_real_one_and_is_right_skewed(database: Path) -> None:
    """Check the lag comes from the export rather than from us.

    The tail is a property of the data, which is what makes fct_claim's lookback
    something to estimate rather than something we already know.
    """
    land_window(*CLAIM_WINDOW, database=database, source=SOURCE)

    [(earliest, median, latest)] = landed(
        database,
        """select min(lag_days), quantile_cont(lag_days, 0.5), max(lag_days)
           from meta.injection_log where not is_restatement""",
    )
    assert earliest == 0
    assert median == 0, "most claims are billed the day of service"
    assert latest > 30, "the long tail that forces a lookback is missing"


def test_restatement_lags_use_the_whole_range(database: Path) -> None:
    """Guard against a silent halving of the restatement lag space.

    A bound modulus promotes md5_number's UHUGEINT to DOUBLE, whose mantissa
    drops the low bits, leaving only even lags and raising nothing.
    """
    land_window(*CLAIM_WINDOW, database=database, source=SOURCE)

    # Paired within the log, so a restatement whose original landed in an
    # earlier window cannot contribute a spurious zero.
    [(distinct, lowest, highest, even, odd)] = landed(
        database,
        """select count(distinct r.lag_days - o.lag_days),
                  min(r.lag_days - o.lag_days), max(r.lag_days - o.lag_days),
                  count(*) filter (where (r.lag_days - o.lag_days) % 2 = 0),
                  count(*) filter (where (r.lag_days - o.lag_days) % 2 = 1)
           from meta.injection_log r
           join meta.injection_log o on r.claim_id = o.claim_id
          where r.is_restatement and not o.is_restatement""",
    )
    assert lowest >= 1
    assert highest <= claims_module.MAX_RESTATE_LAG_DAYS
    assert distinct > claims_module.MAX_RESTATE_LAG_DAYS * 0.6, "some restatement lags are unreachable"
    # Both parities, because the DOUBLE promotion collapses to one or the other
    # depending on where the +1 lands.
    assert even > 0, "the restatement lag hash lost its low bits"
    assert odd > 0, "the restatement lag hash lost its low bits"


def test_a_restated_claim_arrives_twice_under_one_id(database: Path) -> None:
    land_window(*CLAIM_WINDOW, database=database, source=SOURCE)

    restated = landed(
        database,
        """select c.claim_id from raw.claims c
           join meta.injection_log l on c.claim_id = l.claim_id and l.is_restatement
           group by 1 having count(*) = 2""",
    )
    assert restated, "no claim landed both an original and a restatement"

    unchanged = landed(
        database,
        "select count(*) from meta.injection_log where is_restatement and amount_before = amount_after",
    )
    assert unchanged == [(0,)], "a restatement left the amount untouched, so it is invisible downstream"


def test_restatements_are_about_five_percent_of_claims(database: Path) -> None:
    land_window(*CLAIM_WINDOW, database=database, source=SOURCE)

    [(pct,)] = landed(
        database,
        """select 100.0 * count(*) filter (where is_restatement) / count(distinct claim_id)
           from meta.injection_log""",
    )
    assert 4 < pct < 6, f"restatement rate drifted to {pct:.2f}%"


def test_an_encounter_fans_out_across_several_claims(database: Path) -> None:
    land_window(*CLAIM_WINDOW, database=database, source=SOURCE)

    [(most, fanned_out)] = landed(
        database,
        """select max(claims), count(*) filter (where claims > 1) from (
             select count(distinct claim_id) as claims from raw.claims group by encounter_id)""",
    )
    assert fanned_out > 0, "every encounter billed a single claim, so there is no fan-out to get wrong"
    assert most > 2, "the heavier encounters should bill many claims"


def test_a_window_lands_every_claim_its_encounters_billed(database: Path) -> None:
    """An encounter's claims share a billing date, so a window never holds part of one."""
    land_window(*CLAIM_WINDOW, database=database, source=SOURCE)

    [(mismatched,)] = landed(
        database,
        f"""with landed_counts as (
                select encounter_id, count(distinct claim_id) as claims from raw.claims group by 1
            ),
            source_counts as (
                select APPOINTMENTID as encounter_id, count(*) as claims
                from read_csv_auto('{SOURCE / "claims.csv"}')
                where LASTBILLEDDATE1::date >= '{CLAIM_WINDOW[0]}' and LASTBILLEDDATE1::date < '{CLAIM_WINDOW[1]}'
                group by 1
            )
            select count(*) from landed_counts l join source_counts s using (encounter_id)
            where l.claims <> s.claims""",
    )
    assert mismatched == 0


def test_the_claims_for_an_encounter_sum_to_its_cost(database: Path) -> None:
    """The fan-out challenge of §6: several claims per encounter must not inflate its cost."""
    land_window(*CLAIM_WINDOW, database=database, source=SOURCE)

    [(checked, mismatched)] = landed(
        database,
        f"""with billed as (
                select encounter_id, sum(amount_after) as claimed
                from meta.injection_log where not is_restatement group by 1
            ),
            source as (select Id, TOTAL_CLAIM_COST from read_csv_auto('{SOURCE / "encounters.csv"}'))
            select count(*), count(*) filter (where abs(b.claimed - s.TOTAL_CLAIM_COST) > 0.005)
            from billed b join source s on b.encounter_id = s.Id""",
    )
    assert checked > 0
    assert mismatched == 0, "an encounter's claims do not add up to what it cost"


def test_no_claim_is_covered_for_more_than_it_billed(database: Path) -> None:
    land_window(*CLAIM_WINDOW, database=database, source=SOURCE)

    assert landed(database, "select count(*) from raw.claims where payer_coverage > billed_amount") == [(0,)]


def test_the_injection_log_accounts_for_every_landed_claim(database: Path) -> None:
    land_window(*CLAIM_WINDOW, database=database, source=SOURCE)

    for measure in ("count(*)", "count(distinct claim_id)", "round(sum(billed_amount), 2)"):
        claims = landed(database, f"select {measure} from raw.claims")
        log = landed(database, f"select {measure.replace('billed_amount', 'amount_after')} from meta.injection_log")
        assert claims == log, measure


def landed_as_of(database: Path, as_of: date, source: Path = SOURCE) -> None:
    """Land the window ending on `as_of`, so patients_current is evaluated there."""
    land_window(as_of - timedelta(days=1), as_of, database=database, source=source)


def test_every_patient_alive_at_the_window_end_appears_once(database: Path) -> None:
    landed_as_of(database, AS_OF_LATER)

    [(rows, distinct, as_of)] = landed(
        database, "select count(*), count(distinct patient_id), max(_as_of_date) from raw.patients_current"
    )
    assert rows == distinct, "a patient resolved to more than one row"
    assert as_of == AS_OF_LATER
    assert rows > 0


def test_a_patient_resolves_to_exactly_one_payer(database: Path) -> None:
    """Coverage periods must neither overlap nor leave a living patient uncovered."""
    landed_as_of(database, AS_OF_LATER)

    assert landed(database, "select count(*) from raw.patients_current where not is_deceased and payer_id is null") == [
        (0,)
    ]


def test_the_landed_payer_matches_the_transition_history(database: Path) -> None:
    landed_as_of(database, AS_OF_LATER)

    [(mismatched,)] = landed(
        database,
        f"""with expected as (
                select PATIENT as patient_id, PAYER as payer_id
                from read_csv_auto('{SOURCE / "payer_transitions.csv"}')
                where START_DATE::date <= '{AS_OF_LATER}' and END_DATE::date > '{AS_OF_LATER}'
            )
            select count(*) from raw.patients_current p join expected e using (patient_id)
            where p.payer_id is distinct from e.payer_id""",
    )
    assert mismatched == 0


def test_a_later_window_moves_some_patients_to_a_different_payer(database: Path, tmp_path: Path) -> None:
    """The reason this table exists: without it there is nothing for a snapshot to observe."""
    landed_as_of(database, AS_OF_EARLIER)
    later = tmp_path / "later.duckdb"
    landed_as_of(later, AS_OF_LATER)

    earlier_payers = dict(landed(database, "select patient_id, payer_id from raw.patients_current"))
    later_payers = dict(landed(later, "select patient_id, payer_id from raw.patients_current"))

    changed = [p for p, payer in earlier_payers.items() if later_payers.get(p) != payer]
    assert changed, "no patient changed payer, so the source does not mutate"


def test_a_patient_not_yet_born_is_absent(database: Path) -> None:
    landed_as_of(database, date(1950, 1, 1))

    [(landed_count, born_by_then)] = landed(
        database,
        f"""select (select count(*) from raw.patients_current),
                   (select count(*) from read_csv_auto('{SOURCE / "patients.csv"}')
                    where BIRTHDATE <= '1950-01-01')""",
    )
    assert landed_count == born_by_then
    assert landed_count > 0


def test_a_death_is_recorded_only_once_it_has_happened(database: Path) -> None:
    """A snapshot then sees a change when the patient dies, rather than a row vanishing.

    Evaluated mid-history, because every death in the export precedes the recent
    windows: at those dates nobody dies later, so a death dated ahead of the
    as-of date would be indistinguishable from one dated correctly.
    """
    landed_as_of(database, AS_OF_MIDLIFE)

    [(deceased, dated, premature)] = landed(
        database,
        f"""select count(*) filter (where is_deceased), count(deceased_date),
                   count(*) filter (where deceased_date > '{AS_OF_MIDLIFE}')
            from raw.patients_current""",
    )
    assert deceased > 0
    assert dated == deceased, "a patient carries a death date without being marked deceased"
    assert premature == 0, "a death is dated after the window it was reported in"

    [(still_to_die,)] = landed(
        database,
        f"""select count(*) from raw.patients_current p
            where not p.is_deceased and exists (
                select 1 from read_csv_auto('{SOURCE / "patients.csv"}') s
                where s.Id = p.patient_id and s.DEATHDATE > '{AS_OF_MIDLIFE}')""",
    )
    assert still_to_die > 0, "no patient dies after this date, so the assertion above proves nothing"
