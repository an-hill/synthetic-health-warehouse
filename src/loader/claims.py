"""Derives claims from the Synthea export and lands those that arrived in a window.

Claims come from Synthea's own claims export, one row per real claim, linked to
its encounter by `APPOINTMENTID`. An encounter bills 1 to 46 of them, and they
carry a real billing lag: mostly same-day, with a long right tail out to 100
days, which is a property of the data rather than a bound the loader chose.

The one distortion the export lacks is restatement: it contains no ADJUSTMENT
transactions, so no claim ever re-arrives amended. That alone is injected, and
it is the only thing `meta.injection_log` records.
"""

from datetime import date

import duckdb

RESTATED_PERCENT = 5
MAX_RESTATE_LAG_DAYS = 30
DEFAULT_SEED = 1

CLAIM_COLUMNS = (
    "claim_id, encounter_id, patient_id, claim_type_id, service_date, received_date, billed_amount, payer_coverage"
)
LOG_COLUMNS = (
    "claim_id, encounter_id, service_date, received_date, lag_days, is_restatement,"
    " amount_before, billed_amount as amount_after"
)


def create_tables(con: duckdb.DuckDBPyConnection) -> None:
    """Create the claim tables, declared rather than inferred from the export.

    The other raw tables mirror their export file and take its columns wholesale.
    `raw.claims` cannot: it renames the keys it uses, and its amounts are
    apportioned from the encounter rather than read from the claims export,
    which carries no charge column. `meta.injection_log` has no source file at all.

    `raw.claims` carries no restatement flag on purpose. Raw holds what the
    source sent, and knowing which arrival amended another is the answer key's
    job. Keeping them apart is what forces `fct_claim` to collapse duplicates by
    merging on the claim id rather than by filtering a convenient column.
    """
    con.execute("""
        create table if not exists raw.claims (
            claim_id varchar, encounter_id varchar, patient_id varchar, claim_type_id integer,
            service_date date, received_date date,
            billed_amount double, payer_coverage double,
            _loaded_at timestamp with time zone
        )
    """)
    con.execute("""
        create table if not exists meta.injection_log (
            claim_id varchar, encounter_id varchar,
            service_date date, received_date date, lag_days integer,
            is_restatement boolean, amount_before double, amount_after double,
            _loaded_at timestamp with time zone
        )
    """)


def land_arrivals(
    con: duckdb.DuckDBPyConnection,
    *,
    claims_path: str,
    encounters_path: str,
    start: date,
    end: date,
    seed: int,
) -> int:
    """Land the claims billed in the window, plus any restatement arriving in it.

    Args:
        con: Connection to land into, already inside the caller's transaction.
        claims_path: Path to the Synthea claims export.
        encounters_path: Path to the encounters export, which carries the cost
            that the claims apportion.
        start: First received date to land, inclusive.
        end: First received date beyond the window, exclusive.
        seed: Salts the injected restatements.

    Returns:
        The number of claim arrivals landed.
    """
    _derive_arrivals(con, claims_path=claims_path, encounters_path=encounters_path, seed=seed)
    _replace_window(con, "raw.claims", CLAIM_COLUMNS, start, end)
    _replace_window(con, "meta.injection_log", LOG_COLUMNS, start, end)

    counted = con.execute(
        "select count(*) from raw.claims where received_date >= ? and received_date < ?", [start, end]
    ).fetchone()
    return counted[0] if counted else 0


def _derive_arrivals(con: duckdb.DuckDBPyConnection, *, claims_path: str, encounters_path: str, seed: int) -> None:
    """Build every arrival for every claim, whichever window it belongs to.

    The billing date comes from the export, so no lookback is needed to find a
    window's arrivals. Restatement is hashed from the claim id, so an arrival
    falls in the same window whatever order a backfill runs in.
    """
    con.execute(
        """
        create or replace temp table claim_arrivals as
        with priced as (
            -- Apportioning is a property of the encounter, so it runs over every
            -- claim the encounter ever billed rather than the ones in this
            -- window. Filtering first would make a claim's amount depend on
            -- which window happened to load it.
            select
                c.Id as claim_id,
                c.APPOINTMENTID as encounter_id,
                c.PATIENTID as patient_id,
                c.HEALTHCARECLAIMTYPEID1 as claim_type_id,
                c.SERVICEDATE::date as service_date,
                c.LASTBILLEDDATE1::date as received_date,
                e.TOTAL_CLAIM_COST as encounter_cost,
                e.PAYER_COVERAGE as encounter_coverage,
                count(*) over (partition by c.APPOINTMENTID) as claims_for_encounter,
                row_number() over (partition by c.APPOINTMENTID order by c.Id) as claim_seq
            from read_parquet($claims_path) c
            join read_parquet($encounters_path) e on c.APPOINTMENTID = e.Id
        ),
        apportioned as (
            select *,
                -- The last claim takes the remainder rather than its own rounded
                -- share, so an encounter's claims sum to its cost exactly and
                -- per-encounter totals survive the fan-out.
                case when claim_seq < claims_for_encounter
                     then round(encounter_cost / claims_for_encounter, 2)
                     else encounter_cost
                          - round(encounter_cost / claims_for_encounter, 2) * (claims_for_encounter - 1)
                end as billed_amount,
                case when claim_seq < claims_for_encounter
                     then round(encounter_coverage / claims_for_encounter, 2)
                     else encounter_coverage
                          - round(encounter_coverage / claims_for_encounter, 2) * (claims_for_encounter - 1)
                end as payer_coverage
            from priced
        ),
        arrivals as (
            select *,
                -- The modulus is cast, not left to bind as an integer. md5_number
                -- returns UHUGEINT, and UHUGEINT % INTEGER promotes to DOUBLE,
                -- whose 53-bit mantissa silently discards the low bits, halving
                -- the range with no error anywhere.
                (md5_number(claim_id || '|restated|' || $seed) % 100::uint128)::int < $restated_pct as is_restated,
                (md5_number(claim_id || '|restatelag|' || $seed) % $restate_range::uint128)::int + 1 as restate_lag,
                -- Straddles 1.0 without landing on it, so a restatement always
                -- moves the amount and can never be a silent no-op.
                case when (md5_number(claim_id || '|factor|' || $seed) % 50::uint128)::int < 25
                     then 0.75 + (md5_number(claim_id || '|factor|' || $seed) % 50::uint128)::int / 100.0
                     else 1.01 + ((md5_number(claim_id || '|factor|' || $seed) % 50::uint128)::int - 25) / 100.0
                end as factor
            from apportioned
        )
        select claim_id, encounter_id, patient_id, claim_type_id, service_date, received_date,
               received_date - service_date as lag_days,
               billed_amount, payer_coverage,
               false as is_restatement, null::double as amount_before
        from arrivals
        union all
        select claim_id, encounter_id, patient_id, claim_type_id, service_date,
               received_date + restate_lag,
               received_date + restate_lag - service_date,
               round(billed_amount * factor, 2), round(payer_coverage * factor, 2),
               true, billed_amount
        from arrivals where is_restated
        """,
        {
            "seed": seed,
            "restated_pct": RESTATED_PERCENT,
            "restate_range": MAX_RESTATE_LAG_DAYS,
            "claims_path": claims_path,
            "encounters_path": encounters_path,
        },
    )


def _replace_window(con: duckdb.DuckDBPyConnection, table: str, columns: str, start: date, end: date) -> None:
    """Replace one received-date window of a derived table from the arrivals just computed."""
    con.execute(f"delete from {table} where received_date >= ? and received_date < ?", [start, end])
    con.execute(
        f"""
        insert into {table}
        select {columns}, now() from claim_arrivals
        where received_date >= ? and received_date < ?
        """,
        [start, end],
    )
