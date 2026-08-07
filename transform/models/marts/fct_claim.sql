{{ config(
    materialized='incremental',
    unique_key='claim_id',
    incremental_strategy='merge'
) }}

with batch as (
    select claim.*
    from {{ ref('stg_claims') }} as claim
    {% if is_incremental() %}
    -- Compared per claim rather than against a table-wide max. A global
    -- high-water mark is sound only while windows land in ascending order and
    -- are never re-landed, and it fails silently the moment they do not: an
    -- arrival newer than the row it amends gets skipped because some unrelated
    -- claim happened to arrive later.
    left join {{ this }} as held on held.claim_id = claim.claim_id
    where held.claim_id is null or claim.received_date > held.received_date
    {% endif %}
)

-- A merge takes one source row per key, and DuckDB does not object when handed
-- two: it picks one, the grain still comes out right, and only the
-- reconciliation against the injection log notices the wrong arrival won.
select
    claim_id,
    encounter_id,
    patient_id,
    claim_type_id,
    service_date,
    received_date,
    billed_amount,
    payer_coverage,
    received_date - service_date as billing_lag_days
from batch
qualify row_number() over (partition by claim_id order by received_date desc) = 1
