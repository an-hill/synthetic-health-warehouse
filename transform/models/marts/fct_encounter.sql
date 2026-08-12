-- Counted in CTEs rather than joined in directly: at 1.58 conditions and 2.02
-- medications at the encounters that record any, joining both multiplies the
-- grain and every measure hanging off it.
with condition_counts as (
    select
        encounter_id,
        count(*) as condition_count
    from {{ ref('stg_conditions') }}
    group by encounter_id
),

medication_counts as (
    select
        encounter_id,
        count(*) as medication_count
    from {{ ref('stg_medications') }}
    group by encounter_id
)

select
    e.encounter_id,
    e.patient_id,
    e.provider_id,
    e.payer_id,
    e.organization_id,
    e.encounter_class,
    e.encounter_code,
    e.encounter_description,
    e.reason_code,
    e.started_at,
    e.stopped_at,
    e.base_cost,
    e.total_cost,
    e.payer_coverage,
    e.total_cost - e.payer_coverage as patient_responsibility,
    coalesce(c.condition_count, 0) as condition_count,
    coalesce(m.medication_count, 0) as medication_count,
    e._service_date
from {{ ref('stg_encounters') }} as e
left join condition_counts as c on c.encounter_id = e.encounter_id
left join medication_counts as m on m.encounter_id = e.encounter_id
