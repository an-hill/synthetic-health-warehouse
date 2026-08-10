-- The denominator is the row count, so a join that quietly drops the
-- admissions with no readmission leaves a table where every other check still
-- passes: the grain is unique, no column is null, and the rate reads 100%.
-- Comparing the rows against the encounters they should have come from is the
-- only assertion that separates the denominator from the numerator.
with expected as (
    select encounter_id
    from {{ ref('fct_encounter') }}
    where encounter_class = 'inpatient'
      and date_diff('day', stopped_at::date, (select max(_service_date) from {{ ref('fct_encounter') }})) >= 30
)

select
    coalesce(e.encounter_id, f.index_encounter_id) as encounter_id,
    e.encounter_id is null as unexpected,
    f.index_encounter_id is null as missing
from expected as e
full outer join {{ ref('fct_readmission') }} as f on f.index_encounter_id = e.encounter_id
where e.encounter_id is null or f.index_encounter_id is null
