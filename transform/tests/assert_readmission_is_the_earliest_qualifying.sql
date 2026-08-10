-- Which readmission was picked cannot be seen in the shape of the table. Any
-- of a patient's qualifying stays gives a unique grain, a gap inside the
-- window, and the same is_readmitted, so ordering the window the wrong way
-- round produces a table that passes every other check while naming the wrong
-- encounter and the wrong gap. Both directions are checked: a stay recorded
-- when an earlier one qualified, and no stay recorded when one did.
with qualifying as (
    select
        f.index_encounter_id,
        f.readmission_encounter_id,
        min(date_diff('day', f.discharge_date, e.started_at::date)) as earliest_gap
    from {{ ref('fct_readmission') }} as f
    left join {{ ref('fct_encounter') }} as e
        on e.patient_id = f.patient_id
        and e.encounter_class = 'inpatient'
        and date_diff('day', f.discharge_date, e.started_at::date) between 1 and 30
    group by 1, 2
)

select q.index_encounter_id, q.earliest_gap, f.days_to_readmission
from qualifying as q
join {{ ref('fct_readmission') }} as f on f.index_encounter_id = q.index_encounter_id
where f.days_to_readmission is distinct from q.earliest_gap
