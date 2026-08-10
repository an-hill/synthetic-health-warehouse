with inpatient as (
    select
        encounter_id,
        patient_id,
        started_at::date as admit_date,
        stopped_at::date as discharge_date
    from {{ ref('fct_encounter') }}
    where encounter_class = 'inpatient'
),

-- The latest admission date landed, which is the last day a readmission could
-- have been seen arriving. An index admission needs thirty of them after it.
observation_end as (
    select max(_service_date) as observed_to
    from {{ ref('fct_encounter') }}
),

index_admission as (
    select inpatient.*
    from inpatient, observation_end
    where date_diff('day', discharge_date, observed_to) >= 30
),

-- The earliest qualifying stay rather than the next one: 24 stays begin at or
-- before the previous one's discharge, so the next stay and the next stay that
-- counts are different encounters.
readmission as (
    select
        index_admission.encounter_id as index_encounter_id,
        inpatient.encounter_id as readmission_encounter_id,
        date_diff('day', index_admission.discharge_date, inpatient.admit_date) as days_to_readmission
    from index_admission
    join inpatient
        on inpatient.patient_id = index_admission.patient_id
        and date_diff('day', index_admission.discharge_date, inpatient.admit_date) between 1 and 30
    qualify row_number() over (
        partition by index_admission.encounter_id
        order by inpatient.admit_date, inpatient.encounter_id
    ) = 1
)

select
    i.encounter_id as index_encounter_id,
    i.patient_id,
    i.admit_date,
    i.discharge_date,
    r.readmission_encounter_id,
    r.days_to_readmission,
    r.readmission_encounter_id is not null as is_readmitted
from index_admission as i
left join readmission as r on r.index_encounter_id = i.encounter_id
