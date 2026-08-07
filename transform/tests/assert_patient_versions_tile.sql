-- A patient's versions should tile: each range moves forwards, and each ends
-- exactly where the next begins. The check strategy has no guard against its
-- source going backwards, so landing an earlier window after a later one closes
-- the open version at a timestamp before it began and nothing inside dbt says
-- so. This is the only thing that would notice, and it has to, because a
-- snapshot that has written a false row cannot be repaired.
with versioned as (
    select
        patient_id,
        valid_from,
        valid_to,
        lead(valid_from) over (partition by patient_id order by valid_from) as next_valid_from
    from {{ ref('dim_patient') }}
)

select patient_id, valid_from, valid_to, next_valid_from
from versioned
where valid_to <= valid_from
   or valid_to is distinct from next_valid_from
