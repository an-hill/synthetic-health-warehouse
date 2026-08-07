-- A version that repeats its predecessor is a version that should not exist.
-- The snapshot cannot take check_cols='all', because _as_of_at moves on every
-- window and would carry every patient with it, so the columns are listed and
-- this is what catches the list going wrong. The mistake is otherwise silent:
-- the grain, the tiling, and the current-version tests all pass over a history
-- versioned 554-fold.
with attributed as (
    -- One value rather than eight comparisons, so that adding a versioned
    -- attribute is a single edit and nulls compare as nulls.
    select
        patient_id,
        valid_from,
        {
            'birthdate': birthdate, 'gender': gender, 'race': race,
            'ethnicity': ethnicity, 'birthplace': birthplace,
            'payer_id': payer_id, 'is_deceased': is_deceased,
            'deceased_date': deceased_date
        } as attributes
    from {{ ref('dim_patient') }}
),

versioned as (
    select
        patient_id,
        valid_from,
        attributes,
        -- A patient's first version has nothing to differ from, and lag would
        -- otherwise hand it a null that a genuinely uncovered patient matches.
        lag(valid_from) over patient_history as previous_valid_from,
        lag(attributes) over patient_history as previous_attributes
    from attributed
    window patient_history as (partition by patient_id order by valid_from)
)

select patient_id, valid_from
from versioned
where previous_valid_from is not null
  and attributes is not distinct from previous_attributes
