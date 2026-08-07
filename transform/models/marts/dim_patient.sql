select
    patient_id,
    payer_id,
    birthdate,
    gender,
    race,
    ethnicity,
    birthplace,
    is_deceased,
    deceased_date,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    dbt_valid_to is null as is_current
from {{ ref('snap_patient') }}
