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
    _as_of_date,
    _loaded_at
from {{ source('raw', 'patients_current') }}
