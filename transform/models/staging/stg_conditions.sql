select
    ENCOUNTER as encounter_id,
    PATIENT as patient_id,
    SYSTEM as code_system,
    CODE::varchar as condition_code,
    DESCRIPTION as condition_description,
    START as onset_on,
    STOP as resolved_on,
    _service_date,
    _loaded_at
from {{ source('raw', 'conditions') }}
