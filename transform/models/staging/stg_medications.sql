select
    ENCOUNTER as encounter_id,
    PATIENT as patient_id,
    PAYER as payer_id,
    CODE::varchar as medication_code,
    DESCRIPTION as medication_description,
    REASONCODE::varchar as reason_code,
    REASONDESCRIPTION as reason_description,
    START as started_at,
    STOP as stopped_at,
    DISPENSES as dispenses,
    BASE_COST as base_cost,
    TOTALCOST as total_cost,
    PAYER_COVERAGE as payer_coverage,
    _service_date,
    _loaded_at
from {{ source('raw', 'medications') }}
