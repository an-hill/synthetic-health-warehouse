select
    Id as encounter_id,
    PATIENT as patient_id,
    ORGANIZATION as organization_id,
    PROVIDER as provider_id,
    PAYER as payer_id,
    ENCOUNTERCLASS as encounter_class,
    CODE::varchar as encounter_code,
    DESCRIPTION as encounter_description,
    REASONCODE::varchar as reason_code,
    REASONDESCRIPTION as reason_description,
    START as started_at,
    STOP as stopped_at,
    BASE_ENCOUNTER_COST as base_cost,
    TOTAL_CLAIM_COST as total_cost,
    PAYER_COVERAGE as payer_coverage,
    _service_date,
    _loaded_at
from {{ source('raw', 'encounters') }}
