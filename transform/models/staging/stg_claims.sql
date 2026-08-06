-- One row per submission, not per claim. A restatement re-arrives under the
-- same claim_id, and collapsing that is fct_claim's job.
select
    claim_id,
    encounter_id,
    patient_id,
    claim_type_id,
    service_date,
    received_date,
    billed_amount,
    payer_coverage,
    _loaded_at
from {{ source('raw', 'claims') }}
