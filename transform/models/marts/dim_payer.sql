select
    payer_id,
    payer_name,
    ownership
from {{ ref('stg_payers') }}
