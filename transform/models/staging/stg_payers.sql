select
    Id as payer_id,
    NAME as payer_name,
    OWNERSHIP as ownership,
    _loaded_at
from {{ source('raw', 'payers') }}
