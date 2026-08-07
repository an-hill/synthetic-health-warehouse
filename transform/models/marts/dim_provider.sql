select
    provider_id,
    organization_id,
    provider_name,
    gender,
    city,
    state,
    zip
from {{ ref('stg_providers') }}
