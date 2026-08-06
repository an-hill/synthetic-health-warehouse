select
    Id as provider_id,
    ORGANIZATION as organization_id,
    NAME as provider_name,
    GENDER as gender,
    SPECIALITY as speciality,
    CITY as city,
    STATE as state,
    ZIP as zip,
    _loaded_at
from {{ source('raw', 'providers') }}
