{% snapshot snap_patient %}

{{ config(
    unique_key='patient_id',
    strategy='check',
    check_cols=[
        'birthdate', 'gender', 'race', 'ethnicity', 'birthplace',
        'payer_id', 'is_deceased', 'deceased_date'
    ],
    updated_at='_as_of_at'
) }}

-- Listed exhaustively rather than 'all', which would take _as_of_at with it and
-- version every patient on every window. An omission here is silent: a snapshot
-- never updates a row it considers unchanged, so an unlisted column keeps its
-- first value for ever.

-- Cast because dbt compares updated_at's type against snapshot_get_time(),
-- which is now()::timestamp on DuckDB, and warns on every build if they differ.
select
    patient_id,
    birthdate,
    gender,
    race,
    ethnicity,
    birthplace,
    payer_id,
    is_deceased,
    deceased_date,
    _as_of_date::timestamp as _as_of_at
from {{ ref('stg_patients_current') }}

{% endsnapshot %}
