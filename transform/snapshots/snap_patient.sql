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

-- Every column but _as_of_at, which cannot take 'all' because _as_of_at moves
-- on every window and would version every patient with it. A demographic that
-- is not listed is not merely unversioned: a snapshot never updates a row it
-- considers unchanged, so a corrected birthdate would leave no trace at all.

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
