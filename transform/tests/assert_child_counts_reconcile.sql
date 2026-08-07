-- A fan-out bug that keeps the grain multiplies only the counts, which every
-- uniqueness and grain test passes over. Totalling them against their sources
-- is what catches it. Exact rather than approximate: the loader lands children
-- with their parent encounter, so every staging row has one row to count on.
select
    (select sum(condition_count) from {{ ref('fct_encounter') }}) as fact_conditions,
    (select count(*) from {{ ref('stg_conditions') }}) as source_conditions,
    (select sum(medication_count) from {{ ref('fct_encounter') }}) as fact_medications,
    (select count(*) from {{ ref('stg_medications') }}) as source_medications
where fact_conditions <> source_conditions
   or fact_medications <> source_medications
