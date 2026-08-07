{% macro refuse_a_backwards_as_of_date() %}

{#
    snapshot_check_strategy compares columns and nothing else, so handed an
    as-of date earlier than the history it holds it closes the open version at a
    timestamp before that version began. assert_patient_versions_tile reports
    that afterwards, which is too late: a snapshot cannot be repaired, and
    --full-refresh discards the history rather than mending it.

    An equal date is allowed. Building twice without landing in between is
    ordinary, and a change arriving under a date already recorded would produce
    a zero-length version that the tiling test does catch.
#}
{% if execute and load_relation(this) %}
    {% set boundary = run_query(
        "select (select max(_as_of_date)::timestamp from " ~ ref('stg_patients_current') ~ ") as arriving,"
        ~ " (select max(dbt_valid_from) from " ~ this ~ ") as held"
    ) %}
    {% set arriving = boundary.columns[0][0] %}
    {% set held = boundary.columns[1][0] %}

    {% if held is not none and arriving < held %}
        {% do exceptions.raise_compiler_error(
            this.identifier ~ " holds history to " ~ held ~ " and was handed " ~ arriving
            ~ ". Re-land the latest window before building, since a snapshot already written cannot be repaired."
        ) %}
    {% endif %}
{% endif %}

select 1

{% endmacro %}
