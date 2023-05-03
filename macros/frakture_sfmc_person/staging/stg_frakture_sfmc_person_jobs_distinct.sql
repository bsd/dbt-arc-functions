{% macro create_stg_frakture_sfmc_person_jobs_distinct(
    reference_name="stg_frakture_sfmc_person_jobs"
) %}
    select distinct * from {{ ref(reference_name) }}
{% endmacro %}
