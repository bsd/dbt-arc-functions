{% macro create_stg_frakture_everyaction_person_jobs(
    reference_name='stg_everyaction_person_table_unioned') %}
SELECT DISTINCT SAFE_CAST(remote_person_id as INT) as person_id,
SAFE_CAST(email_domain as VARCHAR) as email_domain
FROM  {{ ref(reference_name) }} 
{% endmacro %}