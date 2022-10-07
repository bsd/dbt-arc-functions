{% macro create_stg_frakture_everyaction_person_details(
    reference_name='stg_frakture_everyaction_person_table_unioned') %}
SELECT DISTINCT SAFE_CAST(remote_person_id as STRING) as person_id,
SAFE_CAST(email_domain as STRING) as email_domain
FROM  {{ ref(reference_name) }} 
{% endmacro %}