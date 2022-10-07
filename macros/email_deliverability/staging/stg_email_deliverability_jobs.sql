{% macro create_stg_email_deliverability_jobs(
    reference_name='stg_frakture_timeline_email_person_unioned') %}
SELECT DISTINCT SAFE_CAST(remote_person_id AS STRING) AS remote_person_id,
    SAFE_CAST(domain_name AS STRING) AS domain_name
FROM {{ ref(reference_name) }} 
{% endmacro %}