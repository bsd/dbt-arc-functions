{% macro create_stg_frakture_actionkit_email_jobs(
    reference_name='stg_frakture_actionkit_email_summary_unioned') %}

with casting as (
SELECT DISTINCT SAFE_CAST(message_id AS STRING), 
    SAFE_CAST(from_name AS STRING),
    SAFE_CAST(from_email AS STRING),
    SAFE_CAST(publish_date AS TIMESTAMP),
    SAFE_CAST(NULL AS TIMESTAMP),
    SAFE_CAST(NULL AS TIMESTAMP),
    SAFE_CAST(NULL AS TIMESTAMP),
    SAFE_CAST(label AS STRING),
    SAFE_CAST(subject AS STRING),
    SAFE_CAST(final_primary_source_code AS STRING) 
FROM {{ ref(reference_name) }}


)


SELECT DISTINCT message_id,
    from_name,
    from_email,
    COALESCE({{ dbt_date.convert_timezone('publish_date') }}, publish_date) AS best_guess_timestamp,
    scheduled_timestamp,
    pickup_timestamp,
    delivered_timestamp,
    label AS email_name,
    subject AS email_subject, 
    final_primary_source_code as source_code
FROM {{ ref(reference_name) }}

{% endmacro %}