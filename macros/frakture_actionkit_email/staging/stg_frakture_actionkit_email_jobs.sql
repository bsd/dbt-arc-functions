{% macro create_stg_frakture_actionkit_email_jobs(
    reference_name='stg_frakture_actionkit_email_summary_unioned') %}

with casting as (
SELECT DISTINCT SAFE_CAST(message_id AS STRING) as message_id, 
    SAFE_CAST(from_name AS STRING) as from_name,
    SAFE_CAST(from_email AS STRING) as from_email,
    SAFE_CAST(publish_date AS TIMESTAMP) as publish_date,
    SAFE_CAST(NULL AS TIMESTAMP) as scheduled_timestamp,
    SAFE_CAST(NULL AS TIMESTAMP) as pickup_timestamp,
    SAFE_CAST(NULL AS TIMESTAMP) as delivered_timestamp,
    SAFE_CAST(label AS STRING) as label,
    SAFE_CAST(subject AS STRING) as subject,
    SAFE_CAST(final_primary_source_code AS STRING) as final_primary_source_code
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
FROM casting

{% endmacro %}