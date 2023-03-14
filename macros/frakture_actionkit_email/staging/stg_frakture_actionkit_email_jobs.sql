{% macro create_stg_frakture_actionkit_email_jobs(
    reference_name='stg_frakture_actionkit_email_summary_unioned') %}
SELECT DISTINCT SAFE_CAST(message_id AS STRING) AS message_id,
    SAFE_CAST(from_name AS STRING) AS from_name,
    SAFE_CAST(from_email AS STRING) AS from_email,
    SAFE_CAST(COALESCE({{ dbt_date.convert_timezone("publish_date_dt") }}, publish_date) AS TIMESTAMP) AS best_guess_timestamp,
    SAFE_CAST(NULL AS TIMESTAMP) AS scheduled_timestamp,
    SAFE_CAST(NULL AS TIMESTAMP) AS pickup_timestamp,
    SAFE_CAST(NULL AS TIMESTAMP) AS delivered_timestamp,
    SAFE_CAST(label AS STRING) AS email_name,
    SAFE_CAST(subject AS STRING) AS email_subject, 
    SAFE_CAST(final_primary_source_code AS STRING) as source_code
FROM {{ ref(reference_name) }}

{% endmacro %}