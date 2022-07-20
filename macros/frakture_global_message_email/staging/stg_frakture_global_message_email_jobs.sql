{% macro create_stg_frakture_global_message_email_jobs(
    reference_name='stg_frakture_global_message_email_message_unioned') %}
SELECT 
   SAFE_CAST(message_id AS STRING) AS message_id,
   SAFE_CAST(campaign_id AS STRING) AS campaign_id,
    SAFE_CAST(from_name AS STRING) AS from_name,
    SAFE_CAST(from_name AS STRING) AS from_email,
    SAFE_CAST(publish_date AS TIMESTAMP) AS best_guess_timestamp,
    SAFE_CAST(NULL AS TIMESTAMP) AS scheduled_timestamp,
    SAFE_CAST(NULL AS TIMESTAMP) AS pickup_timestamp,
    SAFE_CAST(NULL AS TIMESTAMP) AS delivered_timestamp,
    SAFE_CAST(label AS STRING) AS email_name,
    SAFE_CAST(subject AS STRING) AS email_subject,
    SAFE_CAST(campaign_name AS STRING) AS campaign_name
FROM {{ ref(reference_name) }}
{% endmacro %}
