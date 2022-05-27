{% macro create_stg_frakture_everyaction_email_jobs(
    reference_name='stg_frakture_everyaction_email_message_unioned') %}
SELECT SAFE_CAST(messages.message_id AS STRING) AS message_id,
    SAFE_CAST(messages.campaign_id AS STRING) AS campaign_id,
    SAFE_CAST(messages.from_name AS STRING) AS from_name,
    SAFE_CAST(messages.from_email AS STRING) AS from_email,
    SAFE_CAST(messages.publish_date AS TIMESTAMP) AS best_guess_timestamp,
    SAFE_CAST(NULL AS TIMESTAMP) AS scheduled_timestamp,
    SAFE_CAST(NULL AS TIMESTAMP) AS pickup_timestamp,
    SAFE_CAST(NULL AS TIMESTAMP) AS delivered_timestamp,
    SAFE_CAST(messages.label AS STRING) AS email_name,
    SAFE_CAST(messages.subject AS STRING) AS email_subject,
    SAFE_CAST(messages.campaign_name AS STRING) AS campaign_name
FROM {{ ref(reference_name) }} messages
WHERE messages.channel = 'email'
{% endmacro %}