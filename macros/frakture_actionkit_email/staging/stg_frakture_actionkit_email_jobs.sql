{% macro create_stg_frakture_actionkit_email_jobs(
    reference_name='stg_frakture_actionkit_email_summary_unioned') %}

SELECT DISTINCT SAFE_CAST(message_id AS STRING) as message_id, 
    SAFE_CAST(from_name AS STRING) as from_name,
    SAFE_CAST(from_email AS STRING) as from_email,
    SAFE_CAST({{ dbt_date.convert_timezone('cast(publish_date as TIMESTAMP)') }} as TIMESTAMP as best_guess_timestamp,
    SAFE_CAST(NULL AS TIMESTAMP) as scheduled_timestamp,
    SAFE_CAST(NULL AS TIMESTAMP) as pickup_timestamp,
    SAFE_CAST(NULL AS TIMESTAMP) as delivered_timestamp,
    SAFE_CAST(label AS STRING) as email_name,
    SAFE_CAST(subject AS STRING) as email_subject,
    SAFE_CAST(final_primary_source_code AS STRING) as source_code
FROM {{ ref(reference_name) }}


{% endmacro %}