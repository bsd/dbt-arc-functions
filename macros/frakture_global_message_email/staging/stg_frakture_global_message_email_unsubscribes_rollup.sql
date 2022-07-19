{% macro create_stg_frakture_global_message_email_unsubscribes_rollup(
    reference_name='stg_frakture_global_message_email_summary_by_date') %}
SELECT DISTINCT SAFE_CAST(message_id AS STRING) AS message_id,
SAFE_CAST(publish_date AS TIMESTAMP) AS date_timestamp,
 SAFE_CAST(unsubscribes AS INT) AS unsubscribes
FROM {{ ref(reference_name) }} 
{% endmacro %}
