{% macro create_stg_frakture_global_message_email_recipients_rollup(
    reference_name='stg_frakture_global_message_email_summary_by_date') %}
SELECT SAFE_CAST(message_id AS STRING) AS message_id,
  SUM(SAFE_CAST(sent AS INT)) AS recipients
FROM {{ ref(reference_name) }} 
GROUP BY 1
{% endmacro %}
