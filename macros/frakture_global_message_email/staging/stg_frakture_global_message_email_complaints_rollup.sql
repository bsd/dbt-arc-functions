{% macro create_stg_frakture_global_message_email_complaints_rollup(
    reference_name='stg_frakture_global_message_email_summary') %}
SELECT SAFE_CAST(message_id AS STRING) AS message_id,  
  SUM(SAFE_CAST(complaints AS INT)) AS complaints
FROM {{ ref(reference_name) }} 
GROUP BY 1
{% endmacro %}
