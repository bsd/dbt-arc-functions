{% macro create_stg_frakture_global_message_email_clicks_rollup(
    reference_name='stg_frakture_global_message_email_summary_unioned') %}
SELECT SAFE_CAST(message_id AS STRING) AS message_id,  
  SUM(SAFE_CAST(clicks AS INT)) AS clicks
FROM {{ ref(reference_name) }} 
GROUP BY 1
{% endmacro %}
