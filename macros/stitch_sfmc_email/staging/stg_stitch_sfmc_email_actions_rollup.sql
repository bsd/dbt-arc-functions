{% macro create_stg_stitch_sfmc_email_actions_rollup(
    reference_name='stg_stitch_sfmc_email_summary') %}
SELECT SAFE_CAST(message_id AS STRING) AS message_id,  
  SUM(SAFE_CAST(actions AS INT)) AS actions
FROM {{ ref(reference_name) }} 
GROUP BY 1
{% endmacro %}
