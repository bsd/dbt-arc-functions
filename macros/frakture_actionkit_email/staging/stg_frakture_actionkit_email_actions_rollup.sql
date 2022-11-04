{% macro create_stg_frakture_actionkit_email_actions_rollup(
    reference_name='stg_frakture_actionkit_email_summary_unioned') %}
SELECT SAFE_CAST(message_id AS STRING) AS message_id,
  SUM(SAFE_CAST(actions AS INT)) AS actions
FROM  {{ ref(reference_name) }} 
GROUP BY 1
{% endmacro %}