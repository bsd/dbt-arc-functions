{% macro create_stg_frakture_actionkit_email_bounces_rollup(
    reference_name='stg_frakture_actionkit_email_summary_unioned') %}
SELECT SAFE_CAST(message_id AS STRING) AS message_id,
  SUM(SAFE_CAST(email_hard_bounces + email_soft_bounces AS INT)) AS total_bounces,
  SUM(SAFE_CAST(email_blocked AS INT)) AS block_bounces,
  SUM(SAFE_CAST(0 AS INT)) AS tech_bounces,
  SUM(SAFE_CAST(email_soft_bounces AS INT)) AS soft_bounces,
  SUM(SAFE_CAST(email_hard_bounces AS INT)) AS hard_bounces
FROM  {{ ref(reference_name) }} 
GROUP BY 1
{% endmacro %}