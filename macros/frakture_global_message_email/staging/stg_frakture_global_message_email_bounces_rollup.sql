{% macro create_stg_frakture_global_message_email_bounces_rollup(
    reference_name='stg_frakture_global_message_email_summary_unioned') %}
SELECT DISTINCT SAFE_CAST(message_id AS STRING) AS message_id,
 SAFE_CAST(hard_bounces + soft_bounces AS INT) AS total_bounces,
  SAFE_CAST(0 AS INT) AS block_bounces,
  SAFE_CAST(0 AS INT) AS tech_bounces,
  SAFE_CAST(soft_bounces AS INT) AS soft_bounces,
  SAFE_CAST(hard_bounces AS INT) AS unknown_bounces
FROM {{ ref(reference_name) }} 
{% endmacro %}
