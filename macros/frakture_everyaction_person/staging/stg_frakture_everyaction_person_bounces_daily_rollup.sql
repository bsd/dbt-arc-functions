{% macro create_stg_frakture_everyaction_person_bounces_daily_rollup(
    reference_name='stg_frakture_everyaction_person_message_stat_unioned') %}
SELECT 
  SAFE_CAST(sent_ts as DATE) AS sent_date,
  SAFE_CAST(message_id AS STRING) AS message_id,
  SAFE_CAST(remote_person_id AS STRING) AS person_id,
  SUM(SAFE_CAST(hard_bounce AS INT)) as hard_bounces,
  SUM(SAFE_CAST(soft_bounce AS INT)) as soft_bounces,
  SAFE_CAST(hard_bounce as INT) + SAFE_CAST(soft_bounce as INT) AS total_bounces
FROM  {{ ref(reference_name) }} 
GROUP BY 1, 2, 3
{% endmacro %}