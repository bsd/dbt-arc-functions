{% macro create_stg_frakture_everyaction_person_opens_daily_rollup(
    reference_name='stg_frakture_everyaction_person_message_stat_unioned') %}
SELECT 
  SAFE_CAST(sent_ts as DATE) AS sent_date,
  SAFE_CAST(message_id AS STRING) AS message_id,
  SAFE_CAST(remote_person_id AS STRING) AS person_id,
  SUM(SAFE_CAST(opened AS INT)) as opens
FROM  {{ ref(reference_name) }} 
GROUP BY 1, 2, 3
{% endmacro %}