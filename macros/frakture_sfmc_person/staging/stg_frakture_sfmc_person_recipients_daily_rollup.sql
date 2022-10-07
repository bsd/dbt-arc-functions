{% macro create_stg_frakture_sfmc_person_recipients_daily_rollup(
    reference_name='stg_frakture_sfmc_person_message_stat_unioned') %}
SELECT 
  SAFE_CAST(sent_ts as DATE) AS sent_date,
  SAFE_CAST(message_id AS STRING) AS message_id,
  SAFE_CAST(remote_person_id AS STRING) AS person_id,
  SUM(SAFE_CAST(received AS INT)) as recipients
FROM  {{ ref(reference_name) }} 
GROUP BY 1, 2, 3
{% endmacro %}