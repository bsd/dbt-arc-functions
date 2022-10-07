{% macro create_stg_frakture_everyaction_person_jobs(
    reference_name='stg_frakture_everyaction_person_message_stat_unioned') %}
  SELECT SAFE_CAST(message_id AS STRING) AS message_id,
  SAFE_CAST(sent_ts as DATE) AS sent_date,
  SAFE_CAST(remote_person_id AS STRING) AS person_id
FROM  {{ ref(reference_name) }} 
{% endmacro %}