{% macro create_stg_frakture_everyaction_person_jobs(
    person_stat='stg_frakture_everyaction_person_message_stat_unioned',
    person='stg_frakture_everyaction_person_table_unioned'
    ) %}
  SELECT SAFE_CAST(person_stat.message_id AS STRING) AS message_id,
  SAFE_CAST(person_stat.sent_ts as DATE) AS sent_date,
  person.email_domain
FROM  {{ ref(person_stat) }} person_stat 
LEFT JOIN {{ ref(person) }} person
ON person_stat.remote_person_id = person.remote_person_id
{% endmacro %}