{% macro create_stg_frakture_everyaction_person_jobs(
    reference_name='stg_frakture_everyaction_person_message_stat_unioned_with_domain'
    ) %}
  SELECT SAFE_CAST(message_id AS STRING) AS message_id,
  SAFE_CAST(sent_ts as DATE) AS sent_date,
  SAFE_CAST(person.email_domain as STRING) as email_domain
FROM  {{ ref(reference_name) }} 
{% endmacro %}