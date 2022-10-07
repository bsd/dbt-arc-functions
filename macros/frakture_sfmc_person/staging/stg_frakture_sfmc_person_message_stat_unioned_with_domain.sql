{% macro create_stg_frakture_sfmc_person_message_stat_unioned_with_domain(
  person_stat='stg_frakture_sfmc_person_message_stat_unioned',
  person='stg_frakture_sfmc_person_table_unioned') %}
SELECT
person_stat.*,
person.email_domain
FROM  {{ ref(person_stat) }} person_stat 
LEFT JOIN {{ ref(person) }} person
ON person_stat.remote_person_id = person.remote_person_id
{% endmacro %}