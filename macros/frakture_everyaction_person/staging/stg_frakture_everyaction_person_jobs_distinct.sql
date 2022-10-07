{% macro create_stg_frakture_everyaction_person_jobs_distinct(
    reference_name='stg_frakture_everyaction_person_jobs') %}
  SELECT DISTINCT * FROM  {{ ref(reference_name) }} 
{% endmacro %}