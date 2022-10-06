{% macro create_stg_frakture_timeline_email_person_unioned() %}
{% set relations = dbt_arc_functions.relations_that_match_regex('^\w+([.-]?\w+)*_[A-Za-z0-9]{3}_person$',
  is_source=True,
  source_name='frakture_timeline_email',
  schema_to_search='src_frakture') 
%}
SELECT DISTINCT * FROM {{ var("relations") }}
WHERE remote_person_id IS NOT NULL
{% endmacro %}