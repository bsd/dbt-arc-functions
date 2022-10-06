{% macro create_stg_frakture_timeline_email_person() %}
{% set relations = dbt_arc_functions.relations_that_match_regex('[A-Za-z0-9]_person$',
  is_source=True,
  source_name='frakture_timeline_email',
  schema_to_search='src_frakture') 
%}

SELECT DISTINCT * FROM {{ source('frakture_timeline_email',relations) }}

{% endmacro %}
