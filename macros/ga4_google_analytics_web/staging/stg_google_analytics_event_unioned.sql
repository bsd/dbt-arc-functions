{% macro create_stg_google_analytics_event_unioned() %}


{% set relations = dbt_arc_functions.relations_that_match_regex('^stg_google_analytics_event_.+$',
  is_source=False,
  schema_to_search=target.schema) 

%}

SELECT DISTINCT * FROM ({{ dbt_utils.union_relations(relations) }})

{% endmacro %}
