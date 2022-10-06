{% macro create_stg_frakture_timeline_per_person_message_stat_unioned() %}
{% set relations = dbt_arc_functions.relations_that_match_regex('[A-Za-z0-9]_person',
  is_source=True,
  source_name='frakture_timeline_email',
  schema_to_search='src_frakture') 
%}
SELECT DISTINCT * FROM ({{ dbt_utils.union_relations(relations) }})
WHERE message_id IS NOT NULL
{% endmacro %}