{% macro create_stg_frakture_actionkit_email_summary_unioned() %}
{% set relations = dbt_arc_functions.relations_that_match_regex('^actionkit_[A-Za-z0-9]{3}_transaction$',
  is_source=True,
  source_name='frakture_actionkit_email',
  schema_to_search='src_frakture') 
%}
SELECT DISTINCT * FROM ({{ dbt_utils.union_relations(relations) }})
WHERE order_status = 'complete'
{% endmacro %}