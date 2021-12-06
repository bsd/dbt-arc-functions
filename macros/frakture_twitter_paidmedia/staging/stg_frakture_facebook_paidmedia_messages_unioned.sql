{% macro create_stg_frakture_twitter_paidmedia_messages_unioned() %}
{% set relations = dbt_arc_functions.relations_that_match_regex('^twitter_bizman_[A-Za-z0-9]{3}_message$',
  is_source=True,
  source_name='frakture_twitter_paidmedia',
  schema_to_search='src_frakture') 
%}
SELECT DISTINCT * FROM ({{ dbt_utils.union_relations(relations) }})
 WHERE message_id IS NOT NULL
{% endmacro %}