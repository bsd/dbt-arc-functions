{% macro create_stg_frakture_facebook_paidmedia_messages_unioned() %}
{% set relations = relations_that_match_regex('^facebook_bizman_[A-Za-z0-9]{3}_message$',
  is_source=True,
  source_name='frakture_facebook',
  schema_to_search='src_frakture') 
%}
SELECT DISTINCT * FROM ({{ dbt_utils.union_relations(relations) }})
 WHERE message_id IS NOT NULL
{% endmacro %}