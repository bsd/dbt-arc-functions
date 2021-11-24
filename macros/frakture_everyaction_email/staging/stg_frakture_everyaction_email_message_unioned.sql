{% macro create_stg_frakture_everyaction_email_message_unioned() %}
{% set relations = relations_that_match_regex('^everyaction_[A-Za-z0-9]{3}_message$',
  is_source=True,
  source_name='frakture_everyaction_email',
  schema_to_search='src_frakture') 
%}
SELECT DISTINCT * FROM ({{ dbt_utils.union_relations(relations) }})
WHERE channel = 'email'
AND message_id IS NOT NULL
{% endmacro %}