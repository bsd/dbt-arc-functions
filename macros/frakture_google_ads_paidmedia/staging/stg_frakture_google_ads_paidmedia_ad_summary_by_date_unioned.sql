{% macro create_stg_frakture_google_ads_paidmedia_ad_summary_by_date_unioned() %}
{% set relations = dbt_arc_functions.relations_that_match_regex('^google_ads_[A-Za-z0-9]{3}_ad_summary_by_date$',
  is_source=True,
  source_name='frakture_google_ads_paidmedia',
  schema_to_search='src_frakture') 
%}
SELECT DISTINCT * FROM ({{ dbt_utils.union_relations(relations) }})
 WHERE message_id IS NOT NULL
{% endmacro %}