{% macro create_stg_paidmedia_campaigns_unioned() %}
{% set relations = dbt_arc_functions.relations_that_match_regex('^stg_.*_paidmedia_campaigns$') %}
{{ dbt_utils.union_relations(relations) }}
{% endmacro %}