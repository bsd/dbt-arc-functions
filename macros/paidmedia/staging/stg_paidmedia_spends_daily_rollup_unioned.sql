{% macro create_stg_paidmedia_spends_daily_rollup_unioned() %}
{% set relations = relations_that_match_regex('^stg_.*_paidmedia_spends_daily_rollup$') %}
{{ dbt_utils.union_relations(relations) }}
{% endmacro %}