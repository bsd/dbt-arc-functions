{% macro create_stg_email_clicks_rollup_unioned() %}
{% set relations = relations_that_match_regex('^stg_.*_email_clicks_rollup$') %}
{{ dbt_utils.union_relations(relations) }}
{% endmacro %}