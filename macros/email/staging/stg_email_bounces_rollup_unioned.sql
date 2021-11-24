{% macro create_stg_email_bounces_rollup_unioned() %}
{% set relations = relations_that_match_regex('^stg_.*_email_bounces_rollup$') %}
{{ dbt_utils.union_relations(relations) }}
{% endmacro %}