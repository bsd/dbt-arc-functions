{% macro create_stg_email_campaigns_rollup_unioned() %}
{% set relations = dbt_arc_functions.relations_that_match_regex('^stg_.*_email_campaigns_and_dates_rollup_join$') %}
{{ dbt_utils.union_relations(relations) }}
{% endmacro %}