{% macro create_stg_email_campaign_dates_rollup_unioned() %}
{% set relations = dbt_arc_functions.relations_that_match_regex('^stg_.*_email_campaign_dates$') %}
{{ dbt_utils.union_relations(relations) }}
{% endmacro %}