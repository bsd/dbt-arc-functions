{% macro create_stg_email_complaints_rollup_unioned() %}
{% set relations = dbt_arc_functions.relations_that_match_regex('^stg_.*_email_complaints_rollup$') %}
{{ dbt_utils.union_relations(relations) }}
{% endmacro %}