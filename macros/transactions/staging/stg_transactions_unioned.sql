{% macro create_stg_transactions_unioned() %}
{% set relations = dbt_arc_functions.relations_that_match_regex('^stg_.*_transactions$') %}
{{ dbt_utils.union_relations(relations) }}
{% endmacro %}