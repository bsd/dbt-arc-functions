{% macro create_stg_stitch_sfmc_transactions_unioned() %}
{% set relations = dbt_arc_functions.relations_that_match_regex(
    "^stg_stitch_.*_transaction$"
) %}
{{ dbt_utils.union_relations(relations) }}
{% endmacro %}
