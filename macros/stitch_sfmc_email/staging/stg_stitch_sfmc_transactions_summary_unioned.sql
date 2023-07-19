{% macro create_stg_stitch_sfmc_transactions_summary_unioned() %}
    {% set relations = dbt_arc_functions.relations_that_match_regex(
        "^stg_stitch_sfmc_.*_transactions$",
    ) %}
    {{ dbt_utils.union_relations(relations) }}
{% endmacro %}
