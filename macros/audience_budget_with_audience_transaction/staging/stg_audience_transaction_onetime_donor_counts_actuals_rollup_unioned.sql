{% macro create_stg_audience_transaction_onetime_donor_counts_actuals_rollup_unioned() %}
    {% set relations = dbt_arc_functions.relations_that_match_regex(
        "^stg_.*_audience_transaction_onetime_donor_counts_actuals_rollup$"
    ) %}
    {{ dbt_utils.union_relations(relations) }}
{% endmacro %}
