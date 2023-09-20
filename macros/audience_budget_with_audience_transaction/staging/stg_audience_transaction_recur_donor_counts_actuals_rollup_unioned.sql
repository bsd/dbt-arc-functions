{% macro create_stg_audience_transaction_recur_donor_counts_actuals_rollup_unioned() %}
{% set relations = dbt_arc_functions.relations_that_match_regex(
    "^stg_\w+_audience_transaction_recur_donor_counts_combined$"
) %}
{{ dbt_utils.union_relations(relations) }}

{% endmacro %}