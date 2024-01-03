{% macro create_mart_cashflow_actuals_and_budget() %}

{{ dbt_arc_functions.util_mart_cashflow_actuals_and_budget(
    combined_or_onetime='combined'
)}}

{% endmacro %}
