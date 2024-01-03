{% macro create_mart_cashflow_actuals_and_budget_1x_revenue() %}

{{ dbt_arc_functions.util_mart_cashflow_actuals_and_budget(
    combined_or_onetime='onetime'
)}}

{% endmacro %}
