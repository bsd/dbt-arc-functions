{% macro create_mart_cashflow_actuals_and_budget_1x_revenue(
    reference_name='mart_cashflow_actuals_and_budget'
) %}

    select * from {{ref(reference_name)}}
    where recur_flag is null or recur_flag = false

{% endmacro %}
