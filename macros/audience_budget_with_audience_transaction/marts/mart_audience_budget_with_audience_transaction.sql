{% macro create_mart_audience_budget_with_audience_transaction() %}
    
    {{ dbt_arc_functions.util_mart_audience_budget_with_audience_transaction(recur_status="onetime") }}

{% endmacro %}
