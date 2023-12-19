{% macro create_mart_audience_budget_with_audience_transaction() %}
    
    {{ util_mart_audience_budget_with_audience_transaction(recur_status="onetime") }}

{% endmacro %}
