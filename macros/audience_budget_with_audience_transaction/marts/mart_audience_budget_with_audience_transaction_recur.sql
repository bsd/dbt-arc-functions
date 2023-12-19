{% macro create_mart_audience_budget_with_audience_transaction_recur() %}

{{ util_mart_audience_budget_with_audience_transaction(recur_status="recurring") }}

{% endmacro %}
