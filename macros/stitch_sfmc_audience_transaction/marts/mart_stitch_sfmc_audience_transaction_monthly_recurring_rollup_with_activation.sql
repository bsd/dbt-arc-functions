{% macro create_mart_stitch_sfmc_audience_transaction_monthly_recurring_rollup_with_activation(
    reference_name="stg_stitch_sfmc_audience_transaction_monthly_recurring_rollup_with_activation"
) %}
    select
        join_month_year, join_month_year_str, activation, total_revenue, total_donors,
    from {{ ref(reference_name) }}
{% endmacro %}
