{% macro create_stg_paidmedia_pacing_actuals_rollup(
    reference_name="mart_paidmedia_daily_revenue_performance"
) %}
select
    safe_cast(date_timestamp as date) as date_day,
    channel as platform,
    campaign_name,
    sum(spend_amount) as actual_spend,
    sum(total_revenue) as actual_revenue

from {{ ref(reference_name) }}
group by 1, 2, 3

{% endmacro %}
