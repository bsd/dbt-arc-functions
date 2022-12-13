{% macro create_stg_paidmedia_pacing_actuals_rollup(
    reference_name='mart_paidmedia_daily_revenue_performance') %}
SELECT
safe_cast(date_timestamp as date) as date_day,
channel as platform,
campaign_name,
sum(spend_amount) as actual_spend,
sum(total_revenue) as actual_revenue

FROM {{ref(reference_name)}}
GROUP BY 1, 2, 3



{% endmacro %}