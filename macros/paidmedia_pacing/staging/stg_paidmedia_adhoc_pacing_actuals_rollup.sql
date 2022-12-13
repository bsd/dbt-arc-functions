{% macro create_stg_paidmedia_adhoc_pacing_actuals_rollup(
    reference_name='mart_paidmedia_daily_revenue_performance') %}
SELECT
safe_cast(date_timestamp as date) as date_day,
best_guess_entity,
objective,
channel_category as channel,
channel_type,
channel as platform,
campaign_name,
sum(spend_amount) as actual_spend,
sum(total_revenue) as actual_revenue

FROM {{ref('reference_name')}}
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8



{% endmacro %}