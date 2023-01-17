{% macro create_stg_paidmedia_pacing_actuals_campaign_and_date_rollup(
    reference_name='mart_paidmedia_daily_revenue_performance') %}
SELECT
safe_cast(date_timestamp as date) as date_day,
lower(safe_cast(campaign_name as string)) as campaign_name,
sum(spend_amount) as daily_spend,
sum(spend_amount) over (partition by campaign_name order by date_day) as cumulative_spend

FROM {{ref(reference_name)}}
GROUP BY 1, 2

{% endmacro %}