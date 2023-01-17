{% macro create_stg_paidmedia_pacing_actuals_campaign_and_date_rollup(
    reference_name='mart_paidmedia_daily_revenue_performance') %}

with safe_casting as (
SELECT
safe_cast(date_timestamp as date) as date_day,
lower(safe_cast(campaign_name as string)) as campaign_name,
safe_cast(spend_amount as int) as spend_amount
FROM {{ref(reference_name)}}

)

SELECT
date_day,
campaign_name,
sum(spend_amount) as daily_spend,
sum(spend_amount) over (partition by campaign_name order by date_day) as cumulative_spend

FROM safe_casting
GROUP BY 1, 2

{% endmacro %}