{% macro create_stg_paidmedia_goals_monthly_actuals_rollup(
    reference_name='mart_paidmedia_daily_revenue_performance') %}
SELECT
     FORMAT_DATE('%B %Y', CAST(date_timestamp AS date)) as month_year, 
     case when channel_type is not null then SAFE_CAST(lower(channel_category) || ' ' || lower(channel_type) as string)
      else SAFE_CAST(lower(channel_category) as string) end
      as channel,
      SAFE_CAST(lower(objective) as string) as objective,
      SAFE_CAST(lower(channel) as string) as platform,
      SAFE_CAST(SUM(spend_amount) as float64) as actual_spend,
      SAFE_CAST(SUM(total_revenue) as float64) as actual_revenue,
      SAFE_CAST(SUM(total_gifts) as float64) as actual_donations
FROM {{ ref(reference_name) }} 
GROUP BY 1, 2, 3, 4
{% endmacro %}