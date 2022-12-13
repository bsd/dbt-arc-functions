{% macro create_stg_paidmedia_pacing_actuals_campaigns(
    reference_name='mart_paidmedia_daily_revenue_performance') %}

SELECT
campaign_name,
channel as platform,
max(best_guess_entity),
max(objective),
max(channel_category) as channel,
max(channel_type)
FROM {{ ref(reference_name) }}
GROUP BY 1, 2


{% endmacro %}