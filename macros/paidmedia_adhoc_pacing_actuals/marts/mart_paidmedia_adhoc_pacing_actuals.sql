{% macro create_mart_paidmedia_adhoc_pacing_actuals(
    reference_name="stg_paidmedia_pacing_actuals_campaigns_rollup_join"
) %}

select
    date_day,
    date_trunc(date_day, week) as date_week_sunday,
    date_trunc(date_day, week(monday)) as date_week_monday,
    date_trunc(date_day, month) as date_month,
    date_trunc(date_day, quarter) as date_quarter,
    objective,
    channel,
    channel_type,
    case when platform = 'ad' then 'bing_ad' else platform end as platform,
    campaign_name,
    actual_spend,
    actual_revenue

from {{ ref(reference_name) }}

{% endmacro %}
