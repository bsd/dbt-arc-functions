{% macro create_stg_paidmedia_pacing_actuals_campaigns(
    reference_name="mart_paidmedia_daily_revenue_performance"
) %}

select
    campaign_name,
    channel as platform,
    max(objective) as objective,
    max(channel_category) as channel,
    max(channel_type) as channel_type
from {{ ref(reference_name) }}
group by 1, 2

{% endmacro %}
