{% macro create_stg_paidmedia_pacing_actuals_campaigns_rollup_join(
    campaigns="stg_paidmedia_pacing_actuals_campaigns",
    rollups="stg_paidmedia_pacing_actuals_rollup"
) %}

select
    rollups.date_day,
    campaigns.objective,
    campaigns.channel,
    rollups.platform,
    campaigns.channel_type,
    rollups.campaign_name,
    rollups.actual_spend,
    rollups.actual_revenue

from {{ ref(rollups) }} rollups
left join
    {{ ref(campaigns) }} campaigns
    on lower(rollups.campaign_name) = lower(campaigns.campaign_name)
    and lower(rollups.platform) = lower(campaigns.platform)

{% endmacro %}
