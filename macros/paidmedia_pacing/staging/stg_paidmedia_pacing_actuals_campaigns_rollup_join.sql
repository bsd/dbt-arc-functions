{% macro create_stg_paidmedia_pacing_actuals_campaigns_rollup_join(
    campaigns='stg_paidmedia_pacing_actuals_campaigns',
    rollups='stg_paidmedia_pacing_actuals_rollup'
    ) %}

SELECT
rollups.date_day,
campaigns.objective,
campaigns.channel,
rollups.platform,
campaigns.channel_type,
rollups.campaign_name,
rollups.actual_spend,
rollups.actual_revenue

FROM {{ref(rollups)}} rollups
LEFT JOIN {{ref(campaigns)}} campaigns
on lower(rollups.campaign_name) = lower(campaigns.campaign_id)
and lower(rollups.platform) = lower(campaigns.platform)

{% endmacro %}