{% macro create_stg_paidmedia_pacing_actuals_campaigns_rollup_join(
    campaigns='stg_paidmedia_pacing_actuals_campaigns',
    rollups='stg_paidmedia_pacing_actuals_rollup'
    ) %}

SELECT
rollups.date_day,
rollups.platform,
rollups.campaign_name,
campaigns.best_guess_entity,
campaigns.objective,
campaigns.channel,
campaigns.channel_type,
rollups.actual_spend,
rollups.actual_revenue

FROM {{ref('rollups')}} rollups
LEFT JOIN {{ref('campaigns')}} campaigns
on lower(rollups.campaign_name) = lower(campaign.campaign_id)
and lower(rollups.platform) = lower(campaign.platform)

{% endmacro %}