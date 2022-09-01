{% macro create_stg_frakture_global_message_email_campaigns_and_dates_rollup_join(
    campaigns='stg_frakture_global_message_email_campaigns_rollup',
    campaign_dates='stg_frakture_global_message_email_campaign_dates_rollup'
    ) %}
SELECT DISTINCT campaigns.message_id,
    campaigns.crm_entity,
    campaigns.source_code_entity,
    campaigns.audience,
    campaigns.recurtype,
    campaigns.campaign_category,
    campaigns.campaign_name,
    campaign_dates.campaign_start_timestamp,
    campaign_dates.campaign_latest_timestamp
FROM {{ ref(campaigns) }} campaigns
FULL JOIN {{ ref(campaign_dates) }} campaign_dates
USING (campaign_name)
{% endmacro %}