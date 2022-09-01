{% macro create_mart_email_performance_with_revenue(
    jobs='stg_email_jobs_unioned',
    campaigns='stg_email_campaigns_rollup_unioned',
    campaign_dates='stg_email_campaign_dates_rollup_unioned',
    bounces='stg_email_bounces_rollup_unioned',
    clicks='stg_email_clicks_rollup_unioned',
    opens='stg_email_opens_rollup_unioned',
    actions='stg_email_actions_rollup_unioned',
    recipients='stg_email_recipients_rollup_unioned',
    transactions='stg_email_transactions_sourced_rollup_unioned',
    unsubscribes='stg_email_unsubscribes_rollup_unioned') %}
SELECT jobs.message_id,
    jobs.from_name,
    jobs.from_email,
    jobs.best_guess_timestamp,
    jobs.scheduled_timestamp,
    jobs.pickup_timestamp,
    jobs.delivered_timestamp,
    campaign_dates.campaign_start_timestamp,
    campaign_dates.campaign_latest_timestamp,
    jobs.email_name,
    jobs.email_subject,
    jobs.source_code,
    campaigns.crm_entity,
    campaigns.source_code_entity,
    case when (campaigns.crm_entity is not null and campaigns.source_code_entity is not null)
          then CONCAT(campaigns.crm_entity,'-', campaigns.source_code_entity)
          else COALESCE(campaigns.crm_entity,campaigns.source_code_entity) END
          AS best_guess_entity,
    campaigns.audience,
    campaigns.campaign_category,
    COALESCE(campaigns.campaign_name, campaign_dates.campaign_name) as campaign_name,
    campaigns.recurtype,
    recipients.recipients,
    opens.opens,
    clicks.clicks,
    actions.actions,
    bounces.total_bounces,
    bounces.block_bounces,
    bounces.tech_bounces,
    bounces.soft_bounces,
    bounces.unknown_bounces,
    unsubscribes.unsubscribes,
    transactions.total_revenue,
    transactions.total_gifts,
    transactions.total_donors,
    transactions.one_time_revenue,
    transactions.one_time_gifts,
    transactions.new_monthly_revenue,
    transactions.new_monthly_gifts
FROM {{ ref(jobs) }} jobs
FULL JOIN {{ ref(bounces) }} bounces
USING (message_id)
FULL JOIN {{ ref(clicks) }} clicks
USING (message_id)
FULL JOIN {{ ref(actions) }} actions
USING (message_id)
FULL JOIN {{ ref(opens) }} opens
USING (message_id)
FULL JOIN {{ ref(recipients) }}  recipients
USING (message_id)
FULL JOIN {{ ref(transactions) }}  transactions
USING (message_id)
FULL JOIN {{ ref(unsubscribes) }} unsubscribes
USING (message_id)
LEFT JOIN {{ ref(campaigns) }} campaigns
USING (message_id)
LEFT JOIN {{ ref(campaign_dates)}} campaign_dates
ON campaigns.campaign_name = campaign_dates.campaign_name
{% endmacro %}
