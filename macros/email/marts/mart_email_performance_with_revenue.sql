{% macro create_mart_email_performance_with_revenue(
    jobs="stg_email_jobs_unioned",
    campaigns="stg_email_campaigns_rollup_unioned",
    campaign_dates="stg_email_campaign_dates_rollup_unioned",
    bounces="stg_email_bounces_rollup_unioned",
    clicks="stg_email_clicks_rollup_unioned",
    opens="stg_email_opens_rollup_unioned",
    actions="stg_email_actions_rollup_unioned",
    complaints="stg_email_complaints_rollup_unioned",
    recipients="stg_email_recipients_rollup_unioned",
    transactions="stg_email_transactions_sourced_rollup_unioned",
    unsubscribes="stg_email_unsubscribes_rollup_unioned"
) %}
    select
        jobs.message_id,
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
        case
            when
                (
                    campaigns.crm_entity is not null
                    and campaigns.source_code_entity is not null
                )
            then concat(campaigns.crm_entity, '-', campaigns.source_code_entity)
            else coalesce(campaigns.crm_entity, campaigns.source_code_entity)
        end as best_guess_entity,
        campaigns.audience,
        campaigns.campaign_category,
        coalesce(
            campaigns.campaign_name, campaign_dates.campaign_name
        ) as campaign_name,
        campaigns.recurtype,
        recipients.recipients,
        opens.opens,
        clicks.clicks,
        actions.actions,
        complaints.complaints,
        bounces.total_bounces,
        bounces.block_bounces,
        bounces.tech_bounces,
        bounces.soft_bounces,
        bounces.hard_bounces,
        unsubscribes.unsubscribes,
        transactions.total_revenue,
        transactions.total_gifts,
        transactions.total_donors,
        transactions.one_time_revenue,
        transactions.one_time_gifts,
        transactions.new_monthly_revenue,
        transactions.new_monthly_gifts,
        transactions.total_monthly_revenue,
        transactions.total_monthly_gifts
    from {{ ref(jobs) }} jobs
    full join {{ ref(bounces) }} bounces using (message_id)
    full join {{ ref(campaigns) }} campaigns using (message_id)
    full join {{ ref(clicks) }} clicks using (message_id)
    full join {{ ref(actions) }} actions using (message_id)
    full join {{ ref(complaints) }} complaints using (message_id)
    full join {{ ref(opens) }} opens using (message_id)
    full join {{ ref(recipients) }} recipients using (message_id)
    full join {{ ref(transactions) }} transactions using (message_id)
    full join {{ ref(unsubscribes) }} unsubscribes using (message_id)
    full join
        {{ ref(campaign_dates) }} campaign_dates
        on campaigns.campaign_name = campaign_dates.campaign_name
    where jobs.message_id is not null
{% endmacro %}
