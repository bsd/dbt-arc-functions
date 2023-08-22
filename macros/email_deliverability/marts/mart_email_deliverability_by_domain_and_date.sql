{% macro create_mart_email_deliverability_by_domain_and_date(
    jobs="stg_email_deliverability_jobs_distinct_unioned",
    bounces="stg_email_deliverability_bounces_daily_rollup_unioned",
    clicks="stg_email_deliverability_clicks_daily_rollup_unioned",
    opens="stg_email_deliverability_opens_daily_rollup_unioned",
    actions="stg_email_deliverability_actions_daily_rollup_unioned",
    recipients="stg_email_deliverability_recipients_daily_rollup_unioned",
    unsubscribes="stg_email_deliverability_unsubscribes_daily_rollup_unioned"
) %}
with base as (
    select
        jobs.sent_date,
        jobs.message_id,
        jobs.email_domain,
        case
            when lower(jobs.email_domain) like 'live.%'
            then 'Microsoft'
            when lower(jobs.email_domain) like 'hotmail.%'
            then 'Microsoft'
            when lower(jobs.email_domain) like 'outlook.%'
            then 'Microsoft'
            when lower(jobs.email_domain) like 'msn.%'
            then 'Microsoft'
            when lower(jobs.email_domain) like 'gmail.com'
            then 'Google'
            when lower(jobs.email_domain) like 'googlemail.com'
            then 'Google'
            when lower(jobs.email_domain) like 'google.com'
            then 'Google'
            when lower(jobs.email_domain) like 'gmail.com'
            then 'Google'
            when lower(jobs.email_domain) like 'yahoo.%'
            then 'Yahoo'
            when lower(jobs.email_domain) like 'myyahoo.%'
            then 'Yahoo'
            when lower(jobs.email_domain) like 'me.com'
            then 'Apple'
            when lower(jobs.email_domain) like 'apple.com'
            then 'Apple'
            when lower(jobs.email_domain) like 'icloud.com'
            then 'Apple'
            when lower(jobs.email_domain) like 'mac.com'
            then 'Apple'
            else 'Other'
        end as domain_category,
        recipients.recipients,
        opens.opens,
        clicks.clicks,
        actions.actions,
        case
            when bounces.soft_bounces + bounces.hard_bounces is null
            then 0
            else bounces.soft_bounces + bounces.hard_bounces
        end as total_bounces,
        bounces.soft_bounces,
        bounces.hard_bounces,
        unsubscribes.unsubscribes
    from {{ ref(jobs) }} jobs
    full outer join
        {{ ref(bounces) }} bounces
        on jobs.message_id = bounces.message_id
        and jobs.sent_date = bounces.sent_date
        and jobs.email_domain = bounces.email_domain
    full outer join
        {{ ref(clicks) }} clicks
        on jobs.message_id = clicks.message_id
        and jobs.sent_date = clicks.sent_date
        and jobs.email_domain = clicks.email_domain
    full outer join
        {{ ref(actions) }} actions
        on jobs.message_id = actions.message_id
        and jobs.sent_date = actions.sent_date
        and jobs.email_domain = actions.email_domain
    full outer join
        {{ ref(opens) }} opens
        on jobs.message_id = opens.message_id
        and jobs.sent_date = opens.sent_date
        and jobs.email_domain = opens.email_domain
    full outer join
        {{ ref(recipients) }} recipients
        on jobs.message_id = recipients.message_id
        and jobs.sent_date = recipients.sent_date
        and jobs.email_domain = recipients.email_domain
    full outer join
        {{ ref(unsubscribes) }} unsubscribes
        on jobs.message_id = unsubscribes.message_id
        and jobs.sent_date = unsubscribes.sent_date
        and jobs.email_domain = unsubscribes.email_domain)

    select distinct * from base 
    where sent_date is not null
    or message_id is not null
{% endmacro %}
