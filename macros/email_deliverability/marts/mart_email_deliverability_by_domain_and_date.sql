{% macro create_mart_email_deliverability_by_domain_and_date(
    jobs='stg_email_deliverability_person_jobs_distinct_unioned',
    bounces='stg_email_deliverability_person_bounces_daily_rollup_unioned',
    clicks='stg_email_deliverability_person_clicks_daily_rollup_unioned',
    opens='stg_email_deliverability_person_opens_daily_rollup_unioned',
    actions='stg_email_deliverability_person_actions_daily_rollup_unioned',
    recipients='stg_email_deliverability_person_recipients_daily_rollup_unioned',
    unsubscribes='stg_email_deliverability_person_unsubscribes_daily_rollup_unioned') %}
SELECT
    jobs.sent_date,
    jobs.message_id,
    jobs.email_domain,
    CASE WHEN lower(jobs.email_domain) LIKE 'live.%' THEN 'Microsoft' 
    WHEN lower(jobs.email_domain) LIKE 'hotmail.%' THEN 'Microsoft' 
    WHEN lower(jobs.email_domain) LIKE 'outlook.%' THEN 'Microsoft' 
    WHEN lower(jobs.email_domain) LIKE 'msn.%' THEN 'Microsoft'
    WHEN lower(jobs.email_domain) LIKE 'gmail.com' THEN 'Google'
    WHEN lower(jobs.email_domain) LIKE 'googlemail.com' THEN 'Google'
    WHEN lower(jobs.email_domain) LIKE 'google.com' THEN 'Google'  
    WHEN lower(jobs.email_domain) LIKE 'gmail.com' THEN 'Google'
    WHEN lower(jobs.email_domain) LIKE 'yahoo.%' THEN 'Yahoo'
    WHEN lower(jobs.email_domain) LIKE 'myyahoo.%' THEN 'Yahoo'
    WHEN lower(jobs.email_domain)LIKE 'me.com' THEN 'Apple'
    WHEN lower(jobs.email_domain)LIKE 'apple.com' THEN 'Apple'
    WHEN lower(jobs.email_domain) LIKE 'icloud.com' THEN 'Apple'
    WHEN lower(jobs.email_domain) LIKE 'mac.com' THEN 'Apple'
    ELSE 'Other'
    END AS domain_category,
    recipients.recipients,
    opens.opens,
    clicks.clicks,
    actions.actions,
    (bounces.soft_bounces + bounces.hard_bounces) AS total_bounces,
    bounces.soft_bounces,
    bounces.hard_bounces,
    unsubscribes.unsubscribes
FROM {{ ref(jobs) }} jobs
LEFT JOIN {{ ref(bounces) }} bounces
ON jobs.message_id = bounces.message_id
AND jobs.sent_date = bounces.sent_date
AND jobs.email_domain = bounces.email_domain
LEFT JOIN {{ ref(clicks) }} clicks
ON jobs.message_id = clicks.message_id
AND jobs.sent_date = clicks.sent_date
AND jobs.email_domain = clicks.email_domain
LEFT JOIN {{ ref(actions) }} actions
ON jobs.message_id = actions.message_id
AND jobs.sent_date = actions.sent_date
AND jobs.email_domain = actions.email_domain
LEFT JOIN {{ ref(opens) }} opens
ON jobs.message_id = opens.message_id
AND jobs.sent_date = opens.sent_date
AND jobs.email_domain = opens.email_domain
LEFT JOIN {{ ref(recipients) }}  recipients
ON jobs.message_id = recipients.message_id
AND jobs.sent_date = recipients.sent_date
AND jobs.email_domain = recipients.email_domain
LEFT JOIN {{ ref(unsubscribes) }} unsubscribes
ON jobs.message_id = unsubscribes.message_id
AND jobs.sent_date = unsubscribes.sent_date
AND jobs.email_domain = unsubscribes.email_domain
{% endmacro %}
