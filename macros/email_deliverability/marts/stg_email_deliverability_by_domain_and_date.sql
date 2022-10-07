{% macro create_stg_email_deliverability_by_domain_and_date(
    details='stg_email_deliverability_person_details_unioned',
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
    details.email_domain,
    CASE WHEN lower(details.email_domain) LIKE 'live.%' THEN 'Microsoft' 
    WHEN lower(details.email_domain) LIKE 'hotmail.%' THEN 'Microsoft' 
    WHEN lower(details.email_domain) LIKE 'outlook.%' THEN 'Microsoft' 
    WHEN lower(details.email_domain) LIKE 'msn.%' THEN 'Microsoft'
    WHEN lower(details.email_domain) LIKE 'gmail.com' THEN 'Google'
    WHEN lower(details.email_domain) LIKE 'googlemail.com' THEN 'Google'  
    WHEN lower(details.email_domain) LIKE 'gmail.com' THEN 'Google'
    WHEN lower(details.email_domain) LIKE 'yahoo.%' THEN 'Yahoo'
    WHEN lower(details.email_domain) LIKE 'myyahoo.%' THEN 'Yahoo'
    WHEN lower(details.email_domain) LIKE 'me.com' THEN 'Apple'
    WHEN lower(details.email_domain) LIKE 'icloud.com' THEN 'Apple'
    WHEN lower(details.email_domain) LIKE 'mac.com' THEN 'Apple'
    ELSE 'Other'
    END AS domain_category,
    recipients.recipients,
    opens.opens,
    clicks.clicks,
    actions.actions,
    (bounces.soft_bounces + bounces.hard_bounces) AS total_bounces
    bounces.soft_bounces,
    bounces.hard_bounces,
    unsubscribes.unsubscribes
FROM {{ ref(jobs) }} jobs
LEFT JOIN {{ ref(details) }} details
ON details.person_id = jobs.person_id
FULL JOIN {{ ref(bounces) }} bounces
ON jobs.message_id = bounces.message_id
AND jobs.sent_date = bounces.sent_date
FULL JOIN {{ ref(clicks) }} clicks
ON jobs.message_id = clicks.message_id
AND jobs.sent_date = clicks.sent_date
FULL JOIN {{ ref(actions) }} actions
ON jobs.message_id = actions.message_id
AND jobs.sent_date = actions.sent_date
FULL JOIN {{ ref(opens) }} opens
ON jobs.message_id = opens.message_id
AND jobs.sent_date = opens.sent_date
FULL JOIN {{ ref(recipients) }}  recipients
ON jobs.message_id = recipients.message_id
AND jobs.sent_date = recipients.sent_date
FULL JOIN {{ ref(unsubscribes) }} unsubscribes
ON jobs.message_id = unsubscribes.message_id
AND jobs.sent_date = unsubscribes.sent_date
{% endmacro %}
