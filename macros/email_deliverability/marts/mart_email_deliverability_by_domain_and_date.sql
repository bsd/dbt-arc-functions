{% macro create_mart_email_deliverability_by_domain_and_date(
    details='stg_email_deliverability_person_details_unioned',
    bounces='stg_email_deliverability_person_bounces_daily_rollup_unioned',
    clicks='stg_email_deliverability_person_clicks_daily_rollup_unioned',
    opens='stg_email_deliverability_person_opens_daily_rollup_unioned',
    actions='stg_email_deliverability_person_actions_daily_rollup_unioned',
    recipients='stg_email_deliverability_person_recipients_daily_rollup_unioned',
    unsubscribes='stg_email_deliverability_person_unsubscribes_daily_rollup_unioned') %}
SELECT details.person_id,
    details.email_domain,
    recipients.recipients,
    opens.opens,
    clicks.clicks,
    actions.actions,
    bounces.total_bounces,
    bounces.soft_bounces,
    bounces.hard_bounces,
    unsubscribes.unsubscribes
FROM {{ ref(details) }} details
FULL JOIN {{ ref(bounces) }} bounces
USING (person_id)
FULL JOIN {{ ref(clicks) }} clicks
USING (person_id))
FULL JOIN {{ ref(actions) }} actions
USING (person_id)
FULL JOIN {{ ref(opens) }} opens
USING (person_id)
FULL JOIN {{ ref(recipients) }}  recipients
USING (person_id)
FULL JOIN {{ ref(unsubscribes) }} unsubscribes
USING (person_id)
{% endmacro %}
