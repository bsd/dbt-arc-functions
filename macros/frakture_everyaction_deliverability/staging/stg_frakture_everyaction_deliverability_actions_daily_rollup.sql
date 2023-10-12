{% macro create_stg_frakture_everyaction_deliverability_actions_daily_rollup(
    reference_name="stg_frakture_everyaction_deliverability_message_stat_unioned_with_domain"
) %}
WITH unique_actions AS (
    SELECT
        safe_cast(sent_ts as date) as sent_date,
        safe_cast(message_id as string) as message_id,
        safe_cast(email_domain as string) as email_domain,
        subscriber_key,
        ROW_NUMBER() OVER (PARTITION BY message_id, subscriber_key ORDER BY action_ts) AS action_row_number
    FROM {{ ref(reference_name) }}
)

SELECT
    sent_date,
    message_id,
    email_domain,
    COUNT(DISTINCT CASE WHEN action_row_number = 1 THEN subscriber_key END) AS actions
FROM unique_actions
GROUP BY 1, 2, 3
{% endmacro %}
