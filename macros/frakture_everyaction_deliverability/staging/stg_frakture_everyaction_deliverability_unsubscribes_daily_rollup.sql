{% macro create_stg_frakture_everyaction_deliverability_unsubscribes_daily_rollup(
    reference_name="stg_frakture_everyaction_deliverability_message_stat_unioned_with_domain"
) %}
WITH unique_unsubscribes AS (
    SELECT
        safe_cast(sent_ts as date) as sent_date,
        safe_cast(message_id as string) as message_id,
        safe_cast(email_domain as string) as email_domain,
        subscriber_key,
        ROW_NUMBER() OVER (PARTITION BY message_id, subscriber_key ORDER BY unsubscribe_ts) AS unsubscribe_row_number
    FROM {{ ref(reference_name) }}
)

SELECT
    sent_date,
    message_id,
    email_domain,
    SUM(CASE WHEN unsubscribe_row_number = 1 THEN 1 ELSE 0 END) AS unsubscribes
FROM unique_unsubscribes
GROUP BY 1, 2, 3
{% endmacro %}
