{% macro create_stg_frakture_everyaction_deliverability_clicks_daily_rollup(
    reference_name="stg_frakture_everyaction_deliverability_message_stat_unioned_with_domain"
) %}
WITH unique_clicks AS (
    SELECT
        safe_cast(event_dt as date) as sent_date,
        safe_cast(message_id as string) as message_id,
        safe_cast(email_domain as string) as email_domain,
        subscriber_key,
        ROW_NUMBER() OVER (PARTITION BY message_id, subscriber_key ORDER BY event_dt) AS click_row_number
    FROM {{ ref(reference_name) }}
)

SELECT
    sent_date,
    message_id,
    email_domain,
    SUM(CASE WHEN click_row_number = 1 THEN 1 ELSE 0 END) AS clicks
FROM unique_clicks
GROUP BY 1, 2, 3
{% endmacro %}
