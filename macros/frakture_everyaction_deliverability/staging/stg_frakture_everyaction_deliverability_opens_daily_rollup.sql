{% macro create_stg_frakture_everyaction_deliverability_opens_daily_rollup(
    reference_name="stg_frakture_everyaction_deliverability_message_stat_unioned_with_domain"
) %}
WITH unique_opens AS (
    SELECT
        safe_cast(sent_ts as date) as sent_date,
        safe_cast(message_id as string) as message_id,
        safe_cast(email_domain as string) as email_domain,
        subscriber_key,
        ROW_NUMBER() OVER (PARTITION BY message_id, subscriber_key ORDER BY open_ts) AS open_row_number,
        safe_cast(opened as int) as opened
    FROM {{ ref(reference_name) }}
)

SELECT
    sent_date,
    message_id,
    email_domain,
    SUM(CASE WHEN open_row_number = 1 THEN opened ELSE 0 END) AS opens
FROM unique_opens
GROUP BY 1, 2, 3
{% endmacro %}
