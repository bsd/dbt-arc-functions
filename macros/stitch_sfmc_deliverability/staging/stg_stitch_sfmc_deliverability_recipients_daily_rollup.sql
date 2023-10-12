{% macro create_stg_stitch_sfmc_deliverability_recipients_daily_rollup(
    reference_name="stg_src_stitch_email_sent"
) %}
WITH unique_recipients AS (
    SELECT
        safe_cast(event_dt as date) as sent_date,
        safe_cast(job_id as string) as message_id,
        safe_cast(domain as string) as email_domain,
        subscriber_key,
        ROW_NUMBER() OVER (PARTITION BY job_id, subscriber_key ORDER BY event_dt) AS recipient_row_number
    FROM {{ ref(reference_name) }}
)

SELECT
    sent_date,
    message_id,
    email_domain,
    COUNT(DISTINCT CASE WHEN recipient_row_number = 1 THEN subscriber_key END) AS recipients
FROM unique_recipients
GROUP BY 1, 2, 3
{% endmacro %}
