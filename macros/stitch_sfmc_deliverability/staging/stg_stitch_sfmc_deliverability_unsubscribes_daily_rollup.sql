{% macro create_stg_stitch_sfmc_deliverability_unsubscribes_daily_rollup(
    reference_name="stg_src_stitch_email_unsubscribe"
) %}
WITH unique_unsubscribes AS (
    SELECT
        safe_cast(event_dt as date) as sent_date,
        safe_cast(job_id as string) as message_id,
        safe_cast(domain as string) as email_domain,
        subscriber_key,
        ROW_NUMBER() OVER (PARTITION BY job_id, subscriber_key ORDER BY event_dt) AS unsubscribe_row_number
    FROM {{ ref(reference_name) }}
)

SELECT
    sent_date,
    message_id,
    email_domain,
    COUNT(DISTINCT CASE WHEN unsubscribe_row_number = 1 THEN subscriber_key END) AS unsubscribes
FROM unique_unsubscribes
GROUP BY 1, 2, 3
{% endmacro %}
