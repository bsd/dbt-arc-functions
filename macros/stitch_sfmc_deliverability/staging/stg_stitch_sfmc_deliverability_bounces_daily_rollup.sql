{% macro create_stg_stitch_sfmc_deliverability_bounces_daily_rollup(
    reference_name="stg_src_stitch_email_bounce"
) %}

WITH unique_bounces AS (
    SELECT
        safe_cast(event_dt as date) AS sent_date,
        safe_CAST(job_id AS STRING) AS message_id,
        safe_CAST(domain AS STRING) AS email_domain,
        subscriber_key,
        ROW_NUMBER() OVER (PARTITION BY job_id, subscriber_key ORDER BY event_dt) AS bounce_row_number
    FROM {{ ref(reference_name) }}
)

SELECT
    sent_date,
    message_id,
    email_domain,
    COUNT(DISTINCT CASE WHEN bounce_category_id = '1' AND bounce_row_number = 1 THEN subscriber_key END) AS hard_bounces,
    COUNT(DISTINCT CASE WHEN bounce_category_id = '2' AND bounce_row_number = 1 THEN subscriber_key END) AS soft_bounces,
    COUNT(DISTINCT CASE WHEN bounce_row_number = 1 THEN subscriber_key END) AS total_bounces
FROM unique_bounces
GROUP BY 1, 2, 3
{% endmacro %}

