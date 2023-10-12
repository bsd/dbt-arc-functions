{% macro create_stg_frakture_everyaction_deliverability_bounces_daily_rollup(
    reference_name="stg_frakture_everyaction_deliverability_message_stat_unioned_with_domain"
) %}
WITH unique_bounces AS (
    SELECT
        safe_cast(sent_ts as date) as sent_date,
        safe_cast(message_id as string) as message_id,
        safe_cast(email_domain as string) as email_domain,
        subscriber_key,
        ROW_NUMBER() OVER (PARTITION BY message_id, subscriber_key ORDER BY bounce_ts) AS bounce_row_number,
        safe_cast(hard_bounce as int) as hard_bounce,
        safe_cast(soft_bounce as int) as soft_bounce
    FROM {{ ref(reference_name) }}
)

SELECT
    sent_date,
    message_id,
    email_domain,
    SUM(CASE WHEN bounce_row_number = 1 THEN hard_bounce ELSE 0 END) AS hard_bounces,
    SUM(CASE WHEN bounce_row_number = 1 THEN soft_bounce ELSE 0 END) AS soft_bounces,
    SUM(CASE WHEN bounce_row_number = 1 THEN hard_bounce + soft_bounce ELSE 0 END) AS total_bounces
FROM unique_bounces
GROUP BY 1, 2, 3
{% endmacro %}
