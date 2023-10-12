{% macro create_stg_frakture_everyaction_deliverability_bounces_daily_rollup(
    reference_name="stg_frakture_everyaction_deliverability_message_stat_unioned_with_domain"
) %}


with unique_bounces AS (
    SELECT
        safe_cast(event_dt as date) as sent_date,
        safe_cast(job_id as string) as message_id,
        safe_cast(domain as string) as email_domain,
        subscriber_key,
        ROW_NUMBER() OVER (PARTITION BY job_id, subscriber_key ORDER BY event_dt) AS bounce_row_number,
        safe_cast((case when bounce_category_id = '1' then 1 else 0 end) as int) as hard_bounce,
        safe_cast((case when bounce_category_id = '2' then 1 else 0 end) as int) as soft_bounce
    FROM {{ref(reference_name)}}
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
