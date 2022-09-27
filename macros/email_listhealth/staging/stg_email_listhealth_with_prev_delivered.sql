{% macro create_mart_email_listhealth_by_month(
    reference_name='stg_email_listhealth_by_year_and_month_concat'
) %}
SELECT
SAFE_CAST(concat_date AS DATE) AS date_month,
SAFE_CAST(max_recipients AS INT) AS max_recipients,
SAFE_CAST(max_delivered AS INT) AS max_delivered,
LAG(max_delivered) 
OVER (ORDER BY date_month desc) AS delivered_prev_month
SAFE_CAST(total_unsubscribes AS INT) AS total_unsubscribes,
SAFE_CAST(total_hard_bounces AS INT) AS total_hard_bounces,
SAFE_CAST(total_complaints AS INT) AS total_complaints 
FROM  {{ ref(reference_name) }} mart_email
WHERE concat_date IS NOT NULL

{% endmacro %}

