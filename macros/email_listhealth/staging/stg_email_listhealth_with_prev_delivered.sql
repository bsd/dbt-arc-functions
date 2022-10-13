{% macro create_stg_email_listhealth_with_prev_delivered(
    reference_name='stg_email_listhealth_by_year_and_month_concat'
) %}
SELECT
date_month,
max_recipients,
max_delivered,
LAG(max_delivered) 
OVER (ORDER BY date_month asc) AS delivered_prev_month,
total_unsubscribes,
total_hard_bounces,
total_complaints 
FROM  {{ ref(reference_name) }} mart_email

{% endmacro %}

