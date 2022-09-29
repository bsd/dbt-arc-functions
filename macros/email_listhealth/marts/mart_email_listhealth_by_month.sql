{% macro create_mart_email_listhealth_by_month(
    reference_name='stg_email_listhealth_with_prev_delivered'
) %}
SELECT
date_month,
max_recipients,
max_delivered,
CAST(max_delivered AS INT) - CAST(delivered_prev_month as INT) as diff_delivered_prev_month,
total_unsubscribes,
total_hard_bounces,
total_complaints 
FROM  {{ ref(reference_name) }} mart_email

{% endmacro %}

