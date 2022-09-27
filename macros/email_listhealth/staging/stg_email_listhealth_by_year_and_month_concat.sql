{% macro create_stg_email_listhealth_year_and_month_concat(
    reference_name='stg_email_listhealth_by_year_and_month'
) %}
SELECT
CONCAT(extract_year,'-',extract_month,'-01') as concat_date,
max_recipients,
max_delivered,
LAG(max_delivered)
  OVER (ORDER BY date_month DESC) AS delivered_prev_month,
total_hard_bounces,
total_unsubscribes,
total_complaints
FROM  {{ ref(reference_name) }} mart_email

{% endmacro %}

