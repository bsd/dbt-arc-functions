{% macro create_stg_email_list_size_year_and_month(
    reference_name='mart_email_performance_with_revenue'
) %}
SELECT
extract(YEAR from best_guess_timestamp) as extract_year,
extract(MONTH from best_guess_timestamp) as extract_month,
MAX(recipients) AS max_recipients,
MAX(recipients - total_bounces) AS max_delivered
FROM  {{ ref(reference_name) }} mart_email
GROUP BY 1, 2

{% endmacro %}


