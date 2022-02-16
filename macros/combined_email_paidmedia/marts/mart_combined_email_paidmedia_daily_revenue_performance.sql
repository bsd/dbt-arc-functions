{% macro create_mart_combined_email_paidmedia_daily_revenue_performance(
    reference_0_name='mart_email_performance_with_revenue',
    reference_1_name='mart_paidmedia_daily_revenue_performance') %}
SELECT 
    TIMESTAMP_TRUNC(best_guess_timestamp,DAY) as date_timestamp,
    'email' AS channel,
    SUM(COALESCE(total_revenue,0)) AS total_revenue,
    SUM(COALESCE(total_gifts,0)) AS total_gifts
FROM {{ ref(reference_0_name) }}
WHERE TIMESTAMP_TRUNC(best_guess_timestamp,DAY) IS NOT NULL
GROUP BY 1,2
UNION ALL
SELECT
    date_timestamp,
    channel,
    SUM(COALESCE(total_revenue,0)) AS total_revenue,
    SUM(COALESCE(total_gifts,0)) AS total_gifts
FROM {{ ref(reference_1_name) }}
WHERE date_timestamp IS NOT NULL
GROUP BY 1,2

{% endmacro %}