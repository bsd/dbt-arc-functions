{% macro create_stg_stitch_sfmc_audience_transaction_monthly_recurring_rollup(
    reference_name='stg_stitch_sfmc_audience_transaction_with_join_date') %}
SELECT
    date_trunc(transaction_date_day, MONTH) AS transaction_month_year_date,
    date_trunc(join_month_year_date, MONTH) AS join_month_year,
    sum(amount) AS total_revenue,
    count(DISTINCT person_id) AS total_donors
FROM {{ ref(reference_name) }}
GROUP BY 1, 2
ORDER BY 1, 2

{% endmacro %}