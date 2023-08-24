{% macro create_mart_stitch_sfmc_arc_revenue_and_donor_count_by_lifetime_gifts(
    reference_name='stg_stitch_sfmc_arc_audience_union_transaction_joined') %}
SELECT
    EXTRACT(YEAR FROM transaction_date_day) AS transaction_date_year,
    EXTRACT(MONTH FROM transaction_date_day) AS transaction_date_month,
    transaction_date_day AS transaction_date_day,
    CASE
        WHEN gift_count < 6 THEN "less than 6"
        WHEN gift_count BETWEEN 6 AND 12 THEN "6-12"
        WHEN gift_count BETWEEN 13 AND 24 THEN "13-24"
        WHEN gift_count BETWEEN 25 AND 36 THEN "25-36"
        ELSE "37+" END
        AS recurring_gift_cumulative_str,
    COUNT(DISTINCT person_id) AS donors,
    SUM(amount) AS summed_amount,
FROM {{ ref(reference_name) }}
WHERE recurring = true
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3, 4

{% endmacro %}