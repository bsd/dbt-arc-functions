{% macro create_stg_stitch_sfmc_audience_transaction_yoy(
    reference_name="stg_src_stitch_sfmc_transactions_unioned"
) %}

SELECT
    transaction_date,
    person_id,
    MAX(
        CASE
            WHEN EXTRACT(YEAR FROM CURRENT_DATE()) - EXTRACT(YEAR FROM transaction_date) = 0 THEN 1
            ELSE 0
        END
    ) AS donated_this_year,
    MAX(
        CASE
            WHEN DATEDIFF(current_date(), transaction_date) <= 14 THEN 1 ELSE 0
        END
    ) AS donated_within_14_months,
    MAX(
        CASE
            WHEN EXTRACT(YEAR FROM CURRENT_DATE()) - EXTRACT(YEAR FROM transaction_date) = 1 THEN 1
            ELSE 0
        END
    ) AS donated_last_year,
    MAX(
        CASE
            WHEN EXTRACT(YEAR FROM CURRENT_DATE()) - EXTRACT(YEAR FROM transaction_date) = 2 THEN 1
            ELSE 0
        END
    ) AS donated_two_years_ago,
    MAX(
        CASE
            WHEN EXTRACT(YEAR FROM CURRENT_DATE()) - EXTRACT(YEAR FROM transaction_date) = 3 THEN 1
            ELSE 0
        END
    ) AS donated_three_years_ago
FROM {{ ref(reference_name) }}
WHERE transaction_date >= DATE_TRUNC('year', CURRENT_DATE() - INTERVAL '3 year')
GROUP BY person_id, transaction_date


{% endmacro %}
