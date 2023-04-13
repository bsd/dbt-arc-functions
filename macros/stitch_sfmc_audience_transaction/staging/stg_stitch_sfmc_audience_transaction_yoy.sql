{% macro create_stg_stitch_sfmc_audience_transaction_yoy(
    reference_name="stg_src_stitch_sfmc_transactions_unioned"
) %}

SELECT
    transaction_date,
    person_id,
    SELECT
    transaction_date,
    person_id,
    MAX(
        CASE
            WHEN DATE_TRUNC('year', transaction_date) = DATE_TRUNC('year', transaction_date) THEN 1
            ELSE 0
        END
    ) AS donated_this_year,
    MAX(
        CASE
            WHEN DATE_TRUNC('year', DATEADD('year', -1, transaction_date)) = DATE_TRUNC('year', DATEADD('year', -1, transaction_date)) THEN 1
            ELSE 0
        END
    ) AS donated_last_year,
    MAX(
        CASE
            WHEN DATE_TRUNC('year', DATEADD('year', -2, transaction_date)) = DATE_TRUNC('year', DATEADD('year', -2, transaction_date)) THEN 1
            ELSE 0
        END
    ) AS donated_two_years_ago,
    MAX(
        CASE
            WHEN DATE_TRUNC('year', DATEADD('year', -3, transaction_date)) = DATE_TRUNC('year', DATEADD('year', -3, transaction_date)) THEN 1
            ELSE 0
        END
    ) AS donated_three_years_ago,
    MAX(
        CASE
            WHEN transaction_date >= DATE_TRUNC('year', transaction_date) AND 
                 person_id NOT IN (
                     SELECT DISTINCT person_id
                     FROM {{ ref(reference_name) }}
                     WHERE transaction_date >= DATE_TRUNC('year', DATEADD('year', -1, transaction_date)) AND 
                           transaction_date < DATE_TRUNC('year', transaction_date)
                 ) THEN 1
            ELSE 0
        END
    ) AS new_donor,
    MAX(
        CASE
            WHEN transaction_date >= DATEADD('month', -14, transaction_date) THEN 1
            ELSE 0
        END
    ) AS donated_within_14_months
    MAX(
        CASE
            WHEN transaction_date >= DATEADD('month', -13, transaction_date) THEN 1
            ELSE 0
        END
    ) AS donated_within_13_months
FROM {{ ref(reference_name) }}
GROUP BY transaction_date, person_id


{% endmacro %}
