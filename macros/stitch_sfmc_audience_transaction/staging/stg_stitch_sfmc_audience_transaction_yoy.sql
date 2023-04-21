{% macro create_stg_stitch_sfmc_audience_transaction_yoy(
    reference_name="stg_stitch_sfmc_transactions_unioned"
) %}

WITH fiscal_years AS (
  SELECT 
    DATE_TRUNC(DATE_ADD(transaction_date, INTERVAL 6 MONTH), YEAR) as fiscal_year_start,
    DATE_TRUNC(DATE_ADD(transaction_date, INTERVAL 18 MONTH), YEAR) as fiscal_year_end,
    *
  FROM {{ ref(reference_name) }}
)

SELECT
    transaction_date,
    person_id,
    1 AS donated_current_fiscal_year_july_to_june,
    MAX(
        CASE
            WHEN
                transaction_date >= DATE_TRUNC(DATE_ADD(fiscal_years.fiscal_year_start, INTERVAL -1 YEAR), YEAR)
                AND transaction_date < fiscal_years.fiscal_year_start
            THEN 1
            ELSE 0
        END
    ) AS donated_last_fiscal_year_july_to_june,
    MAX(
        CASE
            WHEN
                transaction_date >= DATE_TRUNC(DATE_ADD(fiscal_years.fiscal_year_start, INTERVAL -2 YEAR), YEAR)
                AND transaction_date < DATE_TRUNC(DATE_ADD(fiscal_years.fiscal_year_start, INTERVAL -1 YEAR), YEAR)
            THEN 1
            ELSE 0
        END
    ) AS donated_two_fiscal_years_ago_july_to_june,
    MAX(
        CASE
            WHEN
                transaction_date >= DATE_TRUNC(DATE_ADD(fiscal_years.fiscal_year_start, INTERVAL -3 YEAR), YEAR)
                AND transaction_date < DATE_TRUNC(DATE_ADD(fiscal_years.fiscal_year_start, INTERVAL -2 YEAR), YEAR)
            THEN 1
            ELSE 0
        END
    ) AS donated_three_fiscal_years_ago_july_to_june,
    MAX(
        CASE
            WHEN
                transaction_date >= DATE_TRUNC(DATE_ADD(fiscal_years.fiscal_year_start, INTERVAL -4 YEAR), YEAR)
                AND transaction_date < DATE_TRUNC(DATE_ADD(fiscal_years.fiscal_year_start, INTERVAL -3 YEAR), YEAR)
            THEN 1
            ELSE 0
        END
    ) AS donated_four_fiscal_years_ago_july_to_june,
    MAX(
        CASE
            WHEN transaction_date >= DATE_ADD(DATE_TRUNC(transaction_date, MONTH), INTERVAL -14 MONTH)
            THEN 1
            ELSE 0
        END
    ) AS donated_within_14_months,
    MAX(
        CASE
            WHEN transaction_date >= DATE_ADD(DATE_TRUNC(transaction_date, MONTH), INTERVAL -13 MONTH)
            THEN 1
            ELSE 0
        END
    ) AS donated_within_13_months
FROM fiscal_years
GROUP BY 1, 2;

{% endmacro %}
