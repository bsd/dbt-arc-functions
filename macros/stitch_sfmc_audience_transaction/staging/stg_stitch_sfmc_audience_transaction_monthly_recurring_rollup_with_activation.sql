{% macro create_stg_stitch_sfmc_audience_transaction_monthly_recurring_rollup_with_activation(
    reference_name='stg_stitch_sfmc_audience_transaction_monthly_recurring_rollup') %}
SELECT
    join_month_year,
    CONCAT(
        CAST(EXTRACT(YEAR FROM join_month_year) AS string),
        '-',
        LPAD(
            CAST(EXTRACT(MONTH FROM join_month_year) AS string),
            2, '0'
        )
    ) AS join_month_year_str,
    CONCAT(
        'Act',
        LPAD(
            CAST(
                DATE_DIFF(
                    transaction_month_year_date, join_month_year, MONTH
                ) AS string
            ),
            2, '0'
        )
    ) AS activation,
    total_revenue,
    total_donors,
FROM {{ ref(reference_name) }}

{% endmacro %}