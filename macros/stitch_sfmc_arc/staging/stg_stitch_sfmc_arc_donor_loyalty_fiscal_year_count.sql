{% macro create_stg_stitch_sfmc_arc_donor_loyalty_fiscal_year_count(
    audience_union_transaction_joined="stg_stitch_sfmc_arc_audience_union_transaction_joined"
) %}


SELECT
    person_id,
    fiscal_year,
    MIN(transaction_date_day) AS start_date,
    DATE_SUB(
        DATE(CONCAT(
            fiscal_year,
            '-',
            '{{ var('fiscal_year_start') }}'
        )),
        INTERVAL 1 DAY
    ) AS end_date,

    ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY fiscal_year desc) AS row_num
FROM
    {{ ref(audience_union_transaction_joined) }}
GROUP BY
    person_id,
    fiscal_year
ORDER BY
    person_id,
    fiscal_year

{% endmacro %}