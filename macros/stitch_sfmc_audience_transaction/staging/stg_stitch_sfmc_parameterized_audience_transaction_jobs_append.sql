{% macro create_stg_stitch_sfmc_parameterized_audience_transaction_jobs_append(
    reference_name="stg_stitch_sfmc_parameterized_audience_transactions_summary_unioned",
    arc_person = "stg_stitch_sfmc_arc_person",
    first_gift = "stg_stitch_sfmc_parameterized_audience_transaction_first_gift",
    client_donor_audience="NULL"
) %}

    {{
        config(
            materialized="table",
            partition_by={
                "field": "transaction_date_day",
                "data_type": "date",
                "granularity": "day",
            },
        )
    }}

WITH
    calculations AS (
        SELECT
            transaction_date_day,
            person_id,
            SUM(amount) AS total_amount,
            SUM(CASE WHEN recurring = TRUE THEN amount ELSE 0 END) AS recur_amount
        from {{ ref(reference_name) }}
        GROUP BY 1, 2
    ),
    join_dates AS (
        SELECT
            p.person_id,
            p.date_created,
            fg.first_transaction_date
        FROM {{ ref(arc_person) }} p
        LEFT JOIN {{ ref(first_gift) }} fg ON p.person_id = fg.person_id
    ),
    day_person_rollup AS (
        SELECT
            c.transaction_date_day,
            c.person_id,
            SUM(c.total_amount) OVER (
                PARTITION BY c.person_id
                ORDER BY UNIX_SECONDS(TIMESTAMP(c.transaction_date_day))
                RANGE BETWEEN 63113904 PRECEDING AND CURRENT ROW
            ) AS cumulative_amount_24_months,
            SUM(c.recur_amount) OVER (
                PARTITION BY c.person_id
                ORDER BY UNIX_SECONDS(TIMESTAMP(c.transaction_date_day))
                RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW
            ) AS cumulative_amount_90_days_recur,
            jd.date_created,
            jd.first_transaction_date
        FROM calculations c
        LEFT JOIN join_dates jd ON c.person_id = jd.person_id
        GROUP BY c.transaction_date_day, c.person_id, c.total_amount, c.recur_amount, jd.date_created, jd.first_transaction_date
    ),
    base AS (
        SELECT DISTINCT
            transaction_date_day,
            person_id,
            case
                    when cumulative_amount_24_months >= 25000
                    then 'Major'
                    when cumulative_amount_24_months between 1000 and 24999.99
                    then 'Leadership Giving'
                    when cumulative_amount_90_days_recur > 0
                    then 'Monthly'
                    else 'Mass'
                end as bluestate_donor_audience,  -- modeled after UUSA
            {{ client_donor_audience }} as donor_audience
        FROM day_person_rollup
    ),
    dedupe AS (
        SELECT
            transaction_date_day,
            person_id,
            donor_audience,
            ROW_NUMBER() OVER (
                PARTITION BY transaction_date_day, person_id, donor_audience
                ORDER BY transaction_date_day DESC
            ) AS row_number
        FROM base
    )
SELECT transaction_date_day, person_id, donor_audience
FROM dedupe
WHERE row_number = 1


{% endmacro %}
