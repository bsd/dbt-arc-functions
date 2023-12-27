{% macro create_stg_stitch_sfmc_parameterized_audience_transaction_jobs_append(
    reference_name="stg_stitch_sfmc_parameterized_audience_transactions_summary_unioned",
    client_donor_audience='NULL'
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

    with
        calculations as (
            select
                transaction_date_day,
                person_id,
                sum(amount) as total_amount,
                sum(case when recurring = true then amount else 0 end) as recur_amount
            from {{ ref(reference_name) }}
            group by 1, 2
        ),
        day_person_rollup as (
            select
                transaction_date_day,
                person_id,
                -- Calculate cumulative recur and 1x amount for the past 24 months
                sum(total_amount) over (
                    partition by person_id
                    order by unix_seconds(timestamp(transaction_date_day))  -- Convert date to Unix timestamp
                    range between 63113904 preceding and current row  -- 63,113,904 seconds in 24 months
                ) as cumulative_amount_24_months,
                -- Calculate cumulative recurring amount over past 90 days
                sum(recur_amount) over (
                    partition by person_id
                    order by unix_seconds(timestamp(transaction_date_day))  -- Convert date to Unix timestamp
                    range between 7776000 preceding and current row  -- unix seconds in 90 days
                ) as cumulative_amount_90_days_recur
            from calculations
            group by transaction_date_day, person_id, total_amount, recur_amount
        ),
        base as

        (
            select distinct
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
            from day_person_rollup
        ),
        dedupe as (
            select
                transaction_date_day,
                person_id,
                donor_audience,
                row_number() over (
                    partition by transaction_date_day, person_id, donor_audience
                    order by transaction_date_day desc
                ) as row_number
            from base
        )

    select transaction_date_day, person_id, donor_audience
    from dedupe
    where row_number = 1

{% endmacro %}
