{% macro create_stg_stitch_sfmc_audience_transaction_jobs_append(
    reference_name="stg_stitch_sfmc_audience_transactions_summary_unioned"
) %}


{{ config(
    materialized='table',
    partition_by={
      "field": "transaction_date_day",
      "data_type": "date",
      "granularity": "day"
    }
)}}

{#

UUSA Client Audience Requirements: 
Major: 25,000 cumulative or more in the last 24 months including 1x and recurring gifts.
Midlevel: 1,000 to 24,999 cumulative in the last 24 months including 1x and recurring
Sustainer: Has a recurring gift in the last 90 days and is not in either midlevel or major
Mass: Is not in Sustainer, Midlevel or Major. 

#}
with
    calculations as (
        select
            transaction_date_day,
            person_id,
            -- Calculate cumulative recur and 1x amount for the past 24 months
            sum(amount) over (
                partition by person_id
                order by
                    unix_seconds(timestamp(transaction_date_day))  -- Convert date to Unix timestamp
                    range between 63113904 preceding and current row  -- 63,113,904 seconds in 24 months
            ) as cumulative_amount_24_months,
            -- Calculate cumulative recurring amount over past 90 days
            case
                when recurring = true
                then
                    sum(amount) over (
                        partition by person_id
                        order by
                            unix_seconds(timestamp(transaction_date_day))  -- Convert date to Unix timestamp
                            range between 7776000 preceding and current row  -- unix seconds in 90 days
                    )
                else 0
            end as cumulative_amount_90_days_recur
        from {{ ref(reference_name) }}
        group by transaction_date_day, person_id, amount, recurring
    ),
    day_person_rollup as (
        select
            transaction_date_day,
            person_id,
            sum(cumulative_amount_24_months) as cumulative_amount_24_months,
            sum(cumulative_amount_90_days_recur) as cumulative_amount_90_days_recur
        from calculations
        group by 1, 2
    ),
    base as

    (
        select distinct
            transaction_date_day,
            person_id,
            case
                when cumulative_amount_24_months >= 25000
                then 'Major'
                when cumulative_amount_24_months between 1000 and 24999
                then 'Leadership Giving'
                when cumulative_amount_90_days_recur > 0
                then 'Monthly'
                else 'Mass'
            end as bluestate_donor_audience,  -- modeled after UUSA
            case
                when cumulative_amount_24_months >= 25000
                then 'Major'
                when cumulative_amount_24_months between 1000 and 24999
                then 'Leadership Giving'
                when cumulative_amount_90_days_recur > 0
                then 'Monthly'
                else 'Mass'
            end as donor_audience
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
