{% macro create_stg_stitch_sfmc_audience_transaction_cumulative(
    reference_name="stg_stitch_sfmc_audience_transactions_summary_unioned"
) %}

    select
        transaction_date_day,
        person_id,
        -- Calculate cumulative amount for the past 12 months
        sum(amount) over (
            partition by person_id
            order by unix_seconds(transaction_date_day)  -- Convert date to Unix timestamp
            range between 31556952 preceding and current row  -- 31,556,952 seconds in 12 months
        ) as cumulative_amount_12_months,
        -- Calculate cumulative amount for the past 24 months
        sum(amount) over (
            partition by person_id
            order by unix_seconds(transaction_date_day)  -- Convert date to Unix timestamp
            range between 63113904 preceding and current row  -- 63,113,904 seconds in 24 months
        ) as cumulative_amount_24_months,
        -- Calculate cumulative amount for the past 30 days for recurring transactions
        case
            when recurring = true
            then
                sum(amount) over (
                    partition by person_id
                    order by unix_seconds(transaction_date_day)  -- Convert date to Unix timestamp
                    range between 2592000 preceding and current row  -- 2,592,000 seconds in 30 days
                )
            else 0
        end as cumulative_amount_30_days_recur,
         max(
            case
                when
                    transaction_date >= date_add(
                        date_trunc(transaction_date_day, month), interval - 14 month
                    )
                then 1
                else 0
            end
        ) as donated_within_14_months

    from {{ ref(reference_name) }}

{% endmacro %}
