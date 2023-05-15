{% macro create_stg_stitch_sfmc_audience_transaction_cumulative(
    reference_name="stg_stitch_sfmc_audience_transactions_summary_unioned"
) %}

    select
        transaction_date_day,
        person_id,
        sum(amount) over (
            partition by person_id
            order by unix_seconds(timestamp(transaction_date_day))
            range between 31556952 preceding and current row
        ) as cumulative_amount_12_months,
        sum(amount) over (
            partition by person_id
            order by unix_seconds(timestamp(transaction_date_day))
            range between 63113904 preceding and current row
        ) as cumulative_amount_24_months,
        case
            when recurring = true
            then
                sum(amount) over (
                    partition by person_id
                    order by unix_seconds(timestamp(transaction_date_day))
                    range between 2592000 preceding and current row
                )
            else 0
        end as cumulative_amount_30_days_recur

    from {{ ref(reference_name) }}

{% endmacro %}
