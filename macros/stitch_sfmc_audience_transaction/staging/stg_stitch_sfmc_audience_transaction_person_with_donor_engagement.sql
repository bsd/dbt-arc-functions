{% macro create_stg_stitch_sfmc_audience_transaction_person_with_donor_engagement(
    stg_stitch_sfmc_audience_transaction_person_with_all_txns="stg_stitch_sfmc_audience_transaction_person_with_all_txns"
) %}

    with
        person_with_transactions_in_last_14_months as (
            select
                *,
                sum(transaction_on_this_date) over (
                    partition by person_id
                    order by date_day
                    rows between 426 preceding and current row
                ) as transactions_within_last_14_months
            from {{ ref(stg_stitch_sfmc_audience_transaction_person_with_all_txns) }}
        )
    select
        person_id,
        date_day,
        case
            when transactions_within_last_14_months > 0 then 'active' else 'lapsed'
        end as donor_engagement
    from person_with_transactions_in_last_14_months
    order by date_day
{% endmacro %}
