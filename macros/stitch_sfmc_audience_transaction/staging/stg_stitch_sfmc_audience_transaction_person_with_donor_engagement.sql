{% macro create_stg_stitch_sfmc_audience_transaction_person_with_donor_engagement(
    stg_stitch_sfmc_audience_transactions_summary_unioned="stg_stitch_sfmc_audience_transactions_summary_unioned"
) %}
    with
        date_spine as (
            select *
            from
                unnest(
                    generate_date_array(
                        (
                            select min(date(transaction_date))
                            from
                                {{ ref(stg_stitch_sfmc_audience_transactions_summary_unioned) }}
                        ),
                        (
                            select max(date(transaction_date))
                            from
                                {{ ref(stg_stitch_sfmc_audience_transactions_summary_unioned) }}
                        )
                    )
                ) as date_day
        ),
        person_with_all_transactions as (
            select
                person_with_min_transaction_date.person_id,
                person_with_min_transaction_date.min_transaction_date,
                date_spine.date_day,
                case
                    when person_with_all_transaction_dates.transaction_date is not null
                    then 1
                    else 0
                end as transaction_on_this_date
            from date_spine
            left join
                (
                    select person_id, date(min(transaction_date)) min_transaction_date
                    from
                        {{ ref(stg_stitch_sfmc_audience_transactions_summary_unioned) }}
                    group by 1
                ) person_with_min_transaction_date
                on date_spine.date_day
                >= person_with_min_transaction_date.min_transaction_date
            left join
                (
                    select distinct
                        person_id, date(transaction_date) as transaction_date
                    from
                        {{ ref(stg_stitch_sfmc_audience_transactions_summary_unioned) }}
                ) as person_with_all_transaction_dates
                on person_with_min_transaction_date.person_id
                = person_with_all_transaction_dates.person_id
                and date_spine.date_day
                = person_with_all_transaction_dates.transaction_date
            order by 1, 2, 3
        ),
        person_with_transactions_in_last_14_months as (
            select
                *,
                sum(transaction_on_this_date) over (
                    partition by person_id
                    order by date_day
                    rows between 426 preceding and current row
                ) as transactions_within_last_14_months
            from person_with_all_transactions
        )
    select
        person_id,
        date_day,
        case
            when transactions_within_last_14_months > 0 then 'Active' else 'Lapsed'
        end as donor_engagement
    from person_with_transactions_in_last_14_months
    order by date_day
{% endmacro %}