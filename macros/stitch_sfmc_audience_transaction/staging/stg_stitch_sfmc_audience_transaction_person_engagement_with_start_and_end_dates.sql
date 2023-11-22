{% macro create_stg_stitch_sfmc_audience_transaction_person_engagement_with_start_and_end_dates(
    stg_stitch_sfmc_audience_transactions_summary_unioned="stg_stitch_sfmc_audience_transactions_summary_unioned",
    max_date=""
) %}

    with
        start_of_active_and_lapsed as (
            select
                person_id,
                transaction_date,
                case
                    when
                        lag(transaction_date) over (
                            partition by person_id order by transaction_date
                        )
                        is null
                    then transaction_date
                    when
                        date_diff(
                            transaction_date,
                            lag(transaction_date) over (
                                partition by person_id order by transaction_date
                            ),
                            day
                        )
                        > 426
                    then transaction_date
                end as start_of_active,
                case
                    when
                        date_diff(
                            lead(transaction_date) over (
                                partition by person_id order by transaction_date
                            ),
                            transaction_date,
                            day
                        )
                        > 426
                    then date_add(transaction_date, interval 426 day)
                    when
                        lead(transaction_date) over (
                            partition by person_id order by transaction_date
                        )
                        is null
                        and date_diff(current_date, transaction_date, day) > 426
                    then date_add(transaction_date, interval 426 day)
                end as start_of_lapsed
            from
                (
                    select distinct
                        person_id, date(transaction_date) as transaction_date
                    from
                        {{ ref('stg_stitch_sfmc_audience_transactions_summary_unioned') }}
                ) as person_with_all_transaction_dates

            order by 1, 2
        ),
        donor_engagement_start_dates as (
            select
                person_id, 'active' as donor_engagement, start_of_active as start_date,
            from start_of_active_and_lapsed
            where start_of_active is not null
            union all
            select
                person_id, 'lapsed' as donor_engagement, start_of_lapsed as start_date,
            from start_of_active_and_lapsed
            where start_of_lapsed is not null
            order by 1, 3
        )
    select
        person_id,
        donor_engagement,
        start_date,
        lead(start_date) over (partition by person_id order by start_date)
        - 1 as end_date
    from donor_engagement_start_dates
    order by 1, 3

{% endmacro %}
