{% macro create_stg_stitch_sfmc_audience_transaction_person_channel_engagement_with_start_and_end_dates(
    stg_stitch_sfmc_arc_audience_union_transaction_joined_enriched="stg_stitch_sfmc_arc_audience_union_transaction_joined_enriched") %}

    with
        start_of_active_and_lapsed as (
            select
                person_id,
                channel,
                transaction_date_day,
                case
                    when
                        lag(transaction_date_day) over (
                            partition by person_id, channel order by transaction_date_day
                        )
                        is null
                    then transaction_date_day
                    when
                        date_diff(
                            transaction_date_day,
                            lag(transaction_date_day) over (
                                partition by person_id, channel order by transaction_date_day
                            ),
                            day
                        )
                        > 426
                    then transaction_date_day
                end as start_of_active,
                case
                    when
                        date_diff(
                            lead(transaction_date_day) over (
                                partition by person_id, channel order by transaction_date_day
                            ),
                            transaction_date_day,
                            day
                        )
                        > 426
                    then date_add(transaction_date_day, interval 426 day)
                    when
                        lead(transaction_date_day) over (
                            partition by person_id, channel order by transaction_date_day
                        )
                        is null
                        and date_diff(current_date, transaction_date_day, day) > 426
                    then date_add(transaction_date_day, interval 426 day)
                end as start_of_lapsed
            from
                (
                    select distinct
                        person_id, channel, date(transaction_date_day) as transaction_date_day
                    from
                        {{
                            ref(
                                stg_stitch_sfmc_arc_audience_union_transaction_joined_enriched
                            )
                        }}
                ) as person_with_all_transaction_date_days

            order by 1, 2
        ),
        donor_engagement_start_dates as (
            select
                person_id, channel, 'active' as donor_engagement, start_of_active as start_date,
            from start_of_active_and_lapsed
            where start_of_active is not null
            union all
            select
                person_id, channel, 'lapsed' as donor_engagement, start_of_lapsed as start_date,
            from start_of_active_and_lapsed
            where start_of_lapsed is not null
            order by 1, 3
        )
    select
        person_id,
        channel,
        donor_engagement,
        start_date,
        lead(start_date) over (partition by person_id, channel order by start_date)
        - 1 as end_date
    from donor_engagement_start_dates
    order by 1, 2, 4

{% endmacro %}
