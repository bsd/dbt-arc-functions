{% macro create_mart_channel_engagement_daily_rollup(
    person_channel_engagement_with_start_and_end_dates="stg_stitch_sfmc_audience_transaction_person_channel_engagement_with_start_and_end_dates"
) %}
    {{
        config(
            materialized="incremental",
            unique_key=["channel", "date_day"],
        )
    }}
    with
        date_spine as (
            select date_day
            from
                unnest(
                    generate_date_array(
                        (
                            select min(start_date),
                            from
                                {{
                                    ref(
                                        person_channel_engagement_with_start_and_end_dates
                                    )
                                }}
                        ),

                        current_date()
                    )
                ) as date_day

        ),
        channel_date_spine as (
            select channel, date_day
            from
                (
                    select distinct channel
                    from {{ ref(person_channel_engagement_with_start_and_end_dates) }}
                )
            cross join date_spine
        )

    select
        channel_date_spine.channel,
        channel_date_spine.date_day,
        count(
            case when donor_engagement = 'active' then person_id end
        ) as active_donors,
        count(
            case when donor_engagement = 'lapsed' then person_id end
        ) as lapsed_donors,
    from channel_date_spine
    left join
        {{ ref(person_channel_engagement_with_start_and_end_dates) }} engagement
        on channel_date_spine.date_day >= engagement.start_date
        and channel_date_spine.date_day <= coalesce(engagement.end_date, current_date())
        and channel_date_spine.channel = engagement.channel
    {% if is_incremental()  %}
        where channel_date_spine.date_day >= (select max(date_day) from {{ this }})
    {% endif %}
    group by 1, 2

{% endmacro %}
