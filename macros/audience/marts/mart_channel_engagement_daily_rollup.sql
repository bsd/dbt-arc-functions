{% macro create_mart_channel_engagement_daily_rollup(
    start_and_end="stg_audience_channel_engagement_start_and_end_dates"
) %}
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
                                        start_and_end
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
                    from {{ ref(start_and_end) }}
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
        {{ ref(start_and_end) }} engagement
        on channel_date_spine.date_day >= engagement.start_date
        and channel_date_spine.date_day <= coalesce(engagement.end_date, current_date())
        and channel_date_spine.channel = engagement.channel
    group by 1, 2

{% endmacro %}
