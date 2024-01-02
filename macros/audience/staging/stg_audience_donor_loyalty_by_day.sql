{% macro create_stg_audience_donor_loyalty_by_day(
    start_and_end='stg_audience_donor_loyalty_start_and_end'
) %}

    with

        date_spine as (
            select date
            from
                unnest(
                    generate_date_array(
                        (select min(start_date) from {{ ref(start_and_end) }}),
                        coalesce(
                            (select max(start_date) from {{ ref(start_and_end) }}),
                            current_date()
                        )
                    )
                ) as date

        ),
        loyalty_by_date_day as (

            select
                date_spine.date as date_day,
                start_and_end.person_id,
                start_and_end.donor_loyalty
            from date_spine
            inner join
                {{ref(start_and_end)}} start_and_end
                on date_spine.date >= date(start_and_end.start_date)
                and (
                    date_spine.date <= date(start_and_end.end_date)
                    or date(start_and_end.start_date) is null
                )
        ),
        deduplicated_table as (
            select
                date_day,
                person_id,
                donor_loyalty,
                row_number() over (
                    partition by date_day, person_id order by donor_loyalty
                ) as row_num
            from loyalty_by_date_day

        )

    -- Selects the deduplicated engagement statuses for each person on each date
    select date_day, person_id, donor_loyalty
    from deduplicated_table
    where row_num = 1

{% endmacro %}
