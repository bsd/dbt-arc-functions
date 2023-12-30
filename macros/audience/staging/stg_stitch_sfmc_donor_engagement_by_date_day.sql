{% macro create_stg_stitch_sfmc_donor_engagement_by_date_day(
    donor_engagement="stg_stitch_sfmc_audience_transaction_person_engagement_with_start_and_end_dates"
) %}


with
        change as (
            select
                person_id,
                start_date as transaction_date_day,
                donor_engagement,
                lag(donor_engagement) over (
                    partition by person_id order by start_date
                ) as prev_donor_engagement
            from {{ ref(donor_engagement) }}
        ),
        date_spine as (
            select date
            from
                unnest(
                    generate_date_array(
                        (select min(start_date), from {{ ref(donor_engagement) }}),
                        ifnull(
                            (select max(start_date) from {{ ref(donor_engagement) }}),
                            current_date()
                        )
                    )
                ) as date

        ),

    filtered_changes as (
            select
                person_id,
                transaction_date_day,
                donor_engagement,
                lead(transaction_date_day) over (
                    partition by person_id order by transaction_date_day
                ) as next_date
            from change
            where
                prev_donor_engagement is null
                or donor_engagement != prev_donor_engagement

    ),

    donor_engagement_scd as (
    select
        person_id,
        min(transaction_date_day) as start_date,
        ifnull(max(next_date) - 1, (select max(date) from date_spine)) as end_date,
        donor_engagement
    from filtered_changes
    group by person_id, donor_engagement, next_date
    order by person_id, start_date

    ),

        -- Creates a date spine spanning the range of dates present in the donor
        -- engagement SCD table
        scd_date_spine as (
            select date_day
            from
                unnest(
                    generate_date_array(
                        (
                            select min(start_date),
                            from {{ ref(donor_engagement_scd_table) }}
                        ),
                        ifnull(
                            (
                                select max(start_date)
                                from {{ ref(donor_engagement_scd_table) }}
                            ),
                            current_date()
                        )
                    )
                ) as date_day

        ),
        engagement_by_date_day as (
            -- Joins the generated date spine with the donor engagement SCD table to
            -- determine engagement status for each date
            select
                date_spine.date_day,
                donor_engagement_scd.person_id,
                donor_engagement_scd.donor_engagement
            from scd_date_spine
            inner join
                donor_engagement_scd
                on scd_date_spine.date_day >= date(donor_engagement_scd.start_date)
                and (
                    scd_date_spine.date_day <= date(donor_engagement_scd.end_date)
                    or date(donor_engagement_scd.start_date) is null
                )
        ),
        deduplicated_table as (
            -- Deduplicates engagement statuses for each person on each date to fetch
            -- the first record per person
            select
                date_day,
                person_id,
                donor_engagement,
                row_number() over (
                    partition by date_day, person_id order by donor_engagement
                ) as row_num
            from engagement_by_date_day

        )

    -- Selects the deduplicated engagement statuses for each person on each date
    select date_day, person_id, donor_engagement
    from deduplicated_table
    where row_num = 1

{% endmacro %}
