{% macro create_stg_stitch_sfmc_donor_engagement_by_date_day(
    audience_transactions_summary_unioned="stg_stitch_sfmc_parameterized_audience_transactions_summary_unioned"
) %}


with

person_with_all_transaction_dates as (
    -- Selects distinct person IDs with their transaction dates from
    -- the specified summary table.
    select distinct
        person_id, date(transaction_date) as transaction_date
    from
        {{ref(audience_transactions_summary_unioned)}}
),

start_of_active_and_lapsed as (
    -- Identifies the start of active and lapsed donor engagements based on
    -- transaction dates.
    select
        person_id,
        transaction_date,
        -- Calculates the start of the active engagement.
        case
            -- If there's no previous transaction, consider the current
            -- transaction date as the start of active engagement
            when
                lag(transaction_date) over (
                    partition by person_id order by transaction_date
                )
                is null
            then transaction_date
            -- If the gap between the current and previous transaction exceeds
            -- 426 days, consider it a new active engagement
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
        -- Calculates the start of the lapsed engagement.
        case
            -- If the gap between the current and next transaction exceeds 426
            -- days, consider it the start of lapsed engagement
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
            -- If there's no next transaction and the gap between the current
            -- transaction and today exceeds 426 days, mark it as lapsed
            when
                lead(transaction_date) over (
                    partition by person_id order by transaction_date
                )
                is null
                and date_diff(current_date, transaction_date, day) > 426
            then date_add(transaction_date, interval 426 day)
        end as start_of_lapsed
    from person_with_all_transaction_dates

),
donor_engagement_start_dates as (
    -- Collects the start dates for active and lapsed engagements.
    select
        person_id, 'active' as donor_engagement, start_of_active as start_date,
    from start_of_active_and_lapsed
    where start_of_active is not null
    union all
    select
        person_id, 'lapsed' as donor_engagement, start_of_lapsed as start_date,
    from start_of_active_and_lapsed
    where start_of_lapsed is not null
),

donor_engagement as (
select
    person_id,
    donor_engagement,
    start_date,
    -- Determines the end date as the day before the next start date (or today)
    -- for each person and engagement type
    lead(start_date) over (partition by person_id order by start_date)
    - 1 as end_date
from donor_engagement_start_dates
),
change as (
    select
        person_id,
        start_date as transaction_date_day,
        donor_engagement,
        lag(donor_engagement) over (
            partition by person_id order by start_date
        ) as prev_donor_engagement
    from donor_engagement
),

date_spine as (
    select date
    FROM unnest(
        generate_date_array(
            (select min(start_date) from donor_engagement),
            coalesce(
                (select max(start_date) from donor_engagement),
                current_date())
        )
    ) AS date

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
                            from donor_engagement_scd
                        ),
                        ifnull(
                            (
                                select max(start_date)
                                from donor_engagement_scd
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
