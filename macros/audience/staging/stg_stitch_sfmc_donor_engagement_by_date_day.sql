{% macro create_stg_stitch_sfmc_donor_engagement_by_date_day(
    audience_transactions_summary_unioned="stg_stitch_sfmc_parameterized_audience_transactions_summary_unioned"
) %}

/*

## Purpose
The purpose of this macro is to identify and categorize donor engagements as "active" or "lapsed" based on transaction dates, and then generate a deduplicated table containing the first record per person for each date indicating their engagement status.

## Parameters
- `audience_transactions_summary_unioned`: Specifies the source table containing transactional data. Defaults to `stg_stitch_sfmc_parameterized_audience_transactions_summary_unioned`.

## Steps
1. **person_with_all_transaction_dates**: Selects unique person IDs with their respective transaction dates from the specified summary table.
2. **start_of_active_and_lapsed**: Determines the start of "active" and "lapsed" donor engagements based on transaction dates. It calculates the start date for each engagement type.
3. **donor_engagement_start_dates**: Collects the start dates for "active" and "lapsed" engagements.
4. **donor_engagement_table**: Calculates the end date for each engagement type based on the next start date or the current date.
5. **change**: Prepares a table with previous engagement status information.
6. **filtered_changes**: Filters and retains only the relevant changes in donor engagement status.
7. **date_spine**: Generates a date spine spanning the range of dates present in the donor engagement table.
8. **donor_engagement_scd**: Aggregates donor engagement changes and determines start and end dates for each person and engagement type.
9. **scd_date_spine**: Generates a date spine based on the engagement table's date range.
10. **engagement_by_date_day**: Joins the date spine with the donor engagement table to identify engagement status for each date.
11. **deduplicated_table**: Deduplicates engagement statuses for each person on each date, retaining the first record per person.

## Output
The final output provides the deduplicated engagement statuses for each person on each date, filtering to select the first record per person for each date.

*/


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
        person_id,
        'active' as donor_engagement, 
        start_of_active as start_date
    from start_of_active_and_lapsed
    where start_of_active is not null

    union all

    select
        person_id,
        'lapsed' as donor_engagement,
        start_of_lapsed as start_date
    from start_of_active_and_lapsed
    where start_of_lapsed is not null
),

donor_engagement_table as (
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
    from donor_engagement_table
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

date_spine as (
    select date
    FROM unnest(
        generate_date_array(
            (select min(start_date) from donor_engagement_table),
            coalesce(
                (select max(start_date) from donor_engagement_table),
                current_date())
        )
    ) AS date

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
                scd_date_spine.date_day,
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
