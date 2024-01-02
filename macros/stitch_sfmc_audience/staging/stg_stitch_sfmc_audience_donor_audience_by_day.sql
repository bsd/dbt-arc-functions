{% macro create_stg_stitch_sfmc_audience_donor_audience_by_day(
    audience_snapshot="stg_stitch_sfmc_audience_parameterized_arc_audience",
    calculated_audience="stg_stitch_sfmc_audience_parameterized_calculated_audience"
) %}

    /*

## Purpose
The purpose of this macro is to unify audience data from two different sources - `stg_stitch_sfmc_parameterized_arc_audience` (referred to as 'audience_snapshot') and `stg_stitch_sfmc_parameterized_calculated_audience` (referred to as 'calculated_audience'). The objective is to merge these sources, generate a date spine, deduplicate audience statuses for each person on each date, and then create a unified table combining data from both sources.

## Parameters
- `audience_snapshot`: Specifies the table containing the audience snapshot data. Defaults to `stg_stitch_sfmc_parameterized_arc_audience`.
- `calculated_audience`: Specifies the table containing the calculated audience data. Defaults to `stg_stitch_sfmc_parameterized_calculated_audience`.

## Steps
1. **date_spine**: Generates a date spine based on the range of transaction dates from the calculated audience and audience snapshot tables.
2. **calculated_with_date_spine**: Joins the calculated audience data with the date spine to retain non-null donor audience records within the date range.
3. **calc_change**: Prepares a table with previous donor audience information.
4. **calc_filtered_changes**: Filters and retains relevant changes in donor audience status.
5. **calculated_audience_scd**: Determines the start and end dates for each person and donor audience type.
6. **calc_audience_by_date_day**: Joins the date spine with the calculated audience SCD table to identify donor audience status for each date.
7. **dedup_calc_audience_by_date_day**: Deduplicates donor audience statuses for each person on each date, retaining the first record per person.
8. **calculated_audience_by_date_day**: Presents the deduplicated calculated audience statuses for each person on each date.
9. **audience_unioned**: Combines audience snapshot data and calculated audience data into a unified table.

## Final Output
The final output is a unified table containing the merged audience data from both sources. 
It includes date, person ID, donor audience status, and a column
indicating the source of the audience data ('unioned_donor_audience' or 'calculated_donor_audience').

*/
    with
        date_spine as (
            select date
            from
                unnest(
                    generate_date_array(
                        (
                            select min(transaction_date_day)
                            from {{ ref(calculated_audience) }}
                        ),
                        ifnull(
                            (
                                select min(date_day - 1)

                                from {{ ref(audience_snapshot) }}
                            ),
                            current_date()
                        )
                    )
                ) as date

        ),

        calculated_with_date_spine as (

            select 
            calculated_audience.transaction_date_day,
            calculated_audience.person_id,
            calculated_audience.donor_audience
            from date_spine
            join {{ ref(calculated_audience) }} calculated_audience on
            date_spine.date = calculated_audience.transaction_date_day
            where
                calculated_audience.transaction_date_day < (select max(date) from date_spine)
                and donor_audience is not null

        ),
        calc_change as (
            select
                person_id,
                transaction_date_day,
                donor_audience,
                lag(donor_audience) over (
                    partition by person_id order by transaction_date_day
                ) as prev_donor_audience
            from calculated_with_date_spine
        ),
        calc_filtered_changes as (
            select
                person_id,
                transaction_date_day,
                donor_audience,
                lead(transaction_date_day) over (
                    partition by person_id order by transaction_date_day
                ) as next_date
            from calc_change
            where prev_donor_audience is null or donor_audience != prev_donor_audience

        ),

        calculated_audience_scd as (
            select
                person_id,
                min(transaction_date_day) as start_date,
                ifnull(
                    max(next_date) - 1, (select max(date) from date_spine)
                ) as end_date,
                donor_audience
            from calc_filtered_changes
            group by person_id, donor_audience, next_date
            order by person_id, start_date
        ),

        calc_audience_by_date_day as (
            select
                date_spine.date as date_day,
                calculated_audience_scd.person_id,
                calculated_audience_scd.donor_audience
            from date_spine
            inner join
                calculated_audience_scd
                on date_spine.date >= date(calculated_audience_scd.start_date)
                and (
                    date_spine.date <= date(calculated_audience_scd.end_date)
                    or date(calculated_audience_scd.start_date) is null
                )
        ),
        dedup_calc_audience_by_date_day as (
            select
                date_day,
                person_id,
                donor_audience,
                row_number() over (
                    partition by date_day, person_id order by donor_audience
                ) as row_num
            from calc_audience_by_date_day

        ),

        calculated_audience_by_date_day as (

            select date_day, person_id, donor_audience
            from dedup_calc_audience_by_date_day
            where row_num = 1
        ),

        audience_unioned as (
            select *
            from {{ ref(audience_snapshot) }}
            union all
            select *
            from calculated_audience_by_date_day

        )

    /* rejoin calculated audience into the final audience, filling in blanks */
    select
        coalesce(
            audience_unioned.date_day, calculated_audience.transaction_date_day
        ) as date_day,
        audience_unioned.person_id,
        coalesce(
            audience_unioned.donor_audience, calculated_audience.donor_audience
        ) as donor_audience,
        case
            when audience_unioned.donor_audience is not null
            then 'unioned_donor_audience'
            else 'calculated_donor_audience'
        end as source_column
    from audience_unioned
    left join
        {{ ref(calculated_audience) }} calculated_audience
        on audience_unioned.date_day = calculated_audience.transaction_date_day

{% endmacro %}
