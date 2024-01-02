{% macro create_stg_stitch_sfmc_audience_calculated_audience_by_day(
    audience_snapshot="stg_stitch_sfmc_audience_parameterized_arc_audience",
    calculated_audience="stg_stitch_sfmc_audience_parameterized_calculated_audience"
) %}

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


            select date_day, person_id, donor_audience
            from dedup_calc_audience_by_date_day
            where row_num = 1

{% endmacro %}