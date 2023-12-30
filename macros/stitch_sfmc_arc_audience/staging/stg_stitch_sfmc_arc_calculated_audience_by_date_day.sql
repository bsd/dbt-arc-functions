{% macro create_stg_stitch_sfmc_arc_calculated_audience_by_date_day(
    calculated_audience="stg_stitch_sfmc_parameterized_audience_transaction_jobs_append",
    audience_snapshot="snp_stitch_sfmc_arc_audience"
) %}

with
        date_spine as (
            select date
            from
                unnest(
                    generate_date_array(
                        (
                            select min(transaction_date_day),
                            from {{ ref(calculated_audience) }}
                        ),
                        ifnull(
                            (
                                select
                                    min(
                                        date(
                                            cast(
                                                concat(
                                                    substr(dbt_valid_from, 0, 22),
                                                    " America/New_York"
                                                ) as timestamp
                                            ),
                                            "America/New_York"
                                        )
                                        - 1
                                    )

                                from {{ ref(audience_snapshot) }}
                            ),
                            current_date()
                        )
                    )
                ) as date

        ),

        calculated_with_date_spine as (

            select transaction_date_day, person_id, donor_audience
            from {{ ref(calculated_audience) }} calc_audience
            join date_spine on date_spine.date = calc_audience.transaction_date_day
            where
                calc_audience.transaction_date_day < (select max(date) from date_spine)
                and donor_audience is not null

        ),
        change as (
            select
                person_id,
                transaction_date_day,
                donor_audience,
                lag(donor_audience) over (
                    partition by person_id order by transaction_date_day
                ) as prev_donor_audience
            from calculated_with_date_spine
        ),
        filtered_changes as (
            select
                person_id,
                transaction_date_day,
                donor_audience,
                lead(transaction_date_day) over (
                    partition by person_id order by transaction_date_day
                ) as next_date
            from change
            where prev_donor_audience is null or donor_audience != prev_donor_audience

        ),

calculated_audience_scd as (
    select
        person_id,
        min(transaction_date_day) as start_date,
        ifnull(max(next_date) - 1, (select max(date) from date_spine)) as end_date,
        donor_audience
    from filtered_changes
    group by person_id, donor_audience, next_date
    order by person_id, start_date
),

        audience_by_date_day as (
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
        deduplicated_table as (
            select
                date_day,
                person_id,
                donor_audience,
                row_number() over (
                    partition by date_day, person_id order by donor_audience
                ) as row_num
            from audience_by_date_day

        )

    select date_day, person_id, donor_audience
    from deduplicated_table
    where row_num = 1

{% endmacro %}
