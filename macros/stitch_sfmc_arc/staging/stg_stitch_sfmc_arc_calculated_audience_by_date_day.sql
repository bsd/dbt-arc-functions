{% macro create_stg_stitch_sfmc_arc_calculated_audience_by_date_day(
    calculated_audience="stg_stitch_sfmc_parameterized_audience_transaction_jobs_append",
    audience_snapshot="snp_stitch_sfmc_arc_audience",
    calculated_audience_scd="stg_stitch_sfmc_donor_audience_calculated_scd"
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
        audience_by_date_day as (
            select
                date_spine.date as date_day,
                calc_audience_scd.person_id,
                calc_audience_scd.donor_audience
            from date_spine
            inner join
                {{ ref(calculated_audience_scd) }} as calc_audience_scd
                on date_spine.date >= date(calc_audience_scd.start_date)
                and (
                    date_spine.date <= date(calc_audience_scd.end_date)
                    or date(calc_audience_scd.start_date) is null
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
