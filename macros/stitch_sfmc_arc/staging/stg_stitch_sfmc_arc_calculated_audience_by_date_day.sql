{% macro create_stg_stitch_sfmc_arc_calculated_audience_by_date_day(
    date_spine="stg_stitch_sfmc_audience_transaction_calculated_date_spine",
    calcualted_audience_scd="stg_stitch_sfmc_donor_audience_calculated_scd"
) %}

    with
        audience_by_date_day as (
            select
                date_spine.date as date_day,
                calc_audience_scd.person_id,
                calc_audience_scd.donor_audience
            from {{ ref(date_spine) }} as date_spine
            inner join
                {{ ref(calcualted_audience_scd) }} as calc_audience_scd
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
