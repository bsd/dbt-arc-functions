{% macro create_stg_stitch_sfmc_arc_audience_unioned(
    arc_audience="stg_stitch_sfmc_arc_audience_by_date_day",
    calculated_audience="stg_stitch_sfmc_arc_calculated_audience_by_date_day"
) %}

    with
        arc_audience as (

            select date_day, person_id, donor_audience  -- UUSA custom audience naming code
            from {{ ref(arc_audience) }}

        ),

        calculated_audience as (select * from {{ ref(calculated_audience) }}),

        unioned_audience as (
            select *
            from arc_audience
            union all
            select *
            from calculated_audience

        )

    select *
    from unioned_audience

{% endmacro %}
