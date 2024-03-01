{% macro create_stg_stitch_sfmc_arc_audience_unioned(
    arc_audience="stg_stitch_sfmc_arc_audience_by_date_day",
    calculated_audience="stg_stitch_sfmc_arc_calculated_audience_by_date_day"
) %}

    {{
        config(
            materialized="table",
            partition_by={
                "field": "date_day",
                "data_type": "month",
                "granularity": "month",
            },
        )
    }}

    with
        arc_audience as (

            select date_day, person_id, donor_audience from {{ ref(arc_audience) }}

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

{% if target.name !== 'prod' %}

where date_day >= date_sub(current_date(), interval 2 year)

{% endif %}


{% endmacro %}
