{% macro create_stg_stitch_sfmc_arc_audience_unioned(
    arc_audience="stg_stitch_sfmc_arc_audience_by_date_day",
    calculated_audience="stg_stitch_sfmc_parameterized_audience_transaction_jobs_append"
) %}

    {{
        config(
            materialized="incremental",
            unique_key=["date_day", "person_id"],
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

        calculated_audience as (
            select transaction_date_day as date_day, person_id, donor_audience
            from {{ ref(calculated_audience) }}
        ),

        unioned_audience as (
            select *
            from arc_audience
            union all
            select *
            from calculated_audience
            {% if is_incremental() %}
                where date_day >= (select max(date_day) from {{ this }})
            {% endif %}

        )

    select *
    from unioned_audience

{% endmacro %}
