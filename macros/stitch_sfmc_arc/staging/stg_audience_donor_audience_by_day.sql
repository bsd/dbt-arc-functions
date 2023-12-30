{% macro create_stg_audience_donor_audience_by_day(
    audience_unioned="stg_stitch_sfmc_arc_audience_unioned",
    calculated_audience="stg_stitch_sfmc_parameterized_audience_transaction_jobs_append"
) %}

{{config(
    materialized='incremental',
    unique_key='id'
)}}

with base as (

    select
        coalesce(
            audience_unioned.date_day, calculated_audience.transaction_date_day
        ) as date_day,
        audience_unioned.person_id,
        audience_unioned.donor_audience as audience_unioned,
        calculated_audience.donor_audience as audience_calculated,
        coalesce(
            audience_unioned.donor_audience, calculated_audience.donor_audience
        ) as coalesced_audience,
        case
            when audience_unioned.donor_audience is not null
            then 'unioned_donor_audience'
            else 'calculated_donor_audience'
        end as source_column
    from {{ ref(audience_unioned) }} audience_unioned
    left join
        {{ ref(calculated_audience) }} calculated_audience
        on audience_unioned.date_day = calculated_audience.transaction_date_day
),


    select 
    {{dbt_utils.generate_surrogate_key(['date_day', 'person_id'])}} as id,
    * 
    from base 
    
    {% if is_incremental() %}
    where date_day >= (select date_sub(max(date_day), 7 day) from {{ this }})

    {% endif %}


{% endmacro %}
