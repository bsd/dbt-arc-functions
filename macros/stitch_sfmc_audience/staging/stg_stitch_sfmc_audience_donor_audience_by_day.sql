{% macro create_stg_stitch_sfmc_audience_donor_audience_by_day(
    audience_snapshot="stg_stitch_sfmc_audience_parameterized_arc_audience",
    calculated_audience_by_day="stg_stitch_sfmc_audience_calculated_audience_by_day"
) %}

    /*

## Final Output
The final output is a unified table containing the merged audience data from both sources. 
It includes date, person ID, donor audience status, and a column
indicating the source of the audience data ('unioned_donor_audience' or 'calculated_donor_audience').

*/
   with 

        audience_unioned as (
            select *
            from {{ ref(audience_snapshot) }}
            union all
            select *
            from {{ref(calculated_audience_by_day)}}

        )

    /* rejoin calculated audience into the final audience, filling in blanks */
    select
        audience_unioned.date_day,
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
        {{ ref(calculated_audience_by_day) }} calculated_audience
        on audience_unioned.date_day = calculated_audience.date_day

{% endmacro %}
