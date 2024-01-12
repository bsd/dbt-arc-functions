{% macro create_stg_stitch_sfmc_audience_donor_audience_by_day(
    audience_unioned="stg_stitch_sfmc_audience_calculated_and_arc_unioned",
    calculated_audience_by_day="stg_stitch_sfmc_audience_calculated_audience_by_date_day"
) %}

 /*  
## Final Output
The final output is a unified table containing the merged audience data from both sources. 
It includes date, person ID, donor audience status, and a column
indicating the source of the audience data ('unioned_donor_audience' or 'calculated_donor_audience').
*/

    select
        coalesce(audience_unioned.date_day, calculated_audience.date_day) as date_day,
        coalesce(audience_unioned.person_id, calculated_audience.person_id) as person_id,
        coalesce(
            audience_unioned.donor_audience, calculated_audience.donor_audience
        ) as donor_audience,
        case
            when audience_unioned.donor_audience is not null
            then 'unioned_donor_audience'
            else 'calculated_donor_audience'
        end as source_column
    from {{ref(audience_unioned)}} audience_unioned
    left join
        {{ ref(calculated_audience_by_day) }} calculated_audience
        on audience_unioned.date_day = calculated_audience.date_day

{% endmacro %}
