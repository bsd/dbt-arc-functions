{% macro create_stg_audience_donor_audience_by_day(
    audience_unioned="stg_stitch_sfmc_arc_audience_unioned",
    calculated_audience="stg_stitch_sfmc_parameterized_audience_transaction_jobs_append"
) %}

    select
        coalesce(
            audience_unioned.date_day, calculated_audience.transaction_date_day
        ) as date_day,
        audience_unioned.person_id,
        audience_unioned.donor_audience as audience_unioned,
        calculated_audience.donor_audience as audience_calculated,
        coalesce(
            audience_unioned.donor_audience, calculated_audience.donor_audience
        ) as coalesced_audience
    from {{ ref(audience_unioned) }} audience_unioned
    left join
        {{ ref(calculated_audience) }} calculated_audience
        on audience_unioned.date_day = calculated_audience.transaction_date_day

{% endmacro %}
