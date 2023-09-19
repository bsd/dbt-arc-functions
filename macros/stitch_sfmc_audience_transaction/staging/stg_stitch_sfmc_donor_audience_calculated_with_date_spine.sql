{% macro create_stg_stitch_sfmc_donor_audience_calculated_with_date_spine(
    calculated_audience="stg_stitch_sfmc_audience_transaction_jobs_append",
    calculated_date_spine="stg_stitch_sfmc_audience_transaction_calculated_date_spine"
) %}

    select transaction_date_day, person_id, donor_audience
    from {{ ref(calculated_audience) }} calc_audience
    join
        {{ ref(calculated_date_spine) }} date_spine
        on date_spine.date = calc_audience.transaction_date_day
    where
        calc_audience.transaction_date_day
        < (select max(date) from {{ ref(calculated_date_spine) }})
        and donor_audience is not null
{% endmacro %}
