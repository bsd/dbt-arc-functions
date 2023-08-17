{% macro create_stg_stitch_sfmc_donor_audience_calculated_with_date_spine(
    calculated_audience ="stg_stitch_sfmc_audience_transaction_jobs_append",
    calculated_date_spine = "stg_stitch_sfmc_audience_transaction_calculated_date_spine"
) %}


SELECT
    transaction_date_day,
    person_id,
    donor_audience
FROM {{ ref(calculated_audience) }} calc_audience
JOIN {{ ref(calculated_date_spine) }} date_spine ON date_spine.date = calc_audience.transaction_date_day
WHERE calc_audience.transaction_date_day < (SELECT MAX(date) FROM {{ ref(calculated_date_spine) }})

{% endmacro %}