{% macro create_stg_stitch_sfmc_audience_transaction_calculated_alldates(
    jobs_append="stg_stitch_sfmc_audience_transaction_jobs_append"
) %}

    select transaction_date_day, person_id, donor_audience from {{ ref(jobs_append) }}

{% endmacro %}
