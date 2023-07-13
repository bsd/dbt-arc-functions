{% macro create_stg_stitch_sfmc_customized_audience_transaction_jobs_append() %}
    select
        transaction_date_day,
        person_id,
        donor_audience,
        donor_engagement,
        donor_loyalty,
    from {{ ref("stg_stitch_sfmc_audience_transaction_jobs_append") }}
{% endmacro %}
