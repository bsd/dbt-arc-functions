{% macro create_stg_stitch_sfmc_donor_engagement_calculated(
    transaction_jobs ="stg_stitch_sfmc_customizable_audience_transaction_jobs_append"
) %}



Select
person_id,
transaction_date_day,
donor_engagement,
 from {{ ref(transaction_jobs) }}

{% endmacro %}