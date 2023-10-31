{% macro create_stg_stitch_sfmc_audience_transaction_jobs(
    reference_name="stg_stitch_sfmc_audience_transactions_summary_unioned"
) %}
with base as (select transaction_date_day, person_id from {{ ref(reference_name) }})
select distinct *
from base
{% endmacro %}
