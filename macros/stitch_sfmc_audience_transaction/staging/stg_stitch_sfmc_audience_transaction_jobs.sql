{% macro create_stg_stitch_sfmc_audience_transaction_jobs(
    reference_name="stg_stitch_sfmc_transactions_unioned"
) %}


with base as (
select
    transaction_date,
    person_id

from {{ ref(reference_name) }}
)

select distinct * from base

{% endmacro %}
