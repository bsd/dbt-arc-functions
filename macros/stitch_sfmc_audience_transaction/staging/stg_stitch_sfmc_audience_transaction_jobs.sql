{% macro create_stg_stitch_sfmc_audience_transaction_jobs(
    reference_name="stg_stitch_sfmc_transactions_unioned"
) %}

select
    transaction_date,
    person_id,
    max(inbound_channel) as channel,
    sum(amount) as amount,
    count(distinct transaction_id) as gifts

from {{ ref(reference_name) }}
group by 1, 2

{% endmacro %}
