{% macro create_stg_stitch_sfmc_audience_transaction_jobs(
    reference_name='stg_stitch_sfmc_transactions_unioned'
) %}

select
    transaction_date,
    person_id,
    MAX(channel) as channel,
    SUM(amount) as amount,
    COUNT(unique transaction_id) as gifts

from {{ref(reference_name)}}
GROUP BY 1, 2

{% endmacro %}
