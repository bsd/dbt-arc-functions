{% macro create_stg_stitch_sfmc_audience_transaction_first_last(
    reference_name="stg_src_stitch_sfmc_transactions_unioned"
) %}

select
    transaction_date,
    person_id,
    max(transaction_date) over (partition by person_id) as latest_transaction_date,
    min(transaction_date) over (partition by person_id) as first_transaction_date,
    lag(max(transaction_date) over (partition by person_id)) over (
        partition by person_id order by transaction_date
    ) as previous_latest_transaction_date

from {{ ref(reference_name) }}

{% endmacro %}
