{% macro create_stg_stitch_sfmc_audience_transaction_first_last(
    reference_name="stg_stitch_sfmc_transactions_unioned"
) %}

with
    subquery as (
        select
            transaction_date,
            person_id,
            max(transaction_date) over (partition by person_id) as max_transaction_date
        from {{ ref(reference_name) }}
    )

select
    transaction_date,
    person_id,
    min(transaction_date) over (partition by person_id) as first_transaction_date,
    lag(max_transaction_date) over (
        partition by person_id order by transaction_date
    ) as previous_latest_transaction_date

from subquery

{% endmacro %}
