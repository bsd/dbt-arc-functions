{% macro create_stg_stitch_sfmc_audience_transaction_cumulative(
    reference_name="stg_src_stitch_sfmc_audience_transactions_unioned"
) %}

select
    transaction_id,
    sum(amount) over (
        partition by person_id order by transaction_date
    ) as cumulative_amount,
    count(*) over (
        partition by person_id order by transaction_date
    ) as cumulative_gifts,
    sum(case when recurring = 0 then amount else 0 end) over (
        partition by person_id order by transaction_date
    ) as cumulative_one_time_amount,
    count(case when recurring = 0 then 1 else 0 end) over (
        partition by person_id order by transaction_date
    ) as cumulative_one_time_gifts,
    sum(case when recurring = 1 then amount else 0 end) over (
        partition by person_id order by transaction_date
    ) as cumulative_recur_amount,
    count(case when recurring = 1 then 1 else 0 end) over (
        partition by person_id order by transaction_date
    ) as cumulative_recur_gifts

from {{ ref(reference_name) }}

{% endmacro %}
