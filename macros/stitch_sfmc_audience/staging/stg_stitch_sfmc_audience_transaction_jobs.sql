{% macro create_stg_stitch_sfmc_audience_transaction_jobs(
    transactions = 'stg_stitch_sfmc_audience_transactions_unioned'
    first_last = 'stg_stitch_sfmc_audience_first_last'
    cumulative = 'stg_stitch_sfmc_audience_cumulative'
    yoy = 'stg_stitch_sfmc_audience_yoy'
    person = 'stg_src_stitch_sfmc_audience_person'
) %}

select
    transactions.transaction_date,
    transactions.transaction_id,
    transactions.person_id,
    transactions.source_code,
    transactions.channel,
    transactions.amount,
    transactions.appeal,
    transactions.application_type,
    transactions.recurring,
    first_last.first_transaction_date,
    first_last.last_transaction_date,
    cumulative.cumulative_amount,
    cumulative.cumulative_gifts,
    cumulative.cumulative_recurring_amount,
    cumulative.cumulative_recurring_gifts,
    cumulative.cumulative_one_time_amount,
    cumulative.cumulative_one_time_gifts,
    yoy.donated_this_year,
    yoy.donated_last_year,
    yoy.donated_two_years_ago,
    yoy.donated_three_years_ago

from {{ ref(transactions) }} transactions
join {{ ref(first_last) }} first_last using (transaction_id)
join {{ ref(cumulative) }} cumulative using (transaction_id)
join {{ ref(yoy) }} yoy using (person_id)
left join {{ ref(person) }} person using (person_id)

{% endmacro %}
