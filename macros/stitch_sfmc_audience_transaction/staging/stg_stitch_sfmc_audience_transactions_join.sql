{% macro create_stg_stitch_sfmc_audience_transaction_jobs(
    jobs = 'stg_src_stitch_sfmc_transactions_jobs'
    totals = 'stg_src_stitch_sfmc_audience_totals'
    first_last = 'stg_stitch_sfmc_audience_transaction_first_last'
    cumulative = 'stg_stitch_sfmc_audience_transaction_cumulative'
    yoy = 'stg_stitch_sfmc_audience_transaction_yoy'
) %}

select
    jobs.transaction_date,
    jobs.person_id,
    jobs.source_code,
    jobs.channel,
    jobs.appeal,
    jobs.application_type,
    jobs.amount,
    jobs.gifts,
    first_last.first_transaction_date,
    first_last.last_transaction_date,
    first_last.previous_latest_transaction_date,
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

from {{ ref(jobs) }} jobs
join {{ ref(totals) }} totals on jobs.transaction_date = totals.transaction_date
    and jobs.person_id = totals.person_id
join {{ ref(first_last) }} first_last on jobs.transaction_date = first_last.transaction_date
    and jobs.person_id = first_last.person_id
join {{ ref(cumulative) }} cumulative on jobs.transaction_date = cumulative.transaction_date
    and jobs.person_id = cumulative.person_id
join {{ ref(yoy) }} yoy on jobs.person_id = yoy.person_id

{% endmacro %}
