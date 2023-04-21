{% macro create_stg_stitch_sfmc_audience_transactions_join(
    jobs = 'stg_src_stitch_sfmc_transactions_jobs',
    totals = 'stg_src_stitch_sfmc_audience_totals',
    first_last = 'stg_stitch_sfmc_audience_transaction_first_last',
    cumulative = 'stg_stitch_sfmc_audience_transaction_cumulative',
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
    cumulative.cumulative_amount_30_days,
    cumulative.cumulative_gifts_30_days,
    cumulative.cumulative_amount_6_months,
    cumulative.cumulative_gifts_6_months,
    cumulative.cumulative_amount_12_months,
    cumulative.cumulative_gifts_12_months,
    cumulative.cumulative_amount_24_months,
    cumulative.cumulative_gifts_24_months,
    cumulative.cumulative_one_time_amount,
    cumulative.cumulative_one_time_gifts,
    yoy.donated_this_year,
    yoy.donated_within_14_months,
    yoy.donated_within_13_months,
    yoy.donated_last_year,
    yoy.donated_two_years_ago,
    yoy.donated_three_years_ago,
    yoy.donated_current_fiscal_year_july_to_june,
    yoy.donated_last_fiscal_year_july_to_june,
    yoy.donated_two_fiscal_years_ago_july_to_june,
    yoy.donated_three_fiscal_years_ago_july_to_june

from {{ ref(jobs) }} jobs
join
    {{ ref(totals) }} totals
    on jobs.transaction_date = totals.transaction_date
    and jobs.person_id = totals.person_id
join
    {{ ref(first_last) }} first_last
    on jobs.transaction_date = first_last.transaction_date
    and jobs.person_id = first_last.person_id
join
    {{ ref(cumulative) }} cumulative
    on jobs.transaction_date = cumulative.transaction_date
    and jobs.person_id = cumulative.person_id
join {{ ref(yoy) }} yoy on jobs.person_id = yoy.person_id

{% endmacro %}
